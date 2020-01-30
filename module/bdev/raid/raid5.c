/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "bdev_raid.h"

#include "spdk/config.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/likely.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk_internal/log.h"

#include <rte_hash.h>
#include <rte_memory.h>

#define RAID5_MAX_STRIPES 1024 /* TODO: make configurable */

struct stripe_request {
	/* The associated raid_bdev_io */
	struct raid_bdev_io *raid_io;

	/* The target stripe */
	struct stripe *stripe;

	/* Counter for remaining chunk requests */
	int remaining;

	/* Status of the request */
	enum spdk_bdev_io_status status;

	/* Offset into the parent bdev_io iovecs */
	uint64_t iov_offset;

	/* First data chunk applicable to this request */
	struct chunk *first_data_chunk;

	/* Last data chunk applicable to this request */
	struct chunk *last_data_chunk;

	/* The stripe's parity chunk */
	struct chunk *parity_chunk;

	/* Link for the stripe's requests list */
	TAILQ_ENTRY(stripe_request) link;

	/* Array of chunks corresponding to base_bdevs */
	struct chunk {
		/* Corresponds to base_bdev index */
		uint8_t index;

		/* Request offset from chunk start */
		uint64_t req_offset;

		/* Request blocks count */
		uint64_t req_blocks;

		/* The iovecs associated with the chunk request */
		struct iovec *iovs;

		/* The number of iovecs */
		int iovcnt;

		/* A single iovec for non-SG buffer request cases */
		struct iovec iov;
	} chunks[0];
};

struct stripe {
	/* The stripe's index in the raid array. Also a key for the hash table. */
	uint64_t index;

	/* Hashed key value */
	hash_sig_t hash;

	/* List of requests queued for this stripe */
	TAILQ_HEAD(requests_head, stripe_request) requests;

	/* Protects the requests list */
	pthread_spinlock_t requests_lock;

	/* Stripe can be reclaimed if this reaches 0 */
	unsigned int refs;

	/* Link for the active/free stripes lists */
	TAILQ_ENTRY(stripe) link;

	/* Array of buffers for chunk parity/preread data */
	void **chunk_buffers;
};

struct raid5_info {
	/* The parent raid bdev */
	struct raid_bdev *raid_bdev;

	/* Number of data blocks in a stripe (without parity) */
	uint64_t stripe_blocks;

	/* Number of stripes on this array */
	uint64_t total_stripes;

	/* Mempool for stripe_requests */
	struct spdk_mempool *stripe_request_mempool;

	/* Pointer to an array of all available stripes */
	struct stripe *stripes;

	/* Hash table containing currently active stripes */
	struct rte_hash *active_stripes_hash;

	/* List of active stripes (in hash table) */
	TAILQ_HEAD(active_stripes_head, stripe) active_stripes;

	/* List of free stripes (not in hash table) */
	TAILQ_HEAD(, stripe) free_stripes;

	/* Lock protecting the stripes hash and lists */
	pthread_spinlock_t active_stripes_lock;
};

#define FOR_EACH_CHUNK(req, c) \
	for (c = req->chunks; \
	     c < req->chunks + req->raid_io->raid_bdev->num_base_bdevs; c++)

#define __NEXT_DATA_CHUNK(req, c) \
	c+1 == req->parity_chunk ? c+2 : c+1

#define FOR_EACH_DATA_CHUNK(req, c) \
	for (c = __NEXT_DATA_CHUNK(req, req->chunks-1); \
	     c < req->chunks + req->raid_io->raid_bdev->num_base_bdevs; \
	     c = __NEXT_DATA_CHUNK(req, c))

static inline struct stripe_request *
raid5_chunk_stripe_req(struct chunk *chunk)
{
	return SPDK_CONTAINEROF((chunk - chunk->index), struct stripe_request, chunks);
}

static inline uint8_t
raid5_chunk_data_index(struct chunk *chunk)
{
	return chunk < raid5_chunk_stripe_req(chunk)->parity_chunk ? chunk->index : chunk->index - 1;
}

static inline struct chunk *
raid5_get_data_chunk(struct stripe_request *stripe_req, uint8_t chunk_data_idx)
{
	uint8_t p_chunk_idx = stripe_req->parity_chunk - stripe_req->chunks;

	return &stripe_req->chunks[chunk_data_idx < p_chunk_idx ? chunk_data_idx : chunk_data_idx + 1];
}

static inline uint8_t
raid5_stripe_data_chunks_num(const struct raid_bdev *raid_bdev)
{
	return raid_bdev->num_base_bdevs - raid_bdev->module->base_bdevs_max_degraded;
}

static int
raid5_chunk_map_iov(struct chunk *chunk, const struct iovec *iov, int iovcnt,
		    uint64_t offset, uint64_t len)
{
	int i;
	size_t off = 0;
	int start_v = -1;
	size_t start_v_off;
	int new_iovcnt = 0;

	for (i = 0; i < iovcnt; i++) {
		if (off + iov[i].iov_len > offset) {
			start_v = i;
			break;
		}
		off += iov[i].iov_len;
	}

	if (start_v == -1) {
		return -EINVAL;
	}

	start_v_off = off;

	for (i = start_v; i < iovcnt; i++) {
		new_iovcnt++;

		if (off + iov[i].iov_len >= offset + len) {
			break;
		}
		off += iov[i].iov_len;
	}

	assert(start_v + new_iovcnt <= iovcnt);

	if (new_iovcnt > chunk->iovcnt) {
		void *tmp;

		if (chunk->iovs == &chunk->iov) {
			chunk->iovs = NULL;
		}
		tmp = realloc(chunk->iovs, new_iovcnt * sizeof(struct iovec));
		if (!tmp) {
			return -ENOMEM;
		}
		chunk->iovs = tmp;
	}
	chunk->iovcnt = new_iovcnt;

	off = start_v_off;
	iov += start_v;

	for (i = 0; i < new_iovcnt; i++) {
		chunk->iovs[i].iov_base = iov->iov_base + (offset - off);
		chunk->iovs[i].iov_len = spdk_min(len, iov->iov_len - (offset - off));

		off += iov->iov_len;
		iov++;
		offset += chunk->iovs[i].iov_len;
		len -= chunk->iovs[i].iov_len;
	}

	if (len > 0) {
		return -EINVAL;
	}

	return 0;
}

static int
raid5_chunk_map_req_data(struct chunk *chunk)
{
	struct stripe_request *stripe_req = raid5_chunk_stripe_req(chunk);
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(stripe_req->raid_io);
	uint64_t len = chunk->req_blocks * bdev_io->bdev->blocklen;
	int ret;

	ret = raid5_chunk_map_iov(chunk, bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				  stripe_req->iov_offset, len);
	if (ret == 0) {
		stripe_req->iov_offset += len;
	}

	return ret;
}

static void
raid5_submit_stripe_request(struct stripe_request *stripe_req);

static void
_raid5_submit_stripe_request(void *_stripe_req)
{
	struct stripe_request *stripe_req = _stripe_req;

	raid5_submit_stripe_request(stripe_req);
}

static void
raid5_stripe_request_put(struct stripe_request *stripe_req)
{
	struct raid5_info *r5info = stripe_req->raid_io->raid_bdev->module_private;
	struct chunk *chunk;

	FOR_EACH_CHUNK(stripe_req, chunk) {
		if (chunk->iovs != &chunk->iov) {
			free(chunk->iovs);
		}
	}

	spdk_mempool_put(r5info->stripe_request_mempool, stripe_req);
}

static void
raid5_complete_stripe_request(struct stripe_request *stripe_req)
{
	struct stripe *stripe = stripe_req->stripe;
	struct raid_bdev_io *raid_io = stripe_req->raid_io;
	enum spdk_bdev_io_status status = stripe_req->status;
	struct stripe_request *next_req;
	struct chunk *chunk;
	uint64_t req_blocks;

	pthread_spin_lock(&stripe->requests_lock);
	next_req = TAILQ_NEXT(stripe_req, link);
	TAILQ_REMOVE(&stripe->requests, stripe_req, link);
	pthread_spin_unlock(&stripe->requests_lock);
	if (next_req) {
		spdk_thread_send_msg(spdk_io_channel_get_thread(spdk_io_channel_from_ctx(
					     next_req->raid_io->raid_ch)),
				     _raid5_submit_stripe_request, next_req);
	}

	req_blocks = 0;
	FOR_EACH_DATA_CHUNK(stripe_req, chunk) {
		req_blocks += chunk->req_blocks;
	}

	raid5_stripe_request_put(stripe_req);

	if (raid_bdev_io_complete_part(raid_io, req_blocks, status)) {
		__atomic_fetch_sub(&stripe->refs, 1, __ATOMIC_SEQ_CST);
	}
}

static inline enum spdk_bdev_io_status errno_to_status(int err)
{
	err = abs(err);
	switch (err) {
	case 0:
		return SPDK_BDEV_IO_STATUS_SUCCESS;
	case ENOMEM:
		return SPDK_BDEV_IO_STATUS_NOMEM;
	default:
		return SPDK_BDEV_IO_STATUS_FAILED;
	}
}

static void
raid5_abort_stripe_request(struct stripe_request *stripe_req, enum spdk_bdev_io_status status)
{
	stripe_req->remaining = 0;
	stripe_req->status = status;
	raid5_complete_stripe_request(stripe_req);
}

static void
raid5_complete_chunk_request(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct chunk *chunk = cb_arg;
	struct stripe_request *stripe_req = raid5_chunk_stripe_req(chunk);

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		stripe_req->status = SPDK_BDEV_IO_STATUS_FAILED;
	}

	if (--stripe_req->remaining == 0) {
		raid5_complete_stripe_request(stripe_req);
	}
}

static void
raid5_submit_chunk_request(struct chunk *chunk)
{
	struct stripe_request *stripe_req = raid5_chunk_stripe_req(chunk);
	struct raid_bdev_io *raid_io = stripe_req->raid_io;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid_base_bdev_info *base_info = &raid_bdev->base_bdev_info[chunk->index];
	struct spdk_io_channel *base_ch = raid_io->raid_ch->base_channel[chunk->index];
	uint64_t base_offset_blocks;
	int ret;

	stripe_req->remaining++;

	base_offset_blocks = (stripe_req->stripe->index << raid_bdev->strip_size_shift) + chunk->req_offset;

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
					     chunk->iovs, chunk->iovcnt,
					     base_offset_blocks, chunk->req_blocks,
					     raid5_complete_chunk_request,
					     chunk);
	} else if (bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) {
		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
					      chunk->iovs, chunk->iovcnt,
					      base_offset_blocks, chunk->req_blocks,
					      raid5_complete_chunk_request,
					      chunk);
	} else {
		assert(false);
	}

	if (spdk_unlikely(ret != 0)) {
		/* TODO: handle this */
		assert(false);
	}
}

static void
raid5_submit_stripe_request(struct stripe_request *stripe_req)
{
	struct chunk *chunk;
	int ret;

	/* TODO: update parity for writes */

	FOR_EACH_DATA_CHUNK(stripe_req, chunk) {
		if (chunk->req_blocks > 0) {
			ret = raid5_chunk_map_req_data(chunk);
			if (ret) {
				raid5_abort_stripe_request(stripe_req, errno_to_status(ret));
				return;
			}

			raid5_submit_chunk_request(chunk);
		}
	}
}

static void
raid5_handle_stripe(struct raid_bdev_io *raid_io, struct stripe *stripe,
		    uint64_t stripe_offset, uint64_t blocks, uint64_t iov_offset)
{
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid5_info *r5info = raid_bdev->module_private;
	struct stripe_request *stripe_req;
	struct chunk *chunk;
	uint64_t stripe_offset_from, stripe_offset_to;
	uint8_t first_chunk_data_idx, last_chunk_data_idx;
	bool do_submit;

	stripe_req = spdk_mempool_get(r5info->stripe_request_mempool);
	if (spdk_unlikely(!stripe_req)) {
		raid_bdev_io_complete_part(raid_io, blocks, SPDK_BDEV_IO_STATUS_NOMEM);
		return;
	}

	stripe_req->raid_io = raid_io;
	stripe_req->iov_offset = iov_offset;
	stripe_req->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	stripe_req->remaining = 0;

	stripe_req->stripe = stripe;
	stripe_req->parity_chunk = &stripe_req->chunks[raid5_stripe_data_chunks_num(
					   raid_bdev) - stripe->index % raid_bdev->num_base_bdevs];

	stripe_offset_from = stripe_offset;
	stripe_offset_to = stripe_offset_from + blocks;
	first_chunk_data_idx = stripe_offset_from >> raid_bdev->strip_size_shift;
	last_chunk_data_idx = (stripe_offset_to - 1) >> raid_bdev->strip_size_shift;

	stripe_req->first_data_chunk = raid5_get_data_chunk(stripe_req, first_chunk_data_idx);
	stripe_req->last_data_chunk = raid5_get_data_chunk(stripe_req, last_chunk_data_idx);

	FOR_EACH_CHUNK(stripe_req, chunk) {
		chunk->index = chunk - stripe_req->chunks;
		chunk->iovs = &chunk->iov;
		chunk->iovcnt = 1;

		if (chunk == stripe_req->parity_chunk ||
		    chunk < stripe_req->first_data_chunk ||
		    chunk > stripe_req->last_data_chunk) {
			chunk->req_offset = 0;
			chunk->req_blocks = 0;
		} else {
			uint64_t chunk_offset_from = raid5_chunk_data_index(chunk) << raid_bdev->strip_size_shift;
			uint64_t chunk_offset_to = chunk_offset_from + raid_bdev->strip_size;

			if (stripe_offset_from > chunk_offset_from) {
				chunk->req_offset = stripe_offset_from - chunk_offset_from;
			} else {
				chunk->req_offset = 0;
			}

			if (stripe_offset_to < chunk_offset_to) {
				chunk->req_blocks = stripe_offset_to - chunk_offset_from;
			} else {
				chunk->req_blocks = raid_bdev->strip_size;
			}

			chunk->req_blocks -= chunk->req_offset;
		}
	}

	pthread_spin_lock(&stripe->requests_lock);
	do_submit = TAILQ_EMPTY(&stripe->requests);
	TAILQ_INSERT_TAIL(&stripe->requests, stripe_req, link);
	pthread_spin_unlock(&stripe->requests_lock);

	if (do_submit) {
		raid5_submit_stripe_request(stripe_req);
	}
}

static int
raid5_reclaim_stripes(struct raid5_info *r5info)
{
	struct stripe *stripe, *tmp;
	int reclaimed = 0;
	int ret;
	int to_reclaim = (RAID5_MAX_STRIPES / 8) - RAID5_MAX_STRIPES +
			 rte_hash_count(r5info->active_stripes_hash);

	TAILQ_FOREACH_REVERSE_SAFE(stripe, &r5info->active_stripes, active_stripes_head, link, tmp) {
		if (__atomic_load_n(&stripe->refs, __ATOMIC_SEQ_CST) > 0) {
			continue;
		}

		TAILQ_REMOVE(&r5info->active_stripes, stripe, link);
		TAILQ_INSERT_TAIL(&r5info->free_stripes, stripe, link);

		ret = rte_hash_del_key_with_hash(r5info->active_stripes_hash,
						 &stripe->index, stripe->hash);
		if (spdk_unlikely(ret < 0)) {
			assert(false);
		}

		if (++reclaimed > to_reclaim) {
			break;
		}
	}

	return reclaimed;
}

static struct stripe *
raid5_get_stripe(struct raid5_info *r5info, uint64_t stripe_index)
{
	struct stripe *stripe;
	hash_sig_t hash;
	int ret;

	hash = rte_hash_hash(r5info->active_stripes_hash, &stripe_index);

	pthread_spin_lock(&r5info->active_stripes_lock);
	ret = rte_hash_lookup_with_hash_data(r5info->active_stripes_hash,
					     &stripe_index, hash, (void **)&stripe);
	if (ret == -ENOENT) {
		stripe = TAILQ_FIRST(&r5info->free_stripes);
		if (!stripe) {
			if (raid5_reclaim_stripes(r5info) > 0) {
				stripe = TAILQ_FIRST(&r5info->free_stripes);
				assert(stripe != NULL);
			} else {
				pthread_spin_unlock(&r5info->active_stripes_lock);
				return NULL;
			}
		}
		TAILQ_REMOVE(&r5info->free_stripes, stripe, link);

		stripe->index = stripe_index;
		stripe->hash = hash;

		ret = rte_hash_add_key_with_hash_data(r5info->active_stripes_hash,
						      &stripe_index, hash, stripe);
		if (spdk_unlikely(ret < 0)) {
			assert(false);
		}
	} else {
		TAILQ_REMOVE(&r5info->active_stripes, stripe, link);
	}
	TAILQ_INSERT_HEAD(&r5info->active_stripes, stripe, link);

	__atomic_fetch_add(&stripe->refs, 1, __ATOMIC_SEQ_CST);

	pthread_spin_unlock(&r5info->active_stripes_lock);

	return stripe;
}

static void
raid5_submit_rw_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid5_info *r5info = raid_io->raid_bdev->module_private;
	uint64_t offset_blocks = bdev_io->u.bdev.offset_blocks;
	uint64_t num_blocks = bdev_io->u.bdev.num_blocks;
	uint64_t stripe_index = offset_blocks / r5info->stripe_blocks;
	uint64_t stripe_offset = offset_blocks % r5info->stripe_blocks;
	struct stripe *stripe;

	stripe = raid5_get_stripe(r5info, stripe_index);
	if (spdk_unlikely(stripe == NULL)) {
		assert(false); /* TODO: handle this */
	}

	raid_io->base_bdev_io_remaining = num_blocks;

	raid5_handle_stripe(raid_io, stripe, stripe_offset, num_blocks, 0);
}

static int
raid5_stripe_init(struct stripe *stripe, struct raid_bdev *raid_bdev)
{
	uint8_t i;

	stripe->chunk_buffers = calloc(raid_bdev->num_base_bdevs, sizeof(void *));
	if (!stripe->chunk_buffers) {
		SPDK_ERRLOG("Failed to allocate chunk buffers array\n");
		return -ENOMEM;
	}

	for (i = 0; i < raid_bdev->num_base_bdevs; i++) {
		void *buf;

		buf = spdk_dma_malloc(raid_bdev->strip_size * raid_bdev->bdev.blocklen,
				      spdk_max(spdk_bdev_get_buf_align(raid_bdev->base_bdev_info[i].bdev), 32),
				      NULL);
		if (!buf) {
			SPDK_ERRLOG("Failed to allocate chunk buffer\n");
			for (; i > 0; --i) {
				spdk_dma_free(stripe->chunk_buffers[i]);
			}
			free(stripe->chunk_buffers);
			return -ENOMEM;
		}

		stripe->chunk_buffers[i] = buf;
	}

	TAILQ_INIT(&stripe->requests);
	pthread_spin_init(&stripe->requests_lock, PTHREAD_PROCESS_PRIVATE);

	return 0;
}

static void
raid5_stripe_deinit(struct stripe *stripe, struct raid_bdev *raid_bdev)
{
	uint8_t i;

	for (i = 0; i < raid_bdev->num_base_bdevs; i++) {
		spdk_dma_free(stripe->chunk_buffers[i]);
	}
	free(stripe->chunk_buffers);

	pthread_spin_destroy(&stripe->requests_lock);
}

static void
raid5_free(struct raid5_info *r5info)
{
	unsigned int i;

	pthread_spin_destroy(&r5info->active_stripes_lock);

	if (r5info->active_stripes_hash) {
		rte_hash_free(r5info->active_stripes_hash);
	}

	if (r5info->stripe_request_mempool) {
		spdk_mempool_free(r5info->stripe_request_mempool);
	}

	if (r5info->stripes) {
		for (i = 0; i < RAID5_MAX_STRIPES; i++) {
			raid5_stripe_deinit(&r5info->stripes[i], r5info->raid_bdev);
		}
		free(r5info->stripes);
	}

	free(r5info);
}

static int
raid5_start(struct raid_bdev *raid_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct raid_base_bdev_info *base_info;
	struct raid5_info *r5info;
	char name_buf[32];
	struct rte_hash_parameters hash_params = { 0 };
	unsigned int i;
	int ret = 0;

	r5info = calloc(1, sizeof(*r5info));
	if (!r5info) {
		SPDK_ERRLOG("Failed to allocate r5info\n");
		return -ENOMEM;
	}
	r5info->raid_bdev = raid_bdev;

	pthread_spin_init(&r5info->active_stripes_lock, PTHREAD_PROCESS_PRIVATE);

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
	}

	r5info->total_stripes = min_blockcnt / raid_bdev->strip_size;
	r5info->stripe_blocks = raid_bdev->strip_size * raid5_stripe_data_chunks_num(raid_bdev);

	raid_bdev->bdev.blockcnt = r5info->stripe_blocks * r5info->total_stripes;
	raid_bdev->bdev.optimal_io_boundary = r5info->stripe_blocks;
	raid_bdev->bdev.split_on_optimal_io_boundary = true;

	r5info->stripes = calloc(RAID5_MAX_STRIPES, sizeof(*r5info->stripes));
	if (!r5info->stripes) {
		SPDK_ERRLOG("Failed to allocate stripes array\n");
		ret = -ENOMEM;
		goto out;
	}

	TAILQ_INIT(&r5info->free_stripes);

	for (i = 0; i < RAID5_MAX_STRIPES; i++) {
		struct stripe *stripe = &r5info->stripes[i];

		ret = raid5_stripe_init(stripe, raid_bdev);
		if (ret) {
			for (; i > 0; --i) {
				raid5_stripe_deinit(&r5info->stripes[i], raid_bdev);
			}
			free(r5info->stripes);
			r5info->stripes = NULL;
			goto out;
		}

		TAILQ_INSERT_TAIL(&r5info->free_stripes, stripe, link);
	}

	snprintf(name_buf, sizeof(name_buf), "raid5_sreq_%p", raid_bdev);

	r5info->stripe_request_mempool = spdk_mempool_create(name_buf,
					 RAID5_MAX_STRIPES * 4,
					 sizeof(struct stripe_request) + sizeof(struct chunk) * raid_bdev->num_base_bdevs,
					 SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
					 SPDK_ENV_SOCKET_ID_ANY);
	if (!r5info->stripe_request_mempool) {
		SPDK_ERRLOG("Failed to allocate stripe_request_mempool\n");
		ret = -ENOMEM;
		goto out;
	}

	snprintf(name_buf, sizeof(name_buf), "raid5_hash_%p", raid_bdev);

	hash_params.name = name_buf;
	hash_params.entries = RAID5_MAX_STRIPES * 2;
	hash_params.key_len = sizeof(uint64_t);

	r5info->active_stripes_hash = rte_hash_create(&hash_params);
	if (!r5info->active_stripes_hash) {
		SPDK_ERRLOG("Failed to allocate active_stripes_hash\n");
		ret = -ENOMEM;
		goto out;
	}

	TAILQ_INIT(&r5info->active_stripes);

	raid_bdev->module_private = r5info;
out:
	if (ret) {
		raid5_free(r5info);
	}
	return ret;
}

static void
raid5_stop(struct raid_bdev *raid_bdev)
{
	struct raid5_info *r5info = raid_bdev->module_private;

	raid5_free(r5info);
}

static struct raid_bdev_module g_raid5_module = {
	.level = RAID5,
	.base_bdevs_min = 3,
	.base_bdevs_max_degraded = 1,
	.start = raid5_start,
	.stop = raid5_stop,
	.submit_rw_request = raid5_submit_rw_request,
};
RAID_MODULE_REGISTER(&g_raid5_module)

SPDK_LOG_REGISTER_COMPONENT("bdev_raid5", SPDK_LOG_BDEV_RAID5)
