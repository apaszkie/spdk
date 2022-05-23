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

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/likely.h"
#include "spdk/log.h"
#include "spdk/xor.h"

/* Maximum allowed base bdevs */
#define RAID5F_MAX_BASE_BDEVS 16

/* Maximum concurrent full stripe writes */
#define RAID5F_MAX_STRIPES 128

struct chunk {
	struct stripe_request *stripe_req;

	/* Corresponds to base_bdev index */
	uint8_t index;

	/* Array of iovecs */
	struct iovec *iovs;

	/* Number of used iovecs */
	int iovcnt;

	/* Total number of available iovecs in the array */
	int iovcnt_max;
};

struct stripe_request {
	struct raid5f_info *r5f_info;

	/* The associated raid_bdev_io */
	struct raid_bdev_io *raid_io;

	/* The stripe's index in the raid array. */
	uint64_t stripe_index;

	/* The stripe's parity chunk */
	struct chunk *parity_chunk;

	/* Buffer for stripe parity */
	void *parity_buf;

	/* Array of chunks corresponding to base_bdevs */
	struct chunk *chunks[RAID5F_MAX_BASE_BDEVS];

	struct spdk_thread *origin_thread;
	struct raid5f_worker *worker;
};

struct raid5f_worker {
	struct raid5f_info *r5f_info;
	struct spdk_thread *thread;
	struct spdk_io_channel *ch;
	TAILQ_ENTRY(raid5f_worker) link;
};

struct raid5f_worker_io_channel {
	/* Array of iovec iterators for each data chunk */
	struct iov_iter {
		struct iovec *iovs;
		int iovcnt;
		int index;
		size_t offset;
	} *chunk_iov_iters;

	/* Array of source buffer pointers for parity calculation */
	void **chunk_xor_buffers;

	/* Bounce buffers for parity calculation in case of unaligned source buffers */
	struct iovec *chunk_xor_bounce_buffers;
};

struct raid5f_info {
	/* The parent raid bdev */
	struct raid_bdev *raid_bdev;

	/* Number of data blocks in a stripe (without parity) */
	uint64_t stripe_blocks;

	/* Number of stripes on this array */
	uint64_t total_stripes;

	/* For initialization */
	int status;

	/* Mempool of chunk structs */
	struct spdk_mempool *chunk_mp;

	/* Parity buffers */
	struct spdk_ring *stripe_parity_buffers;

	TAILQ_HEAD(, raid5f_worker) workers;
};

struct raid5f_io_channel {
	/* Number of in flight raid IOs on this channel */
	uint64_t num_ios;

	struct raid5f_worker *next_worker;
};

struct raid5f_config {
	struct spdk_cpuset worker_cpuset;
};

#define __CHUNK_COND(req, c, cp) \
	(cp < req->chunks + req->r5f_info->raid_bdev->num_base_bdevs) && (c = *cp)

#define FOR_EACH_CHUNK_FROM(req, c, from) \
	for (struct chunk **__chp = from; __CHUNK_COND(req, c, __chp); __chp++)

#define FOR_EACH_CHUNK(req, c) \
	FOR_EACH_CHUNK_FROM(req, c, req->chunks)

#define __NEXT_DATA_CHUNK(req, c) \
	*(c) == req->parity_chunk ? c+1 : c

#define FOR_EACH_DATA_CHUNK(req, c) \
	for (struct chunk **__chp = __NEXT_DATA_CHUNK(req, req->chunks); \
	     __CHUNK_COND(req, c, __chp); \
	     __chp = __NEXT_DATA_CHUNK(req, __chp+1))

static inline uint8_t
raid5f_stripe_data_chunks_num(const struct raid_bdev *raid_bdev)
{
	return raid_bdev->num_base_bdevs - raid_bdev->module->base_bdevs_max_degraded;
}

static inline uint8_t
raid5f_stripe_parity_chunk_index(const struct raid_bdev *raid_bdev, uint64_t stripe_index)
{
	return raid5f_stripe_data_chunks_num(raid_bdev) - stripe_index % raid_bdev->num_base_bdevs;
}

static inline void *
raid5f_parity_buf_get(struct raid5f_info *r5f_info)
{
	void *buf = NULL;

	spdk_ring_dequeue(r5f_info->stripe_parity_buffers, (void **)&buf, 1);

	return buf;
}

static inline void
raid5f_parity_buf_put(struct raid5f_info *r5f_info, void *buf)
{
	spdk_ring_enqueue(r5f_info->stripe_parity_buffers, (void **)&buf, 1, NULL);
}

static int
raid5f_xor_stripe(struct stripe_request *stripe_req)
{
	struct raid_bdev_io *raid_io = stripe_req->raid_io;
	struct raid5f_worker_io_channel *worker_ch = spdk_io_channel_get_ctx(stripe_req->worker->ch);
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	size_t remaining = raid_bdev->strip_size << raid_bdev->blocklen_shift;
	uint8_t n_src = raid5f_stripe_data_chunks_num(raid_bdev);
	void *dest = stripe_req->parity_buf;
	size_t alignment_mask = spdk_xor_get_buf_alignment() - 1;
	struct chunk *chunk;
	int ret;
	uint8_t c;

	c = 0;
	FOR_EACH_DATA_CHUNK(stripe_req, chunk) {
		struct iov_iter *iov_iter = &worker_ch->chunk_iov_iters[c];
		bool aligned = true;
		int i;

		for (i = 0; i < chunk->iovcnt; i++) {
			if (((uintptr_t)chunk->iovs[i].iov_base & alignment_mask) ||
			    (chunk->iovs[i].iov_len & alignment_mask)) {
				aligned = false;
				break;
			}
		}

		if (aligned) {
			iov_iter->iovs = chunk->iovs;
			iov_iter->iovcnt = chunk->iovcnt;
		} else {
			iov_iter->iovs = &worker_ch->chunk_xor_bounce_buffers[c];
			iov_iter->iovcnt = 1;
			spdk_iovcpy(chunk->iovs, chunk->iovcnt, iov_iter->iovs, iov_iter->iovcnt);
		}

		iov_iter->index = 0;
		iov_iter->offset = 0;

		c++;
	}

	while (remaining > 0) {
		size_t len = remaining;
		uint8_t i;

		for (i = 0; i < n_src; i++) {
			struct iov_iter *iov_iter = &worker_ch->chunk_iov_iters[i];
			struct iovec *iov = &iov_iter->iovs[iov_iter->index];

			len = spdk_min(len, iov->iov_len - iov_iter->offset);
			worker_ch->chunk_xor_buffers[i] = iov->iov_base + iov_iter->offset;
		}

		assert(len > 0);

		ret = spdk_xor_gen(dest, worker_ch->chunk_xor_buffers, n_src, len);
		if (spdk_unlikely(ret)) {
			SPDK_ERRLOG("stripe xor failed\n");
			return ret;
		}

		for (i = 0; i < n_src; i++) {
			struct iov_iter *iov_iter = &worker_ch->chunk_iov_iters[i];
			struct iovec *iov = &iov_iter->iovs[iov_iter->index];

			iov_iter->offset += len;
			if (iov_iter->offset == iov->iov_len) {
				iov_iter->offset = 0;
				iov_iter->index++;
			}
		}
		dest += len;

		remaining -= len;
	}

	return 0;
}

static void
raid5f_stripe_request_release(struct stripe_request *stripe_req)
{
	struct raid_bdev_io *raid_io = stripe_req->raid_io;
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid5f_info *r5f_info = raid_bdev->module_private;
	struct raid5f_io_channel *r5ch = (struct raid5f_io_channel *)raid_io->raid_ch->resource;

	raid5f_parity_buf_put(r5f_info, stripe_req->parity_buf);
	spdk_mempool_put_bulk(r5f_info->chunk_mp, (void **)stripe_req->chunks,
			      raid_bdev->num_base_bdevs);
	r5ch->num_ios--;
}

static void
raid5f_chunk_write_complete(struct chunk *chunk, enum spdk_bdev_io_status status)
{
	struct stripe_request *stripe_req = chunk->stripe_req;
	struct raid_bdev_io *raid_io = stripe_req->raid_io;

	if (raid_io->base_bdev_io_remaining == 1) {
		raid5f_stripe_request_release(stripe_req);
	}

	raid_bdev_io_complete_part(raid_io, 1, status);
}

static void
raid5f_chunk_write_complete_bdev_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct chunk *chunk = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5f_chunk_write_complete(chunk, success ? SPDK_BDEV_IO_STATUS_SUCCESS :
				    SPDK_BDEV_IO_STATUS_FAILED);
}

static void
raid5f_stripe_request_submit_chunks(struct stripe_request *stripe_req);

static void
raid5f_chunk_write_retry(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;
	struct stripe_request *stripe_req = (struct stripe_request *)raid_io->module_ctx;

	raid5f_stripe_request_submit_chunks(stripe_req);
}

static int
raid5f_chunk_write(struct chunk *chunk)
{
	struct stripe_request *stripe_req = chunk->stripe_req;
	struct raid_bdev_io *raid_io = stripe_req->raid_io;
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid_base_bdev_info *base_info = &raid_bdev->base_bdev_info[chunk->index];
	struct spdk_io_channel *base_ch = raid_io->raid_ch->base_channel[chunk->index];
	uint64_t base_offset_blocks = (stripe_req->stripe_index << raid_bdev->strip_size_shift);
	int ret;

	ret = spdk_bdev_writev_blocks(base_info->desc, base_ch, chunk->iovs, chunk->iovcnt,
				      base_offset_blocks, raid_bdev->strip_size,
				      raid5f_chunk_write_complete_bdev_io, chunk);
	if (spdk_unlikely(ret)) {
		if (ret == -ENOMEM) {
			raid_bdev_queue_io_wait(raid_io, base_info->bdev, base_ch,
						raid5f_chunk_write_retry);
		} else {
			raid_io->base_bdev_io_remaining = raid_io->base_bdev_io_submitted + 1;
			raid5f_chunk_write_complete(chunk, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}

	return ret;
}

static int
raid5f_stripe_request_map_iovecs(struct stripe_request *stripe_req,
				 const struct iovec *raid_io_iovs,
				 int raid_io_iovcnt)
{
	struct raid_bdev *raid_bdev = stripe_req->raid_io->raid_bdev;
	struct chunk *chunk;
	int raid_io_iov_idx = 0;
	size_t raid_io_offset = 0;
	size_t raid_io_iov_offset = 0;
	int i;

	FOR_EACH_DATA_CHUNK(stripe_req, chunk) {
		int chunk_iovcnt = 0;
		uint64_t len = raid_bdev->strip_size << raid_bdev->blocklen_shift;
		size_t off = raid_io_iov_offset;

		for (i = raid_io_iov_idx; i < raid_io_iovcnt; i++) {
			chunk_iovcnt++;
			off += raid_io_iovs[i].iov_len;
			if (off >= raid_io_offset + len) {
				break;
			}
		}

		assert(raid_io_iov_idx + chunk_iovcnt <= raid_io_iovcnt);

		if (chunk_iovcnt > chunk->iovcnt_max) {
			struct iovec *iovs = chunk->iovs;

			iovs = realloc(iovs, chunk_iovcnt * sizeof(*iovs));
			if (!iovs) {
				return -ENOMEM;
			}
			chunk->iovs = iovs;
			chunk->iovcnt_max = chunk_iovcnt;
		}
		chunk->iovcnt = chunk_iovcnt;

		for (i = 0; i < chunk_iovcnt; i++) {
			struct iovec *chunk_iov = &chunk->iovs[i];
			const struct iovec *raid_io_iov = &raid_io_iovs[raid_io_iov_idx];
			size_t chunk_iov_offset = raid_io_offset - raid_io_iov_offset;

			chunk_iov->iov_base = raid_io_iov->iov_base + chunk_iov_offset;
			chunk_iov->iov_len = spdk_min(len, raid_io_iov->iov_len - chunk_iov_offset);
			raid_io_offset += chunk_iov->iov_len;
			len -= chunk_iov->iov_len;

			if (raid_io_offset >= raid_io_iov_offset + raid_io_iov->iov_len) {
				raid_io_iov_idx++;
				raid_io_iov_offset += raid_io_iov->iov_len;
			}
		}

		if (spdk_unlikely(len > 0)) {
			return -EINVAL;
		}
	}

	return 0;
}

static void
raid5f_stripe_request_submit_chunks(struct stripe_request *stripe_req)
{
	struct raid_bdev_io *raid_io = stripe_req->raid_io;
	struct chunk **start = &stripe_req->chunks[raid_io->base_bdev_io_submitted];
	struct chunk *chunk;

	FOR_EACH_CHUNK_FROM(stripe_req, chunk, start) {
		if (spdk_unlikely(raid5f_chunk_write(chunk) != 0)) {
			break;
		}
		raid_io->base_bdev_io_submitted++;
	}
}


static void
_raid5f_stripe_request_submit_chunks(void *_stripe_req)
{
	struct stripe_request *stripe_req = _stripe_req;

	raid5f_stripe_request_submit_chunks(stripe_req);
}

static void
raid5f_stripe_request_fail(void *_stripe_req)
{
	struct stripe_request *stripe_req = _stripe_req;

	raid5f_stripe_request_release(stripe_req);
	raid_bdev_io_complete(stripe_req->raid_io, SPDK_BDEV_IO_STATUS_FAILED);
}

static void
_raid5f_submit_stripe_request(void *_stripe_req)
{
	struct stripe_request *stripe_req = _stripe_req;

	if (spdk_unlikely(raid5f_xor_stripe(stripe_req) != 0)) {
		spdk_thread_send_msg(stripe_req->origin_thread, raid5f_stripe_request_fail, stripe_req);
		return;
	}

	spdk_thread_send_msg(stripe_req->origin_thread, _raid5f_stripe_request_submit_chunks, stripe_req);
}


static void
raid5f_submit_stripe_request(struct stripe_request *stripe_req)
{
	struct raid5f_io_channel *r5ch = (struct raid5f_io_channel *)stripe_req->raid_io->raid_ch->resource;
	struct raid5f_worker *worker = r5ch->next_worker;

	if (!worker) {
		worker = TAILQ_FIRST(&stripe_req->r5f_info->workers);
	}
	r5ch->next_worker = TAILQ_NEXT(worker, link);

	stripe_req->origin_thread = spdk_get_thread();
	stripe_req->worker = worker;

	spdk_thread_send_msg(worker->thread, _raid5f_submit_stripe_request, stripe_req);
}

static int
raid5f_submit_write_request(struct raid_bdev_io *raid_io, uint64_t stripe_index)
{
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid5f_info *r5f_info = raid_bdev->module_private;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct stripe_request *stripe_req = (struct stripe_request *)raid_io->module_ctx;
	struct chunk *chunk;
	uint8_t i = 0;
	int ret;

	stripe_req->parity_buf = raid5f_parity_buf_get(r5f_info);
	if (!stripe_req->parity_buf) {
		return -ENOMEM;
	}

	ret = spdk_mempool_get_bulk(r5f_info->chunk_mp, (void **)stripe_req->chunks,
				    raid_bdev->num_base_bdevs);
	if (ret) {
		raid5f_parity_buf_put(r5f_info, stripe_req->parity_buf);
		return -ENOMEM;
	}

	stripe_req->r5f_info = r5f_info;
	stripe_req->raid_io = raid_io;
	stripe_req->stripe_index = stripe_index;
	stripe_req->parity_chunk = stripe_req->chunks[raid5f_stripe_parity_chunk_index(raid_bdev,
				   stripe_req->stripe_index)];
	FOR_EACH_CHUNK(stripe_req, chunk) {
		chunk->stripe_req = stripe_req;
		chunk->index = i++;
	}

	ret = raid5f_stripe_request_map_iovecs(stripe_req, bdev_io->u.bdev.iovs,
					       bdev_io->u.bdev.iovcnt);
	if (spdk_unlikely(ret)) {
		spdk_mempool_put_bulk(r5f_info->chunk_mp, (void **)stripe_req->chunks, raid_bdev->num_base_bdevs);
		raid5f_parity_buf_put(r5f_info, stripe_req->parity_buf);
		return ret;
	}

	stripe_req->parity_chunk->iovs[0].iov_base = stripe_req->parity_buf;
	stripe_req->parity_chunk->iovs[0].iov_len = raid_bdev->strip_size <<
			raid_bdev->blocklen_shift;
	stripe_req->parity_chunk->iovcnt = 1;

	raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs;

	raid5f_submit_stripe_request(stripe_req);

	return 0;
}

static void
raid5f_chunk_read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;
	struct raid5f_io_channel *r5ch = (struct raid5f_io_channel *)raid_io->raid_ch->resource;

	spdk_bdev_free_io(bdev_io);

	r5ch->num_ios--;
	raid_bdev_io_complete(raid_io, success ? SPDK_BDEV_IO_STATUS_SUCCESS :
			      SPDK_BDEV_IO_STATUS_FAILED);
}

static void
raid5f_submit_rw_request(struct raid_bdev_io *raid_io);

static void
_raid5f_submit_rw_request(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;

	raid5f_submit_rw_request(raid_io);
}

static int
raid5f_submit_read_request(struct raid_bdev_io *raid_io, uint64_t stripe_index,
			   uint64_t stripe_offset)
{
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	uint8_t chunk_data_idx = stripe_offset >> raid_bdev->strip_size_shift;
	uint8_t p_idx = raid5f_stripe_parity_chunk_index(raid_bdev, stripe_index);
	uint8_t chunk_idx = chunk_data_idx < p_idx ? chunk_data_idx : chunk_data_idx + 1;
	struct raid_base_bdev_info *base_info = &raid_bdev->base_bdev_info[chunk_idx];
	struct spdk_io_channel *base_ch = raid_io->raid_ch->base_channel[chunk_idx];
	uint64_t chunk_offset = stripe_offset - (chunk_data_idx << raid_bdev->strip_size_shift);
	uint64_t base_offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + chunk_offset;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	int ret;

	ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
				     bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				     base_offset_blocks, bdev_io->u.bdev.num_blocks,
				     raid5f_chunk_read_complete, raid_io);
	if (spdk_unlikely(ret == -ENOMEM)) {
		raid_bdev_queue_io_wait(raid_io, base_info->bdev, base_ch,
					_raid5f_submit_rw_request);
		return 0;
	}

	return ret;
}

static void
raid5f_submit_rw_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid5f_info *r5f_info = raid_bdev->module_private;
	struct raid5f_io_channel *r5ch = (struct raid5f_io_channel *)raid_io->raid_ch->resource;
	uint64_t offset_blocks = bdev_io->u.bdev.offset_blocks;
	uint64_t stripe_index = offset_blocks / r5f_info->stripe_blocks;
	uint64_t stripe_offset = offset_blocks % r5f_info->stripe_blocks;
	int ret;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		assert(bdev_io->u.bdev.num_blocks <= raid_bdev->strip_size);
		ret = raid5f_submit_read_request(raid_io, stripe_index, stripe_offset);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		assert(stripe_offset == 0);
		assert(bdev_io->u.bdev.num_blocks == r5f_info->stripe_blocks);
		ret = raid5f_submit_write_request(raid_io, stripe_index);
		break;
	default:
		ret = -EINVAL;
		break;
	}

	if (spdk_unlikely(ret)) {
		if (ret == -ENOMEM) {
			if (r5ch->num_ios > 0) {
				raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_NOMEM);
			} else {
				spdk_thread_send_msg(spdk_get_thread(), _raid5f_submit_rw_request,
						     raid_io);
			}
		} else {
			raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	} else {
		r5ch->num_ios++;
	}
}

static void
raid5f_worker_deinit(void *_worker)
{
	struct raid5f_worker *worker = _worker;

	assert(spdk_get_thread() == worker->thread);

	spdk_put_io_channel(worker->ch);
	spdk_thread_exit(worker->thread);
	free(worker);
}

static void
raid5f_stop_worker_threads(struct raid5f_info *r5f_info)
{
	struct raid5f_worker *worker, *tmp;

	TAILQ_FOREACH_SAFE(worker, &r5f_info->workers, link, tmp) {
		TAILQ_REMOVE(&r5f_info->workers, worker, link);
		spdk_thread_send_msg(worker->thread, raid5f_worker_deinit, worker);
	}
}

static void
raid5f_chunk_dtor(struct spdk_mempool *mp, void *opaque, void *obj, unsigned obj_idx)
{
	struct chunk *chunk = obj;

	free(chunk->iovs);
}

static void raid5f_workers_io_device_unregister_done(void *io_device)
{
	struct raid5f_info *r5f_info = SPDK_CONTAINEROF(io_device, struct raid5f_info, workers);

	free(r5f_info);
}

static void
raid5f_stop(struct raid_bdev *raid_bdev)
{
	struct raid5f_info *r5f_info = raid_bdev->module_private;

	if (r5f_info->chunk_mp) {
		spdk_mempool_obj_iter(r5f_info->chunk_mp, raid5f_chunk_dtor, NULL);
		spdk_mempool_free(r5f_info->chunk_mp);
	}

	if (r5f_info->stripe_parity_buffers) {
		void *buf;

		while ((buf = raid5f_parity_buf_get(r5f_info))) {
			spdk_dma_free(buf);
		}
		spdk_ring_free(r5f_info->stripe_parity_buffers);
	}

	raid5f_stop_worker_threads(r5f_info);

	spdk_io_device_unregister(&r5f_info->workers, raid5f_workers_io_device_unregister_done);
}

static void
raid5f_chunk_ctor(struct spdk_mempool *mp, void *opaque, void *obj, unsigned obj_idx)
{
	struct chunk *chunk = obj;
	struct raid5f_info *r5f_info = opaque;

	chunk->iovcnt_max = 4;
	chunk->iovs = calloc(chunk->iovcnt_max, sizeof(chunk->iovs[0]));
	if (!chunk->iovs) {
		r5f_info->status = -ENOMEM;
	}
}

static void
raid5f_worker_io_channel_destroy(void *io_device, void *ctx_buf)
{
	struct raid5f_worker_io_channel *worker_ch = ctx_buf;

	if (worker_ch->chunk_xor_bounce_buffers) {
		int i;

		for (i = 0; i < RAID5F_MAX_BASE_BDEVS; i++) {
			free(worker_ch->chunk_xor_bounce_buffers[i].iov_base);
		}
		free(worker_ch->chunk_xor_bounce_buffers);
	}

	free(worker_ch->chunk_xor_buffers);
	free(worker_ch->chunk_iov_iters);
}

static int
raid5f_worker_io_channel_create(void *io_device, void *ctx_buf)
{
	struct raid5f_worker_io_channel *worker_ch = ctx_buf;
	struct raid5f_info *r5f_info = SPDK_CONTAINEROF(io_device, struct raid5f_info, workers);
	struct raid_bdev *raid_bdev = r5f_info->raid_bdev;
	size_t chunk_len = raid_bdev->strip_size << raid_bdev->blocklen_shift;
	int status = 0;
	int i;

	worker_ch->chunk_iov_iters = calloc(raid5f_stripe_data_chunks_num(raid_bdev),
				       sizeof(worker_ch->chunk_iov_iters[0]));
	if (!worker_ch->chunk_iov_iters) {
		status = -ENOMEM;
		goto out;
	}

	worker_ch->chunk_xor_buffers = calloc(raid5f_stripe_data_chunks_num(raid_bdev),
					 sizeof(worker_ch->chunk_xor_buffers[0]));
	if (!worker_ch->chunk_xor_buffers) {
		status = -ENOMEM;
		goto out;
	}

	worker_ch->chunk_xor_bounce_buffers = calloc(RAID5F_MAX_BASE_BDEVS,
						sizeof(worker_ch->chunk_xor_bounce_buffers[0]));
	if (!worker_ch->chunk_xor_bounce_buffers) {
		status = -ENOMEM;
		goto out;
	}

	for (i = 0; i < raid5f_stripe_data_chunks_num(raid_bdev); i++) {
		status = posix_memalign(&worker_ch->chunk_xor_bounce_buffers[i].iov_base,
					spdk_xor_get_buf_alignment(), chunk_len);
		if (status) {
			goto out;
		}
		worker_ch->chunk_xor_bounce_buffers[i].iov_len = chunk_len;
	}
out:
	if (status) {
		SPDK_ERRLOG("Failed to initialize io channel\n");
		raid5f_worker_io_channel_destroy(io_device, ctx_buf);
	}
	return status;
}

static void
raid5f_worker_init(void *_worker)
{
	struct raid5f_worker *worker = _worker;

	assert(spdk_get_thread() == worker->thread);

	worker->ch = spdk_get_io_channel(&worker->r5f_info->workers);
}

static int
raid5f_start_worker_threads(struct raid5f_info *r5f_info)
{
	struct raid5f_config *r5f_config = r5f_info->raid_bdev->config->module_cfg;
	struct spdk_cpuset tmp_cpuset;
	struct raid5f_worker *worker;
	char name[128];
	uint32_t i;
	int ret = 0;

	TAILQ_INIT(&r5f_info->workers);

	SPDK_ENV_FOREACH_CORE(i) {
		if (spdk_cpuset_get_cpu(&r5f_config->worker_cpuset, i)) {
			worker = malloc(sizeof(*worker));
			if (!worker) {
				ret = -ENOMEM;
				break;
			}
			worker->r5f_info = r5f_info;

			spdk_cpuset_zero(&tmp_cpuset);
			spdk_cpuset_set_cpu(&tmp_cpuset, i, true);
			snprintf(name, sizeof(name), "raid5f_%p_worker_%u", r5f_info->raid_bdev, i);
			worker->thread = spdk_thread_create(name, &tmp_cpuset);
			if (!worker->thread) {
				free(worker);
				ret = -ENOMEM;
				break;
			}
			TAILQ_INSERT_TAIL(&r5f_info->workers, worker, link);
		}
	}
	if (ret) {
		struct raid5f_worker *tmp;

		TAILQ_FOREACH_SAFE(worker, &r5f_info->workers, link, tmp) {
			TAILQ_REMOVE(&r5f_info->workers, worker, link);
			spdk_thread_exit(worker->thread);
			free(worker);
		}
		return ret;
	}

	TAILQ_FOREACH(worker, &r5f_info->workers, link) {
		spdk_thread_send_msg(worker->thread, raid5f_worker_init, worker);
	}

	return 0;
}

static int
raid5f_start(struct raid_bdev *raid_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct raid_base_bdev_info *base_info;
	struct raid5f_info *r5f_info;
	char name_buf[32];
	size_t alignment;
	unsigned int i;
	int ret;

	r5f_info = calloc(1, sizeof(*r5f_info));
	if (!r5f_info) {
		SPDK_ERRLOG("Failed to allocate r5f_info\n");
		return -ENOMEM;
	}
	raid_bdev->module_private = r5f_info;

	alignment = spdk_xor_get_buf_alignment();
	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
		alignment = spdk_max(alignment, spdk_bdev_get_buf_align(base_info->bdev));
	}

	r5f_info->raid_bdev = raid_bdev;
	r5f_info->total_stripes = min_blockcnt / raid_bdev->strip_size;
	r5f_info->stripe_blocks = raid_bdev->strip_size * raid5f_stripe_data_chunks_num(raid_bdev);
	r5f_info->stripe_parity_buffers = spdk_ring_create(SPDK_RING_TYPE_MP_MC, RAID5F_MAX_STRIPES,
					  SPDK_ENV_SOCKET_ID_ANY);
	if (!r5f_info->stripe_parity_buffers) {
		ret = -ENOMEM;
		goto out;
	}

	for (i = 0; i < RAID5F_MAX_STRIPES; i++) {
		void *buf = spdk_dma_malloc(raid_bdev->strip_size << raid_bdev->blocklen_shift,
					    alignment, NULL);
		if (!buf) {
			ret = -ENOMEM;
			goto out;
		}
		raid5f_parity_buf_put(r5f_info, buf);
	}

	snprintf(name_buf, sizeof(name_buf), "r5f_chunk_%p", raid_bdev);
	r5f_info->chunk_mp = spdk_mempool_create_ctor(name_buf,
			     RAID5F_MAX_STRIPES * raid_bdev->num_base_bdevs,
			     sizeof(struct chunk), 0,
			     SPDK_ENV_SOCKET_ID_ANY, raid5f_chunk_ctor, r5f_info);
	if (!r5f_info->chunk_mp) {
		ret = -ENOMEM;
	} else {
		ret = r5f_info->status;
	}
	if (ret) {
		goto out;
	}

	spdk_io_device_register(&r5f_info->workers, raid5f_worker_io_channel_create,
				raid5f_worker_io_channel_destroy,
				sizeof(struct raid5f_worker_io_channel), NULL);

	ret = raid5f_start_worker_threads(r5f_info);
out:
	if (ret) {
		raid5f_stop(raid_bdev);
		return ret;
	}

	raid_bdev->bdev.blockcnt = r5f_info->stripe_blocks * r5f_info->total_stripes;
	raid_bdev->bdev.optimal_io_boundary = raid_bdev->strip_size;
	raid_bdev->bdev.split_on_optimal_io_boundary = true;
	raid_bdev->bdev.write_unit_size = r5f_info->stripe_blocks;

	return 0;
}

static int
raid5f_decode_cpuset(const struct spdk_json_val *val, void *out)
{
	int ret;
	char *str = NULL;
	struct spdk_cpuset *cpuset = out;

	ret = spdk_json_decode_string(val, &str);
	if (ret == 0 && str != NULL) {
		if (spdk_cpuset_parse(cpuset, str)) {
			ret = -EINVAL;
		}
	}

	free(str);
	return ret;
}

static struct spdk_json_object_decoder raid5f_config_decoders[] = {
	{"worker_cpumask", offsetof(struct raid5f_config, worker_cpuset), raid5f_decode_cpuset, true},
};

static int
raid5f_config_parse(struct raid_bdev_config *raid_cfg, const struct spdk_json_val *module_params)
{
	struct raid5f_config *r5f_config;

	r5f_config = calloc(1, sizeof(*r5f_config));
	if (!r5f_config) {
		return -ENOMEM;
	}

	if (module_params) {
		int ret;

		ret = spdk_json_decode_object(module_params, raid5f_config_decoders,
					      SPDK_COUNTOF(raid5f_config_decoders), r5f_config);
		if (ret) {
			free(r5f_config);
			return ret;
		}
	}

	raid_cfg->module_cfg = r5f_config;

	return 0;
}

static void
raid5f_config_cleanup(struct raid_bdev_config *raid_cfg)
{
	if (raid_cfg->module_cfg) {
		struct raid5f_config *r5f_config = raid_cfg->module_cfg;

		raid_cfg->module_cfg = NULL;
		free(r5f_config);
	}
}

static struct raid_bdev_module g_raid5f_module = {
	.level = RAID5F,
	.base_bdevs_min = 3,
	.base_bdevs_max_degraded = 1,
	.ioch_resource_size = sizeof(struct raid5f_io_channel),
	.raid_io_ctx_size = sizeof(struct stripe_request),
	.config_parse = raid5f_config_parse,
	.config_cleanup = raid5f_config_cleanup,
	.start = raid5f_start,
	.stop = raid5f_stop,
	.submit_rw_request = raid5f_submit_rw_request,
};
RAID_MODULE_REGISTER(&g_raid5f_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raid5f)
