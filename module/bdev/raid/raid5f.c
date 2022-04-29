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

/* Maximum concurrent full stripe writes */
#define RAID5F_MAX_STRIPES 128

struct chunk {
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

	/* Array of chunks corresponding to base_bdevs */
	struct chunk chunks[0];
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

	/* Mempool of all available stripe requests */
	struct spdk_mempool *stripe_request_mp;
};

#define __CHUNK_IN_RANGE(req, c) \
	c < req->chunks + req->r5f_info->raid_bdev->num_base_bdevs

#define FOR_EACH_CHUNK_FROM(req, c, from) \
	for (c = from; __CHUNK_IN_RANGE(req, c); c++)

#define FOR_EACH_CHUNK(req, c) \
	FOR_EACH_CHUNK_FROM(req, c, req->chunks)

#define __NEXT_DATA_CHUNK(req, c) \
	c == req->parity_chunk ? c+1 : c

#define FOR_EACH_DATA_CHUNK(req, c) \
	for (c = __NEXT_DATA_CHUNK(req, req->chunks); __CHUNK_IN_RANGE(req, c); \
	     c = __NEXT_DATA_CHUNK(req, c+1))

static inline struct stripe_request *
raid5f_chunk_stripe_req(struct chunk *chunk)
{
	return SPDK_CONTAINEROF((chunk - chunk->index), struct stripe_request, chunks);
}

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

static void
raid5f_chunk_write_complete(struct chunk *chunk, enum spdk_bdev_io_status status)
{
	struct stripe_request *stripe_req = raid5f_chunk_stripe_req(chunk);
	struct raid5f_info *r5f_info = stripe_req->raid_io->raid_bdev->module_private;

	if (raid_bdev_io_complete_part(stripe_req->raid_io, 1, status)) {
		spdk_mempool_put(r5f_info->stripe_request_mp, stripe_req);
	}
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
	struct stripe_request *stripe_req = raid_io->module_private;

	raid5f_stripe_request_submit_chunks(stripe_req);
}

static int
raid5f_chunk_write(struct chunk *chunk)
{
	struct stripe_request *stripe_req = raid5f_chunk_stripe_req(chunk);
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
	int raid_io_iov_idx = -1;
	size_t raid_io_offset = 0;
	size_t raid_io_iov_offset = 0;
	int i;

	for (i = 0; i < raid_io_iovcnt; i++) {
		if (raid_io_iov_offset + raid_io_iovs[i].iov_len > raid_io_offset) {
			raid_io_iov_idx = i;
			break;
		}
		raid_io_iov_offset += raid_io_iovs[i].iov_len;
	}

	if (spdk_unlikely(raid_io_iov_idx == -1)) {
		return -EINVAL;
	}

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
	struct chunk *start = &stripe_req->chunks[raid_io->base_bdev_io_submitted];
	struct chunk *chunk;

	FOR_EACH_CHUNK_FROM(stripe_req, chunk, start) {
		if (chunk == stripe_req->parity_chunk) {
			continue;
		}

		if (spdk_unlikely(raid5f_chunk_write(chunk) != 0)) {
			break;
		}
		raid_io->base_bdev_io_submitted++;
	}
}

static void
raid5f_submit_stripe_request(struct stripe_request *stripe_req)
{
	/* TODO: parity */

	raid5f_stripe_request_submit_chunks(stripe_req);
}

static int
raid5f_submit_write_request(struct raid_bdev_io *raid_io, uint64_t stripe_index)
{
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid5f_info *r5f_info = raid_bdev->module_private;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct stripe_request *stripe_req;
	int ret;

	stripe_req = spdk_mempool_get(r5f_info->stripe_request_mp);
	if (!stripe_req) {
		return -ENOMEM;
	}

	stripe_req->stripe_index = stripe_index;
	stripe_req->parity_chunk = stripe_req->chunks + raid5f_stripe_parity_chunk_index(raid_bdev,
				   stripe_req->stripe_index);
	stripe_req->raid_io = raid_io;

	ret = raid5f_stripe_request_map_iovecs(stripe_req, bdev_io->u.bdev.iovs,
					       bdev_io->u.bdev.iovcnt);
	if (spdk_unlikely(ret)) {
		spdk_mempool_put(r5f_info->stripe_request_mp, stripe_req);
		return ret;
	}

	raid_io->module_private = stripe_req;
	raid_io->base_bdev_io_remaining = raid5f_stripe_data_chunks_num(raid_bdev);

	raid5f_submit_stripe_request(stripe_req);

	return 0;
}

static void
raid5f_chunk_read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

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
		raid_bdev_io_complete(raid_io, ret == -ENOMEM ? SPDK_BDEV_IO_STATUS_NOMEM :
				      SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
raid5f_stripe_req_dtor(struct spdk_mempool *mp, void *opaque, void *obj, unsigned obj_idx)
{
	struct stripe_request *stripe_req = obj;
	struct chunk *chunk;

	FOR_EACH_CHUNK(stripe_req, chunk) {
		free(chunk->iovs);
	}
}

static void
raid5f_stop(struct raid_bdev *raid_bdev)
{
	struct raid5f_info *r5f_info = raid_bdev->module_private;

	if (r5f_info->stripe_request_mp) {
		spdk_mempool_obj_iter(r5f_info->stripe_request_mp, raid5f_stripe_req_dtor, NULL);
		spdk_mempool_free(r5f_info->stripe_request_mp);
	}

	free(r5f_info);
}

static void
raid5f_stripe_req_ctor(struct spdk_mempool *mp, void *opaque, void *obj, unsigned obj_idx)
{
	struct stripe_request *stripe_req = obj;
	struct raid5f_info *r5f_info = opaque;
	struct chunk *chunk;

	stripe_req->r5f_info = r5f_info;
	FOR_EACH_CHUNK(stripe_req, chunk) {
		chunk->index = chunk - stripe_req->chunks;
		chunk->iovcnt_max = 4;
		chunk->iovs = calloc(chunk->iovcnt_max, sizeof(chunk->iovs[0]));
		if (!chunk->iovs) {
			r5f_info->status = -ENOMEM;
		}
	}
}

static int
raid5f_start(struct raid_bdev *raid_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct raid_base_bdev_info *base_info;
	struct raid5f_info *r5f_info;
	char name_buf[32];
	int ret;

	r5f_info = calloc(1, sizeof(*r5f_info));
	if (!r5f_info) {
		SPDK_ERRLOG("Failed to allocate r5f_info\n");
		return -ENOMEM;
	}
	raid_bdev->module_private = r5f_info;

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
	}

	r5f_info->raid_bdev = raid_bdev;
	r5f_info->total_stripes = min_blockcnt / raid_bdev->strip_size;
	r5f_info->stripe_blocks = raid_bdev->strip_size * raid5f_stripe_data_chunks_num(raid_bdev);

	snprintf(name_buf, sizeof(name_buf), "r5_sreq_%p", raid_bdev);
	r5f_info->stripe_request_mp = spdk_mempool_create_ctor(name_buf, RAID5F_MAX_STRIPES,
				      sizeof(struct stripe_request) + sizeof(struct chunk) * raid_bdev->num_base_bdevs, 0,
				      SPDK_ENV_SOCKET_ID_ANY, raid5f_stripe_req_ctor, r5f_info);
	if (!r5f_info->stripe_request_mp) {
		ret = -ENOMEM;
	} else {
		ret = r5f_info->status;
	}

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

static struct raid_bdev_module g_raid5f_module = {
	.level = RAID5F,
	.base_bdevs_min = 3,
	.base_bdevs_max_degraded = 1,
	.start = raid5f_start,
	.stop = raid5f_stop,
	.submit_rw_request = raid5f_submit_rw_request,
};
RAID_MODULE_REGISTER(&g_raid5f_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raid5f)
