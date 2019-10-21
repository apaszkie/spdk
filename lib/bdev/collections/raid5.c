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

#include "spdk/stdinc.h"

#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/conf.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/io_channel.h"
#include "spdk/util.h"

#include "spdk_internal/bdev.h"
#include "spdk_internal/log.h"

#include "common.h"
#include "vbdev_collections.h"

struct stripe_request {
	struct collection *collection;
	struct collections_io_channel *ch;
	uint64_t stripe;

	uint64_t blocks;
	uint64_t stripe_offset_from;
	uint64_t stripe_offset_to;

	int remaining;

	struct spdk_bdev_io *orig_io;
	struct sg_list *sgl;
	int error;

	struct spdk_thread *thread;

	struct chunk *first_data_chunk;
	struct chunk *last_data_chunk;
	struct chunk *parity_chunk;
	struct chunk *reconstruct_chunk;

	bool rmw;
	bool resync;

	struct stripe_request *next;

	struct chunk {
		struct stripe_request *stripe_req;
		struct sg_list *sgl;
		struct mempool_elem *mempool_elem;
		uint64_t req_offset; // from chunk start
		uint64_t req_blocks;
	} chunks[0];
};

struct chunk_req {
	struct stripe_request *stripe_req;
	struct sg_list *sgl;
	bool preread;
};

struct raid5_info {
	uint64_t stripe_blocks;
	uint64_t total_stripes;
	struct stripe_request **active_stripes;
	pthread_spinlock_t active_stripes_lock;
	struct mempool *chunk_mempool;
	struct sg_list *null_sgl;
	enum {
		SYNC_MODE_OFF,
		SYNC_MODE_RESYNC,
		SYNC_MODE_CHECK,
		SYNC_MODE_REPAIR,
	} sync_mode;
	struct spdk_io_channel *sync_ch;
	uint64_t sync_progress;
	uint64_t sync_mismatch_cnt;
};

#define for_each_chunk(r, c) \
for ((c) = (r)->chunks; (c) < (r)->chunks + (r)->collection->members_count; (c)++)

#define __next_data_chunk(r, c) \
(c)+1 == (r)->parity_chunk ? (c)+2 : (c)+1

#define for_each_data_chunk(r, c) \
for ((c) = __next_data_chunk((r), (r)->chunks-1); (c) < (r)->chunks + (r)->collection->members_count; (c) = __next_data_chunk((r), (c)))

#define for_each_req_data_chunk(r, c) \
for ((c) = (r)->first_data_chunk; (c) <= (r)->last_data_chunk; (c) = __next_data_chunk((r), (c)))

static inline unsigned int chunk_idx(struct chunk *chunk)
{
	return chunk - chunk->stripe_req->chunks;
}

static inline unsigned int data_chunk_idx(struct chunk *chunk)
{
	return chunk < chunk->stripe_req->parity_chunk ? chunk_idx(chunk) : chunk_idx(chunk)-1;
}

static inline struct chunk *get_data_chunk(struct stripe_request *stripe_req, unsigned int data_chunk_idx)
{
	return &stripe_req->chunks[data_chunk_idx < chunk_idx(stripe_req->parity_chunk) ? data_chunk_idx : data_chunk_idx+1];
}

static inline struct collection_member *chunk_member_disk(struct chunk *chunk)
{
	return &chunk->stripe_req->collection->members[chunk_idx(chunk)];
}

static void free_stripe_req(struct stripe_request *stripe_req)
{
	struct chunk *chunk;

	for_each_chunk(stripe_req, chunk) {
		if (chunk->mempool_elem) {
			if (chunk->mempool_elem->sgl != chunk->sgl) {
				free(chunk->sgl);
			}
			mempool_elem_put(chunk->mempool_elem);
		} else {
			free(chunk->sgl);
		}
	}
	free(stripe_req->sgl);
	free(stripe_req);
}

static void raid5_stripe_xor(struct chunk *dest_chunk)
{
	struct collection *collection = dest_chunk->stripe_req->collection;
	unsigned int count = 0;
	struct chunk *chunk;
	uint64_t offset = dest_chunk->req_offset * collection->vbdev->blocklen;
	uint64_t len = dest_chunk->req_blocks * collection->vbdev->blocklen;
	struct sg_list *dest = split_sg_list(dest_chunk->sgl, offset, len);

	for_each_chunk(dest_chunk->stripe_req, chunk) {
		struct sg_list *src;

		if (chunk == dest_chunk) {
			continue;
		}

		src = split_sg_list(chunk->sgl, offset, len);

		if (count++ == 0) {
			memcpy_sgl(dest, src);
		} else {
			xor_sgl(dest, src);
		}

		free(src);
	}

	free(dest);
}

static int raid5_submit_stripe_request(struct stripe_request *stripe_req);

static void __raid5_submit_stripe_request(void *stripe_req)
{
	raid5_submit_stripe_request(stripe_req);
}

static void raid5_complete_stripe_request(struct stripe_request *stripe_req)
{
	struct collection *collection = stripe_req->collection;
	struct raid5_info *r5info = collection->private;
	struct stripe_request *next_req;

	SPDK_DEBUGLOG(SPDK_LOG_COLLECTIONS_RAID5, "stripe_req: %p\n", stripe_req);

	if (!stripe_req->resync) {
		collection_complete_request_part(stripe_req->orig_io, stripe_req->error, stripe_req->blocks);
	}

	pthread_spin_lock(&r5info->active_stripes_lock);
	next_req = stripe_req->next;
	r5info->active_stripes[stripe_req->stripe] = next_req;
	pthread_spin_unlock(&r5info->active_stripes_lock);

	free_stripe_req(stripe_req);

	if (next_req) {
		spdk_thread_send_msg(next_req->thread, __raid5_submit_stripe_request, next_req);
	}
}

static int raid5_stripe_read_preread_complete(struct stripe_request *stripe_req)
{
	raid5_stripe_xor(stripe_req->reconstruct_chunk);
	raid5_complete_stripe_request(stripe_req);
	return 0;
}

static int submit_chunk_request(struct chunk *chunk, enum spdk_bdev_io_type io_type, uint64_t offset_blocks, uint64_t num_blocks, bool preread);

static int raid5_stripe_write_preread_complete(struct stripe_request *stripe_req)
{
	struct collection *collection = stripe_req->collection;
	int ret = 0;
	struct chunk *chunk;

	if (stripe_req->rmw) {
		struct sg_list *src;
		struct sg_list *dest;
		struct sg_list *tmp;
		uint64_t offset;
		uint64_t len;
		uint64_t iov_off = 0;

		for_each_req_data_chunk(stripe_req, chunk) {
			offset = chunk->req_offset * collection->vbdev->blocklen;
			len = chunk->req_blocks * collection->vbdev->blocklen;

			dest = split_sg_list(stripe_req->parity_chunk->sgl, offset, len);
			src = split_sg_list(chunk->sgl, offset, len);

			xor_sgl(dest, src);

			free(src);

			src = split_sg_list(stripe_req->sgl, iov_off, len);

			xor_sgl(dest, src);

			tmp = chunk->sgl;
			chunk->sgl = merge_sg_lists(tmp, src, offset);

			free(tmp);
			free(src);
			free(dest);
			iov_off += len;
		}
	} else {
		raid5_stripe_xor(stripe_req->parity_chunk);
	}

	for_each_chunk(stripe_req, chunk) {
		if (!chunk->req_blocks) {
			continue;
		}

		ret = submit_chunk_request(chunk, SPDK_BDEV_IO_TYPE_WRITE, chunk->req_offset, chunk->req_blocks, false);
		if (ret) {
			break;
		}
	}

	return ret;
}

static int raid5_stripe_preread_complete(struct stripe_request *stripe_req)
{
	return (stripe_req->orig_io->type == SPDK_BDEV_IO_TYPE_READ) ?
			raid5_stripe_read_preread_complete(stripe_req) :
			raid5_stripe_write_preread_complete(stripe_req);
}

static void raid5_complete_chunk_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct chunk_req *chunk_req = cb_arg;
	struct stripe_request *stripe_req = chunk_req->stripe_req;

	SPDK_DEBUGLOG(SPDK_LOG_COLLECTIONS_RAID5, "stripe_req: %p success: %d remaining: %d\n", stripe_req, success, stripe_req->remaining);

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		stripe_req->error = true;
	}

	if (--stripe_req->remaining == 0) {
		if (chunk_req->preread) {
			if (stripe_req->error || raid5_stripe_preread_complete(stripe_req)) {
				raid5_complete_stripe_request(stripe_req);
			}
		} else {
			raid5_complete_stripe_request(stripe_req);
		}
	}

	free(chunk_req->sgl);
	free(chunk_req);
}

static int raid5_member_submit_blocks(struct collection_member *member,
				    enum spdk_bdev_io_type io_type,
				    struct collections_io_channel *ch,
				    uint64_t offset_blocks,
				    uint64_t num_blocks,
				    struct sg_list *sgl,
				    spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	int ret;

	SPDK_DEBUGLOG(SPDK_LOG_VBDEV_COLLECTIONS, "member: %u (%c %lu+%lu)\n", member->idx, bdev_io_type_char(io_type), offset_blocks, num_blocks);

	ret = bdev_iov_submit_blocks(io_type, member->bdev_desc, ch->members_ch[member->idx], offset_blocks, num_blocks, sgl, cb, cb_arg);
	if (ret) {
		SPDK_ERRLOG("bdev_io_submit_blocks failed: %d\n", ret);
	}

	return ret;
}

static int submit_chunk_request(struct chunk *chunk, enum spdk_bdev_io_type io_type, uint64_t offset_blocks, uint64_t num_blocks, bool preread)
{
	struct stripe_request *stripe_req = chunk->stripe_req;
	struct collection *collection = stripe_req->collection;
	uint64_t member_block = stripe_req->stripe * collection->chunk_blocks + offset_blocks;
	struct chunk_req *chunk_req;

	chunk_req = malloc(sizeof(*chunk_req));
	if (!chunk_req) {
		return -ENOMEM;
	}
	chunk_req->stripe_req = stripe_req;

	chunk_req->sgl = split_sg_list(chunk->sgl, offset_blocks * collection->vbdev->blocklen, num_blocks * collection->vbdev->blocklen);
	if (!chunk_req->sgl) {
		return -ENOMEM;
	}

	chunk_req->preread = preread;

	stripe_req->remaining++;
	return raid5_member_submit_blocks(chunk_member_disk(chunk), io_type, stripe_req->ch, member_block, num_blocks, chunk_req->sgl, raid5_complete_chunk_io, chunk_req);
}

static int raid5_stripe_write(struct stripe_request *stripe_req)
{
	struct collection *collection = stripe_req->collection;
	struct raid5_info *r5info = collection->private;
	struct chunk *chunk;
	int ret = 0;
	uint64_t iov_off = 0;
	int preread_balance = 0;

	if (stripe_req->first_data_chunk == stripe_req->last_data_chunk) {
		stripe_req->parity_chunk->req_offset = stripe_req->stripe_offset_from % collection->chunk_blocks;
	} else {
		stripe_req->parity_chunk->req_offset = 0;
	}

	stripe_req->parity_chunk->req_blocks = spdk_min(stripe_req->blocks, collection->chunk_blocks);

	for_each_data_chunk(stripe_req, chunk) {
		if (chunk->req_blocks < stripe_req->parity_chunk->req_blocks) {
			preread_balance++;
		}

		if (chunk->req_blocks > 0) {
			preread_balance--;
		}
	}

	stripe_req->rmw = (preread_balance > 0);

	for_each_chunk(stripe_req, chunk) {
		uint64_t preread_offset;
		uint64_t preread_blocks;

		if (stripe_req->rmw) {
			preread_offset = chunk->req_offset;
			preread_blocks = chunk->req_blocks;
		} else {
			if (chunk == stripe_req->parity_chunk) {
				preread_offset = 0;
				preread_blocks = 0;
			} else if (stripe_req->first_data_chunk == stripe_req->last_data_chunk) {
				if (chunk->req_blocks) {
					preread_offset = 0;
					preread_blocks = 0;
				} else {
					preread_offset = stripe_req->parity_chunk->req_offset;
					preread_blocks = stripe_req->parity_chunk->req_blocks;
				}
			} else {
				if (chunk->req_offset) {
					preread_offset = 0;
					preread_blocks = chunk->req_offset;
				} else {
					preread_offset = chunk->req_blocks;
					preread_blocks = collection->chunk_blocks - chunk->req_blocks;
				}
			}
		}

		if (preread_blocks || chunk == stripe_req->parity_chunk) {
			struct sg_list *sgl;

			// TODO: limit size
			chunk->mempool_elem = mempool_elem_get(r5info->chunk_mempool);
			if (!chunk->mempool_elem) {
				return -ENOMEM;
			}

			if (chunk == stripe_req->parity_chunk) {
				sgl = split_sg_list(chunk->mempool_elem->sgl, 0, chunk->req_blocks * collection->vbdev->blocklen);
				chunk->sgl = merge_sg_lists(r5info->null_sgl, sgl, chunk->req_offset * collection->vbdev->blocklen);
			} else {
				sgl = split_sg_list(chunk->mempool_elem->sgl, 0, preread_blocks * collection->vbdev->blocklen);
				chunk->sgl = merge_sg_lists(r5info->null_sgl, sgl, preread_offset * collection->vbdev->blocklen);
			}
			free(sgl);
		}

		if (!stripe_req->rmw && chunk->req_blocks && chunk != stripe_req->parity_chunk) {
			size_t len = chunk->req_blocks * collection->vbdev->blocklen;
			struct sg_list *sgl = chunk->sgl ?: clone_sg_list(r5info->null_sgl);
			struct sg_list *sgl_req = split_sg_list(stripe_req->sgl, iov_off, len);
			chunk->sgl = merge_sg_lists(sgl, sgl_req, chunk->req_offset * collection->vbdev->blocklen);
			free(sgl);
			free(sgl_req);
			iov_off += len;
		}

		if (preread_blocks) {
			ret = submit_chunk_request(chunk, SPDK_BDEV_IO_TYPE_READ, preread_offset, preread_blocks, true);
			if (ret) {
				break;
			}
		}
	}

	if (ret == 0 && stripe_req->remaining == 0) {
		ret = raid5_stripe_write_preread_complete(stripe_req);
	}

	return ret;
}

static int raid5_stripe_read_reconstruct(struct stripe_request *stripe_req)
{
	struct collection *collection = stripe_req->collection;
	struct raid5_info *r5info = collection->private;
	struct chunk *chunk;
	int ret = 0;
	uint64_t iov_off = 0;

	for_each_chunk(stripe_req, chunk) {
		uint64_t preread_offset;
		uint64_t preread_blocks;
		struct sg_list *tmp, *sgl;

		if (chunk == stripe_req->reconstruct_chunk) {
			preread_offset = 0;
			preread_blocks = 0;
		} else {
			preread_offset = stripe_req->reconstruct_chunk->req_offset;
			preread_blocks = stripe_req->reconstruct_chunk->req_blocks;

			// separate overlapping preread blocks from request blocks
			if (chunk->req_blocks) {
				if (preread_offset <= chunk->req_offset + chunk->req_blocks &&
				    preread_offset + preread_blocks > chunk->req_offset + chunk->req_blocks) {
					preread_offset = chunk->req_offset + chunk->req_blocks;
					preread_blocks = collection->chunk_blocks - preread_offset;
				} else if (chunk->req_offset <= preread_offset + preread_blocks &&
					   chunk->req_offset + chunk->req_blocks >= preread_offset + preread_blocks) {
					preread_blocks = chunk->req_offset;
				}
			}

			if (preread_blocks == 0) {
				preread_offset = 0;
			}
		}

		chunk->sgl = clone_sg_list(r5info->null_sgl);

		if (preread_blocks) {
			// TODO: limit size
			chunk->mempool_elem = mempool_elem_get(r5info->chunk_mempool);
			if (!chunk->mempool_elem) {
				return -ENOMEM;
			}
			tmp = chunk->sgl;
			sgl = split_sg_list(chunk->mempool_elem->sgl, 0, preread_blocks * collection->vbdev->blocklen);
			chunk->sgl = merge_sg_lists(chunk->sgl, sgl, preread_offset * collection->vbdev->blocklen);
			free(tmp);
			free(sgl);
		}

		if (chunk->req_blocks) {
			uint32_t len = chunk->req_blocks * collection->vbdev->blocklen;
			tmp = chunk->sgl;
			sgl = split_sg_list(stripe_req->sgl, iov_off, len);
			chunk->sgl = merge_sg_lists(chunk->sgl, sgl, chunk->req_offset * collection->vbdev->blocklen);
			free(tmp);
			free(sgl);
			iov_off += len;
		}

		if (chunk == stripe_req->reconstruct_chunk) {
			continue;
		} else if (preread_offset == chunk->req_offset + chunk->req_blocks ||
			   chunk->req_offset == preread_offset + preread_blocks) {
			// one read request if preread and req blocks are continuous
			ret = submit_chunk_request(chunk, SPDK_BDEV_IO_TYPE_READ, spdk_min(preread_offset, chunk->req_offset), preread_blocks + chunk->req_blocks, true);
		} else {
			if (preread_blocks) {
				ret = submit_chunk_request(chunk, SPDK_BDEV_IO_TYPE_READ, preread_offset, preread_blocks, true);
			}
			if (!ret && chunk->req_blocks) {
				ret = submit_chunk_request(chunk, SPDK_BDEV_IO_TYPE_READ, chunk->req_offset, chunk->req_blocks, true);
			}
		}
		if (ret) {
			break;
		}
	}

	return ret;
}

static int raid5_stripe_read(struct stripe_request *stripe_req)
{
	struct collection *collection = stripe_req->collection;
	struct raid5_info *r5info = collection->private;
	int ret = 0;
	struct chunk *chunk;
	uint64_t iov_off = 0;

	//stripe_req->reconstruct_chunk = stripe_req->first_data_chunk;

	if (stripe_req->reconstruct_chunk) {
		return raid5_stripe_read_reconstruct(stripe_req);
	}

	for_each_req_data_chunk(stripe_req, chunk) {
		uint32_t len = chunk->req_blocks * collection->vbdev->blocklen;
		struct sg_list *sgl = split_sg_list(stripe_req->sgl, iov_off, len);

		chunk->sgl = merge_sg_lists(r5info->null_sgl, sgl, chunk->req_offset * collection->vbdev->blocklen);
		free(sgl);
		iov_off += len;

		ret = submit_chunk_request(chunk, SPDK_BDEV_IO_TYPE_READ, chunk->req_offset, chunk->req_blocks, false);
		if (ret) {
			break;
		}
	}

	return ret;
}

static int raid5_handle_stripe(struct collection *collection, struct collections_io_channel *ch, struct spdk_bdev_io *bdev_io, uint64_t stripe, uint64_t stripe_offset, uint64_t blocks, struct sg_list *sgl, bool resync);
static void raid5_sync_stop(struct collection *collection);

static void raid5_complete_stripe_sync(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct stripe_request *stripe_req = cb_arg;
	struct collection *collection = stripe_req->collection;
	struct raid5_info *r5info = collection->private;
	uint64_t next_stripe = stripe_req->stripe + 1;
	struct collections_io_channel *ch = stripe_req->ch;

	if (bdev_io) {
		spdk_bdev_free_io(bdev_io);
	}

	raid5_complete_stripe_request(stripe_req);

	r5info->sync_progress++;

	if (!success || r5info->sync_progress == r5info->total_stripes ||
	    r5info->sync_mode == SYNC_MODE_OFF) {
		r5info->sync_mode = SYNC_MODE_OFF;
		spdk_put_io_channel(r5info->sync_ch);
	} else {
		raid5_handle_stripe(collection, ch, NULL, next_stripe, 0, r5info->stripe_blocks, NULL, true);
	}
}

static void raid5_complete_stripe_sync_chunk_read(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct stripe_request *stripe_req = cb_arg;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		stripe_req->error = true;
	}

	if (--stripe_req->remaining == 0) {
		struct collection *collection = stripe_req->collection;
		struct raid5_info *r5info = collection->private;
		uint64_t member_block = stripe_req->stripe * collection->chunk_blocks;
		bool write_parity = false;

		if (stripe_req->error) {
			raid5_complete_stripe_sync(NULL, false, stripe_req);
			return;
		}

		stripe_req->parity_chunk->req_blocks = collection->chunk_blocks;

		if (r5info->sync_mode == SYNC_MODE_RESYNC) {
			raid5_stripe_xor(stripe_req->parity_chunk);
			write_parity = true;
		} else {
			struct mempool_elem *e_orig = stripe_req->parity_chunk->mempool_elem;

			stripe_req->parity_chunk->mempool_elem = mempool_elem_get(r5info->chunk_mempool);
			assert(stripe_req->parity_chunk->mempool_elem);

			stripe_req->parity_chunk->sgl = stripe_req->parity_chunk->mempool_elem->sgl;

			raid5_stripe_xor(stripe_req->parity_chunk);

			if (memcmp_sgl(stripe_req->parity_chunk->sgl, e_orig->sgl) != 0) {
				r5info->sync_mismatch_cnt++;
				if (r5info->sync_mode == SYNC_MODE_REPAIR) {
					write_parity = true;
				}
			}
			mempool_elem_put(e_orig);
		}

		if (write_parity) {
			if (raid5_member_submit_blocks(chunk_member_disk(stripe_req->parity_chunk), SPDK_BDEV_IO_TYPE_WRITE, stripe_req->ch, member_block, collection->chunk_blocks, stripe_req->parity_chunk->sgl, raid5_complete_stripe_sync, stripe_req)) {
				stripe_req->error = true;
				raid5_complete_stripe_sync(NULL, false, stripe_req);
			}
		} else {
			raid5_complete_stripe_sync(NULL, true, stripe_req);
		}
	}
}

static int raid5_stripe_sync(struct stripe_request *stripe_req)
{
	struct collection *collection = stripe_req->collection;
	struct raid5_info *r5info = collection->private;
	struct chunk *chunk;
	int ret = 0;

	for_each_chunk(stripe_req, chunk) {
		chunk->mempool_elem = mempool_elem_get(r5info->chunk_mempool);
		if (!chunk->mempool_elem) {
			return -ENOMEM;
		}
		chunk->sgl = chunk->mempool_elem->sgl;

		if (r5info->sync_mode != SYNC_MODE_RESYNC || chunk != stripe_req->parity_chunk) {
			uint64_t member_block = stripe_req->stripe * collection->chunk_blocks;

			stripe_req->remaining++;
			ret = raid5_member_submit_blocks(chunk_member_disk(chunk), SPDK_BDEV_IO_TYPE_READ, stripe_req->ch, member_block, collection->chunk_blocks, chunk->sgl, raid5_complete_stripe_sync_chunk_read, stripe_req);
			if (ret) {
				break;
			}
		}
	}

	return ret;
}

static int raid5_submit_stripe_request(struct stripe_request *stripe_req)
{
	int ret;

	if (stripe_req->resync) {
		ret = raid5_stripe_sync(stripe_req);
	} else {
		enum spdk_bdev_io_type type = stripe_req->orig_io->type;

		if (type == SPDK_BDEV_IO_TYPE_READ) {
			ret = raid5_stripe_read(stripe_req);
		} else if (type == SPDK_BDEV_IO_TYPE_WRITE) {
			ret = raid5_stripe_write(stripe_req);
		} else {
			ret = -EINVAL;
		}
	}

	if (ret) {
		SPDK_ERRLOG("err: %d\n", ret);
	}

	return ret;
}

static int raid5_handle_stripe(struct collection *collection, struct collections_io_channel *ch, struct spdk_bdev_io *bdev_io, uint64_t stripe, uint64_t stripe_offset, uint64_t blocks, struct sg_list *sgl, bool resync)
{
	struct raid5_info *r5info = collection->private;
	struct stripe_request *stripe_req;
	struct chunk *chunk;

	stripe_req = calloc(1, sizeof(*stripe_req) + sizeof(struct chunk) * collection->members_count);
	if (!stripe_req)
		return -ENOMEM;

	SPDK_DEBUGLOG(SPDK_LOG_COLLECTIONS_RAID5, "stripe_req: %p stripe: %lu stripe_offset: %lu blocks: %lu rw: %c\n", stripe_req, stripe, stripe_offset, blocks, bdev_io_type_char(bdev_io->type));

	stripe_req->collection = collection;
	stripe_req->resync = resync;

	for_each_chunk(stripe_req, chunk) {
		chunk->stripe_req = stripe_req;
	}

	stripe_req->ch = ch;
	stripe_req->stripe = stripe;
	stripe_req->parity_chunk = &stripe_req->chunks[collection->members_count - 1 - stripe % collection->members_count];
	stripe_req->orig_io = bdev_io;
	stripe_req->sgl = sgl;

	stripe_req->stripe_offset_from = stripe_offset;
	stripe_req->stripe_offset_to = stripe_offset + blocks;
	stripe_req->blocks = blocks;

	stripe_req->first_data_chunk = get_data_chunk(stripe_req, stripe_req->stripe_offset_from / collection->chunk_blocks);
	stripe_req->last_data_chunk = get_data_chunk(stripe_req, (stripe_req->stripe_offset_to - 1) / collection->chunk_blocks);

	stripe_req->thread = spdk_get_thread();

	for_each_req_data_chunk(stripe_req, chunk) {
		uint64_t chunk_from = data_chunk_idx(chunk) * collection->chunk_blocks;
		uint64_t chunk_to = chunk_from + collection->chunk_blocks;

		if (stripe_req->stripe_offset_from > chunk_from) {
			chunk->req_offset = stripe_req->stripe_offset_from - chunk_from;
		} else {
			chunk->req_offset = 0;
		}

		if (stripe_req->stripe_offset_to < chunk_to) {
			chunk->req_blocks = stripe_req->stripe_offset_to - chunk_from;
		} else {
			chunk->req_blocks = collection->chunk_blocks;
		}

		chunk->req_blocks -= chunk->req_offset;
	}

	pthread_spin_lock(&r5info->active_stripes_lock);
	if (r5info->active_stripes[stripe_req->stripe]) {
		struct stripe_request *tmp = r5info->active_stripes[stripe_req->stripe];

		while (tmp->next) {
			tmp = tmp->next;
		}
		tmp->next = stripe_req;

		pthread_spin_unlock(&r5info->active_stripes_lock);
		return 0;
	} else if (resync || bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE) {
		// TODO: merge
		r5info->active_stripes[stripe_req->stripe] = stripe_req;
	}
	pthread_spin_unlock(&r5info->active_stripes_lock);

	return raid5_submit_stripe_request(stripe_req);
}

static int raid5_submit_request(struct collection *collection, struct collections_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	uint64_t offset_blocks = bdev_io->u.bdev.offset_blocks;
	uint64_t num_blocks = bdev_io->u.bdev.num_blocks;
	struct raid5_info *r5info = collection->private;
	int ret = 0;
	uint64_t iov_off = 0;

	while (num_blocks) {
		uint64_t stripe = offset_blocks / r5info->stripe_blocks;
		uint64_t stripe_offset = offset_blocks % r5info->stripe_blocks;
		uint64_t blocks = spdk_min(r5info->stripe_blocks - stripe_offset, num_blocks);
		uint32_t len;
		struct sg_list *sgl;

		if (blocks < collection->chunk_blocks &&
		    stripe_offset % collection->chunk_blocks + blocks > collection->chunk_blocks) {
			// split in 2 smaller requests
			blocks = collection->chunk_blocks - (stripe_offset % collection->chunk_blocks);
		}

		len = blocks * collection->vbdev->blocklen;

		sgl = split_iov(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, iov_off, len);
		if (!sgl) {
			return -ENOMEM;
		}

		ret = raid5_handle_stripe(collection, ch, bdev_io, stripe, stripe_offset, blocks, sgl, false);
		if (ret) {
			break;
		}

		num_blocks -= blocks;
		offset_blocks += blocks;
		iov_off += len;
	}

	return ret;
}

static int raid5_start(struct collection *collection)
{
	struct collection_member *member;
	uint64_t member_blockcnt = UINT64_MAX;
	struct raid5_info *r5info;

	collection->vbdev->blocklen = 0;

	for_each_member(collection, member) {
		if (member->bdev->write_cache) {
			collection->vbdev->write_cache = true;
		}
		if (member->bdev->need_aligned_buffer) {
			collection->vbdev->need_aligned_buffer = true;
		}
		if (collection->vbdev->blocklen == 0) {
			collection->vbdev->blocklen = member->bdev->blocklen;
		} else if (collection->vbdev->blocklen != member->bdev->blocklen) {
			SPDK_ERRLOG("%s: block size mismatch\n", collection->vbdev->name);
			return -EINVAL;
		}
		member_blockcnt = spdk_min(member_blockcnt, member->bdev->blockcnt);
	}

	collection->member_size_blocks = (collection->member_size_mb * 1024 / collection->vbdev->blocklen) * 1024;
	if (collection->member_size_blocks) {
		member_blockcnt = spdk_min(member_blockcnt, collection->member_size_blocks);
	}

	r5info = calloc(1, sizeof(*r5info));
	if (!r5info) {
		return -ENOMEM;
	}

	collection->private = r5info;

	collection->chunk_blocks = collection->chunk_bytes / collection->vbdev->blocklen;
	r5info->stripe_blocks = collection->chunk_blocks * (collection->members_count - 1);

	collection->vbdev->blockcnt = ROUND_DOWN(member_blockcnt, collection->chunk_blocks) * (collection->members_count - 1);
	collection->vbdev->optimal_io_boundary = r5info->stripe_blocks;

	r5info->total_stripes = collection->vbdev->blockcnt / r5info->stripe_blocks;

	r5info->active_stripes = calloc(r5info->total_stripes, sizeof(*r5info->active_stripes));
	if (!r5info->active_stripes) {
		free(r5info); // TODO: free_r5info
		return -ENOMEM;
	}
	pthread_spin_init(&r5info->active_stripes_lock, PTHREAD_PROCESS_PRIVATE);

	r5info->chunk_mempool = mempool_alloc(4096, collection->chunk_bytes, 4096);
	if (!r5info->chunk_mempool) {
		free(r5info);
		return -ENOMEM;
	}

	r5info->null_sgl = alloc_sg_list(1);
	if (!r5info->null_sgl) {
		free(r5info);
		return -ENOMEM;
	}
	r5info->null_sgl->iov[0].iov_base = NULL;
	r5info->null_sgl->iov[0].iov_len = collection->chunk_bytes;

	return 0;
}

static void raid5_stop(struct collection *collection)
{
	struct raid5_info *r5info = collection->private;

	mempool_free(r5info->chunk_mempool);
	pthread_spin_destroy(&r5info->active_stripes_lock);
	free(r5info->active_stripes);
	free(r5info->null_sgl);
	free(r5info);
}

static int raid5_sync_start(struct collection *collection, const char *mode)
{
	struct raid5_info *r5info = collection->private;

	if (r5info->sync_mode != SYNC_MODE_OFF) {
		return -EBUSY;
	}

	if (strcmp(mode, "resync") == 0) {
		r5info->sync_mode = SYNC_MODE_RESYNC;
	} else if (strcmp(mode, "check") == 0) {
		r5info->sync_mode = SYNC_MODE_CHECK;
	} else if (strcmp(mode, "repair") == 0) {
		r5info->sync_mode = SYNC_MODE_REPAIR;
	} else {
		return -EINVAL;
	}

	r5info->sync_ch = spdk_get_io_channel(collection);
	if (!r5info->sync_ch) {
		return -EINVAL;
	}

	r5info->sync_progress = 0;
	r5info->sync_mismatch_cnt = 0;

	return raid5_handle_stripe(collection, spdk_io_channel_get_ctx(r5info->sync_ch), NULL, 0, 0, r5info->stripe_blocks, NULL, true);
}

static int raid5_sync_status(struct collection *collection, char **status)
{
	struct raid5_info *r5info = collection->private;
	char *buf;
	size_t buf_len = 100;
	const char *mode;

	if (r5info->sync_mode == SYNC_MODE_OFF) {
		mode = "off";
	} else if (r5info->sync_mode == SYNC_MODE_RESYNC) {
		mode = "resync";
	} else if (r5info->sync_mode == SYNC_MODE_CHECK) {
		mode = "check";
	} else if (r5info->sync_mode == SYNC_MODE_REPAIR) {
		mode = "repair";
	} else {
		return -EINVAL;
	}

	buf = malloc(buf_len);
	if (!buf) {
		return -ENOMEM;
	}

	snprintf(buf, buf_len, "mode: %s\nprogress: %lu / %lu (%lu%%)\nmismatches: %lu", mode, r5info->sync_progress, r5info->total_stripes, r5info->sync_progress * 100 / r5info->total_stripes, r5info->sync_mismatch_cnt);

	*status = buf;

	return 0;
}

static void raid5_sync_stop(struct collection *collection)
{
	struct raid5_info *r5info = collection->private;

	r5info->sync_mode = SYNC_MODE_OFF;
}

static int raid5_custom_op(struct collection *collection, const char *op_name, void *arg)
{
	int ret = 0;

	if (strcmp(op_name, "sync_start") == 0) {
		ret = raid5_sync_start(collection, arg);
	} else if (strcmp(op_name, "sync_status") == 0) {
		ret = raid5_sync_status(collection, arg);
	} else if (strcmp(op_name, "sync_stop") == 0) {
		raid5_sync_stop(collection);
	} else {
		ret = -EINVAL;
	}

	return ret;
}

static struct collection_fn_table raid5_fn_table = {
	.name = "raid5",
	.start = raid5_start,
	.stop = raid5_stop,
	.submit_request = raid5_submit_request,
	.custom_op = raid5_custom_op,
};

COLLECTION_MODULE_REGISTER(&raid5_fn_table)

SPDK_LOG_REGISTER_COMPONENT("collections_raid5", SPDK_LOG_COLLECTIONS_RAID5)
