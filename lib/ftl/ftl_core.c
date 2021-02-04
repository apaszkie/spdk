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

#include "spdk/likely.h"
#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/thread.h"
#include "spdk/bdev_module.h"
#include "spdk/string.h"
#include "spdk_internal/log.h"
#include "spdk/ftl.h"
#include "spdk/crc32.h"

#include "ftl_core.h"
#include "ftl_band.h"
#include "ftl_io.h"
#include "ftl_debug.h"
#include "ftl_reloc.h"

struct ftl_band_flush {
	struct spdk_ftl_dev		*dev;
	/* Number of bands left to be flushed */
	size_t				num_bands;
	/* User callback */
	spdk_ftl_fn			cb_fn;
	/* Callback's argument */
	void				*cb_arg;
	/* List link */
	LIST_ENTRY(ftl_band_flush)	list_entry;
};

struct ftl_flush {
	/* Owner device */
	struct spdk_ftl_dev		*dev;

	/* Number of batches to wait for */
	size_t				num_req;

	/* Callback */
	struct {
		spdk_ftl_fn		fn;
		void			*ctx;
	} cb;

	/* Batch bitmap */
	struct spdk_bit_array		*bmap;

	/* List link */
	LIST_ENTRY(ftl_flush)		list_entry;
};

size_t
spdk_ftl_io_size(void)
{
	return sizeof(struct ftl_io);
}

static void
ftl_io_cmpl_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct ftl_io *io = cb_arg;
	struct spdk_ftl_dev *dev = io->dev;

	if (spdk_unlikely(!success)) {
		io->status = -EIO;
	}

	ftl_trace_completion(dev, io, FTL_TRACE_COMPLETION_DISK);

	if (io->type == FTL_IO_WRITE && ftl_is_append_supported(dev)) {
		assert(io->parent);
		io->parent->addr.offset = spdk_bdev_io_get_append_location(bdev_io);
	}

	ftl_io_dec_req(io);
	if (ftl_io_done(io)) {
		ftl_io_complete(io);
	}

	spdk_bdev_free_io(bdev_io);
}

static int
ftl_read_next_physical_addr(struct ftl_io *io, struct ftl_addr *addr)
{
	struct spdk_ftl_dev *dev = io->dev;
	size_t num_blocks, max_blocks;

	assert(ftl_io_mode_physical(io));
	assert(io->iov_pos < io->iov_cnt);

	if (io->pos == 0) {
		*addr = io->addr;
	} else {
		*addr = ftl_band_next_xfer_addr(io->band, io->addr, io->pos);
	}

	assert(!ftl_addr_invalid(*addr));

	/* Metadata has to be read in the way it's written (jumping across */
	/* the zones in xfer_size increments) */
	if (io->flags & FTL_IO_MD) {
		max_blocks = dev->xfer_size - (addr->offset % dev->xfer_size);
		num_blocks = spdk_min(ftl_io_iovec_len_left(io), max_blocks);
		assert(addr->offset / dev->xfer_size ==
		       (addr->offset + num_blocks - 1) / dev->xfer_size);
	} else {
		num_blocks = ftl_io_iovec_len_left(io);
	}

	return num_blocks;
}

static int
ftl_submit_erase(struct ftl_io *io)
{
	struct spdk_ftl_dev *dev = io->dev;
	struct ftl_band *band = io->band;
	struct ftl_addr addr = io->addr;
	struct ftl_zone *zone;
	int rc = 0;
	size_t i;

	for (i = 0; i < io->num_blocks; ++i) {
		if (i != 0) {
			zone = ftl_band_next_zone(band, ftl_band_zone_from_addr(band, addr));
			assert(zone->info.state == SPDK_BDEV_ZONE_STATE_FULL);
			addr.offset = zone->info.zone_id;
		}

		assert(ftl_addr_get_zone_offset(dev, addr) == 0);

		ftl_trace_submission(dev, io, addr, 1);
		rc = spdk_bdev_zone_management(dev->base_bdev_desc, dev->base_ioch, addr.offset,
					       SPDK_BDEV_ZONE_RESET, ftl_io_cmpl_cb, io);
		if (spdk_unlikely(rc)) {
			ftl_io_fail(io, rc);
			SPDK_ERRLOG("Vector reset failed with status: %d\n", rc);
			break;
		}

		ftl_io_inc_req(io);
		ftl_io_advance(io, 1);
	}

	if (ftl_io_done(io)) {
		ftl_io_complete(io);
	}

	return rc;
}

struct spdk_io_channel *
ftl_get_io_channel(const struct spdk_ftl_dev *dev)
{
	if (ftl_check_core_thread(dev)) {
		return dev->ioch;
	}

	return NULL;
}

static void
ftl_erase_fail(struct ftl_io *io, int status)
{
	struct ftl_zone *zone;
	struct ftl_band *band = io->band;
	char buf[128];

	SPDK_ERRLOG("Erase failed at address: %s, status: %d\n",
		    ftl_addr2str(io->addr, buf, sizeof(buf)), status);

	zone = ftl_band_zone_from_addr(band, io->addr);
	zone->info.state = SPDK_BDEV_ZONE_STATE_OFFLINE;
	ftl_band_remove_zone(band, zone);
	band->tail_md_addr = ftl_band_tail_md_addr(band);
}

static void
ftl_zone_erase_cb(struct ftl_io *io, void *ctx, int status)
{
	struct ftl_zone *zone;

	zone = ftl_band_zone_from_addr(io->band, io->addr);
	zone->busy = false;

	if (spdk_unlikely(status)) {
		ftl_erase_fail(io, status);
		return;
	}

	zone->info.state = SPDK_BDEV_ZONE_STATE_EMPTY;
	zone->info.write_pointer = zone->info.zone_id;
}

static int
ftl_band_erase(struct ftl_band *band)
{
	struct ftl_zone *zone;
	struct ftl_io *io;
	int rc = 0;

	assert(band->state == FTL_BAND_STATE_CLOSED ||
	       band->state == FTL_BAND_STATE_FREE);

	ftl_band_set_state(band, FTL_BAND_STATE_PREP);

	CIRCLEQ_FOREACH(zone, &band->zones, circleq) {
		if (zone->info.state == SPDK_BDEV_ZONE_STATE_EMPTY) {
			continue;
		}

		io = ftl_io_erase_init(band, 1, ftl_zone_erase_cb);
		if (!io) {
			rc = -ENOMEM;
			break;
		}

		zone->busy = true;
		io->addr.offset = zone->info.zone_id;
		rc = ftl_submit_erase(io);
		if (rc) {
			zone->busy = false;
			assert(0);
			/* TODO: change band's state back to close? */
			break;
		}
	}

	return rc;
}

int
ftl_band_set_direct_access(struct ftl_band *band, bool access)
{
	/* TODO */
	return -EINVAL;
}

static const struct spdk_ftl_limit *
ftl_get_limit(const struct spdk_ftl_dev *dev, int type)
{
	assert(type < SPDK_FTL_LIMIT_MAX);
	return &dev->conf.limits[type];
}

static int
ftl_shutdown_complete(struct spdk_ftl_dev *dev)
{
	if (__atomic_load_n(&dev->num_inflight, __ATOMIC_SEQ_CST)) {
		return 0;
	}

	if (__atomic_load_n(&dev->num_io_channels, __ATOMIC_SEQ_CST) != 1) {
		return 0;
	}

	if (!ftl_nv_cache_is_halted(&dev->nv_cache)) {
		ftl_nv_cache_halt(&dev->nv_cache);
		return 0;
	}

	if (!ftl_writer_is_halted(&dev->writer_user)) {
		ftl_writer_halt(&dev->writer_user);
		return 0;
	}

	if (!ftl_reloc_is_halted(dev->reloc)) {
		ftl_reloc_halt(dev->reloc);
		return 0;
	}

	if (!ftl_writer_is_halted(&dev->writer_gc)) {
		ftl_writer_halt(&dev->writer_gc);
		return 0;
	}

	return 1;
}

int
ftl_invalidate_addr(struct spdk_ftl_dev *dev, struct ftl_addr addr)
{
	struct ftl_band *band = ftl_band_from_addr(dev, addr);
	struct ftl_lba_map *lba_map = &band->lba_map;
	uint64_t offset;

	offset = ftl_band_block_offset_from_addr(band, addr);

	/* The bit might be already cleared if two writes are scheduled to the */
	/* same LBA at the same time */
	if (spdk_bit_array_get(lba_map->vld, offset)) {
		assert(lba_map->num_vld > 0);
		spdk_bit_array_clear(lba_map->vld, offset);
		lba_map->num_vld--;
		return 1;
	}

	return 0;
}

static int
ftl_read_retry(int rc)
{
	return rc == -EAGAIN;
}

static int
ftl_read_canceled(int rc)
{
	return rc == -EFAULT || rc == 0;
}

static int
ftl_read_next_logical_addr(struct ftl_io *io, struct ftl_addr *addr,
			   spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct spdk_ftl_dev *dev = io->dev;
	struct ftl_addr next_addr;
	size_t i;

	*addr = ftl_l2p_get(dev, ftl_io_current_lba(io));

	SPDK_DEBUGLOG(SPDK_LOG_FTL_CORE, "Read addr:%lx, lba:%lu\n",
		      addr->offset, ftl_io_current_lba(io));

	/* If the address is invalid, skip it (the buffer should already be zero'ed) */
	if (ftl_addr_invalid(*addr)) {
		return -EFAULT;
	}

	if (ftl_addr_cached(*addr)) {
		if (!ftl_nv_cache_read(io, *addr, 1, cb, cb_arg)) {
			return 0;
		}

		/* If the state changed, we have to re-read the l2p */
		return -EAGAIN;
	}

	for (i = 1; i < ftl_io_iovec_len_left(io); ++i) {
		next_addr = ftl_l2p_get(dev, ftl_io_get_lba(io, io->pos + i));

		if (ftl_addr_invalid(next_addr) || ftl_addr_cached(next_addr)) {
			break;
		}

		if (addr->offset + i != next_addr.offset) {
			break;
		}
	}

	return i;
}

static int
ftl_submit_read(struct ftl_io *io)
{
	struct spdk_ftl_dev *dev = io->dev;
	struct ftl_addr addr;
	int rc = 0, num_blocks;

	assert(LIST_EMPTY(&io->children));

	while (io->pos < io->num_blocks) {
		if (ftl_io_mode_physical(io)) {
			num_blocks = rc = ftl_read_next_physical_addr(io, &addr);
		} else {
			num_blocks = rc = ftl_read_next_logical_addr(io, &addr,
					  ftl_io_cmpl_cb, io);
		}

		/* We might need to retry the read from scratch (e.g. */
		/* because write was under way and completed before */
		/* we could read it from the write buffer */
		if (ftl_read_retry(rc)) {
			continue;
		}

		/* We don't have to schedule the read, as it was read from cache */
		if (ftl_read_canceled(rc)) {
			ftl_io_advance(io, 1);
			ftl_trace_completion(io->dev, io, rc ? FTL_TRACE_COMPLETION_INVALID :
					     FTL_TRACE_COMPLETION_CACHE_SUBMITTION);
			rc = 0;
			continue;
		}

		assert(num_blocks > 0);

		ftl_trace_submission(dev, io, addr, num_blocks);
		rc = spdk_bdev_read_blocks(dev->base_bdev_desc, dev->base_ioch,
					   ftl_io_iovec_addr(io),
					   addr.offset,
					   num_blocks, ftl_io_cmpl_cb, io);
		if (spdk_unlikely(rc)) {
			if (rc == -ENOMEM) {
				TAILQ_INSERT_TAIL(&dev->rd_retry_sq, io, queue_entry);
				rc = 0;
			} else {
				ftl_io_fail(io, rc);
			}
			break;
		}

		ftl_io_inc_req(io);
		ftl_io_advance(io, num_blocks);
	}

	/* If we didn't have to read anything from the device, */
	/* complete the request right away */
	if (ftl_io_done(io)) {
		ftl_io_complete(io);
	}

	return rc;
}

static inline void
ftl_update_nv_cache_l2p_update(struct ftl_io *io)
{
	struct spdk_ftl_dev *dev = io->dev;
	uint64_t lba = io->lba.single;
	struct ftl_addr next_addr = io->addr;
	struct ftl_addr weak_addr = ftl_to_addr(FTL_ADDR_INVALID);
	uint64_t end_block = lba + io->num_blocks;

	do {
		ftl_update_l2p(dev, lba, next_addr, weak_addr);
		next_addr.cache_offset++;
	} while (++lba < end_block);
}

static void
ftl_nv_cache_submit_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct ftl_io *io = cb_arg;
	struct ftl_nv_cache *nv_cache = &io->dev->nv_cache;

	if (spdk_unlikely(!success)) {
		SPDK_ERRLOG("Non-volatile cache write failed at %"PRIx64"\n",
			    io->addr.offset);
		io->status = -EIO;

		/* TODO(mariusz&wojtek) Consider how to handle error */
	} else {
		ftl_update_nv_cache_l2p_update(io);
	}

	ftl_nv_cache_commit_wr_buffer(nv_cache, io);
	ftl_io_dec_req(io);
	if (ftl_io_done(io)) {

		//trace cache write completion
		ftl_trace_completion(io->dev, io, FTL_TRACE_COMPLETION_CACHE_SUBMITTION);


		spdk_mempool_put(nv_cache->md_pool, io->md);
		ftl_io_complete(io);
	}

	spdk_bdev_free_io(bdev_io);
}

static void
ftl_submit_nv_cache(void *ctx)
{
	struct ftl_io *io = ctx;
	struct spdk_ftl_dev *dev = io->dev;
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;
	int rc;

	rc = ftl_nv_cache_write(io, io->addr, io->num_blocks, io->md,
				ftl_nv_cache_submit_cb, io);
	if (rc == -ENOMEM) {
		spdk_thread_send_msg(ftl_get_core_thread(dev),
				     ftl_submit_nv_cache, io);
		return;
	} else if (rc) {
		SPDK_ERRLOG("Write to persistent cache failed: %s (%"PRIu64", %"PRIu64")\n",
			    spdk_strerror(-rc), io->addr.offset, io->num_blocks);
		spdk_mempool_put(nv_cache->md_pool, io->md);
		io->status = -EIO;
		ftl_nv_cache_commit_wr_buffer(nv_cache, io);
		ftl_io_complete(io);
		return;
	}

	ftl_io_advance(io, io->num_blocks);
}

static void
_ftl_write_nv_cache(void *ctx)
{
	struct ftl_io *io = ctx;
	struct spdk_ftl_dev *dev = io->dev;
	struct spdk_thread *thread;

	io->md = spdk_mempool_get(dev->nv_cache.md_pool);
	if (spdk_unlikely(!io->md)) {
		thread = dev->core_thread;
		spdk_thread_send_msg(thread, _ftl_write_nv_cache, io);
		return;
	}

	/* Reserve area on the write buffer cache */
	io->addr.cache_offset = ftl_nv_cache_get_wr_buffer(
					&dev->nv_cache, io);
	io->addr.cached = true;
	if (io->addr.offset == FTL_LBA_INVALID) {
		thread = dev->core_thread;
		spdk_mempool_put(dev->nv_cache.md_pool, io->md);
		spdk_thread_send_msg(thread, _ftl_write_nv_cache, io);
		return;
	}

	ftl_nv_cache_fill_md(io);
	ftl_submit_nv_cache(io);

	assert(!ftl_io_iovec_len_left(io));
}

static int
ftl_write_nv_cache(struct ftl_io *io)
{
	io->flags |= FTL_IO_CACHE;
	_ftl_write_nv_cache(io);

	return 0;
}

int
ftl_nv_cache_scrub(struct ftl_nv_cache *nv_cache, spdk_bdev_io_completion_cb cb_fn, void *cb_arg)
{
	struct spdk_ftl_dev *dev = SPDK_CONTAINEROF(nv_cache, struct spdk_ftl_dev, nv_cache);
	struct spdk_bdev *bdev;

	bdev = spdk_bdev_desc_get_bdev(nv_cache->bdev_desc);

	return spdk_bdev_write_zeroes_blocks(nv_cache->bdev_desc, dev->nv_cache.cache_ioch, 1,
					     spdk_bdev_get_num_blocks(bdev) - 1,
					     cb_fn, cb_arg);
}

void
ftl_update_l2p(struct spdk_ftl_dev *dev, uint64_t lba,
	       struct ftl_addr new_addr, struct ftl_addr weak_addr)
{
	struct ftl_addr prev_addr;

	assert(ftl_check_core_thread(dev));

	prev_addr = ftl_l2p_get(dev, lba);
	if (ftl_addr_invalid(prev_addr)) {
		/* First time write */
		ftl_l2p_set(dev, lba, new_addr);
		return;
	}

	if (ftl_addr_cached(new_addr)) {
		/*
		 * Previous block on NV Cache, New one to NV Cache
		 * Latest write goes to NV cache, so set L2P, background operation
		 * need to handle this situation
		 */
		ftl_l2p_set(dev, lba, new_addr);

		if (ftl_addr_not_cached(prev_addr)) {
			/* Invalidate LBA map */
			ftl_invalidate_addr(dev, prev_addr);
		}

		if (ftl_addr_not_cached(weak_addr)) {
			/* Invalidate LBA map */
			ftl_invalidate_addr(dev, weak_addr);
		}
		return;
	}

	if (ftl_addr_cmp(prev_addr, weak_addr)) {
		/* Validate address */
		ftl_l2p_set(dev, lba, new_addr);

		ftl_band_set_addr(ftl_band_from_addr(dev, new_addr), lba,
				new_addr);
		/* L2P not changed in the meantime we can update location */
		if (ftl_addr_not_cached(prev_addr)) {
			ftl_invalidate_addr(dev, prev_addr);
		}
	} else {
		if (ftl_addr_not_cached(weak_addr)) {
			ftl_invalidate_addr(dev, weak_addr);
		}
	}
}

void
ftl_apply_limits(struct spdk_ftl_dev *dev)
{
	const struct spdk_ftl_limit *limit;
	struct ftl_stats *stats = &dev->stats;
	int i;

	/*  Clear existing limit */
	dev->limit = SPDK_FTL_LIMIT_MAX;

	for (i = SPDK_FTL_LIMIT_CRIT; i < SPDK_FTL_LIMIT_MAX; ++i) {
		limit = ftl_get_limit(dev, i);

		if (dev->num_free <= limit->thld) {
			stats->limits[i]++;
			dev->limit = i;
			break;
		}
	}

	ftl_trace_limits(dev, dev->limit, dev->num_free);
}

bool
ftl_needs_defrag(struct spdk_ftl_dev *dev)
{
	const struct spdk_ftl_limit *limit = ftl_get_limit(dev,
					     SPDK_FTL_LIMIT_START);

	if (dev->halt) {
		return false;
	}

	if (dev->num_free <= limit->thld) {
		return true;
	}

	return false;
}

int
ftl_current_limit(const struct spdk_ftl_dev *dev)
{
	return dev->limit;
}

void
spdk_ftl_dev_get_attrs(const struct spdk_ftl_dev *dev, struct spdk_ftl_attrs *attrs)
{
	attrs->uuid = dev->uuid;
	attrs->num_blocks = dev->num_lbas;
	attrs->block_size = FTL_BLOCK_SIZE;
	attrs->num_zones = ftl_get_num_zones(dev);
	attrs->zone_size = ftl_get_num_blocks_in_zone(dev);
	attrs->conf = dev->conf;
	attrs->base_bdev = spdk_bdev_get_name(spdk_bdev_desc_get_bdev(dev->base_bdev_desc));

	attrs->cache_bdev = NULL;
	if (dev->nv_cache.bdev_desc) {
		attrs->cache_bdev = spdk_bdev_get_name(
					    spdk_bdev_desc_get_bdev(dev->nv_cache.bdev_desc));
	}
}

static void
_handle_io(void *ctx)
{
	struct ftl_io *io = ctx;
	struct spdk_ftl_dev *dev = io->dev;

	switch(io->type) {
	case FTL_IO_READ:
		ftl_io_read(io);
		break;

	case FTL_IO_WRITE:
		if (!ftl_nv_cache_full(dev)) {
			ftl_io_write(io);
		} else {
			/* No space in NV cache */
			TAILQ_INSERT_TAIL(&dev->wr_retry_sq, io, queue_entry);
		}

		break;
	default:
		io->status = false;
		ftl_io_complete(io);
	}
}

void ftl_io_write(struct ftl_io *io)
{
	assert(ftl_check_core_thread(io->dev));

	if (!(io->flags & FTL_IO_MD) && !(io->flags & FTL_IO_WEAK)) {
		ftl_io_call_foreach_child(io, ftl_write_nv_cache);
	} else {
		/* TODO(mbarczak) Legacy write IO path, use requests */
		ftl_io_fail(io, -ENOTSUP);
	}
}

static int _queue_io(struct spdk_ftl_dev *dev, struct ftl_io *io)
{
	size_t result;

	result = spdk_ring_enqueue(dev->queue, (void **)&io, 1, NULL);
	if (spdk_unlikely(0 == result)) {
		/* Ring is full, handle IO using thread message */

		int rc = spdk_thread_send_msg(dev->core_thread, _handle_io, io);
		if (spdk_likely(!rc)) {
			/*
			 * TODO(mbarczak) Mark IO as thread message passed,
			 * the IO completion needs to be passed in the same path
			 */

			result = 1;
		} else {
			/* TODO(mbarczak) Maybe add IO to retry queue */
		}

	}

	if (spdk_likely(result)) {
		struct ftl_io_channel *ioch = ftl_io_channel_get_ctx(io->ioch);
		TAILQ_INSERT_TAIL(&ioch->cq, io, user_queue_entry);
		return 0;
	} else {
		return -EAGAIN;
	}
}

int
spdk_ftl_write(struct spdk_ftl_dev *dev, struct ftl_io *io, struct spdk_io_channel *ch,
	       uint64_t lba, size_t lba_cnt, struct iovec *iov, size_t iov_cnt, spdk_ftl_fn cb_fn,
	       void *cb_arg)
{
	int rc;

	if (iov_cnt == 0) {
		return -EINVAL;
	}

	if (lba_cnt == 0) {
		return -EINVAL;
	}

	if (lba_cnt != ftl_iovec_num_blocks(iov, iov_cnt)) {
		return -EINVAL;
	}

	if (!dev->initialized) {
		return -EBUSY;
	}

	rc = ftl_io_user_init(ch, io, lba, lba_cnt, iov, iov_cnt, cb_fn, cb_arg, FTL_IO_WRITE);
	if (rc) {
		return rc;
	}

	return _queue_io(dev, io);
}

void
ftl_io_read(struct ftl_io *io)
{
	assert(ftl_check_core_thread(io->dev));
	ftl_io_call_foreach_child(io, ftl_submit_read);
}

int
spdk_ftl_read(struct spdk_ftl_dev *dev, struct ftl_io *io, struct spdk_io_channel *ch, uint64_t lba,
	      size_t lba_cnt, struct iovec *iov, size_t iov_cnt, spdk_ftl_fn cb_fn, void *cb_arg)
{
	int rc;

	if (iov_cnt == 0) {
		return -EINVAL;
	}

	if (lba_cnt == 0) {
		return -EINVAL;
	}

	if (lba_cnt != ftl_iovec_num_blocks(iov, iov_cnt)) {
		return -EINVAL;
	}

	if (!dev->initialized) {
		return -EBUSY;
	}

	rc = ftl_io_user_init(ch, io, lba, lba_cnt, iov, iov_cnt, cb_fn, cb_arg, FTL_IO_READ);
	if (rc) {
		return rc;
	}

	return _queue_io(dev, io);;
}

bool
ftl_addr_is_written(struct ftl_band *band, struct ftl_addr addr)
{
	struct ftl_zone *zone = ftl_band_zone_from_addr(band, addr);

	return addr.offset < zone->info.write_pointer;
}

static void ftl_process_media_event(struct spdk_ftl_dev *dev, struct spdk_bdev_media_event event);

static void
_ftl_process_media_event(void *ctx)
{
	struct ftl_media_event *event = ctx;
	struct spdk_ftl_dev *dev = event->dev;

	ftl_process_media_event(dev, event->event);
	spdk_mempool_put(dev->media_events_pool, event);
}

static void
ftl_process_media_event(struct spdk_ftl_dev *dev, struct spdk_bdev_media_event event)
{
	struct ftl_band *band;
	struct ftl_addr addr = { .offset = event.offset };
	size_t block_off;

	if (!ftl_check_core_thread(dev)) {
		struct ftl_media_event *media_event;

		media_event = spdk_mempool_get(dev->media_events_pool);
		if (!media_event) {
			SPDK_ERRLOG("Media event lost due to lack of memory");
			return;
		}

		media_event->dev = dev;
		media_event->event = event;
		spdk_thread_send_msg(ftl_get_core_thread(dev), _ftl_process_media_event,
				     media_event);
		return;
	}

	band = ftl_band_from_addr(dev, addr);
	block_off = ftl_band_block_offset_from_addr(band, addr);

	ftl_reloc_add(dev->reloc, band, block_off, event.num_blocks, 0, false);
}

void
ftl_get_media_events(struct spdk_ftl_dev *dev)
{
#define FTL_MAX_MEDIA_EVENTS 128
	struct spdk_bdev_media_event events[FTL_MAX_MEDIA_EVENTS];
	size_t num_events, i;

	if (!dev->initialized) {
		return;
	}

	do {
		num_events = spdk_bdev_get_media_events(dev->base_bdev_desc,
							events, FTL_MAX_MEDIA_EVENTS);

		for (i = 0; i < num_events; ++i) {
			ftl_process_media_event(dev, events[i]);
		}

	} while (num_events);
}

int
ftl_io_channel_poll(void *arg)
{
	struct ftl_io_channel *ch = arg;
	struct ftl_io *io, *next;

	if (TAILQ_EMPTY(&ch->cq)) {
		return SPDK_POLLER_IDLE;
	}

	TAILQ_FOREACH_SAFE(io, &ch->cq, user_queue_entry, next) {
		if (io->user_done) {
			TAILQ_REMOVE(&ch->cq, io, user_queue_entry);
			io->user_fn(io->cb_ctx, io->status);
		}
	}

	return SPDK_POLLER_BUSY;
}

static void
ftl_process_io_queue(struct spdk_ftl_dev *dev)
{
	#define FTL_IO_QUEUE_BATCH 16
	void *ios[FTL_IO_QUEUE_BATCH];
	size_t count, i;
	TAILQ_HEAD(, ftl_io) retry_queue;
	struct ftl_io *io;

	if (!TAILQ_EMPTY(&dev->rd_retry_sq)) {
		/* Retry reads */
		TAILQ_INIT(&retry_queue);
		TAILQ_SWAP(&dev->rd_retry_sq, &retry_queue, ftl_io, queue_entry);
		while (!TAILQ_EMPTY(&retry_queue)) {
			io = TAILQ_FIRST(&retry_queue);
			TAILQ_REMOVE(&retry_queue, io, queue_entry);

			assert(io->type == FTL_IO_READ);
			ftl_io_read(io);
		}
	}

        if (!ftl_nv_cache_full(dev) && !TAILQ_EMPTY(&dev->wr_retry_sq)) {
		/* Retry writes */
        	TAILQ_INIT(&retry_queue);
        	TAILQ_SWAP(&dev->wr_retry_sq, &retry_queue, ftl_io, queue_entry);
		while (!TAILQ_EMPTY(&retry_queue) && !ftl_nv_cache_full(dev)) {
			io = TAILQ_FIRST(&retry_queue);
			TAILQ_REMOVE(&retry_queue, io, queue_entry);

			assert(io->type == FTL_IO_WRITE);
			ftl_io_write(io);
		}
		if (!TAILQ_EMPTY(&retry_queue)) {
			/* No more space in NV cache, restore retry queue */
			TAILQ_CONCAT(&dev->wr_retry_sq, &retry_queue, queue_entry);
			assert(TAILQ_EMPTY(&retry_queue));
		}
        }

        count = spdk_ring_dequeue(dev->queue, ios, FTL_IO_QUEUE_BATCH);
        if (count == 0) {
        	return;
        }

	for (i = 0; i < count; i++) {
		struct ftl_io *io = ios[i];
		_handle_io(io);
	}
}

int
ftl_task_core(void *ctx)
{
	struct spdk_ftl_dev *dev = ctx;

	if (dev->halt) {
		if (ftl_shutdown_complete(dev)) {
			spdk_poller_unregister(&dev->core_poller);
			return SPDK_POLLER_IDLE;
		}
	}

	ftl_process_io_queue(dev);
	ftl_writer_run(&dev->writer_user);
	ftl_writer_run(&dev->writer_gc);
	ftl_writer_run(&dev->writer_gc);
	ftl_reloc(dev->reloc);
	ftl_nv_cache_compact(dev);

	return SPDK_POLLER_BUSY;
}

struct ftl_band *ftl_band_get_next_free(struct spdk_ftl_dev *dev)
{
	struct ftl_band *band = NULL;

	if (!LIST_EMPTY(&dev->free_bands)) {
		band = LIST_FIRST(&dev->free_bands);
		LIST_REMOVE(band, list_entry);

		if (ftl_band_erase(band)) {
			/* TODO */
			abort();
			LIST_INSERT_HEAD(&dev->free_bands, band, list_entry);
			return NULL;
		}
	}

	return band;
}

SPDK_LOG_REGISTER_COMPONENT("ftl_core", SPDK_LOG_FTL_CORE)
