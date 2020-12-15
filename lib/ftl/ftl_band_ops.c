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

#include <spdk/stdinc.h>
#include <spdk/queue.h>

#include "ftl_band_ops.h"
#include "ftl_rq.h"
#include "ftl_core.h"
#include "ftl_band.h"

static void
_band_iter_advance(struct ftl_band *band, uint64_t num_blocks)
{
	band->iter.offset += num_blocks;

	if (ftl_band_full(band, band->iter.offset)) {
		ftl_band_set_state(band, FTL_BAND_STATE_FULL);
		band->owner.state_change_fn(band);
	}

	band->iter.queue_depth++;
	band->iter.zone->busy = true;
	band->iter.addr = ftl_band_next_xfer_addr(band, band->iter.addr, num_blocks);
	band->iter.zone = ftl_band_next_operational_zone(band, band->iter.zone);

	assert(!ftl_addr_invalid(band->iter.addr));
}

static void _write_rq_end(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct ftl_rq *rq = arg;
	struct ftl_zone *zone = rq->io.zone;
	struct ftl_band *band = rq->io.band;

	rq->success = success;
	if (spdk_likely(success)) {
		if (ftl_is_append_supported(rq->dev)) {
			rq->io.addr.offset = spdk_bdev_io_get_append_location(bdev_io);
		}

		zone->info.write_pointer += rq->num_blocks;
		if (zone->info.write_pointer == zone->info.zone_id + zone->info.capacity) {
			zone->info.state = SPDK_BDEV_ZONE_STATE_FULL;
		}
	}

	zone->busy = false;
	band->iter.queue_depth--;

	rq->owner.cb(rq);
	spdk_bdev_free_io(bdev_io);
}

int ftl_band_rq_write(struct ftl_band *band, struct ftl_rq *rq) {
	struct spdk_ftl_dev *dev = band->dev;
	int rc;

	rq->success = false;
	rq->io.band = band;
	rq->io.zone = band->iter.zone;

	if (ftl_is_append_supported(dev)) {
		rc = spdk_bdev_zone_appendv(dev->base_bdev_desc, dev->base_ioch,
				rq->io_vec, rq->io_vec_size,
				ftl_addr_get_zone_slba(dev, band->iter.addr),
				rq->num_blocks, _write_rq_end, rq);
	} else {
		rq->io.addr.offset = band->iter.addr.offset;
		rc = spdk_bdev_writev_blocks(dev->base_bdev_desc, dev->base_ioch,
				rq->io_vec, rq->io_vec_size,
				band->iter.addr.offset, rq->num_blocks,
				_write_rq_end, rq);
	}

	if (spdk_likely(!rc)) {
		_band_iter_advance(band, rq->num_blocks);
	}

	return rc;
}

static void _write_brq_end(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct ftl_basic_rq *brq = arg;
	struct ftl_zone *zone = brq->io.zone;
	struct ftl_band *band = brq->io.band;

	brq->success = success;
	if (spdk_likely(success)) {
		if (ftl_is_append_supported(brq->dev)) {
			brq->io.addr.offset = spdk_bdev_io_get_append_location(bdev_io);
		}

		zone->info.write_pointer += brq->num_blocks;
		if (zone->info.write_pointer == zone->info.zone_id + zone->info.capacity) {
			zone->info.state = SPDK_BDEV_ZONE_STATE_FULL;
		}
	}

	zone->busy = false;
	band->iter.queue_depth--;

	brq->owner.cb(brq);
	spdk_bdev_free_io(bdev_io);
}

int ftl_band_basic_rq_write(struct ftl_band *band, struct ftl_basic_rq *brq) {
	struct spdk_ftl_dev *dev = band->dev;
	int rc;

	brq->io.band = band;
	brq->io.zone = band->iter.zone;

	if (ftl_is_append_supported(dev)) {
		rc = spdk_bdev_zone_append(dev->base_bdev_desc, dev->base_ioch,
				brq->io_payload,
				ftl_addr_get_zone_slba(dev, band->iter.addr),
				brq->num_blocks, _write_brq_end, brq);
	} else {
		brq->io.addr.offset = band->iter.addr.offset;
		rc = spdk_bdev_write_blocks(dev->base_bdev_desc, dev->base_ioch,
				brq->io_payload, band->iter.addr.offset,
				brq->num_blocks, _write_brq_end, brq);
	}

	if (spdk_likely(!rc)) {
		_band_iter_advance(band, brq->num_blocks);
	}

	return rc;
}

static void _band_open_cb(struct ftl_basic_rq *brq)
{
	// XXX Provides additional check (e.g if write offset is zero)

	if (spdk_likely(brq->success)) {
		ftl_band_set_state(brq->io.band, FTL_BAND_STATE_OPEN);
	} else {
		ftl_band_set_state(brq->io.band, FTL_BAND_STATE_PREP);
	}
}

void ftl_band_open(struct ftl_band *band)
{
	int rc;
	struct spdk_ftl_dev *dev = band->dev;
	void *metadata = band->lba_map.dma_buf;
	uint64_t num_blocks = ftl_head_md_num_blocks(dev);

	ftl_band_set_state(band, FTL_BAND_STATE_OPENING);
	ftl_band_md_pack_head(band);
	ftl_basic_rq_init(dev, &band->metadata_rq, metadata, num_blocks);
	ftl_basic_rq_set_owner(&band->metadata_rq, _band_open_cb, band);

	if (spdk_likely(0 == band->lba_map.num_vld)) {
		rc = ftl_band_basic_rq_write(band, &band->metadata_rq);
		if (rc) {
			/* ERROR, retry it later */
			ftl_band_set_state(band, FTL_BAND_STATE_PREP);
		}
	} else {
		assert(0 == band->lba_map.num_vld);
		abort();
	}
}

static void _band_close_cb(struct ftl_basic_rq *brq)
{
	struct ftl_band *band = brq->io.band;

	// XXX Provides additional check (e.g if write offset of tail metadata
	// is correct)

	if (spdk_likely(brq->success)) {
		ftl_band_set_state(band, FTL_BAND_STATE_CLOSED);
	} else {
		ftl_band_write_failed(band);
	}
}

void ftl_band_close(struct ftl_band *band)
{
	int rc;
	struct spdk_ftl_dev *dev = band->dev;
	void *metadata = band->lba_map.dma_buf;
	uint64_t num_blocks = ftl_tail_md_num_blocks(dev);

	ftl_band_set_state(band, FTL_BAND_STATE_CLOSING);
	ftl_band_md_pack_tail(band);
	ftl_basic_rq_init(dev, &band->metadata_rq, metadata, num_blocks);
	ftl_basic_rq_set_owner(&band->metadata_rq, _band_close_cb, band);

	rc = ftl_band_basic_rq_write(band, &band->metadata_rq);
	if (spdk_unlikely(rc)) {
		/* TODO(mbarczak) ERROR */
		assert(0);
		abort();
	}
}
