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
#include "spdk/ftl.h"
#include "spdk/util.h"
#include "spdk/likely.h"
#include "spdk/string.h"
#include "spdk/crc32.h"

#include "ftl_core.h"
#include "ftl_band.h"
#include "ftl_io.h"

struct ftl_restore_band {
	struct ftl_restore		*parent;
	/* Associated band */
	struct ftl_band			*band;
	/* Status of retrieving this band's metadata */
	enum ftl_md_status		md_status;
	/* Padded queue link  */
	STAILQ_ENTRY(ftl_restore_band)	stailq;
};

struct ftl_nv_cache_restore;

/* Describes single phase to be restored from non-volatile cache */
struct ftl_nv_cache_range {
	struct ftl_nv_cache_restore	*parent;
	/* Start offset */
	uint64_t			start_addr;
	/* Last block's address */
	uint64_t			last_addr;
	/*
	 * Number of blocks (can be smaller than the difference between the last
	 * and the starting block due to range overlap)
	 */
	uint64_t			num_blocks;
	/* Number of blocks already recovered */
	uint64_t			num_recovered;
	/* Current address during recovery */
	uint64_t			current_addr;
	/* Phase of the range */
	unsigned int			phase;
	/* Indicates whether the data from this range needs to be recovered */
	bool				recovery;
};

struct ftl_nv_cache_block {
	struct ftl_nv_cache_restore	*parent;
	/* Data buffer */
	void				*buf;
	/* Metadata buffer */
	void				*md_buf;
	/* Block offset within the cache */
	uint64_t			offset;
};

struct ftl_nv_cache_restore {
	struct ftl_nv_cache		*nv_cache;
	/* IO channel to use */
	struct spdk_io_channel		*ioch;
	/*
	 * Non-volatile cache ranges. The ranges can overlap, as we have no
	 * control over the order of completions. The phase of the range is the
	 * index within the table. The range with index 0 marks blocks that were
	 * never written.
	 */
	struct ftl_nv_cache_range	range[FTL_NV_CACHE_PHASE_COUNT];
#define FTL_NV_CACHE_RESTORE_DEPTH 128
	/* Non-volatile cache buffers */
	struct ftl_nv_cache_block	block[FTL_NV_CACHE_RESTORE_DEPTH];
	/* Current address */
	uint64_t			current_addr;
	/* Number of outstanding requests */
	size_t				num_outstanding;
	/* Recovery/scan status */
	int				status;
	/* Current phase of the recovery */
	unsigned int			phase;
};

struct ftl_restore {
	struct spdk_ftl_dev		*dev;
	/* Completion callback (called for each phase of the restoration) */
	ftl_restore_fn			cb;
	/* Completion callback context */
	void				*cb_arg;
	/* Number of inflight IOs */
	unsigned int			num_ios;
	/* Current band number (index in the below bands array) */
	unsigned int			current;
	/* Array of bands */
	struct ftl_restore_band		*bands;
	/* Queue of bands to be padded (due to unsafe shutdown) */
	STAILQ_HEAD(, ftl_restore_band) pad_bands;
	/* Status of the padding */
	int				pad_status;
	/* Metadata buffer */
	void				*md_buf;
	/* LBA map buffer */
	void				*lba_map;
	/* Indicates we're in the final phase of the restoration */
	bool				final_phase;
	/* Non-volatile cache recovery */
	struct ftl_nv_cache_restore	nv_cache;
};

static int
ftl_restore_tail_md(struct ftl_restore_band *rband);
static void
ftl_pad_zone_cb(struct ftl_io *io, void *arg, int status);
static void
ftl_restore_pad_band(struct ftl_restore_band *rband);

static void
ftl_restore_free(struct ftl_restore *restore)
{
	unsigned int i;

	if (!restore) {
		return;
	}

	for (i = 0; i < FTL_NV_CACHE_RESTORE_DEPTH; ++i) {
		spdk_dma_free(restore->nv_cache.block[i].buf);
	}

	free(restore->bands);
	free(restore);
}

static struct ftl_restore *
ftl_restore_init(struct spdk_ftl_dev *dev, ftl_restore_fn cb, void *cb_arg)
{
	struct ftl_restore *restore;
	struct ftl_restore_band *rband;
	size_t i;

	restore = calloc(1, sizeof(*restore));
	if (!restore) {
		goto error;
	}

	restore->dev = dev;
	restore->cb = cb;
	restore->cb_arg = cb_arg;
	restore->final_phase = false;

	restore->bands = calloc(ftl_get_num_bands(dev), sizeof(*restore->bands));
	if (!restore->bands) {
		goto error;
	}

	STAILQ_INIT(&restore->pad_bands);

	for (i = 0; i < ftl_get_num_bands(dev); ++i) {
		rband = &restore->bands[i];
		rband->band = &dev->bands[i];
		rband->parent = restore;
		rband->md_status = FTL_MD_NO_MD;
	}

	return restore;
error:
	ftl_restore_free(restore);
	return NULL;
}

static void
ftl_restore_complete(struct ftl_restore *restore, int status)
{
	struct ftl_restore *ctx = status ? NULL : restore;
	bool final_phase = restore->final_phase;

	restore->cb(ctx, status, restore->cb_arg);
	if (status || final_phase) {
		ftl_restore_free(restore);
	}
}

static int
ftl_band_cmp(const void *lband, const void *rband)
{
	uint64_t lseq = ((struct ftl_restore_band *)lband)->band->seq;
	uint64_t rseq = ((struct ftl_restore_band *)rband)->band->seq;

	if (lseq < rseq) {
		return -1;
	} else {
		return 1;
	}
}

static int
ftl_restore_check_seq(const struct ftl_restore *restore)
{
	const struct spdk_ftl_dev *dev = restore->dev;
	const struct ftl_restore_band *rband;
	const struct ftl_band *next_band;
	size_t i;

	for (i = 0; i < ftl_get_num_bands(dev); ++i) {
		rband = &restore->bands[i];
		if (rband->md_status != FTL_MD_SUCCESS) {
			continue;
		}

		next_band = LIST_NEXT(rband->band, list_entry);
		if (next_band && rband->band->seq == next_band->seq) {
			return -1;
		}
	}

	return 0;
}

static bool
ftl_restore_head_valid(struct spdk_ftl_dev *dev, struct ftl_restore *restore, size_t *num_valid)
{
	struct ftl_restore_band *rband;
	size_t i;

	for (i = 0; i < ftl_get_num_bands(dev); ++i) {
		rband = &restore->bands[i];

		if (rband->md_status != FTL_MD_SUCCESS &&
		    rband->md_status != FTL_MD_NO_MD &&
		    rband->md_status != FTL_MD_IO_FAILURE) {
			SPDK_ERRLOG("Inconsistent head metadata found on band %u\n",
				    rband->band->id);
			return false;
		}

		if (rband->md_status == FTL_MD_SUCCESS) {
			(*num_valid)++;
		}
	}

	return true;
}

static void
ftl_restore_head_complete(struct ftl_restore *restore)
{
	struct spdk_ftl_dev *dev = restore->dev;
	size_t num_valid = 0;
	int status = -EIO;

	if (!ftl_restore_head_valid(dev, restore, &num_valid)) {
		goto out;
	}

	if (num_valid == 0) {
		SPDK_ERRLOG("Couldn't find any valid bands\n");
		goto out;
	}

	/* Sort bands in sequence number ascending order */
	qsort(restore->bands, ftl_get_num_bands(dev), sizeof(struct ftl_restore_band),
	      ftl_band_cmp);

	if (ftl_restore_check_seq(restore)) {
		SPDK_ERRLOG("Band sequence consistency failed\n");
		goto out;
	}

	dev->num_lbas = dev->global_md.num_lbas;
	status = 0;
out:
	ftl_restore_complete(restore, status);
}
static void ftl_restore_head_md(void *ctx);

static void
ftl_restore_head_cb(struct ftl_io *io, void *ctx, int status)
{
	struct ftl_restore_band *rband = ctx;
	struct ftl_restore *restore = rband->parent;
	unsigned int num_ios;

	rband->md_status = status;
	num_ios = __atomic_fetch_sub(&restore->num_ios, 1, __ATOMIC_SEQ_CST);
	spdk_dma_free(rband->band->lba_map.dma_buf);

	if (num_ios == 1) {
		if (restore->current == ftl_get_num_bands(restore->dev)) {
			ftl_restore_head_complete(restore);
		} else {
			ftl_restore_head_md(restore);
		}
	}
}

static void
ftl_restore_head_md(void *ctx)
{
	struct ftl_restore *restore = ctx;
	struct spdk_ftl_dev *dev = restore->dev;
	struct ftl_restore_band *rband;
	struct ftl_lba_map *lba_map;
	unsigned int num_failed = 0, num_ios;
	size_t i, qdepth = 128;

	for (i = 0; i < qdepth; ++i) {
		rband = &restore->bands[restore->current];
		lba_map = &rband->band->lba_map;

		lba_map->dma_buf = spdk_dma_zmalloc(ftl_head_md_num_blocks(dev) *
						    FTL_BLOCK_SIZE, 0, NULL);
		if (!lba_map->dma_buf) {
			SPDK_ERRLOG("Failed to allocate dma buffer for band %u\n", restore->current);
			ftl_restore_complete(restore, -ENOMEM);
		}

		if (ftl_band_read_head_md(rband->band, ftl_restore_head_cb, rband)) {
			if (spdk_likely(rband->band->num_zones)) {
				SPDK_ERRLOG("Failed to read metadata on band %zu\n", i);

				rband->md_status = FTL_MD_INVALID_CRC;

				/* If the first IO fails, don't bother sending anything else */
				if (i == 0) {
					ftl_restore_complete(restore, -EIO);
				}
			}

			num_failed++;
		}

		restore->current++;
		restore->num_ios++;
		if (restore->current == ftl_get_num_bands(dev)) {
			return;
		}
	}

	if (spdk_unlikely(num_failed > 0)) {
		num_ios = __atomic_fetch_sub(&restore->num_ios, num_failed, __ATOMIC_SEQ_CST);
		if (num_ios == num_failed) {
			ftl_restore_complete(restore, -EIO);
		}
	}
}

int
ftl_restore_md(struct spdk_ftl_dev *dev, ftl_restore_fn cb, void *cb_arg)
{
	struct ftl_restore *restore;

	restore = ftl_restore_init(dev, cb, cb_arg);
	if (!restore) {
		return -ENOMEM;
	}

	spdk_thread_send_msg(ftl_get_core_thread(dev), ftl_restore_head_md, restore);

	return 0;
}

static int
ftl_restore_l2p(struct ftl_band *band)
{
	struct spdk_ftl_dev *dev = band->dev;
	struct ftl_addr addr;
	uint64_t lba;
	size_t i;

	for (i = 0; i < ftl_get_num_blocks_in_band(band->dev); ++i) {
		if (!spdk_bit_array_get(band->lba_map.vld, i)) {
			continue;
		}

		lba = band->lba_map.map[i];
		if (lba >= dev->num_lbas) {
			return -1;
		}

		addr = ftl_l2p_get(dev, lba);
		if (!ftl_addr_invalid(addr)) {
			ftl_invalidate_addr(dev, addr);
		}

		addr = ftl_band_addr_from_block_offset(band, i);

		ftl_band_set_addr(band, lba, addr);
		ftl_l2p_set(dev, lba, addr);
	}

	return 0;
}

static struct ftl_restore_band *
ftl_restore_next_band(struct ftl_restore *restore)
{
	struct ftl_restore_band *rband;

	for (; restore->current < ftl_get_num_bands(restore->dev); ++restore->current) {
		rband = &restore->bands[restore->current];

		if (spdk_likely(rband->band->num_zones) &&
		    rband->md_status == FTL_MD_SUCCESS) {
			restore->current++;
			return rband;
		}
	}

	return NULL;
}

static bool
ftl_pad_zone_pad_finish(struct ftl_restore_band *rband, bool direct_access)
{
	struct ftl_restore *restore = rband->parent;
	struct ftl_restore_band *next_band;
	size_t i, num_pad_zones = 0;

	if (spdk_unlikely(restore->pad_status && !restore->num_ios)) {
		if (direct_access) {
			/* In case of any errors found we want to clear direct access. */
			/* Direct access bands have their own allocated md, which would be lost */
			/* on restore complete otherwise. */
			rband->band->state = FTL_BAND_STATE_CLOSED;
			ftl_band_set_direct_access(rband->band, false);
		}
		ftl_restore_complete(restore, restore->pad_status);
		return true;
	}

	for (i = 0; i < rband->band->num_zones; ++i) {
		if (rband->band->zone_buf[i].info.state != SPDK_BDEV_ZONE_STATE_FULL) {
			num_pad_zones++;
		}
	}

	/* Finished all zones in a band, check if all bands are done */
	if (num_pad_zones == 0) {
		if (direct_access) {
			rband->band->state = FTL_BAND_STATE_CLOSED;
			ftl_band_set_direct_access(rband->band, false);
		}

		next_band = STAILQ_NEXT(rband, stailq);
		if (!next_band) {
			ftl_restore_complete(restore, restore->pad_status);
			return true;
		} else {
			/* Start off padding in the next band */
			ftl_restore_pad_band(next_band);
			return true;
		}
	}

	return false;
}

static struct ftl_io *
ftl_restore_init_pad_io(struct ftl_restore_band *rband, void *buffer,
			struct ftl_addr addr)
{
	struct ftl_band *band = rband->band;
	struct spdk_ftl_dev *dev = band->dev;
	int flags = FTL_IO_PAD | FTL_IO_INTERNAL | FTL_IO_PHYSICAL_MODE | FTL_IO_MD |
		    FTL_IO_DIRECT_ACCESS;
	struct ftl_io_init_opts opts = {
		.dev		= dev,
		.io		= NULL,
		.band		= band,
		.size		= sizeof(struct ftl_io),
		.flags		= flags,
		.type		= FTL_IO_WRITE,
		.num_blocks	= dev->xfer_size,
		.cb_fn		= ftl_pad_zone_cb,
		.cb_ctx		= rband,
		.iovs		= {
			{
				.iov_base = buffer,
				.iov_len = dev->xfer_size * FTL_BLOCK_SIZE,
			}
		},
		.iovcnt		= 1,
		.parent		= NULL,
		.ioch		= ftl_get_io_channel(dev),
	};
	struct ftl_io *io;

	io = ftl_io_init_internal(&opts);
	if (spdk_unlikely(!io)) {
		return NULL;
	}

	io->addr = addr;
	rband->parent->num_ios++;

	return io;
}

static void
ftl_pad_zone_cb(struct ftl_io *io, void *arg, int status)
{
	struct ftl_restore_band *rband = arg;
	struct ftl_restore *restore = rband->parent;
	struct ftl_band *band = io->band;
	struct ftl_zone *zone;
	struct ftl_io *new_io;
	uint64_t offset;

	restore->num_ios--;
	/* TODO check for next unit error vs early close error */
	if (status) {
		restore->pad_status = status;
		goto end;
	}

	offset = io->addr.offset % ftl_get_num_blocks_in_zone(restore->dev);
	if (offset + io->num_blocks == ftl_get_num_blocks_in_zone(restore->dev)) {
		zone = ftl_band_zone_from_addr(band, io->addr);
		zone->info.state = SPDK_BDEV_ZONE_STATE_FULL;
	} else {
		struct ftl_addr addr = io->addr;
		addr.offset += io->num_blocks;
		new_io = ftl_restore_init_pad_io(rband, io->iov[0].iov_base, addr);
		if (spdk_unlikely(!new_io)) {
			restore->pad_status = -ENOMEM;
			goto end;
		}

		ftl_io_write(new_io);
		return;
	}

end:
	spdk_dma_free(io->iov[0].iov_base);
	ftl_pad_zone_pad_finish(rband, true);
}

static void
ftl_restore_pad_band(struct ftl_restore_band *rband)
{
	struct ftl_restore *restore = rband->parent;
	struct ftl_band *band = rband->band;
	struct spdk_ftl_dev *dev = band->dev;
	void *buffer = NULL;
	struct ftl_io *io;
	struct ftl_addr addr;
	size_t i;
	int rc = 0;

	/* Check if some zones are not closed */
	if (ftl_pad_zone_pad_finish(rband, false)) {
		/*
		 * If we're here, end meta wasn't recognized, but the whole band is written
		 * Assume the band was padded and ignore it
		 */
		return;
	}

	band->state = FTL_BAND_STATE_OPEN;
	rc = ftl_band_set_direct_access(band, true);
	if (rc) {
		ftl_restore_complete(restore, rc);
		return;
	}

	for (i = 0; i < band->num_zones; ++i) {
		if (band->zone_buf[i].info.state == SPDK_BDEV_ZONE_STATE_FULL) {
			continue;
		}

		addr.offset = band->zone_buf[i].info.write_pointer;

		buffer = spdk_dma_zmalloc(FTL_BLOCK_SIZE * dev->xfer_size, 0, NULL);
		if (spdk_unlikely(!buffer)) {
			rc = -ENOMEM;
			goto error;
		}

		io = ftl_restore_init_pad_io(rband, buffer, addr);
		if (spdk_unlikely(!io)) {
			rc = -ENOMEM;
			spdk_dma_free(buffer);
			goto error;
		}

		ftl_io_write(io);
	}

	return;

error:
	restore->pad_status = rc;
	ftl_pad_zone_pad_finish(rband, true);
}

static void
ftl_restore_pad_open_bands(void *ctx)
{
	struct ftl_restore *restore = ctx;

	ftl_restore_pad_band(STAILQ_FIRST(&restore->pad_bands));
}

static void
ftl_restore_tail_md_cb(struct ftl_io *io, void *ctx, int status)
{
	struct ftl_restore_band *rband = ctx;
	struct ftl_restore *restore = rband->parent;
	struct spdk_ftl_dev *dev = restore->dev;

	if (status) {
		if (!dev->conf.allow_open_bands) {
			SPDK_ERRLOG("%s while restoring tail md in band %u.\n",
				    spdk_strerror(-status), rband->band->id);
			ftl_band_release_lba_map(rband->band);
			ftl_restore_complete(restore, status);
			return;
		} else {
			SPDK_ERRLOG("%s while restoring tail md. Will attempt to pad band %u.\n",
				    spdk_strerror(-status), rband->band->id);
			STAILQ_INSERT_TAIL(&restore->pad_bands, rband, stailq);
		}
	}

	if (!status && ftl_restore_l2p(rband->band)) {
		ftl_band_release_lba_map(rband->band);
		ftl_restore_complete(restore, -ENOTRECOVERABLE);
		return;
	}
	ftl_band_release_lba_map(rband->band);

	rband = ftl_restore_next_band(restore);
	if (!rband) {
		if (!STAILQ_EMPTY(&restore->pad_bands)) {
			spdk_thread_send_msg(ftl_get_core_thread(dev), ftl_restore_pad_open_bands,
					     restore);
		} else {
			ftl_restore_complete(restore, 0);
		}

		return;
	}

	ftl_restore_tail_md(rband);
}

static int
ftl_restore_tail_md(struct ftl_restore_band *rband)
{
	struct ftl_restore *restore = rband->parent;
	struct ftl_band *band = rband->band;

	if (ftl_band_alloc_lba_map(band)) {
		SPDK_ERRLOG("Failed to allocate lba map\n");
		ftl_restore_complete(restore, -ENOMEM);
		return -ENOMEM;
	}

	if (ftl_band_read_tail_md(band, band->tail_md_addr, ftl_restore_tail_md_cb, rband)) {
		SPDK_ERRLOG("Failed to send tail metadata read\n");
		ftl_restore_complete(restore, -EIO);
		return -EIO;
	}

	return 0;
}

int
ftl_restore_device(struct ftl_restore *restore, ftl_restore_fn cb, void *cb_arg)
{
	struct spdk_ftl_dev *dev = restore->dev;
	struct ftl_restore_band *rband;

	restore->current = 0;
	restore->cb = cb;
	restore->cb_arg = cb_arg;
	restore->final_phase = dev->nv_cache.bdev_desc == NULL;

	/* If restore_device is called, there must be at least one valid band */
	rband = ftl_restore_next_band(restore);
	assert(rband);
	return ftl_restore_tail_md(rband);
}
