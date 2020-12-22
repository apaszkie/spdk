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
#include "spdk_internal/log.h"
#include "spdk/ftl.h"

#include "ftl_reloc.h"
#include "ftl_core.h"
#include "ftl_io.h"
#include "ftl_band.h"
#include "ftl_debug.h"
#include "ftl_rq.h"
#include "ftl_band_ops.h"

/* Maximum active reloc moves */
#define FTL_RELOC_MAX_MOVES 256

struct ftl_reloc;
struct ftl_band_reloc;

enum ftl_reloc_move_state {
	FTL_RELOC_STATE_READ,
	FTL_RELOC_STATE_WRITE,
	FTL_RELOC_STATE_WAIT,
	FTL_RELOC_STATE_HALT
};

struct ftl_reloc_move {
	/* FTL device */
	struct spdk_ftl_dev			*dev;

	struct ftl_reloc			*reloc;

	/* Request for reading */
	struct ftl_rq				*rd;

	/* Request for writing */
	struct ftl_rq				*wr;

	/* Move state (read, write) */
	enum ftl_reloc_move_state		state;

	/* Entry of circular list */
	CIRCLEQ_ENTRY(ftl_reloc_move)	        centry;
};

struct ftl_reloc {
	/* Device associated with relocate */
	struct spdk_ftl_dev			*dev;

	/* Indicates relocate is about to halt */
	bool					halt;

	/* Band which are read to relocate */
	struct ftl_band 			*band;

	/* Bands already read, but waiting for finishing GC */
	LIST_HEAD(, ftl_band)			band_done;

	/* Flags indicating reloc is waiting for a new band */
	bool					band_waiting;

	/* Maximum number of IOs per band */
	size_t					max_qdepth;

	/* Maximum number of active band relocates */
	size_t					max_active;

	/* Queue of free move objects */
	struct ftl_reloc_move			*move_buffer;

	/* Circural list movers */
	CIRCLEQ_HEAD(, ftl_reloc_move)		move_cqueue;

	/* Circural list iterator */
	struct ftl_reloc_move			*move_iter;

	/* Priority band relocates queue */
	TAILQ_HEAD(, ftl_band_reloc)		prio_queue;
};


static void _move_read_cb(struct ftl_rq *rq);
static void _move_write_cb(struct ftl_rq *rq);

static void _reloc_move_deinit(struct ftl_reloc_move *mv)
{
	if (mv) {
		ftl_rq_del(mv->rd);
		ftl_rq_del(mv->wr);
	}
}

static int _reloc_move_init(struct ftl_reloc *reloc, struct ftl_reloc_move *mv)
{
	mv->state = FTL_RELOC_STATE_HALT;
	mv->reloc = reloc;
	mv->dev = reloc->dev;
	mv->rd = ftl_rq_new(mv->dev, mv->dev->xfer_size, mv->dev->md_size);
	mv->wr = ftl_rq_new(mv->dev, mv->dev->xfer_size, mv->dev->md_size);

	if (!mv->rd || !mv->wr) {
		goto ERROR;
	}

	mv->wr->owner.priv = mv;
	mv->wr->owner.cb = _move_write_cb;

	mv->rd->owner.priv = mv;
	mv->rd->owner.cb = _move_read_cb;

	return 0;

ERROR:
	return -ENOMEM;
}

struct ftl_reloc *
ftl_reloc_init(struct spdk_ftl_dev *dev)
{
	struct ftl_reloc *reloc;
	char pool_name[128];
	size_t i;
	int rc;

	reloc = calloc(1, sizeof(*reloc));
	if (!reloc) {
		return NULL;
	}

	reloc->dev = dev;
	reloc->halt = true;
	reloc->max_qdepth = dev->conf.max_reloc_qdepth;
	reloc->max_active = dev->conf.max_active_relocs;

	if (reloc->max_qdepth < reloc->max_active) {
		SPDK_ERRLOG("max_qdepth need to be greater or equal max_acitve relocs\n");
	}

	rc = snprintf(pool_name, sizeof(pool_name), "ftl-moves-%p", dev);
	if (rc < 0 || rc >= (int)sizeof(pool_name)) {
		SPDK_ERRLOG("Failed to create reloc moves pool name\n");
		goto error;
	}

	reloc->move_buffer = calloc(reloc->max_qdepth, sizeof(*reloc->move_buffer));
	if (!reloc->move_buffer) {
		SPDK_ERRLOG("Failed to initialize reloc moves pool");
		goto error;
	}

	CIRCLEQ_INIT(&reloc->move_cqueue);
	struct ftl_reloc_move *move;
	for (i = 0; i < reloc->max_qdepth; ++i) {
		move = &reloc->move_buffer[i];

		if (_reloc_move_init(reloc, move)) {
			goto error;
		}

		CIRCLEQ_INSERT_HEAD(&reloc->move_cqueue, move, centry);
	}
	reloc->move_iter = CIRCLEQ_FIRST(&reloc->move_cqueue);

	LIST_INIT(&reloc->band_done);
	TAILQ_INIT(&reloc->prio_queue);

	return reloc;
error:
	ftl_reloc_free(reloc);
	return NULL;
}

struct ftl_reloc_task_fini {
	struct ftl_reloc_task *task;
	spdk_msg_fn	      cb;
	void		      *cb_arg;
};

void
ftl_reloc_free(struct ftl_reloc *reloc)
{
	size_t i;

	if (!reloc) {
		return;
	}

	if (reloc->move_buffer) {
		for (i = 0; i < reloc->max_qdepth; ++i) {
			_reloc_move_deinit(&reloc->move_buffer[i]);
		}
	}

	free(reloc->move_buffer);
	free(reloc);
}

void
ftl_reloc_halt(struct ftl_reloc *reloc)
{
	reloc->halt = true;
}

void
ftl_reloc_resume(struct ftl_reloc *reloc)
{
	struct ftl_reloc_move *mv = NULL;
	reloc->halt = false;

	CIRCLEQ_FOREACH(mv, &reloc->move_cqueue, centry) {
		if (FTL_RELOC_STATE_HALT == mv->state) {
			mv->state = FTL_RELOC_STATE_READ;
		}
	}
}

static void _get_band_cb(struct ftl_band *band, void *cntx, bool status)
{
	struct ftl_reloc *reloc = cntx;

	if (spdk_likely(status)) {
		reloc->band = band;

		flt_band_iter_init(band);
		ftl_band_iter_advance(band, ftl_head_md_num_blocks(band->dev));
	}
	reloc->band_waiting = false;
}

static struct ftl_band *_get_band(struct ftl_reloc *reloc)
{
	struct spdk_ftl_dev *dev = reloc->dev;
	struct ftl_band *band = reloc->band;

	if (band) {
		if (ftl_band_full(band, band->iter.offset) && !reloc->halt) {
			LIST_INSERT_HEAD(&reloc->band_done, band, list_entry);
			reloc->band = NULL;
		} else {
			return band;
		}

		return NULL;
	} else {
		if (!reloc->band_waiting) {
			if (!ftl_needs_defrag(dev)) {
				return NULL;
			}

			ftl_band_get_next_gc(dev, _get_band_cb, reloc);
			reloc->band_waiting = true;
		}

		return NULL;
	}
}

static void _move_read_cb(struct ftl_rq *rq)
{
	struct ftl_reloc_move *mv = rq->owner.priv;

	if (spdk_likely(rq->success)) {
		mv->state = FTL_RELOC_STATE_WRITE;
	} else {
		// XXX
		abort();
	}
}

static void _move_read(struct ftl_reloc *reloc, struct ftl_reloc_move *mv,
		struct ftl_band* band)
{
	struct ftl_rq *rq = mv->rd;
	uint64_t blocks = spdk_bit_array_capacity(band->lba_map.vld);
	uint64_t pos = band->iter.offset;
	uint64_t next = spdk_bit_array_find_first_set(band->lba_map.vld, pos);
	int rc;

	if (spdk_likely(next < blocks)) {
		if (next > band->iter.offset) {
			ftl_band_iter_advance(band, next - pos);
		} else if (next == band->iter.offset) {
			/* Valid block at the position of iterator */
		} else {
			/* Inconsistent state */
			abort();
		}
	} else if (UINT32_MAX == next) {
		/* No more valid LBAs in the band */
		uint64_t left = ftl_band_user_blocks_left(band,
				band->iter.offset);
		ftl_band_iter_advance(band, left);

		assert(ftl_band_full(band, band->iter.offset));

		/* Move to read next band */
		mv->state = FTL_RELOC_STATE_READ;
		return;
	} else {
		/* Inconsistent state */
		abort();
	}

	rq->iter.idx = 0;
	rq->iter.count = spdk_min(rq->num_blocks,
			ftl_band_user_blocks_left(band, band->iter.offset));

	rc = ftl_band_rq_read(band, rq);
	if (spdk_likely(!rc)) {
		/* Read was issued, we can advance band iterator */
		ftl_band_iter_advance(band, rq->iter.count);
		mv->state = FTL_RELOC_STATE_WAIT;
	} else {
		/* This is error, do nothing and we'll retry reading */
	}
}

static void _move_write_cb(struct ftl_rq *rq)
{
	struct ftl_reloc_move *mv = rq->owner.priv;

	if (spdk_likely(rq->success)) {
		mv->wr->iter.idx = 0;
		ftl_rq_update_l2p(mv->wr);

		if (mv->rd->iter.idx < mv->rd->iter.count) {
			/* Still in read request data to move, go to write state */
			mv->state = FTL_RELOC_STATE_WRITE;
		} else {
			/* No more data in read request, start over reading */
			mv->state = FTL_RELOC_STATE_READ;
		}
	} else {
		// XXX
		abort();
	}
}

static void _move_write(struct ftl_reloc *reloc, struct ftl_reloc_move *mv)
{
	struct spdk_ftl_dev *dev = mv->dev;
	struct ftl_rq_entry *iter;

	struct ftl_rq *wr = mv->wr;
	struct ftl_rq *rd = mv->rd;
	const uint64_t num_entries = wr->num_blocks;
	struct ftl_band *band = rd->io.band;
	struct ftl_addr current_addr;
	uint64_t lba;

	assert(wr->iter.idx < num_entries);
	assert(rd->iter.idx < rd->iter.count);

	iter = &wr->entries[wr->iter.idx];

	while (wr->iter.idx < num_entries && rd->iter.idx < rd->iter.count) {
		uint64_t offset =  ftl_band_block_offset_from_addr(band, rd->io.addr);
		assert(offset < ftl_band_num_usable_blocks(band));

		if (!ftl_band_block_offset_valid(band, offset)) {
			rd->io.addr = ftl_band_next_addr(band, rd->io.addr, 1);
			rd->iter.idx++;
			continue;
		}

		lba = band->lba_map.map[offset];
		current_addr = ftl_l2p_get(dev, lba);
		if (ftl_addr_cmp(current_addr, rd->io.addr)) {
			/*
			 * Swap payload
			 */
			ftl_rq_swap_payload(wr, wr->iter.idx, rd, rd->iter.idx);

			iter->addr = rd->io.addr;
			iter->owner.priv = band;
			iter->lba = lba;

			/* Advance within batch */
			iter++;
			wr->iter.idx++;
		} else {
			/* Inconsistent state */
			abort();
		}

		/* Advance within reader */
		rd->iter.idx++;
		rd->io.addr = ftl_band_next_addr(band, rd->io.addr, 1);
	}

	if (num_entries == wr->iter.idx) {
		/*
		 *Request contains data to be placed on FTL, compact it
		 */
		ftl_writer_queue_rq(&dev->writer_gc, wr);
		mv->state = FTL_RELOC_STATE_WAIT;
	} else {
		assert(rd->iter.idx == rd->iter.count);
		mv->state = FTL_RELOC_STATE_READ;
	}
}

static void _move_run(struct ftl_reloc *reloc, struct ftl_reloc_move *mv)
{
	switch(mv->state) {
	case FTL_RELOC_STATE_READ: {
		struct ftl_band* band;

		if (spdk_unlikely(reloc->halt)) {
			mv->state = FTL_RELOC_STATE_HALT;
			break;
		}

		band = _get_band(reloc);
		if (!band) {
			break;
		}

		_move_read(reloc, mv, band);
	}
	break;

	case FTL_RELOC_STATE_WRITE:
		if (spdk_unlikely(reloc->halt)) {
			mv->state = FTL_RELOC_STATE_HALT;
			break;
		}

		_move_write(reloc, mv);
		break;

	case FTL_RELOC_STATE_HALT:
	case FTL_RELOC_STATE_WAIT:
		break;

	default:
		assert(0);
		abort();
		break;
	}
}

static void _release_bands(struct ftl_reloc *reloc)
{
	struct ftl_band* band, *next;

	LIST_FOREACH_SAFE(band, &reloc->band_done, list_entry, next) {
		if (ftl_band_empty(band) && ftl_band_full(band, band->iter.offset)) {
			ftl_band_release_lba_map(band);
			ftl_band_set_state(band, FTL_BAND_STATE_FREE);
			assert(0 == band->lba_map.ref_cnt);
		}
	}
}

bool
ftl_reloc_is_halted(const struct ftl_reloc *reloc)
{
	struct ftl_reloc_move *mv = NULL;

	CIRCLEQ_FOREACH(mv, &reloc->move_cqueue, centry) {
		if (FTL_RELOC_STATE_HALT != mv->state) {
			return false;
		}
	}

	return true;
}

void
ftl_reloc(struct ftl_reloc *reloc)
{
	_move_run(reloc, reloc->move_iter);
	reloc->move_iter = CIRCLEQ_LOOP_NEXT(&reloc->move_cqueue,
			reloc->move_iter, centry);

	_release_bands(reloc);
}

void
ftl_reloc_add(struct ftl_reloc *reloc, struct ftl_band *band, size_t offset,
	      size_t num_blocks, int prio, bool is_defrag)
{
	/* TODO(mbarczak) Add band to high priority list */
	abort();
}
