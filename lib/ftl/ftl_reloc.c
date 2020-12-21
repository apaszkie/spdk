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

enum ftl_band_reloc_state {
	FTL_BAND_RELOC_STATE_INACTIVE,
	FTL_BAND_RELOC_STATE_PENDING,
	FTL_BAND_RELOC_STATE_ACTIVE,
	FTL_BAND_RELOC_STATE_HIGH_PRIO
};

struct ftl_reloc_task {
	/* Parent */
	struct ftl_reloc			*reloc;

	/* Poller with movement logic */
	struct spdk_poller			*poller;

	/* Queue of moves that task is processing */
	struct spdk_ring			*move_queue;

	STAILQ_ENTRY(ftl_reloc_task)		entry;
};

struct ftl_reloc_move {
	/* FTL device */
	struct spdk_ftl_dev			*dev;

	struct ftl_reloc			*reloc;

	/* Request for reading */
	struct ftl_rq				*rd;

	/* Request for writing */
	struct ftl_rq				*wr;

	struct ftl_band_reloc			*breloc;

	struct ftl_reloc_task			*task;

	/* Number of logical blocks */
	size_t					num_blocks;

	/* Data buffer */
	void					*data;

	/* Move state (read, write) */
	enum ftl_reloc_move_state		state;

	/* IO associated with move */
	struct ftl_io				*io;

	/* Entry of circular list */
	CIRCLEQ_ENTRY(ftl_reloc_move)	        centry;

	STAILQ_ENTRY(ftl_reloc_move)		entry;

#define FTL_MAX_RELOC_VECTOR_LBA 128
	uint64_t				lba_vector[FTL_MAX_RELOC_VECTOR_LBA];

	struct ftl_addr				dest_addr;
	struct ftl_reloc_chunk			chunk_vector[FTL_MAX_RELOC_VECTOR_LBA];
	size_t					num_chunks;
};

struct ftl_band_reloc {
	struct ftl_reloc			*parent;

	/* Band being relocated */
	struct ftl_band				*band;

	/* Number of logical blocks to be relocated */
	size_t					num_blocks;

	/* Bitmap of logical blocks to be relocated */
	struct spdk_bit_array			*reloc_map;

	/*  State of the band reloc */
	enum ftl_band_reloc_state		state;

	/* The band is being defragged */
	bool					defrag;

	/* Reloc map iterator */
	struct {
		/* Array of zone offsets */
		size_t				*zone_offset;

		/* Current zone */
		size_t				zone_current;
	} iter;

	/* Number of outstanding moves */
	size_t					num_outstanding;

	TAILQ_ENTRY(ftl_band_reloc)		entry;
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

	/* Maximum transfer size (in logical blocks) per single IO */
	size_t					xfer_size;
	/* Number of bands being defragged */
	size_t					num_defrag_bands;

	/* Array of band relocates */
	struct ftl_band_reloc			*brelocs;

	/* Number of active/priority band relocates */
	size_t					num_active;

	/* Queue of free move objects */
	struct ftl_reloc_move			*move_buffer;

	/* Circural list movers */
	CIRCLEQ_HEAD(, ftl_reloc_move)		move_cqueue;

	/* Circural list iterator */
	struct ftl_reloc_move			*move_iter;

	STAILQ_HEAD(, ftl_reloc_move)		free_move_queue;
	void					*payload;

	/* Move completion queue */
	struct spdk_ring			*move_cmpl_queue;

	/* Priority band relocates queue */
	TAILQ_HEAD(, ftl_band_reloc)		prio_queue;

	/* Active band relocates queue */
	TAILQ_HEAD(, ftl_band_reloc)		active_queue;

	/* Pending band relocates queue */
	TAILQ_HEAD(, ftl_band_reloc)		pending_queue;

	/* Queue of relocation tasks running on different threads */
	STAILQ_HEAD(, ftl_reloc_task)		task_queue;
};


static void _move_read_cb(struct ftl_rq *rq);
static void _move_write_cb(struct ftl_rq *rq);

bool
ftl_reloc_is_defrag_active(const struct ftl_reloc *reloc)
{
	return reloc->num_defrag_bands > 0;
}

static size_t
ftl_reloc_iter_zone_offset(struct ftl_band_reloc *breloc)
{
	size_t zone = breloc->iter.zone_current;

	return breloc->iter.zone_offset[zone];
}

static size_t
ftl_reloc_iter_zone_done(struct ftl_band_reloc *breloc)
{
	size_t num_blocks = ftl_get_num_blocks_in_zone(breloc->parent->dev);

	return ftl_reloc_iter_zone_offset(breloc) == num_blocks;
}

static void
ftl_reloc_clr_block(struct ftl_band_reloc *breloc, size_t block_off)
{
	if (!spdk_bit_array_get(breloc->reloc_map, block_off)) {
		return;
	}

	spdk_bit_array_clear(breloc->reloc_map, block_off);
	assert(breloc->num_blocks);
	breloc->num_blocks--;
}

static void
ftl_reloc_activate(struct ftl_band_reloc *breloc)
{
	struct ftl_reloc *reloc = breloc->parent;

	breloc->state = FTL_BAND_RELOC_STATE_ACTIVE;
	TAILQ_INSERT_HEAD(&reloc->active_queue, breloc, entry);
}

static void
ftl_reloc_read_lba_map_cb(struct ftl_io *io, void *arg, int status)
{
	struct ftl_band_reloc *breloc = arg;

	assert(status == 0);

	ftl_reloc_activate(breloc);
}

static int
ftl_reloc_read_lba_map(struct ftl_band_reloc *breloc)
{
	struct ftl_band *band = breloc->band;

	if (ftl_band_alloc_lba_map(band)) {
		return -ENOMEM;
	}

	return ftl_band_read_lba_map(band, 0, ftl_get_num_blocks_in_band(band->dev),
				     ftl_reloc_read_lba_map_cb, breloc);
}

static int
ftl_reloc_prepare(struct ftl_band_reloc *breloc)
{
	struct ftl_band *band = breloc->band;
	int rc = 0;

	if (!band->high_prio) {
		rc = ftl_reloc_read_lba_map(breloc);
	} else {
		ftl_band_acquire_lba_map(band);
		ftl_reloc_activate(breloc);
	}

	return rc;
}

static void
ftl_reloc_write_cb(struct ftl_io *io, void *arg, int status)
{
	struct ftl_reloc_move *move = arg;
	struct ftl_band_reloc *breloc = move->breloc;
	struct ftl_reloc *reloc = breloc->parent;

	move->dest_addr = io->addr;
	spdk_ring_enqueue(reloc->move_cmpl_queue, (void **)&move, 1, NULL);

	if (status) {
		SPDK_ERRLOG("Reloc write failed with status: %d\n", status);
		assert(false);
		return;
	}
}

static void
ftl_reloc_read_child_cb(struct ftl_io *io, void *arg, int status)
{
	/* TODO: We should handle fail on relocation read. We need to inform */
	/* user that this group of blocks is bad (update l2p with bad block address and */
	/* put it to lba_map/sector_lba). Maybe we could also retry read with smaller granularity? */
	if (status) {
		SPDK_ERRLOG("Reloc child read failed with status: %d\n", status);
		assert(false);
		return;
	}
}

static void
ftl_reloc_read_cb(struct ftl_io *io, void *arg, int status)
{
	struct ftl_reloc_move *move = arg;
	struct ftl_reloc_task *task = move->task;

	/* TODO: We should handle fail on relocation read. We need to inform */
	/* user that this group of blocks is bad (update l2p with bad block address and */
	/* put it to lba_map/sector_lba). Maybe we could also retry read with smaller granularity? */
	if (status) {
		SPDK_ERRLOG("Reloc read failed with status: %d\n", status);
		assert(false);
		return;
	}

	move->state = FTL_RELOC_STATE_WRITE;
	move->io = NULL;
	spdk_ring_enqueue(task->move_queue, (void **)&move, 1, NULL);
}

static void
ftl_reloc_iter_reset(struct ftl_band_reloc *breloc)
{
	memset(breloc->iter.zone_offset, 0, ftl_get_num_punits(breloc->band->dev) *
	       sizeof(*breloc->iter.zone_offset));
	breloc->iter.zone_current = 0;
}

static size_t
ftl_reloc_iter_block_offset(struct ftl_band_reloc *breloc)
{
	size_t zone_offset = breloc->iter.zone_current * ftl_get_num_blocks_in_zone(breloc->parent->dev);

	return breloc->iter.zone_offset[breloc->iter.zone_current] + zone_offset;
}

static void
ftl_reloc_iter_next_zone(struct ftl_band_reloc *breloc)
{
	size_t num_zones = ftl_get_num_punits(breloc->band->dev);

	breloc->iter.zone_current = (breloc->iter.zone_current + 1) % num_zones;
}

static int
ftl_reloc_block_valid(struct ftl_band_reloc *breloc, size_t block_off)
{
	struct ftl_addr addr = ftl_band_addr_from_block_offset(breloc->band, block_off);

	return ftl_addr_is_written(breloc->band, addr) &&
	       spdk_bit_array_get(breloc->reloc_map, block_off) &&
	       ftl_band_block_offset_valid(breloc->band, block_off);
}

static int
ftl_reloc_iter_next(struct ftl_band_reloc *breloc, size_t *block_off)
{
	size_t zone = breloc->iter.zone_current;

	*block_off = ftl_reloc_iter_block_offset(breloc);

	if (ftl_reloc_iter_zone_done(breloc)) {
		return 0;
	}

	breloc->iter.zone_offset[zone]++;

	if (!ftl_reloc_block_valid(breloc, *block_off)) {
		ftl_reloc_clr_block(breloc, *block_off);
		return 0;
	}

	return 1;
}

static int
ftl_reloc_first_valid_block(struct ftl_band_reloc *breloc, size_t *block_off)
{
	size_t i, num_blocks = ftl_get_num_blocks_in_zone(breloc->parent->dev);

	for (i = ftl_reloc_iter_zone_offset(breloc); i < num_blocks; ++i) {
		if (ftl_reloc_iter_next(breloc, block_off)) {
			return 1;
		}
	}

	return 0;
}

static int
ftl_reloc_iter_done(struct ftl_band_reloc *breloc)
{
	size_t i;
	size_t num_zones = ftl_get_num_punits(breloc->band->dev);
	size_t num_blocks = ftl_get_num_blocks_in_zone(breloc->parent->dev);

	for (i = 0; i < num_zones; ++i) {
		if (breloc->iter.zone_offset[i] != num_blocks) {
			return 0;
		}
	}

	return 1;
}

static size_t
ftl_reloc_find_valid_blocks(struct ftl_band_reloc *breloc,
			    size_t _num_blocks, struct ftl_addr *addr)
{
	size_t block_off, num_blocks = 0;

	if (!ftl_reloc_first_valid_block(breloc, &block_off)) {
		return 0;
	}

	*addr = ftl_band_addr_from_block_offset(breloc->band, block_off);

	for (num_blocks = 1; num_blocks < _num_blocks; num_blocks++) {
		if (!ftl_reloc_iter_next(breloc, &block_off)) {
			break;
		}
	}

	return num_blocks;
}

static size_t
ftl_reloc_next_blocks(struct ftl_band_reloc *breloc, struct ftl_addr *addr, size_t _num_blocks)
{
	size_t i, num_blocks = 0;
	struct spdk_ftl_dev *dev = breloc->parent->dev;

	for (i = 0; i < ftl_get_num_punits(dev); ++i) {
		num_blocks = ftl_reloc_find_valid_blocks(breloc, _num_blocks, addr);
		ftl_reloc_iter_next_zone(breloc);

		if (num_blocks || ftl_reloc_iter_done(breloc)) {
			break;
		}
	}

	return num_blocks;
}

static struct ftl_io *
ftl_reloc_io_init(struct ftl_band_reloc *breloc, struct ftl_reloc_move *move,
		  ftl_io_fn fn, enum ftl_io_type io_type, int flags, struct ftl_addr _addr, size_t num_blocks)
{
	size_t block_off, i, j;
	struct ftl_addr addr = _addr;
	struct ftl_io *io = NULL;
	struct spdk_ftl_dev *dev = breloc->parent->dev;
	struct ftl_io_init_opts opts = {
		.dev		= dev,
		.band		= breloc->band,
		.size		= sizeof(*io),
		.flags		= flags | FTL_IO_INTERNAL | FTL_IO_PHYSICAL_MODE,
		.type		= io_type,
		.num_blocks	= num_blocks,
		.iovs		= {
			{
				.iov_base = move->data,
				.iov_len = num_blocks * FTL_BLOCK_SIZE,
			}
		},
		.iovcnt		= 1,
		.cb_fn		= fn,
		.ioch		= dev->ioch,
		.lba_vector	= move->lba_vector,
	};

	io = ftl_io_init_internal(&opts);
	if (!io) {
		return NULL;
	}

	io->cb_ctx = move;
	io->addr = addr;

	if (!(flags & FTL_IO_VECTOR_LBA)) {
		return io;
	}

	size_t lba_idx = 0;
	io->num_chunks = move->num_chunks;
	for (i = 0; i < move->num_chunks; ++i) {
		addr = move->chunk_vector[i].addr;
		io->chunk[i] = move->chunk_vector[i];

		for (j = 0; j < move->chunk_vector[i].num_blocks; ++j) {
			block_off = ftl_band_block_offset_from_addr(breloc->band, addr);
			addr.offset++;

			if (!ftl_band_block_offset_valid(breloc->band, block_off)) {
				io->lba.vector[lba_idx] = FTL_LBA_INVALID;
				lba_idx++;
				continue;
			}

			io->lba.vector[lba_idx] = breloc->band->lba_map.map[block_off];
			lba_idx++;
		}
	}

	for (i = move->num_blocks; i < num_blocks; ++i) {
		io->lba.vector[i] = FTL_LBA_INVALID;
	}

	return io;
}

static int
ftl_reloc_io_child_init(struct ftl_io *parent, struct ftl_band_reloc *breloc,
			struct ftl_reloc_move *move,
			ftl_io_fn fn, enum ftl_io_type io_type, int flags, void *data, struct ftl_addr *addr,
			size_t num_blocks)
{
	struct ftl_io *io = NULL;
	struct spdk_ftl_dev *dev = breloc->parent->dev;
	struct ftl_io_init_opts opts = {
		.dev		= dev,
		.parent		= parent,
		.io		= NULL,
		.band		= breloc->band,
		.size		= sizeof(*io),
		.flags		= flags | FTL_IO_INTERNAL | FTL_IO_PHYSICAL_MODE,
		.type		= io_type,
		.num_blocks	= num_blocks,
		.iovs		= {
			{
				.iov_base = data,
				.iov_len = num_blocks * FTL_BLOCK_SIZE,
			}
		},
		.iovcnt		= 1,
		.cb_fn		= fn,
		.ioch		= dev->ioch,
	};

	io = ftl_io_init_internal(&opts);
	if (!io) {
		assert(0);
		return -ENOMEM;
	}

	io->cb_ctx = move;
	io->addr = *addr;

	return 0;
}

static int
ftl_reloc_write(struct ftl_reloc_move *move)
{
	struct ftl_band_reloc *breloc = move->breloc;
	int io_flags =  FTL_IO_WEAK | FTL_IO_VECTOR_LBA | FTL_IO_BYPASS_CACHE;

	move->io = ftl_reloc_io_init(breloc, move, ftl_reloc_write_cb,
				     FTL_IO_WRITE, io_flags, move->chunk_vector[0].addr, breloc->parent->xfer_size);

	/* Try again later if IO allocation fails */
	if (!move->io) {
		return -ENOMEM;
	}

	ftl_io_write(move->io);
	return 0;
}

static int
ftl_reloc_read(struct ftl_reloc_move *move)
{
	struct ftl_band_reloc *breloc = move->breloc;
	struct ftl_addr addr;
	size_t num_blocks = 0, chunk_off;
	void *data = move->data;

	move->io = ftl_reloc_io_init(breloc, move, ftl_reloc_read_cb, FTL_IO_READ, 0,
				     move->chunk_vector[0].addr, move->num_blocks);
	if (!move->io) {
		return -ENOMEM;
	}

	for (chunk_off = 0; chunk_off < move->num_chunks; ++chunk_off) {
		num_blocks = move->chunk_vector[chunk_off].num_blocks;
		addr = move->chunk_vector[chunk_off].addr;

		ftl_reloc_io_child_init(move->io, breloc, move, ftl_reloc_read_child_cb, FTL_IO_READ, 0,
					data, &addr, num_blocks);

		data = (char *)data + num_blocks * FTL_BLOCK_SIZE;
	}

	ftl_io_read(move->io);
	return 0;
}

static void
ftl_reloc_process_moves(struct ftl_band_reloc *breloc)
{
	struct ftl_reloc *reloc = breloc->parent;
	struct ftl_band *band = breloc->band;
	struct ftl_reloc_move *move;
	struct ftl_reloc_task *task;
	struct ftl_addr addr = {};
	size_t max_qdepth, i, num_remaining, num_blocks;

	max_qdepth = band->high_prio ? reloc->max_qdepth : reloc->max_qdepth / reloc->num_active;

	while (true) {
		STAILQ_FOREACH(task, &reloc->task_queue, entry) {
			if (breloc->num_outstanding == max_qdepth) {
				return;
			}

			move = STAILQ_FIRST(&reloc->free_move_queue);
			if (!move) {
				return;
			}

			i = 0;
			num_remaining = reloc->xfer_size;
			while (num_remaining) {
				num_blocks = ftl_reloc_next_blocks(breloc, &addr, num_remaining);
				if (num_blocks == 0) {
					break;
				}

				move->chunk_vector[i].addr = addr;
				move->chunk_vector[i].num_blocks = num_blocks;
				move->num_chunks++;
				num_remaining -= num_blocks;
				move->num_blocks += num_blocks;
				i++;
			}

			if (!move->num_blocks) {
				return;
			}

			STAILQ_REMOVE_HEAD(&reloc->free_move_queue, entry);
			breloc->num_outstanding++;
			move->state = FTL_RELOC_STATE_READ;
			move->task = task;
			move->breloc = breloc;

			spdk_ring_enqueue(task->move_queue, (void **)&move, 1, NULL);
		}
	}
}

static void
ftl_reloc_release(struct ftl_band_reloc *breloc)
{
	struct ftl_reloc *reloc = breloc->parent;
	struct ftl_band *band = breloc->band;

	ftl_reloc_iter_reset(breloc);
	ftl_band_release_lba_map(band);
	reloc->num_active--;

	if (breloc->state == FTL_BAND_RELOC_STATE_HIGH_PRIO) {
		/* High prio band must be relocated as a whole and ANM events will be ignored */
		assert(breloc->num_blocks == 0 && ftl_band_empty(band));
		TAILQ_REMOVE(&reloc->prio_queue, breloc, entry);
		band->high_prio = 0;
		breloc->state = FTL_BAND_RELOC_STATE_INACTIVE;
	} else {
		assert(breloc->state == FTL_BAND_RELOC_STATE_ACTIVE);
		TAILQ_REMOVE(&reloc->active_queue, breloc, entry);
		breloc->state = FTL_BAND_RELOC_STATE_INACTIVE;

		/* If we got ANM event during relocation put such band back to pending queue */
		if (breloc->num_blocks != 0) {
			breloc->state = FTL_BAND_RELOC_STATE_PENDING;
			TAILQ_INSERT_TAIL(&reloc->pending_queue, breloc, entry);
			return;
		}
	}

	if (ftl_band_empty(band) && band->state == FTL_BAND_STATE_CLOSED) {
		ftl_band_set_state(breloc->band, FTL_BAND_STATE_FREE);

		if (breloc->defrag) {
			breloc->defrag = false;
			assert(reloc->num_defrag_bands > 0);
			reloc->num_defrag_bands--;
		}
	} else {
		if (breloc->defrag) {
			SPDK_ERRLOG("This should never happen in this scenario valid: %lu\n", band->lba_map.num_vld);
		}
	}
}

static void
ftl_process_reloc(struct ftl_band_reloc *breloc)
{
	ftl_reloc_process_moves(breloc);

	if (breloc->num_outstanding == 0 && ftl_reloc_iter_done(breloc)) {
		ftl_reloc_release(breloc);
	}
}

static int
ftl_band_reloc_init(struct ftl_reloc *reloc, struct ftl_band_reloc *breloc,
		    struct ftl_band *band)
{
	breloc->band = band;
	breloc->parent = reloc;

	breloc->reloc_map = spdk_bit_array_create(ftl_get_num_blocks_in_band(reloc->dev));
	if (!breloc->reloc_map) {
		SPDK_ERRLOG("Failed to initialize reloc map");
		return -1;
	}

	breloc->iter.zone_offset = calloc(ftl_get_num_punits(band->dev),
					  sizeof(*breloc->iter.zone_offset));
	if (!breloc->iter.zone_offset) {
		SPDK_ERRLOG("Failed to initialize reloc iterator");
		return -1;
	}

	return 0;
}

static void
ftl_band_reloc_free(struct ftl_band_reloc *breloc)
{
	if (!breloc) {
		return;
	}

	spdk_bit_array_free(&breloc->reloc_map);
	free(breloc->iter.zone_offset);
}

static void
ftl_reloc_process_move_completions(struct ftl_reloc *reloc)
{
	struct ftl_reloc_move *moves[FTL_RELOC_MAX_MOVES] = {0};
	struct ftl_reloc_move *move;
	struct ftl_band_reloc *breloc;
	struct ftl_band *dest_band;
	struct ftl_addr addr;
	size_t i, j, num_outstanding, chunk_off, pos;
	struct ftl_wbuf_entry entry = { 0 };
	struct spdk_ftl_dev *dev = reloc->dev;

	if (spdk_ring_count(reloc->move_cmpl_queue) == 0) {
		return;
	}

	num_outstanding = spdk_ring_dequeue(reloc->move_cmpl_queue, (void **)moves, reloc->max_qdepth);

	for (i = 0; i < num_outstanding; ++i) {
		move = moves[i];
		breloc = move->breloc;
		pos = 0;

		for (chunk_off = 0; chunk_off < move->num_chunks; ++chunk_off) {
			addr = move->chunk_vector[chunk_off].addr;
			for (j = 0; j < move->chunk_vector[chunk_off].num_blocks; ++j) {
				size_t block_off;
				block_off = ftl_band_block_offset_from_addr(breloc->band, addr);
				ftl_reloc_clr_block(breloc, block_off);
				entry.addr = addr;
				entry.lba = move->lba_vector[pos];
				entry.io_flags = FTL_IO_WEAK;

				if (entry.lba != FTL_LBA_INVALID) {
					dest_band = ftl_band_from_addr(dev, move->dest_addr);
					assert(dest_band != breloc->band);

					ftl_update_l2p(reloc->dev, entry.lba, move->dest_addr, addr);
				}

				move->dest_addr.offset++;
				addr.offset++;
				pos++;
			}
		}

		dev->reloc_outstanding--;
		breloc->num_outstanding--;
		move->num_blocks = 0;
		move->num_chunks = 0;
		STAILQ_INSERT_HEAD(&reloc->free_move_queue, move, entry);
	}
}

static int
ftl_reloc_task_poller(void *ctx)
{
	struct ftl_reloc_task *task = ctx;
	struct ftl_reloc_move *moves[FTL_RELOC_MAX_MOVES] = {0};
	struct ftl_reloc *reloc = task->reloc;
	struct ftl_reloc_move *move;
	size_t i, num_outstanding;
	int rc = 0;

	num_outstanding = spdk_ring_dequeue(task->move_queue, (void **)moves, reloc->max_qdepth);

	for (i = 0; i < num_outstanding; ++i) {
		move = moves[i];
		switch (move->state) {
		case FTL_RELOC_STATE_READ:
			rc = ftl_reloc_read(move);
			break;

		case FTL_RELOC_STATE_WRITE:
			rc = ftl_reloc_write(move);
			break;

		default:
			assert(false);
			break;
		}

		if (rc == -ENOMEM) {
			spdk_ring_enqueue(task->move_queue, (void **)&move, 1, NULL);
		}
	}

	return num_outstanding;
}

static void
_ftl_reloc_init_task(void *ctx)
{
	struct ftl_reloc_task *task = ctx;

	task->poller = SPDK_POLLER_REGISTER(ftl_reloc_task_poller, task, 0);
	if (!task->poller) {
		SPDK_ERRLOG("Unable to register reloc poller\n");
	}
}

static struct ftl_reloc_task *
ftl_reloc_init_task(struct ftl_reloc *reloc, struct spdk_thread *thread)
{
	struct ftl_reloc_task *task;
	char pool_name[128];
	int rc;

	task = calloc(1, sizeof(*task));
	if (!task) {
		return NULL;
	}

	task->reloc = reloc;
	task->move_queue = spdk_ring_create(SPDK_RING_TYPE_MP_SC,
					    spdk_align32pow2(reloc->max_qdepth + 1),
					    SPDK_ENV_SOCKET_ID_ANY);
	if (!task->move_queue) {
		SPDK_ERRLOG("Failed to create move queue\n");
		goto error;
	}

	rc = snprintf(pool_name, sizeof(pool_name), "ftl-buffer-%p", task);
	if (rc < 0 || rc >= (int)sizeof(pool_name)) {
		SPDK_ERRLOG("Failed to create reloc moves pool name\n");
		goto error;
	}

	spdk_thread_send_msg(reloc->dev->core_thread, _ftl_reloc_init_task, task);
	return task;
error:
	spdk_ring_free(task->move_queue);
	free(task);
	return NULL;
}

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
	struct ftl_reloc_task *task;
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
	reloc->xfer_size = dev->xfer_size;
	reloc->num_defrag_bands = 0;

	if (reloc->max_qdepth < reloc->max_active) {
		SPDK_ERRLOG("max_qdepth need to be greater or equal max_acitve relocs\n");
	}

	if (dev->xfer_size > FTL_MAX_RELOC_VECTOR_LBA) {
		SPDK_ERRLOG("Reloc vector lba is too small");
		goto error;
	}

	reloc->brelocs = calloc(ftl_get_num_bands(dev), sizeof(*reloc->brelocs));
	if (!reloc->brelocs) {
		goto error;
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

	reloc->payload = spdk_zmalloc(reloc->max_qdepth * reloc->xfer_size * FTL_BLOCK_SIZE,
				      FTL_BLOCK_SIZE, NULL,
				      SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

	if (!reloc->payload) {
		SPDK_ERRLOG("Failed to create reloc buffer pool\n");
		goto error;
	}

	STAILQ_INIT(&reloc->free_move_queue);
	CIRCLEQ_INIT(&reloc->move_cqueue);
	struct ftl_reloc_move *move;
	for (i = 0; i < reloc->max_qdepth; ++i) {
		move = &reloc->move_buffer[i];

		if (_reloc_move_init(reloc, move)) {
			goto error;
		}

		move->data = (char *)reloc->payload + i * FTL_BLOCK_SIZE * dev->xfer_size;
		CIRCLEQ_INSERT_HEAD(&reloc->move_cqueue, move, centry);
		STAILQ_INSERT_TAIL(&reloc->free_move_queue, move, entry);
	}
	reloc->move_iter = CIRCLEQ_FIRST(&reloc->move_cqueue);

	reloc->move_cmpl_queue = spdk_ring_create(SPDK_RING_TYPE_MP_SC,
				 spdk_align32pow2(reloc->max_qdepth + 1),
				 SPDK_ENV_SOCKET_ID_ANY);
	if (!reloc->move_cmpl_queue) {
		SPDK_ERRLOG("Failed to create move completion queue\n");
		goto error;
	}

	for (i = 0; i < ftl_get_num_bands(reloc->dev); ++i) {
		if (ftl_band_reloc_init(reloc, &reloc->brelocs[i], &dev->bands[i])) {
			goto error;
		}
	}

	LIST_INIT(&reloc->band_done);
	TAILQ_INIT(&reloc->pending_queue);
	TAILQ_INIT(&reloc->active_queue);
	TAILQ_INIT(&reloc->prio_queue);
	STAILQ_INIT(&reloc->task_queue);

	task = ftl_reloc_init_task(reloc, dev->core_thread);
	if (!task) {
		goto error;
	}

	STAILQ_INSERT_TAIL(&reloc->task_queue, task, entry);

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

static void
ftl_reloc_free_task(void *ctx)
{
	struct ftl_reloc_task_fini *fini_ctx = ctx;
	struct ftl_reloc_task *task = fini_ctx->task;
	struct ftl_reloc *reloc = task->reloc;

	spdk_poller_unregister(&task->poller);
	spdk_ring_free(task->move_queue);

	task = STAILQ_FIRST(&reloc->task_queue);
	if (task) {
		STAILQ_REMOVE_HEAD(&reloc->task_queue, entry);
		fini_ctx->task = task;
		spdk_thread_send_msg(reloc->dev->core_thread, ftl_reloc_free_task, fini_ctx);
	} else {
		spdk_thread_send_msg(reloc->dev->core_thread, fini_ctx->cb, fini_ctx->cb_arg);
		free(fini_ctx);
	}

	free(task);
}

int
ftl_reloc_free_tasks(struct ftl_reloc *reloc, spdk_msg_fn cb, void *cb_arg)
{
	struct ftl_reloc_task_fini *fini_ctx;
	struct ftl_reloc_task *task;

	task = STAILQ_FIRST(&reloc->task_queue);
	if (!task) {
		cb(cb_arg);
		return 0;
	}

	fini_ctx = calloc(1, sizeof(*fini_ctx));
	if (!fini_ctx) {
		SPDK_ERRLOG("Failed to allocate task: %p finish context\n", task);
		return -1;
	}

	STAILQ_REMOVE_HEAD(&reloc->task_queue, entry);
	fini_ctx->task = task;
	fini_ctx->cb = cb;
	fini_ctx->cb_arg = cb_arg;
	spdk_thread_send_msg(reloc->dev->core_thread,
			ftl_reloc_free_task, fini_ctx);

	return 0;
}

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

	for (i = 0; i < ftl_get_num_bands(reloc->dev); ++i) {
		ftl_band_reloc_free(&reloc->brelocs[i]);
	}

	spdk_ring_free(reloc->move_cmpl_queue);
	spdk_dma_free(reloc->payload);
	free(reloc->move_buffer);
	free(reloc->brelocs);
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
	struct ftl_band_reloc *breloc, *tbreloc;

	_move_run(reloc, reloc->move_iter);
	reloc->move_iter = CIRCLEQ_LOOP_NEXT(&reloc->move_cqueue,
			reloc->move_iter, centry);

	_release_bands(reloc);
	return;

	ftl_reloc_process_move_completions(reloc);

	if (ftl_reloc_is_halted(reloc)) {
		return;
	}

	/* Process first band from priority queue and return */
	breloc = TAILQ_FIRST(&reloc->prio_queue);
	if (breloc) {
		ftl_process_reloc(breloc);
		return;
	}

	TAILQ_FOREACH_SAFE(breloc, &reloc->pending_queue, entry, tbreloc) {
		if (reloc->num_active == reloc->max_active) {
			break;
		}

		/* Wait for band to close before relocating */
		if (breloc->band->state != FTL_BAND_STATE_CLOSED) {
			continue;
		}

		assert(breloc->state == FTL_BAND_RELOC_STATE_PENDING);
		TAILQ_REMOVE(&reloc->pending_queue, breloc, entry);

		/* If reloc preparation fails due to lack of resources try again later */
		if (ftl_reloc_prepare(breloc)) {
			TAILQ_INSERT_HEAD(&reloc->pending_queue, breloc, entry);
			break;
		}

		reloc->num_active++;
	}

	TAILQ_FOREACH_SAFE(breloc, &reloc->active_queue, entry, tbreloc) {
		assert(breloc->state == FTL_BAND_RELOC_STATE_ACTIVE);
		ftl_process_reloc(breloc);
	}
}

bool
ftl_reloc_done(struct ftl_reloc *reloc)
{
	struct ftl_band_reloc *breloc;
	size_t num_outstanding = 0;

	TAILQ_FOREACH(breloc, &reloc->active_queue, entry) {
		num_outstanding += breloc->num_outstanding;
	}

	return num_outstanding == 0;
}

void
ftl_reloc_add(struct ftl_reloc *reloc, struct ftl_band *band, size_t offset,
	      size_t num_blocks, int prio, bool is_defrag)
{
	struct ftl_band_reloc *breloc = &reloc->brelocs[band->id];
	size_t i;

	/* No need to add anything if already at high prio - whole band should be relocated */
	if (!prio && band->high_prio) {
		return;
	}

	pthread_spin_lock(&band->lba_map.lock);
	if (band->lba_map.num_vld == 0) {
		pthread_spin_unlock(&band->lba_map.lock);

		/* If the band is closed and has no valid blocks, free it */
		if (band->state == FTL_BAND_STATE_CLOSED) {
			ftl_band_set_state(band, FTL_BAND_STATE_FREE);
		}

		return;
	}
	pthread_spin_unlock(&band->lba_map.lock);

	for (i = offset; i < offset + num_blocks; ++i) {
		if (spdk_bit_array_get(breloc->reloc_map, i)) {
			continue;
		}
		spdk_bit_array_set(breloc->reloc_map, i);
		breloc->num_blocks++;
	}

	/* If the band is coming from the defrag process, mark it appropriately */
	if (is_defrag) {
		assert(offset == 0 && num_blocks == ftl_get_num_blocks_in_band(band->dev));
		reloc->num_defrag_bands++;
		breloc->defrag = true;
	}

	if (!prio) {
		if (breloc->state == FTL_BAND_RELOC_STATE_INACTIVE) {
			breloc->state = FTL_BAND_RELOC_STATE_PENDING;
			TAILQ_INSERT_HEAD(&reloc->pending_queue, breloc, entry);
		}
	} else {
		bool active = false;
		/* If priority band is already on pending or active queue, remove it from it */
		switch (breloc->state) {
		case FTL_BAND_RELOC_STATE_PENDING:
			TAILQ_REMOVE(&reloc->pending_queue, breloc, entry);
			break;
		case FTL_BAND_RELOC_STATE_ACTIVE:
			active = true;
			TAILQ_REMOVE(&reloc->active_queue, breloc, entry);
			break;
		default:
			break;
		}

		breloc->state = FTL_BAND_RELOC_STATE_HIGH_PRIO;
		TAILQ_INSERT_TAIL(&reloc->prio_queue, breloc, entry);

		/*
		 * If band has been already on active queue it doesn't need any additional
		 * resources
		 */
		if (!active) {
			if (ftl_reloc_prepare(breloc)) {
				SPDK_ERRLOG("Failed to prepare relocation for high prio band\n");
				assert(false);
			}

			reloc->num_active++;
		}
	}
}
