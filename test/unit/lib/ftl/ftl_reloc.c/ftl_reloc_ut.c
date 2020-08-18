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

#include "spdk_cunit.h"
#include "common/lib/ut_multithread.c"

#include "ftl/ftl_reloc.c"
#include "../common/utils.c"

#define MAX_ACTIVE_RELOCS 5
#define MAX_RELOC_QDEPTH  31

struct base_bdev_geometry g_geo = {
	.write_unit_size    = 8,
	.optimal_open_zones = 8,
	.zone_size	    = 1024,
	.blockcnt	    = 1024 * 1024 * 12,
};

DEFINE_STUB(ftl_dev_tail_md_disk_size, size_t, (const struct spdk_ftl_dev *dev), 1);
DEFINE_STUB(ftl_addr_is_written, bool, (struct ftl_band *band, struct ftl_addr addr), true);
DEFINE_STUB(ftl_get_io_channel, struct spdk_io_channel *, (const struct spdk_ftl_dev *dev), NULL);
DEFINE_STUB_V(ftl_band_set_state, (struct ftl_band *band, enum ftl_band_state state));
DEFINE_STUB_V(ftl_free_io, (struct ftl_io *io));
#if defined(DEBUG)
DEFINE_STUB_V(ftl_trace_lba_io_init, (struct spdk_ftl_dev *dev, const struct ftl_io *io));
#endif

void
ftl_band_set_addr_unlocked(struct ftl_band *band, uint64_t lba, struct ftl_addr addr)
{

}

struct ftl_band *
ftl_band_from_addr(struct spdk_ftl_dev *dev, struct ftl_addr addr)
{
	return NULL;

}

bool ftl_update_l2p(struct spdk_ftl_dev *dev, const struct ftl_wbuf_entry *entry,
	       struct ftl_addr addr)
{
	return true;
}

void
ftl_band_set_addr(struct ftl_band *band, uint64_t lba, struct ftl_addr addr)
{

}

int
ftl_band_alloc_lba_map(struct ftl_band *band)
{
	struct spdk_ftl_dev *dev = band->dev;

	ftl_band_acquire_lba_map(band);
	band->lba_map.map = spdk_mempool_get(dev->lba_pool);

	return 0;
}

void
ftl_band_release_lba_map(struct ftl_band *band)
{
	struct spdk_ftl_dev *dev = band->dev;

	band->lba_map.ref_cnt--;
	spdk_mempool_put(dev->lba_pool, band->lba_map.map);
	band->lba_map.map = NULL;
}

void
ftl_band_acquire_lba_map(struct ftl_band *band)
{
	band->lba_map.ref_cnt++;
}

size_t
ftl_lba_map_num_blocks(const struct spdk_ftl_dev *dev)
{
	return spdk_divide_round_up(ftl_get_num_blocks_in_band(dev) * sizeof(uint64_t), FTL_BLOCK_SIZE);
}

int
ftl_band_read_lba_map(struct ftl_band *band, size_t offset,
		      size_t num_blocks, ftl_io_fn fn, void *ctx)
{
	fn(ctx, ctx, 0);
	return 0;
}

uint64_t
ftl_band_block_offset_from_addr(struct ftl_band *band, struct ftl_addr addr)
{
	return test_offset_from_addr(addr, band);
}

struct ftl_addr
ftl_band_addr_from_block_offset(struct ftl_band *band, uint64_t block_off)
{
	struct ftl_addr addr = {};

	addr.offset = block_off + band->id * ftl_get_num_blocks_in_band(band->dev);
	return addr;
}

void
ftl_io_read(struct ftl_io *io)
{
	io->cb_fn(io, io->cb_ctx, 0);
	free(io);
}

void
ftl_io_write(struct ftl_io *io)
{
	io->cb_fn(io, io->cb_ctx, 0);
	free(io->lba.vector);
	free(io);
}

struct ftl_io *
ftl_io_init_internal(const struct ftl_io_init_opts *opts)
{
	struct ftl_io *io = opts->io;

	if (!io) {
		io = calloc(1, opts->size);
	}

	SPDK_CU_ASSERT_FATAL(io != NULL);

	io->dev = opts->dev;
	io->band = opts->band;
	io->flags = opts->flags;
	io->cb_fn = opts->cb_fn;
	io->cb_ctx = io;
	io->num_blocks = opts->num_blocks;
	memcpy(&io->iov, &opts->iovs, sizeof(io->iov));
	io->iov_cnt = opts->iovcnt;

	if (opts->flags & FTL_IO_VECTOR_LBA) {
		io->lba.vector = calloc(io->num_blocks, sizeof(uint64_t));
		SPDK_CU_ASSERT_FATAL(io->lba.vector != NULL);
	}

	return io;
}

struct ftl_io *
ftl_io_alloc(struct spdk_io_channel *ch)
{
	size_t io_size = sizeof(struct ftl_md_io);

	return malloc(io_size);
}

void
ftl_io_reinit(struct ftl_io *io, ftl_io_fn fn, void *ctx, int flags, int type)
{
	io->cb_fn = fn;
	io->cb_ctx = ctx;
	io->type = type;
}

static void
single_reloc_move(struct ftl_band_reloc *breloc)
{
	ftl_reloc_process_moves(breloc);
	poll_threads();
	ftl_reloc_process_move_completions(breloc->parent);
}

static void
add_to_active_queue(struct ftl_reloc *reloc, struct ftl_band_reloc *breloc)
{
	TAILQ_REMOVE(&reloc->pending_queue, breloc, entry);
	breloc->state = FTL_BAND_RELOC_STATE_ACTIVE;
	TAILQ_INSERT_HEAD(&reloc->active_queue, breloc, entry);
	reloc->num_active++;
}

static int
channel_create_cb(void *io_device, void *ctx)
{
	struct _ftl_io_channel *_ioch = ctx;
	struct ftl_io_channel *ioch;
	ioch = calloc(1, sizeof(*ioch));
	SPDK_CU_ASSERT_FATAL(ioch != NULL);

	_ioch->ioch = ioch;

	return 0;
}

static void
channel_destroy_cb(void *io_device, void *ctx)
{}

static void
setup_reloc(struct spdk_ftl_dev **_dev, struct ftl_reloc **_reloc,
	    const struct base_bdev_geometry *geo)
{
	size_t i;
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;

	allocate_threads(16);
	set_thread(0);

	dev = calloc(1, sizeof(*dev));
	SPDK_CU_ASSERT_FATAL(dev != NULL);

	dev->optimal_open_zones = geo->write_unit_size;
	dev->zone_size = geo->zone_size;
	dev->xfer_size = geo->write_unit_size;
	dev->core_thread = spdk_get_thread();
	dev->num_bands = geo->blockcnt / (geo->zone_size * geo->optimal_open_zones);
	dev->bands = calloc(dev->num_bands, sizeof(*dev->bands));
	SPDK_CU_ASSERT_FATAL(dev->bands != NULL);

	dev->lba_pool = spdk_mempool_create("ftl_ut", 2, 0x18000,
					    SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
					    SPDK_ENV_SOCKET_ID_ANY);
	SPDK_CU_ASSERT_FATAL(dev->lba_pool != NULL);

	LIST_INIT(&dev->free_bands);
	LIST_INIT(&dev->shut_bands);

	dev->conf.max_active_relocs = MAX_ACTIVE_RELOCS;
	dev->conf.max_reloc_qdepth = MAX_RELOC_QDEPTH;

	SPDK_CU_ASSERT_FATAL(ftl_get_num_bands(dev) > 0);

	for (i = 0; i < ftl_get_num_bands(dev); ++i) {
		test_init_ftl_band(dev, i, geo->zone_size);
	}

	spdk_io_device_register(dev, channel_create_cb, channel_destroy_cb, 0, NULL);

	reloc = ftl_reloc_init(dev);
	dev->reloc = reloc;
	CU_ASSERT_PTR_NOT_NULL_FATAL(reloc);
	ftl_reloc_resume(reloc);

	*_dev = dev;
	*_reloc = reloc;
}

static void free_reloc_task_cb(void *ctx)
{}

static void
cleanup_reloc(struct spdk_ftl_dev *dev, struct ftl_reloc *reloc)
{
	size_t i;

	for (i = 0; i < ftl_get_num_bands(reloc->dev); ++i) {
		SPDK_CU_ASSERT_FATAL(reloc->brelocs[i].state == FTL_BAND_RELOC_STATE_INACTIVE);
	}

	ftl_reloc_free_tasks(reloc, free_reloc_task_cb, NULL);
	poll_threads();
	ftl_reloc_free(reloc);

	for (i = 0; i < ftl_get_num_bands(dev); ++i) {
		test_free_ftl_band(&dev->bands[i]);
	}

	free(dev->bands);
	spdk_mempool_free(dev->lba_pool);
	spdk_io_device_unregister(dev, NULL);
	free_threads();
	free(dev);
}

static void
set_band_valid_map(struct ftl_band *band, size_t offset, size_t num_blocks)
{
	struct ftl_lba_map *lba_map = &band->lba_map;
	size_t i;

	SPDK_CU_ASSERT_FATAL(lba_map != NULL);
	for (i = offset; i < offset + num_blocks; ++i) {
		spdk_bit_array_set(lba_map->vld, i);
		lba_map->num_vld++;
	}
}

static void
test_reloc_iter_full(void)
{
	size_t num_blocks, num_iters, reminder, i;
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;
	struct ftl_band_reloc *breloc;
	struct ftl_band *band;
	struct ftl_addr addr;

	setup_reloc(&dev, &reloc, &g_geo);

	breloc = &reloc->brelocs[0];
	band = breloc->band;

	set_band_valid_map(band, 0, ftl_get_num_blocks_in_band(dev));

	ftl_reloc_add(reloc, band, 0, ftl_get_num_blocks_in_band(dev), 0, true);

	CU_ASSERT_EQUAL(breloc->num_blocks, ftl_get_num_blocks_in_band(dev));

	num_iters = ftl_get_num_punits(dev) *
		    (ftl_get_num_blocks_in_zone(dev) / reloc->xfer_size);

	for (i = 0; i < num_iters; i++) {
		num_blocks = ftl_reloc_next_blocks(breloc, &addr, reloc->xfer_size);
		CU_ASSERT_EQUAL(num_blocks, reloc->xfer_size);
	}

	num_iters = ftl_get_num_punits(dev);

	/* ftl_reloc_next_blocks is searching for maximum xfer_size */
	/* contiguous valid logic blocks in zone, so we can end up */
	/* with some reminder if number of logical blocks in zone */
	/* is not divisible by xfer_size */
	reminder = ftl_get_num_blocks_in_zone(dev) % reloc->xfer_size;
	for (i = 0; i < num_iters; i++) {
		num_blocks = ftl_reloc_next_blocks(breloc, &addr, reloc->xfer_size);
		CU_ASSERT_EQUAL(reminder, num_blocks);
	}

	/* num_blocks should remain intact since all the blocks are valid */
	CU_ASSERT_EQUAL(breloc->num_blocks, ftl_get_num_blocks_in_band(dev));
	breloc->state = FTL_BAND_RELOC_STATE_INACTIVE;

	cleanup_reloc(dev, reloc);
}

static void
test_reloc_empty_band(void)
{
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;
	struct ftl_band_reloc *breloc;
	struct ftl_band *band;

	setup_reloc(&dev, &reloc, &g_geo);

	breloc = &reloc->brelocs[0];
	band = breloc->band;

	ftl_reloc_add(reloc, band, 0, ftl_get_num_blocks_in_band(dev), 0, true);

	CU_ASSERT_EQUAL(breloc->num_blocks, 0);

	cleanup_reloc(dev, reloc);
}

static void
test_reloc_full_band(void)
{
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;
	struct ftl_band_reloc *breloc;
	struct ftl_band *band;
	size_t num_moves, num_iters, num_block, i;

	setup_reloc(&dev, &reloc, &g_geo);

	breloc = &reloc->brelocs[0];
	band = breloc->band;
	num_moves = MAX_RELOC_QDEPTH * reloc->xfer_size;
	num_iters = ftl_get_num_blocks_in_band(dev) / num_moves;

	set_band_valid_map(band, 0, ftl_get_num_blocks_in_band(dev));

	ftl_reloc_add(reloc, band, 0, ftl_get_num_blocks_in_band(dev), 0, true);

	CU_ASSERT_EQUAL(breloc->num_blocks, ftl_get_num_blocks_in_band(dev));

	ftl_reloc_prepare(breloc);
	add_to_active_queue(reloc, breloc);

	for (i = 1; i <= num_iters; ++i) {
		single_reloc_move(breloc);
		num_block = ftl_get_num_blocks_in_band(dev) - (i * num_moves);
		CU_ASSERT_EQUAL(breloc->num_blocks, num_block);

	}

	/*  Process reminder blocks */
	single_reloc_move(breloc);

	CU_ASSERT_EQUAL(breloc->num_blocks, 0);
	ftl_reloc_release(breloc);

	cleanup_reloc(dev, reloc);
}

static void
test_reloc_scatter_band(void)
{
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;
	struct ftl_band_reloc *breloc;
	struct ftl_band *band;
	size_t num_iters, i;

	setup_reloc(&dev, &reloc, &g_geo);

	breloc = &reloc->brelocs[0];
	band = breloc->band;
	num_iters = spdk_divide_round_up(ftl_get_num_blocks_in_band(dev), MAX_RELOC_QDEPTH * 2);

	for (i = 0; i < ftl_get_num_blocks_in_band(dev); ++i) {
		if (i % 2) {
			set_band_valid_map(band, i, 1);
		}
	}

	ftl_reloc_add(reloc, band, 0, ftl_get_num_blocks_in_band(dev), 0, true);
	ftl_reloc_prepare(breloc);
	add_to_active_queue(reloc, breloc);

	CU_ASSERT_EQUAL(breloc->num_blocks, ftl_get_num_blocks_in_band(dev));

	for (i = 0; i < num_iters ; ++i) {
		single_reloc_move(breloc);
	}

	CU_ASSERT_EQUAL(breloc->num_blocks, 0);
	ftl_reloc_release(breloc);

	cleanup_reloc(dev, reloc);
}

static void
test_reloc_zone(void)
{
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;
	struct ftl_band_reloc *breloc;
	struct ftl_band *band;
	size_t num_io, num_iters, num_block, i;

	setup_reloc(&dev, &reloc, &g_geo);

	breloc = &reloc->brelocs[0];
	band = breloc->band;
	/* High priority band have allocated lba map */
	band->high_prio = 1;
	ftl_band_alloc_lba_map(band);
	num_io = MAX_RELOC_QDEPTH * reloc->xfer_size;
	num_iters = ftl_get_num_blocks_in_zone(dev) / num_io;

	set_band_valid_map(band, 0, ftl_get_num_blocks_in_band(dev));

	ftl_reloc_add(reloc, band, ftl_get_num_blocks_in_zone(dev) * 3,
		      ftl_get_num_blocks_in_zone(dev), 1, false);
	add_to_active_queue(reloc, breloc);

	CU_ASSERT_EQUAL(breloc->num_blocks, ftl_get_num_blocks_in_zone(dev));

	for (i = 1; i <= num_iters ; ++i) {
		single_reloc_move(breloc);
		num_block = ftl_get_num_blocks_in_zone(dev) - (i * num_io);

		CU_ASSERT_EQUAL(breloc->num_blocks, num_block);
	}

	/* In case num_blocks_in_zone % num_io != 0 one extra iteration is needed  */
	single_reloc_move(breloc);

	CU_ASSERT_EQUAL(breloc->num_blocks, 0);
	ftl_reloc_release(breloc);

	cleanup_reloc(dev, reloc);
}

static void
test_reloc_single_block(void)
{
	struct spdk_ftl_dev *dev;
	struct ftl_reloc *reloc;
	struct ftl_band_reloc *breloc;
	struct ftl_band *band;
#define TEST_RELOC_OFFSET 6

	setup_reloc(&dev, &reloc, &g_geo);

	breloc = &reloc->brelocs[0];
	band = breloc->band;

	set_band_valid_map(band, TEST_RELOC_OFFSET, 1);

	ftl_reloc_add(reloc, band, TEST_RELOC_OFFSET, 1, 0, false);
	SPDK_CU_ASSERT_FATAL(breloc == TAILQ_FIRST(&reloc->pending_queue));
	ftl_reloc_prepare(breloc);
	add_to_active_queue(reloc, breloc);

	CU_ASSERT_EQUAL(breloc->num_blocks, 1);

	single_reloc_move(breloc);

	CU_ASSERT_EQUAL(breloc->num_blocks, 0);
	ftl_reloc_release(breloc);

	cleanup_reloc(dev, reloc);
}

int
main(int argc, char **argv)
{
	CU_pSuite suite = NULL;
	unsigned int num_failures;

	CU_set_error_action(CUEA_ABORT);
	CU_initialize_registry();

	suite = CU_add_suite("ftl_band_suite", NULL, NULL);


	CU_ADD_TEST(suite, test_reloc_iter_full);
	CU_ADD_TEST(suite, test_reloc_empty_band);
	CU_ADD_TEST(suite, test_reloc_full_band);
	CU_ADD_TEST(suite, test_reloc_scatter_band);
	CU_ADD_TEST(suite, test_reloc_zone);
	CU_ADD_TEST(suite, test_reloc_single_block);

	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	num_failures = CU_get_number_of_failures();
	CU_cleanup_registry();

	return num_failures;
}
