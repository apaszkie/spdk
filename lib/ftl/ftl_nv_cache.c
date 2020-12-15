
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

#include "ftl_nv_cache.h"

#include "ftl_core.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/ftl.h"
#include "spdk/log.h"

#define FTL_NV_CACHE_CHUNK_DATA_SIZE(xfer_size) (((uint64_t)xfer_size * 4ULL) * FTL_BLOCK_SIZE)
#define FTL_NV_CACHE_CHUNK_META_SIZE 64ULL
#define FTL_NV_CACHE_CHUNK_SIZE(xfer_size) \
	(FTL_NV_CACHE_CHUNK_DATA_SIZE(xfer_size) + FTL_NV_CACHE_CHUNK_META_SIZE)

SPDK_STATIC_ASSERT(
	sizeof(struct ftl_nv_cache_chunk) <= FTL_NV_CACHE_CHUNK_META_SIZE,
	"FTL NV Chunk metadata struct too big");

static struct ftl_nv_cache_compaction *compaction_alloc(
	struct spdk_ftl_dev *dev);

static void compaction_free(struct spdk_ftl_dev *dev,
			    struct ftl_nv_cache_compaction *compaction);

static void compaction_process_ftl_done(struct ftl_rq *rq);

static void compaction_process_start(
	struct ftl_nv_cache_compaction *compaction);

static inline uint32_t __spdk_bdev_get_md_size(const struct spdk_bdev *bdev)
{
	return 64;
}

static void __load_vss(struct spdk_ftl_dev *dev,
		struct ftl_nv_cache_block_metadata *vss,
		int64_t vss_size, int64_t vss_md_size)
{
	uint64_t i;

	for (i = 0; i < dev->num_lbas; i++) {
		struct ftl_addr addr = ftl_l2p_get(dev, i);

		if (addr.offset == FTL_ADDR_INVALID) {
			continue;
		}

		if (addr.cached) {
			assert(addr.cache_offset < vss_size);
			vss[addr.cache_offset].lba = i;
		}
	}
}

static int __spdk_bdev_readv_blocks_with_md(
	struct ftl_nv_cache *nv_cache,
	struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
	struct iovec *iov, int iovcnt, void *md_buf,
	uint64_t offset_blocks, uint64_t num_blocks,
	spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	void *iter_buf = md_buf;
	int64_t iter_offset = offset_blocks;
	int64_t iter_offset_end = offset_blocks + num_blocks;

	while (iter_offset < iter_offset_end) {
		struct ftl_nv_cache_block_metadata *md = iter_buf;

		assert(iter_offset < nv_cache->vss_size);
		md->lba = nv_cache->vss[iter_offset].lba;

		iter_offset++;
		iter_buf += nv_cache->vss_md_size;
	}

	return spdk_bdev_readv_blocks(desc, ch, iov, iovcnt,
			offset_blocks, num_blocks,
			cb, cb_arg);
}

static int __spdk_bdev_read_blocks_with_md(
	struct ftl_nv_cache *nv_cache,
	struct spdk_bdev_desc *desc, struct spdk_io_channel *ch, void *buf,
	void *md_buf, int64_t offset_blocks, uint64_t num_blocks,
	spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	void *iter_buf = md_buf;
	int64_t iter_offset = offset_blocks;
	int64_t iter_offset_end = offset_blocks + num_blocks;

	while (iter_offset < iter_offset_end) {
		struct ftl_nv_cache_block_metadata *md = iter_buf;

		assert(iter_offset < nv_cache->vss_size);
		md->lba = nv_cache->vss[iter_offset].lba;

		iter_offset++;
		iter_buf += nv_cache->vss_md_size;
	}

	return spdk_bdev_read_blocks(desc, ch, buf, offset_blocks, num_blocks, cb,
				     cb_arg);
}

static int __spdk_bdev_write_blocks_with_md(
	struct ftl_nv_cache *nv_cache,
	struct spdk_bdev_desc *desc, struct spdk_io_channel *ch, void *buf,
	void *md_buf, int64_t offset_blocks, uint64_t num_blocks,
	spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	void *iter_buf = md_buf;
	int64_t iter_offset = offset_blocks;
	int64_t iter_offset_end = offset_blocks + num_blocks;

	while (iter_offset < iter_offset_end) {
		struct ftl_nv_cache_block_metadata *md = iter_buf;

		assert(iter_offset < nv_cache->vss_size);
		nv_cache->vss[iter_offset].lba = md->lba;

		iter_offset++;
		iter_buf += nv_cache->vss_md_size;
	}

	return spdk_bdev_write_blocks(desc, ch, buf, offset_blocks, num_blocks, cb,
				      cb_arg);
}

static void _nv_cache_bdev_event_cb(enum spdk_bdev_event_type type,
				    struct spdk_bdev *bdev, void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		assert(0);
		break;
	default:
		break;
	}
}

/* Dummy bdev module used to to claim bdevs. */
static struct spdk_bdev_module _ftl_bdev_nv_cache_module = {
	.name = "ftl_lib_nv_cache",
};

void ftl_nv_cache_deinit(struct spdk_ftl_dev *dev)
{
	struct ftl_nv_cache_compaction *compaction;
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;

	while(!TAILQ_EMPTY(&nv_cache->compaction_list)) {
		compaction = TAILQ_FIRST(&nv_cache->compaction_list);
		TAILQ_REMOVE(&nv_cache->compaction_list, compaction, entry);

		compaction_free(dev, compaction);
	}

	if (dev->nv_cache.cache_ioch) {
		spdk_put_io_channel(dev->nv_cache.cache_ioch);
		dev->nv_cache.cache_ioch = NULL;
	}

	if (dev->nv_cache.bdev_desc) {
		spdk_bdev_module_release_bdev(
			spdk_bdev_desc_get_bdev(dev->nv_cache.bdev_desc));
		spdk_bdev_close(dev->nv_cache.bdev_desc);
	}

	if (nv_cache->md_pool) {
		if (nv_cache->md_rd) {
			//spdk_mempool_put(nv_cache->md_pool, nv_cache->md_rd);
			nv_cache->md_rd = NULL;
		}

		spdk_mempool_free(nv_cache->md_pool);
		nv_cache->md_pool = NULL;
	}

	if (nv_cache->vss) {
		free(nv_cache->vss);
		nv_cache->vss = NULL;
	}

	if (nv_cache->chunk) {
		free(nv_cache->chunk);
		nv_cache->chunk = NULL;
	}
}

int ftl_nv_cache_init(struct spdk_ftl_dev *dev, const char *bdev_name)
{
	struct spdk_bdev *bdev;
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;
	struct spdk_ftl_conf *conf = &dev->conf;
	char pool_name[128];
	int rc;
	uint64_t i, blocks, offset;

	if (!bdev_name) {
		return 0;
	}

	nv_cache->ftl_dev = dev;
	bdev = spdk_bdev_get_by_name(bdev_name);
	if (!bdev) {
		SPDK_ERRLOG("Unable to find bdev: %s\n", bdev_name);
		return -1;
	}

	if (spdk_bdev_open_ext(bdev_name, true, _nv_cache_bdev_event_cb, dev,
			       &nv_cache->bdev_desc)) {
		SPDK_ERRLOG("Unable to open bdev: %s\n", bdev_name);
		return -1;
	}

	if (spdk_bdev_module_claim_bdev(bdev, nv_cache->bdev_desc,
					&_ftl_bdev_nv_cache_module)) {
		spdk_bdev_close(nv_cache->bdev_desc);
		nv_cache->bdev_desc = NULL;
		SPDK_ERRLOG("Unable to claim bdev %s\n", bdev_name);
		return -1;
	}

	SPDK_INFOLOG(SPDK_LOG_FTL_INIT, "Using %s as write buffer cache\n",
		     spdk_bdev_get_name(bdev));

	if (spdk_bdev_get_block_size(bdev) != FTL_BLOCK_SIZE) {
		SPDK_ERRLOG("Unsupported block size (%d)\n",
			    spdk_bdev_get_block_size(bdev));
		return -1;
	}

	nv_cache->cache_ioch = spdk_bdev_get_io_channel(nv_cache->bdev_desc);
	if (!nv_cache->cache_ioch) {
		SPDK_ERRLOG("Failed to create cache IO channel for NV Cache\n");
		return -1;
	}

	//	if (!spdk_bdev_is_md_separate(bdev)) {
	//		SPDK_ERRLOG("Bdev %s doesn't support separate metadata buffer
	//IO\n", 			    spdk_bdev_get_name(bdev)); 		return -1;
	//	}
	//
	//	if (____spdk_bdev_get_md_size(bdev) < sizeof(struct
	//ftl_nv_cache_block_metadata)) { 		SPDK_ERRLOG("Bdev's %s metadata is too small
	//(%"PRIu32")\n", 			    spdk_bdev_get_name(bdev), ____spdk_bdev_get_md_size(bdev));
	//		return -1;
	//	}
	//
	//	if (spdk_bdev_get_dif_type(bdev) != SPDK_DIF_DISABLE) {
	//		SPDK_ERRLOG("Unsupported DIF type used by bdev %s\n",
	//			    spdk_bdev_get_name(bdev));
	//		return -1;
	//	}

	/* The cache needs to be capable of storing at least two full bands. This
	 * requirement comes from the fact that cache works as a protection against
	 * power loss, so before the data inside the cache can be overwritten, the
	 * band it's stored on has to be closed. Plus one extra block is needed to
	 * store the header.
	 */
	if (spdk_bdev_get_num_blocks(bdev) <
	    ftl_get_num_blocks_in_band(dev) * 2 + 1) {
		SPDK_ERRLOG(
			"Insufficient number of blocks for write buffer cache (available: "
			"%" PRIu64 ", required: %" PRIu64 ")\n",
			spdk_bdev_get_num_blocks(bdev),
			ftl_get_num_blocks_in_band(dev) * 2 + 1);
		return -1;
	}

	rc = snprintf(pool_name, sizeof(pool_name), "ftl-nvpool-%p", dev);
	if (rc < 0 || rc >= 128) {
		return -1;
	}

	nv_cache->md_size = __spdk_bdev_get_md_size(bdev);
	nv_cache->md_pool = spdk_mempool_create(
				    pool_name, conf->nv_cache.max_request_cnt * 2,
				    nv_cache->md_size * conf->nv_cache.max_request_size,
				    SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
	if (!nv_cache->md_pool) {
		SPDK_ERRLOG("Failed to initialize non-volatile cache metadata pool\n");
		return -1;
	}

	nv_cache->md_rd = spdk_mempool_get(dev->nv_cache.md_pool);
	if (!nv_cache->md_rd) {
		SPDK_ERRLOG("Failed to initialize non-volatile cache metadata for reads\n");
		return -1;
	}

	/*
	 * Calculate chunk size
	 */
	nv_cache->chunk_size = FTL_NV_CACHE_CHUNK_SIZE(dev->xfer_size) / FTL_BLOCK_SIZE;

	/*
	 * Calculate number of chunks
	 */
	blocks = spdk_bdev_get_num_blocks(bdev);
	nv_cache->chunk_count = (blocks * FTL_BLOCK_SIZE) /
			FTL_NV_CACHE_CHUNK_SIZE(dev->xfer_size);
	nv_cache->num_data_blocks =  nv_cache->chunk_count *
			nv_cache->chunk_size;

	/*
	 * Calculate number of blocks for NV cache super block
	 */
	nv_cache->num_meta_blocks = blocks - nv_cache->num_data_blocks;

	nv_cache->ready = false;

	nv_cache->vss = calloc(blocks, sizeof(nv_cache->vss[0]));
	for (i = 0; i < blocks; i++) {
		nv_cache->vss[i].lba = FTL_LBA_INVALID;
	}
	nv_cache->vss_size = blocks;
	nv_cache->vss_md_size = __spdk_bdev_get_md_size(bdev);

	TAILQ_INIT(&nv_cache->chunk_free_list);
	TAILQ_INIT(&nv_cache->chunk_full_list);

	/* Allocate memory for chunks with alignment to block */

	nv_cache->chunk = calloc(nv_cache->num_meta_blocks, FTL_BLOCK_SIZE);
	assert(nv_cache->num_meta_blocks * FTL_BLOCK_SIZE >=
			nv_cache->chunk_count * sizeof(nv_cache->chunk[i]));
	nv_cache->chunk_free_count = nv_cache->chunk_count;
	if (!nv_cache->chunk) {
		SPDK_ERRLOG("Cannot allocate memory for chunks\n");
		return -1;
	}

	offset = nv_cache->num_meta_blocks;
	for (i = 0; i < nv_cache->chunk_count; i++) {
		struct ftl_nv_cache_chunk *chunk = &nv_cache->chunk[i];

		chunk->offset = offset;
		offset += nv_cache->chunk_size;
		TAILQ_INSERT_TAIL(&nv_cache->chunk_free_list, chunk, entry);
	}

	/* Start compaction when full chunks exceed given % of entire chunks */
	nv_cache->chunk_compaction_threshold = nv_cache->chunk_count * 8 / 10;
	TAILQ_INIT(&nv_cache->compaction_list);
	for (i = 0; i < 256; i++) {
		struct ftl_nv_cache_compaction *compact = compaction_alloc(dev);

		if (!compact) {
			SPDK_ERRLOG("Cannot allocate compaction process\n");
			return -1;
		}

		TAILQ_INSERT_TAIL(&nv_cache->compaction_list, compact, entry);
	}

	/*
	 * TODO(mbarczak) Check if end of NV cache superblock does not cross
	 * the device write boundary
	 */

	return 0;
}

static bool _is_compaction_required(struct ftl_nv_cache *nv_cache)
{
	uint64_t full;

	if (nv_cache->ftl_dev->halt) {
		return false;
	}

	full = nv_cache->chunk_full_count - nv_cache->compaction_active_count;
	if (full > nv_cache->chunk_compaction_threshold) {
		return true;
	}

	return false;
}

static bool is_chunk_compacted(struct ftl_nv_cache_chunk *chunk)
{
	uint64_t blocks_to_compact = chunk->blocks_written - chunk->blocks_skipped;

	assert(chunk->blocks_written != 0);

	if (blocks_to_compact == chunk->blocks_compacted) {
		return true;
	}

	return false;
}

static bool is_chunk_to_read(struct ftl_nv_cache_chunk *chunk)
{
	uint64_t blocks_to_read = chunk->blocks_written - chunk->blocks_skipped;

	assert(chunk->blocks_written != 0);

	if (blocks_to_read == chunk->read_pointer) {
		return false;
	}

	return true;
}

static uint64_t chunk_blocks_to_read(struct ftl_nv_cache_chunk *chunk)
{
	uint64_t blocks_written;
	uint64_t blocks_to_read;

	assert(chunk->blocks_written >= chunk->blocks_skipped);
	blocks_written = chunk->blocks_written - chunk->blocks_skipped;

	assert(blocks_written >= chunk->read_pointer);
	blocks_to_read = blocks_written - chunk->read_pointer;

	return blocks_to_read;
}

static struct ftl_nv_cache_chunk *get_chunk_for_compaction(
	struct ftl_nv_cache_compaction *compaction)
{
	struct ftl_nv_cache *nv_cache = compaction->nv_cache;
	struct ftl_nv_cache_chunk *chunk = NULL;

	if (!TAILQ_EMPTY(&compaction->chunk_list)) {
		chunk = TAILQ_FIRST(&compaction->chunk_list);
		if (is_chunk_to_read(chunk)) {
			return chunk;
		}
	}

	if (!TAILQ_EMPTY(&nv_cache->chunk_full_list)) {
		chunk = TAILQ_FIRST(&nv_cache->chunk_full_list);
		TAILQ_REMOVE(&nv_cache->chunk_full_list, chunk, entry);

		assert(chunk->write_pointer);
	} else {
		return NULL;
	}

	if (spdk_likely(chunk)) {
		assert(chunk->write_pointer != 0);
		TAILQ_INSERT_HEAD(&compaction->chunk_list, chunk, entry);

		compaction->rd->iter.idx = 0;
		compaction->rd->iter.count = 0;
		compaction->rd_ptr = 0;
	}

	return chunk;
}

static void chunk_compaction_advance(struct ftl_nv_cache_compaction *compaction,
				     struct ftl_nv_cache_chunk *chunk)
{
	uint64_t offset = chunk->offset;
	struct ftl_nv_cache *nv_cache = compaction->nv_cache;

	chunk->blocks_compacted++;
	if (!is_chunk_compacted(chunk)) {
		return;
	}

	TAILQ_REMOVE(&compaction->chunk_list, chunk, entry);
	/* Reset chunk */
	offset = chunk->offset;
	memset(chunk, 0, sizeof(*chunk));
	chunk->offset = offset;

	TAILQ_INSERT_TAIL(&nv_cache->chunk_free_list, chunk, entry);
	nv_cache->chunk_free_count++;
	nv_cache->chunk_full_count--;
}

static uint64_t _chunk_get_free_space(struct ftl_nv_cache *nv_cache,
				      struct ftl_nv_cache_chunk *chunk)
{
	if (spdk_likely(chunk->write_pointer <= nv_cache->chunk_size)) {
		return nv_cache->chunk_size - chunk->write_pointer;
	} else {
		assert(0);
		return 0;
	}
}

static void _chunk_skip_blocks(struct ftl_nv_cache *nv_cache,
			       struct ftl_nv_cache_chunk *chunk,
			       uint64_t skipped_blocks)
{
	nv_cache->chunk_current = NULL;

	if (0 == skipped_blocks) {
		return;
	}

	chunk->blocks_skipped = skipped_blocks;
	chunk->blocks_written += skipped_blocks;

	if (chunk->blocks_written == nv_cache->chunk_size) {
		/* Chunk full move it on full list */
		TAILQ_INSERT_TAIL(&nv_cache->chunk_full_list, chunk, entry);
		nv_cache->chunk_full_count++;
	} else if (spdk_unlikely(chunk->blocks_written > nv_cache->chunk_size)) {
		assert(0);
	}
}

static void _chunk_advance_blocks(struct ftl_nv_cache *nv_cache,
				  struct ftl_nv_cache_chunk *chunk,
				  uint64_t advanced_blocks)
{
	chunk->blocks_written += advanced_blocks;

	if (chunk->blocks_written == nv_cache->chunk_size) {
		/* Chunk full move it on full list */
		TAILQ_INSERT_TAIL(&nv_cache->chunk_full_list, chunk, entry);
		nv_cache->chunk_full_count++;
	} else if (spdk_unlikely(chunk->blocks_written > nv_cache->chunk_size)) {
		assert(0);
	}
}

uint64_t ftl_nv_cache_get_wr_buffer(struct ftl_nv_cache *nv_cache,
				    struct ftl_io *io)
{
	uint64_t address = FTL_LBA_INVALID;
	uint64_t num_blocks = ftl_io_iovec_len_left(io);
	uint64_t free_space;
	struct ftl_nv_cache_chunk *chunk;

AGAIN:
	chunk = nv_cache->chunk_current;
	if (!chunk) {
		if (!TAILQ_EMPTY(&nv_cache->chunk_free_list)) {
			chunk = TAILQ_FIRST(&nv_cache->chunk_free_list);
			TAILQ_REMOVE(&nv_cache->chunk_free_list, chunk, entry);
			nv_cache->chunk_free_count--;
			nv_cache->chunk_current = chunk;
		} else {
			goto END;
		}
	}

	free_space = _chunk_get_free_space(nv_cache, chunk);

	if (free_space >= num_blocks) {
		/* Enough space in chunk */

		/* Calculate address in NV cache */
		address = chunk->offset + chunk->write_pointer;

		/* Set chunk in IO */
		io->nv_cache_chunk = chunk;

		/* Move write pointer */
		chunk->write_pointer += num_blocks;
	} else {
		/* Not enough space in nv_cache_chunk */
		_chunk_skip_blocks(nv_cache, chunk, free_space);
		goto AGAIN;
	}

END:
	if (address != FTL_LBA_INVALID) {
		nv_cache->load_blocks += num_blocks;
	}

	return address;
}

void ftl_nv_cache_commit_wr_buffer(struct ftl_nv_cache *nv_cache,
				   struct ftl_io *io)
{
	if (!io->nv_cache_chunk) {
		/* No chunk, nothing to do */
		return;
	}

	assert(nv_cache->load_blocks >= io->num_blocks);
	nv_cache->load_blocks -= io->num_blocks;

	_chunk_advance_blocks(nv_cache, io->nv_cache_chunk, io->num_blocks);
	io->nv_cache_chunk = NULL;
}

static void compaction_free(struct spdk_ftl_dev *dev,
			    struct ftl_nv_cache_compaction *compaction)
{
	if (!compaction) {
		return;
	}

	ftl_rq_del(compaction->wr);
	ftl_rq_del(compaction->rd);
	free(compaction);
}

static void compaction_process_ftl_done(struct ftl_rq *rq)
{
	struct spdk_ftl_dev *dev = rq->dev;
	struct ftl_nv_cache_compaction *compaction = rq->owner.priv;
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;
	struct ftl_rq_entry *entry = rq->entries;
	uint64_t i;

	if (spdk_unlikely(false == rq->success)) {
		/* IO error retry writing */
		ftl_writer_queue_rq(&dev->writer_user, rq);
		return;
	}

	/* Update L2P table */
	ftl_rq_update_l2p(rq);

	for (i = 0; i < rq->num_blocks; i++, entry++) {
		struct ftl_nv_cache_chunk *chunk = entry->owner.priv;
		chunk_compaction_advance(compaction, chunk);
	}

	compaction->wr->iter.idx = 0;

	if (_is_compaction_required(nv_cache)) {
		compaction_process_start(compaction);
	} else {
		nv_cache->compaction_active_count--;
		TAILQ_INSERT_TAIL(&nv_cache->compaction_list, compaction, entry);
	}
}

static void compaction_process_finish_read(struct ftl_nv_cache_compaction *compaction)
{
	struct spdk_ftl_dev *dev = compaction->nv_cache->ftl_dev;
	struct ftl_rq_entry *iter;
	const uint64_t num_entries = compaction->wr->num_blocks;
	struct ftl_nv_cache_block_metadata *md;
	struct ftl_addr current_addr = ftl_to_addr(FTL_ADDR_INVALID);
	struct ftl_addr cached_addr = { .cached = true };

	assert(compaction->wr->iter.idx < num_entries);
	assert(compaction->rd->iter.idx < compaction->rd->iter.count);

	iter = &compaction->wr->entries[compaction->wr->iter.idx];

	/*
	 * Calculate current position within cache for the chunk:
	 * - Chunk rd_ptr
	 * - Plus offset within reader (read pointer - read count)
	 * - Plus current reader index of iterator
	 */
	cached_addr.cache_offset = compaction->current_chunk->offset;
	cached_addr.cache_offset += compaction->rd_ptr;
	cached_addr.cache_offset -= compaction->rd->iter.count;
	cached_addr.cache_offset += compaction->rd->iter.idx;

	while (compaction->wr->iter.idx < num_entries &&
		compaction->rd->iter.idx < compaction->rd->iter.count) {

		/* Get metadata */
		md = compaction->rd->entries[compaction->rd->iter.idx].io_md;
		if (md->lba == FTL_LBA_INVALID) {
			cached_addr.cache_offset++;
			compaction->rd->iter.idx++;
			continue;
		}

		current_addr = ftl_l2p_get(dev, md->lba);
		if (ftl_addr_cmp(current_addr, cached_addr)) {
			/*
			 * Swap payload
			 */
			ftl_rq_swap_payload(compaction->wr, compaction->wr->iter.idx,
					compaction->rd, compaction->rd->iter.idx);
			/*
			 * Address still the same, we may continue to compact it back to
			 * FTL, set valid number of entries within this batch
			 */
			iter->addr = current_addr;
			iter->owner.priv = compaction->current_chunk;
			iter->lba = md->lba;

			/* Advance within batch */
			iter++;
			compaction->wr->iter.idx++;
		} else {
			/* This address already invalidated, just omit this block */
			chunk_compaction_advance(compaction,
					compaction->current_chunk);
		}

		/* Advance within reader */
		compaction->rd->iter.idx++;
		cached_addr.cache_offset++;
	}

	if (num_entries == compaction->wr->iter.idx) {
		/*
		 *Request contains data to be placed on FTL, compact it
		 */
		ftl_writer_queue_rq(&dev->writer_user, compaction->wr);
	} else {
		struct ftl_nv_cache *nv_cache = compaction->nv_cache;

		if (_is_compaction_required(compaction->nv_cache)) {
			compaction_process_start(compaction);
		} else {
			nv_cache->compaction_active_count--;
			TAILQ_INSERT_HEAD(&nv_cache->compaction_list, compaction, entry);
		}
	}
}

static void compaction_process_read_cb(struct spdk_bdev_io *bdev_io,
				       bool success, void *cb_arg)
{
	struct ftl_nv_cache_compaction *compaction = cb_arg;

	spdk_bdev_free_io(bdev_io);
	compaction_process_finish_read(compaction);
}

static void msg_compaction_process_start(void *compaction)
{
	compaction_process_start(compaction);
}

static void compaction_process_start(
	struct ftl_nv_cache_compaction *compaction)
{
	int rc;
	struct ftl_nv_cache *nv_cache = compaction->nv_cache;
	struct ftl_nv_cache_chunk *chunk;
	uint64_t to_read, addr;

	/* Check if all read blocks done */
	assert(compaction->rd->iter.idx <= compaction->rd->iter.count);
	if (compaction->rd->iter.idx < compaction->rd->iter.count) {
		compaction_process_finish_read(compaction);
		return;
	}

	/*
	 * Get currently handled chunk
	 */
	chunk = get_chunk_for_compaction(compaction);
	if (!chunk) {
		nv_cache->compaction_active_count--;
		TAILQ_INSERT_HEAD(&nv_cache->compaction_list, compaction, entry);
		return;
	}
	compaction->current_chunk = chunk;

	/*
	 * Get numbers of blocks to read
	 */
	compaction->rd->iter.count = chunk_blocks_to_read(chunk);
	to_read = compaction->rd->iter.count = spdk_min(
			compaction->rd->iter.count,
			compaction->rd->num_blocks);
	compaction->rd->iter.idx = 0;

	/* Read data and metadata from NV cache */
	addr = chunk->offset + compaction->rd_ptr;
	rc = __spdk_bdev_readv_blocks_with_md(nv_cache, nv_cache->bdev_desc,
			nv_cache->cache_ioch,
			compaction->rd->io_vec, to_read,
			compaction->rd->io_md,
			addr, to_read,
			compaction_process_read_cb, compaction);

	if (spdk_likely(rc)) {
		spdk_thread_send_msg(spdk_get_thread(),
				msg_compaction_process_start,
				compaction);
		/*
		 * TODO(mbarczak) We need to repeat read procedure
		 */

		return;
	}

	/*
	 * Move read pointer in chunk and reader
	 */
	chunk->read_pointer += to_read;
	compaction->rd_ptr += to_read;
}

static struct ftl_nv_cache_compaction *compaction_alloc(
	struct spdk_ftl_dev *dev)
{
	struct ftl_nv_cache_compaction *compaction;

	compaction = calloc(1, sizeof(*compaction));
	if (!compaction) {
		goto ERROR;
	}

	/* Allocate help request for writing */
	compaction->wr = ftl_rq_new(dev, 0, dev->md_size);
	if (!compaction->wr) {
		goto ERROR;
	}

	/* Allocate help request for reading */
	compaction->rd = ftl_rq_new(dev,
			dev->conf.nv_cache.max_request_size,
			dev->nv_cache.md_size);
	if (!compaction->rd) {
		goto ERROR;
	}

	compaction->nv_cache = &dev->nv_cache;
	compaction->wr->owner.priv = compaction;
	compaction->wr->owner.cb = compaction_process_ftl_done;

	TAILQ_INIT(&compaction->chunk_list);
	return compaction;

ERROR:
	compaction_free(dev, compaction);
	return NULL;
}

void ftl_nv_cache_compact(struct spdk_ftl_dev *dev)
{
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;

	if (!dev->nv_cache.bdev_desc) {
		return;
	}

	if (_is_compaction_required(nv_cache) &&
	    !TAILQ_EMPTY(&nv_cache->compaction_list)) {
		struct ftl_nv_cache_compaction *comp =
			TAILQ_FIRST(&nv_cache->compaction_list);

		TAILQ_REMOVE(&nv_cache->compaction_list, comp, entry);

		nv_cache->compaction_active_count++;
		compaction_process_start(comp);
	}
}

int ftl_nv_cache_read(struct ftl_io *io, struct ftl_addr addr,
		      uint32_t num_blocks, spdk_bdev_io_completion_cb cb,
		      void *cb_arg)
{
	int rc;
	struct ftl_nv_cache *nv_cache = &io->dev->nv_cache;

	assert(addr.cached);

	rc = __spdk_bdev_read_blocks_with_md(
			 nv_cache,
		     nv_cache->bdev_desc, nv_cache->cache_ioch, ftl_io_iovec_addr(io),
		     nv_cache->md_rd, addr.cache_offset, num_blocks, cb, cb_arg);

	if (spdk_likely(0 == rc)) {
		ftl_io_inc_req(io);
	}

	return rc;
}

int ftl_nv_cache_write(struct ftl_io *io, struct ftl_addr addr,
		       uint32_t num_blocks, void *md,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	int rc;
	struct ftl_nv_cache *nv_cache = &io->dev->nv_cache;

	assert(addr.cached);

	//trace cache write buffer
	ftl_trace_submission(io->dev, io, addr, num_blocks);

	rc = __spdk_bdev_write_blocks_with_md(
		     nv_cache,
		     nv_cache->bdev_desc, nv_cache->cache_ioch,
		     ftl_io_iovec_addr(io), md,
		     addr.cache_offset, num_blocks, cb, cb_arg);

	if (spdk_likely(0 == rc)) {
		ftl_io_inc_req(io);
	}

	return rc;
}

void ftl_nv_cache_fill_md(struct ftl_io *io)
{
	struct spdk_bdev *bdev;
	struct ftl_nv_cache *nv_cache = &io->dev->nv_cache;
	uint64_t block_off;
	void *md_buf = io->md;
	uint64_t meta_size;

	bdev = spdk_bdev_desc_get_bdev(nv_cache->bdev_desc);
	meta_size = __spdk_bdev_get_md_size(bdev);

	for (block_off = 0; block_off < io->num_blocks; ++block_off) {
		ftl_nv_cache_pack_lba(ftl_io_get_lba(io, block_off), md_buf);
		md_buf += meta_size;
	}
}

struct state_context {
	struct ftl_nv_cache *nv_cache;
	void (*cb)(void *cntx, bool status);
	void *cb_cntx;
	bool status;
	uint64_t iter;
	void *iter_data;
	void *meta;
	void *dma_data;
};

static void state_context_free(struct state_context *cntx)
{
	if (cntx) {
		if (cntx->dma_data) {
			spdk_free(cntx->dma_data);
		}

		if (cntx->meta) {
			spdk_mempool_put(cntx->nv_cache->md_pool, cntx->meta);
		}

		free(cntx);
	}
}

static struct state_context* state_context_allocate(struct ftl_nv_cache *nv_cache,
		void (*cb)(void *cntx, bool status), void *cb_cntx)
{
	struct state_context *cntx = calloc(1, sizeof(*cntx));

	if (!cntx) {
		return NULL;
	}

	cntx->nv_cache = nv_cache;
	cntx->cb = cb;
	cntx->cb_cntx = cb_cntx;
	cntx->iter_data = nv_cache->chunk;
	cntx->status = true;

	cntx->meta = spdk_mempool_get(nv_cache->md_pool);
	if (!cntx->meta) {
		state_context_free(cntx);
		return NULL;
	}
	memset(cntx->meta, 0, nv_cache->md_size);

	cntx->dma_data = spdk_zmalloc(FTL_BLOCK_SIZE, FTL_BLOCK_SIZE, NULL,
			SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	if (!cntx->dma_data) {
		state_context_free(cntx);
		return NULL;
	}

	return cntx;
}

static void save_state_iter(struct state_context *cntx);

static void save_state_finish(struct state_context *cntx)
{
	if (cntx->status) {
		SPDK_NOTICELOG("FTL NV Cache: state saved successfully\n");
	} else {
		SPDK_ERRLOG("FTL NV Cache: state saving ERROR\n");
	}
	cntx->cb(cntx->cb_cntx, cntx->status);
	state_context_free(cntx);
}

static void save_state_cb(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct state_context *cntx = arg;

	spdk_bdev_free_io(bdev_io);

	cntx->status &= success;
	if (!cntx->status) {
		save_state_finish(cntx);
		return;
	}

	cntx->iter++;
	cntx->iter_data += FTL_BLOCK_SIZE;
	save_state_iter(cntx);
}

static void save_state_iter(struct state_context *cntx)
{
	struct ftl_nv_cache *nv_cache = cntx->nv_cache;
	int rc;

	if (cntx->iter < nv_cache->num_meta_blocks) {
		memcpy(cntx->dma_data, cntx->iter_data, FTL_BLOCK_SIZE);

		rc = __spdk_bdev_write_blocks_with_md(nv_cache,
			     nv_cache->bdev_desc, nv_cache->cache_ioch,
			     cntx->dma_data, cntx->meta,
			     cntx->iter, 1, save_state_cb, cntx);

		if (rc) {
			save_state_finish(cntx);
			return;
		}
	} else {
		save_state_finish(cntx);
	}
}

void ftl_nv_cache_save_state(struct ftl_nv_cache *nv_cache,
		void (*cb)(void *cntx, bool status), void *cb_cntx)
{
	bool status = true;
	uint64_t i;

	struct state_context *cntx;
	SPDK_NOTICELOG("FTL NV Cache: Saving state...\n");

	if (nv_cache->compaction_active_count) {
		/* All compaction process have to be inactive */
		assert(0);
		cb(cb_cntx, false);
		return;
	}

	/* Iterate over chunks and check if they are in consistent state */
	for (i = 0; i < nv_cache->chunk_count; i++) {
		struct ftl_nv_cache_chunk *chunk = &nv_cache->chunk[i];

		if (chunk->read_pointer)  {
			/* Only full chunks can be compacted */
			if(chunk->blocks_written != nv_cache->chunk_size) {
				assert(0);
				status = false;
				break;
			}

			/*
			 * Chunk in the middle of compaction, start over after
			 * load
			 */
			chunk->read_pointer = chunk->blocks_compacted = 0;
		} else if (chunk->blocks_written == nv_cache->chunk_size) {
			/* Full chunk */
		} else if (chunk->blocks_written && nv_cache->chunk_current == chunk) {
			/* Current chunk */
		} else if (0 == chunk->blocks_written) {
			/* Empty chunk */
		} else {
			assert(0);
			status = false;
			break;
		}
	}

	if (!status) {
		cb(cb_cntx, false);
		return;
	}

	cntx = state_context_allocate(nv_cache, cb, cb_cntx);
	if (!cntx) {
		cntx->status = false;
		save_state_finish(cntx);
		return;
	}

	save_state_iter(cntx);
}

static void load_state_finish(struct state_context *arg)
{
	uint64_t chunks_number, offset;
	struct state_context *cntx = arg;
	struct ftl_nv_cache *nv_cache = cntx->nv_cache;
	uint64_t i;

	if (!cntx->status) {
		SPDK_ERRLOG("FTL NV Cache: loading state ERROR\n");
		cntx->cb(cntx->cb_cntx, cntx->status);
		state_context_free(cntx);
		return;
	}

	nv_cache->chunk_current = NULL;
	TAILQ_INIT(&nv_cache->chunk_free_list);
	TAILQ_INIT(&nv_cache->chunk_full_list);
	nv_cache->chunk_full_count = nv_cache->chunk_free_count = 0;

	offset = nv_cache->num_meta_blocks;
	for (i = 0; i < nv_cache->chunk_count; i++) {
		struct ftl_nv_cache_chunk *chunk = &nv_cache->chunk[i];

		if (offset != chunk->offset) {
			cntx->status = false;
			assert(0);
			break;
		}

		if (chunk->blocks_written == nv_cache->chunk_size) {
			/* Chunk full, move it on full list */
			TAILQ_INSERT_TAIL(&nv_cache->chunk_full_list, chunk, entry);
			nv_cache->chunk_full_count++;
		} else if (chunk->blocks_written && !nv_cache->chunk_current) {
			nv_cache->chunk_current = chunk;
		} else if (0 == chunk->blocks_written) {
			/* Chunk empty, move it on empty list */
			TAILQ_INSERT_TAIL(&nv_cache->chunk_free_list, chunk, entry);
			nv_cache->chunk_free_count++;
		} else {
			cntx->status = false;
			assert(0);
			break;
		}

		offset += nv_cache->chunk_size;
	}

	chunks_number = nv_cache->chunk_free_count + nv_cache->chunk_full_count;
	if (nv_cache->chunk_current) {
		chunks_number++;
	}

	if (chunks_number != nv_cache->chunk_count) {
		cntx->status = false;
		assert(0);
	}

	SPDK_NOTICELOG("FTL NV Cache: full chunks = %lu, empty chunks = %lu\n",
			nv_cache->chunk_full_count, nv_cache->chunk_free_count);

        __load_vss(nv_cache->ftl_dev, nv_cache->vss, nv_cache->vss_size,
                   nv_cache->vss_md_size);

        if (cntx->status) {
		SPDK_NOTICELOG("FTL NV Cache: state loaded successfully\n");
	} else {
		SPDK_ERRLOG("FTL NV Cache: loading state ERROR\n");
	}

	cntx->cb(cntx->cb_cntx, cntx->status);
	state_context_free(cntx);
}

static void load_state_iter(struct state_context *cntx);

static void load_state_cb(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct state_context *cntx = arg;

	spdk_bdev_free_io(bdev_io);

	cntx->status &= success;
	if (!success) {
		load_state_finish(cntx);
		return;
	}

	memcpy(cntx->iter_data, cntx->dma_data, FTL_BLOCK_SIZE);

	cntx->iter++;
	cntx->iter_data += FTL_BLOCK_SIZE;

	load_state_iter(cntx);
}

static void load_state_iter(struct state_context *cntx)
{
	struct ftl_nv_cache *nv_cache = cntx->nv_cache;
	int rc;

	if (cntx->iter < nv_cache->num_meta_blocks) {
		rc = __spdk_bdev_read_blocks_with_md(nv_cache,
			     nv_cache->bdev_desc, nv_cache->cache_ioch,
			     cntx->dma_data, cntx->meta,
			     cntx->iter, 1, load_state_cb, cntx);

		if (rc) {
			load_state_finish(cntx);
			return;
		}
	} else {
		load_state_finish(cntx);
	}
}


void ftl_nv_cache_load_state(struct ftl_nv_cache *nv_cache,
		void (*cb)(void *cntx, bool status), void *cb_cntx)
{
	struct state_context *cntx;
	SPDK_NOTICELOG("FTL NV Cache: Loading state...\n");

	cntx = state_context_allocate(nv_cache, cb, cb_cntx);
	if (!cntx) {
		cntx->status = false;
		load_state_finish(cntx);
		return;
	}

	load_state_iter(cntx);
}
