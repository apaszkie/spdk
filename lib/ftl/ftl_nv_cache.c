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

#define FTL_NV_CACHE_CHUNK_SIZE_BYTES (256 * (1ULL << 10))

static struct ftl_nv_cache_compaction *compaction_alloc(
	struct spdk_ftl_dev *dev);

static void compaction_process_ftl_done(struct spdk_ftl_dev *ftl,
					struct ftl_batch *batch);

static void compaction_process_start(
	struct ftl_nv_cache_compaction *compaction);

static void compaction_process_read(struct ftl_nv_cache_compaction *compaction);

static inline uint32_t __spdk_bdev_get_md_size(const struct spdk_bdev *bdev)
{
	return 64;
}

static bool compaction_entry_is_invalid(struct ftl_wbuf_entry *entry)
{
	return 0 == (entry->io_flags & FTL_IO_NV_CACHE_COMPACT);
}

static void compaction_entry_set_valid(struct ftl_wbuf_entry *entry)
{
	entry->io_flags |= FTL_IO_NV_CACHE_COMPACT;
}

static void compaction_entry_clear_valid(struct ftl_wbuf_entry *entry)
{
	entry->io_flags &= ~FTL_IO_NV_CACHE_COMPACT;
}

static struct ftl_nv_cache_block_metadata *vss;
static int64_t vss_size;
static int64_t vss_md_size;

static int __spdk_bdev_read_blocks_with_md(
	struct spdk_bdev_desc *desc, struct spdk_io_channel *ch, void *buf,
	void *md_buf, int64_t offset_blocks, uint64_t num_blocks,
	spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	void *iter_buf = md_buf;
	int64_t iter_offset = offset_blocks;
	int64_t iter_offset_end = offset_blocks + num_blocks;

	while (iter_offset < iter_offset_end) {
		struct ftl_nv_cache_block_metadata *md = iter_buf;

		assert(iter_offset < vss_size);
		md->lba = vss[iter_offset].lba;

		iter_offset++;
		iter_buf += vss_md_size;
	}

	return spdk_bdev_read_blocks(desc, ch, buf, offset_blocks, num_blocks, cb,
				     cb_arg);
}

static int __spdk_bdev_write_blocks_with_md(
	struct spdk_bdev_desc *desc, struct spdk_io_channel *ch, void *buf,
	void *md_buf, int64_t offset_blocks, uint64_t num_blocks,
	spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	void *iter_buf = md_buf;
	int64_t iter_offset = offset_blocks;
	int64_t iter_offset_end = offset_blocks + num_blocks;

	while (iter_offset < iter_offset_end) {
		struct ftl_nv_cache_block_metadata *md = iter_buf;

		assert(iter_offset < vss_size);
		vss[iter_offset].lba = md->lba;

		iter_offset++;
		iter_buf += vss_md_size;
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
	if (!dev->nv_cache.bdev_desc) {
		return;
	}

	spdk_bdev_module_release_bdev(
		spdk_bdev_desc_get_bdev(dev->nv_cache.bdev_desc));
	spdk_bdev_close(dev->nv_cache.bdev_desc);

	/* TODO(mbarczak) Cleanup fully */
}

int ftl_nv_cache_init(struct spdk_ftl_dev *dev, const char *bdev_name)
{
	struct spdk_bdev *bdev;
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;
	struct spdk_ftl_conf *conf = &dev->conf;
	char pool_name[128];
	int rc;
	uint64_t i;

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

	nv_cache->md_pool = spdk_mempool_create(
				    pool_name, conf->nv_cache.max_request_cnt * 2,
				    __spdk_bdev_get_md_size(bdev) * conf->nv_cache.max_request_size,
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

	nv_cache->dma_buf =
		spdk_dma_zmalloc(FTL_BLOCK_SIZE, spdk_bdev_get_buf_align(bdev), NULL);
	if (!nv_cache->dma_buf) {
		SPDK_ERRLOG("Memory allocation failure\n");
		return -1;
	}

	nv_cache->current_addr = FTL_NV_CACHE_DATA_OFFSET;
	nv_cache->num_data_blocks = spdk_bdev_get_num_blocks(bdev);
	nv_cache->num_available = nv_cache->num_data_blocks;
	nv_cache->ready = false;

	vss = calloc(nv_cache->num_data_blocks, sizeof(vss[0]));
	for (i = 0; i < nv_cache->num_data_blocks; i++) {
		vss[i].lba = FTL_LBA_INVALID;
	}
	vss_size = nv_cache->num_data_blocks;
	vss_md_size = __spdk_bdev_get_md_size(bdev);

	TAILQ_INIT(&nv_cache->chunk_free_list);
	TAILQ_INIT(&nv_cache->chunk_full_list);

	nv_cache->chunk_size = FTL_NV_CACHE_CHUNK_SIZE_BYTES / FTL_BLOCK_SIZE;
	nv_cache->chunk_count = spdk_bdev_get_num_blocks(bdev) / nv_cache->chunk_size;
	nv_cache->chunk = calloc(nv_cache->chunk_count, sizeof(nv_cache->chunk[0]));
	nv_cache->chunk_free_count = nv_cache->chunk_count;
	if (!nv_cache->chunk) {
		SPDK_ERRLOG("Cannot allocate memory for chunks\n");
		return -1;
	}
	for (i = 0; i < nv_cache->chunk_count; i++) {
		struct ftl_nv_cache_chunk *chunk = &nv_cache->chunk[i];

		chunk->offset = i * nv_cache->chunk_size;
		TAILQ_INSERT_TAIL(&nv_cache->chunk_free_list, chunk, entry);
	}

	/* Start compaction when full chunks exceed given % of entire chunks */
	nv_cache->chunk_compaction_threshold = nv_cache->chunk_count * 9 / 10;
	TAILQ_INIT(&nv_cache->compaction_list);
	for (i = 0; i < 128; i++) {
		struct ftl_nv_cache_compaction *compact = compaction_alloc(dev);

		if (!compact) {
			SPDK_ERRLOG("Cannot allocate compaction process\n");
			return -1;
		}

		TAILQ_INSERT_TAIL(&nv_cache->compaction_list, compact, entry);
	}

	return 0;
}

static bool _is_compaction_required(struct ftl_nv_cache *nv_cache)
{
	if (nv_cache->chunk_full_count < nv_cache->chunk_compaction_threshold) {
		return false;
	}

	if (nv_cache->ftl_dev->halt) {
		return false;
	}

	uint64_t compacted_load_blocks =
		nv_cache->compaction_active_count *
		(FTL_NV_CACHE_CHUNK_SIZE_BYTES / FTL_BLOCK_SIZE);

	if (compacted_load_blocks > nv_cache->load_blocks) {
		return false;
	}

	return true;
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
	} else {
		assert(0);
	}

	TAILQ_INSERT_HEAD(&compaction->chunk_list, chunk, entry);

	assert(chunk->write_pointer != 0);

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
	uint64_t i;

	if (!compaction) {
		return;
	}

	if (compaction->entries) {
		for (i = 0; i < compaction->batch->num_entries; ++i) {
			if (compaction->entries[i].payload) {
				spdk_free(compaction->entries[i].payload);
			}
		}
	}

	if (compaction->batch) {
		if (compaction->batch->metadata) {
			spdk_mempool_put(dev->nv_cache.md_pool, compaction->batch->metadata);
		}

		for (i = 0; i < compaction->batch->num_entries; ++i) {
			compaction->batch->iov[i].iov_base = NULL;
			compaction->batch->iov[i].iov_len = 0;
		}
		compaction->batch->num_entries = 0;
		TAILQ_INIT(&compaction->batch->entries);

		TAILQ_INSERT_TAIL(&dev->free_batches, compaction->batch, tailq);
	}

	free(compaction);
}

static struct ftl_nv_cache_block_metadata *compaction_get_metadata(
	struct ftl_batch *batch, uint64_t md_size, uint64_t idx)
{
	off_t offset = md_size * idx;
	return (struct ftl_nv_cache_block_metadata *)(batch->metadata + offset);
}

static void compaction_process_ftl_done(struct spdk_ftl_dev *ftl,
					struct ftl_batch *batch)
{
	struct ftl_wbuf_entry *entry;
	struct ftl_nv_cache_compaction *compaction = batch->priv_data;
	uint64_t num_entries = batch->num_entries;
	struct ftl_nv_cache *nv_cache = &ftl->nv_cache;

	entry = compaction->entries;
	compaction->iter.idx = 0;
	while (compaction->iter.idx < num_entries) {
		struct ftl_nv_cache_chunk *chunk = entry->priv_data;
		chunk_compaction_advance(compaction, chunk);
		compaction_entry_clear_valid(entry);

		entry++;
		compaction->iter.idx++;
	}

	compaction->iter.io_count = 0;
	compaction->iter.valid_count = 0;
	compaction->iter.idx = 0;
	compaction->iter.entry = compaction->entries;

	if (_is_compaction_required(nv_cache)) {
		compaction_process_start(compaction);
	} else {
		nv_cache->compaction_active_count--;
		TAILQ_INSERT_TAIL(&nv_cache->compaction_list, compaction, entry);
	}
}

static void compaction_process_read_cb(struct spdk_bdev_io *bdev_io,
				       bool success, void *cb_arg)
{
	struct spdk_ftl_dev *dev;
	struct ftl_nv_cache_block_metadata *md;
	struct ftl_nv_cache_compaction *compaction;
	struct ftl_wbuf_entry *entry = cb_arg;
	struct ftl_nv_cache_chunk *chunk = entry->priv_data;
	uint32_t idx = entry->index;
	struct ftl_addr current_addr;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		/* TODO(mbarczak) Handle this */;
		assert(0);
	}

	compaction =
		SPDK_CONTAINEROF(entry, struct ftl_nv_cache_compaction, entries[idx]);

	md = compaction_get_metadata(compaction->batch, compaction->metadata_size,
				     idx);

	dev = compaction->nv_cache->ftl_dev;

	if (md->lba != FTL_LBA_INVALID) {
		current_addr = ftl_l2p_get(dev, md->lba);
	} else {
		current_addr.offset = FTL_ADDR_INVALID;
	}

	entry->lba = md->lba;

	/* Decrease number of IO which needed to for validating batch */
	compaction->iter.io_count--;

	if (ftl_addr_cmp(current_addr, entry->addr)) {
		/*
		 * Address still the same, we may continue to compact it back to
		 * FTL, set valid number of entries within this batch
		 */
		compaction->iter.valid_count++;
	} else {
		/* This address already invalidated, just omit this block */
		chunk_compaction_advance(compaction, chunk);

		/* Need to collect this entry once again */
		compaction_entry_clear_valid(entry);
	}

	if (0 == compaction->iter.io_count) {
		if (compaction->batch->num_entries == compaction->iter.valid_count) {
			/*
			 * Batch already collected, compact it
			 */
			TAILQ_INSERT_TAIL(&dev->pending_batches, compaction->batch, tailq);
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
}

static void msg_compaction_process_read(void *compaction)
{
	compaction_process_read(compaction);
}

static void compaction_process_read(
	struct ftl_nv_cache_compaction *compaction)
{
	int rc;
	struct ftl_wbuf_entry *entry;
	struct ftl_nv_cache *nv_cache = compaction->nv_cache;

	struct ftl_io_channel *ioch =
		ftl_io_channel_get_ctx(ftl_get_io_channel(nv_cache->ftl_dev));

	uint64_t num_entries = compaction->batch->num_entries;
	uint64_t md_size = compaction->metadata_size;
	void *md;

	entry = compaction->iter.entry;
	md =
		compaction_get_metadata(compaction->batch, md_size, compaction->iter.idx);

	while (compaction->iter.idx < num_entries) {
		if (compaction_entry_is_invalid(entry)) {
			rc = __spdk_bdev_read_blocks_with_md(
				     nv_cache->bdev_desc, ioch->cache_ioch, entry->payload, md,
				     entry->addr.cache_offset, 1, compaction_process_read_cb, entry);

			if (spdk_likely(0 == rc)) {
				compaction_entry_set_valid(entry);
			}

		} else {
			rc = 0;
		}

		if (spdk_unlikely(rc)) {
			/* TODO Log error and handle */
			break;
		}

		compaction->iter.idx++;
		entry++;
		md += md_size;
	}

	if (spdk_likely(compaction->iter.idx == num_entries)) {
		/* All IOs send, we'll continue from callbacks */

		/* Reset compaction iterator */
		compaction->iter.idx = 0;
		compaction->iter.entry = compaction->entries;
	} else {
		compaction->iter.entry = entry;

		spdk_thread_send_msg(spdk_get_thread(), msg_compaction_process_read,
				     compaction);
	}
}

static void msg_compaction_process_start(void *compaction)
{
	compaction_process_start(compaction);
}

static void compaction_process_start(
	struct ftl_nv_cache_compaction *compaction)
{
	struct ftl_nv_cache_chunk *chunk;
	struct ftl_wbuf_entry *entry;
	uint64_t num_entries = compaction->batch->num_entries;

	entry = compaction->iter.entry;
	while (compaction->iter.idx < num_entries) {
		if (compaction_entry_is_invalid(entry)) {
			chunk = get_chunk_for_compaction(compaction);
			entry->priv_data = chunk;

			uint64_t read_ptr = chunk->read_pointer++;
			entry->addr.cache_offset = chunk->offset + read_ptr;

			compaction->iter.io_count++;
		}

		compaction->iter.idx++;
		entry++;
	}

	assert(compaction->iter.io_count + compaction->iter.valid_count ==
	       num_entries);

	if (compaction->iter.idx == num_entries) {
		compaction->iter.idx = 0;
		compaction->iter.entry = compaction->entries;

		compaction_process_read(compaction);
	} else {
		compaction->iter.entry = entry;

		spdk_thread_send_msg(spdk_get_thread(), msg_compaction_process_start,
				     compaction);
	}
}

static struct ftl_nv_cache_compaction *compaction_alloc(
	struct spdk_ftl_dev *dev)
{
	struct spdk_bdev *bdev = spdk_bdev_desc_get_bdev(dev->nv_cache.bdev_desc);
	struct ftl_batch *batch;
	struct ftl_nv_cache_compaction *compaction;
	struct ftl_wbuf_entry *entry;
	uint64_t i;
	size_t size;

	size =
		sizeof(*compaction) + (sizeof(compaction->entries[0]) * dev->xfer_size);
	compaction = calloc(1, size);
	if (!compaction) {
		goto ERROR;
	}
	compaction->nv_cache = &dev->nv_cache;

	if (TAILQ_EMPTY(&dev->free_batches)) {
		goto ERROR;
	}

	compaction->batch = batch = TAILQ_FIRST(&dev->free_batches);
	TAILQ_REMOVE(&dev->free_batches, batch, tailq);
	batch->num_entries = dev->xfer_size;
	batch->priv_data = compaction;
	batch->cb = compaction_process_ftl_done;

	batch->metadata = spdk_mempool_get(dev->nv_cache.md_pool);
	if (!batch->metadata) {
		goto ERROR;
	}

	entry = compaction->entries;
	for (i = 0; i < batch->num_entries; ++i, ++entry) {
		entry->payload = spdk_zmalloc(FTL_BLOCK_SIZE, FTL_BLOCK_SIZE, NULL,
					      SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
		if (!entry->payload) {
			goto ERROR;
		}

		entry->addr.offset = FTL_ADDR_INVALID;
		entry->lba = FTL_LBA_INVALID;
		entry->addr.cached = true;
		entry->index = i;
		pthread_spin_init(&entry->lock, PTHREAD_PROCESS_PRIVATE);

		batch->iov[i].iov_base = entry->payload;
		batch->iov[i].iov_len = FTL_BLOCK_SIZE;

		TAILQ_INSERT_TAIL(&batch->entries, entry, tailq);
	}

	compaction->iter.entry = compaction->entries;
	compaction->iter.idx = 0;
	TAILQ_INIT(&compaction->chunk_list);
	compaction->metadata_size = __spdk_bdev_get_md_size(bdev);

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
	struct ftl_io_channel *ioch =
		ftl_io_channel_get_ctx(ftl_get_io_channel(io->dev));

	assert(addr.cached);

	rc = __spdk_bdev_read_blocks_with_md(
		     nv_cache->bdev_desc, ioch->cache_ioch, ftl_io_iovec_addr(io),
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
	struct ftl_io_channel *ioch =
		ftl_io_channel_get_ctx(ftl_get_io_channel(io->dev));

	assert(addr.cached);

	//trace cache write buffer
	ftl_trace_submission(io->dev, io, addr, num_blocks);

	rc = __spdk_bdev_write_blocks_with_md(
		     nv_cache->bdev_desc, ioch->cache_ioch, ftl_io_iovec_addr(io), md,
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
