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

#include "spdk/ftl.h"
#include "spdk/log.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

#include "ftl_nv_cache.h"
#include "ftl_core.h"

#define FTL_NV_CACHE_CHUNK_SIZE_BYTES (128ULL * (1ULL << 20))

static struct ftl_nv_cache_compaction *
compaction_alloc(struct spdk_ftl_dev *dev);

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
	.name   = "ftl_lib_nv_cache",
};

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

	if (spdk_bdev_open_ext(bdev_name, true, _nv_cache_bdev_event_cb,
			       dev, &nv_cache->bdev_desc)) {
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
		SPDK_ERRLOG("Unsupported block size (%d)\n", spdk_bdev_get_block_size(bdev));
		return -1;
	}

	if (!spdk_bdev_is_md_separate(bdev)) {
		SPDK_ERRLOG("Bdev %s doesn't support separate metadata buffer IO\n",
			    spdk_bdev_get_name(bdev));
		return -1;
	}

	if (spdk_bdev_get_md_size(bdev) < sizeof(struct ftl_nv_cache_block_metadata)) {
		SPDK_ERRLOG("Bdev's %s metadata is too small (%"PRIu32")\n",
			    spdk_bdev_get_name(bdev), spdk_bdev_get_md_size(bdev));
		return -1;
	}

	if (spdk_bdev_get_dif_type(bdev) != SPDK_DIF_DISABLE) {
		SPDK_ERRLOG("Unsupported DIF type used by bdev %s\n",
			    spdk_bdev_get_name(bdev));
		return -1;
	}

	/* The cache needs to be capable of storing at least two full bands. This requirement comes
	 * from the fact that cache works as a protection against power loss, so before the data
	 * inside the cache can be overwritten, the band it's stored on has to be closed. Plus one
	 * extra block is needed to store the header.
	 */
	if (spdk_bdev_get_num_blocks(bdev) < ftl_get_num_blocks_in_band(dev) * 2 + 1) {
		SPDK_ERRLOG("Insufficient number of blocks for write buffer cache (available: %"
			    PRIu64", required: %"PRIu64")\n", spdk_bdev_get_num_blocks(bdev),
			    ftl_get_num_blocks_in_band(dev) * 2 + 1);
		return -1;
	}

	rc = snprintf(pool_name, sizeof(pool_name), "ftl-nvpool-%p", dev);
	if (rc < 0 || rc >= 128) {
		return -1;
	}

	nv_cache->md_pool = spdk_mempool_create(pool_name,
						conf->nv_cache.max_request_cnt * 2,
						spdk_bdev_get_md_size(bdev) *
						conf->nv_cache.max_request_size,
						SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
						SPDK_ENV_SOCKET_ID_ANY);
	if (!nv_cache->md_pool) {
		SPDK_ERRLOG("Failed to initialize non-volatile cache metadata pool\n");
		return -1;
	}

	nv_cache->dma_buf = spdk_dma_zmalloc(FTL_BLOCK_SIZE, spdk_bdev_get_buf_align(bdev), NULL);
	if (!nv_cache->dma_buf) {
		SPDK_ERRLOG("Memory allocation failure\n");
		return -1;
	}

	if (pthread_spin_init(&nv_cache->lock, PTHREAD_PROCESS_PRIVATE)) {
		SPDK_ERRLOG("Failed to initialize cache lock\n");
		return -1;
	}

	nv_cache->current_addr = FTL_NV_CACHE_DATA_OFFSET;
	nv_cache->num_data_blocks = spdk_bdev_get_num_blocks(bdev) - 1;
	nv_cache->num_available = nv_cache->num_data_blocks;
	nv_cache->ready = false;

	TAILQ_INIT(&nv_cache->chunk_free_list);
	TAILQ_INIT(&nv_cache->chunk_full_list);
	TAILQ_INIT(&nv_cache->chunk_compacted_list);

	nv_cache->chunk_size = FTL_NV_CACHE_CHUNK_SIZE_BYTES / FTL_BLOCK_SIZE;
	nv_cache->chunk_count = spdk_bdev_get_num_blocks(bdev) / nv_cache->chunk_size;
	nv_cache->chunk = calloc(nv_cache->chunk_count, sizeof(nv_cache->chunk[0]));
	if (!nv_cache->chunk) {
		SPDK_ERRLOG("Cannot allocate memory for chunks\n");
		return -1;
	}
	for (i = 0; i < nv_cache->chunk_count; i++) {
		struct ftl_nv_cache_chunk *chunk = &nv_cache->chunk[i];

		chunk->id = i;
		chunk->offset = i * nv_cache->chunk_size;
		TAILQ_INSERT_TAIL(&nv_cache->chunk_free_list, chunk, entry);
	}

	/* Start compaction when full chunks exceed given % of entire chunks */
	nv_cache->chunk_compaction_threshold = nv_cache->chunk_count * 9 / 10;
	nv_cache->compaction_process = compaction_alloc(dev);
	if (!nv_cache->compaction_process) {
		SPDK_ERRLOG("Cannot allocate compaction process\n");
		return -1;
	}

	return 0;
}

/* TODO Do we need deinit function? */

static bool _is_compaction_required(struct ftl_nv_cache *nv_cache)
{
	return nv_cache->chunk_full_count >=
	       nv_cache->chunk_compaction_threshold;
}

static bool is_chunk_compacted(struct ftl_nv_cache_chunk *chunk)
{
	uint64_t blocks_to_compact = chunk->blocks_written -
				     chunk->blocks_skipped;

	if (blocks_to_compact == chunk->blocks_compacted) {
		return true;
	}

	return false;
}

static bool is_chunk_to_read(struct ftl_nv_cache_chunk *chunk)
{
	uint64_t blocks_to_read = chunk->blocks_written - chunk->blocks_skipped;

	if (blocks_to_read == chunk->read_pointer) {
		return false;
	}

	return true;
}

static struct ftl_nv_cache_chunk *
get_chunk_for_compaction(struct ftl_nv_cache_compaction *compaction)
{
	struct ftl_nv_cache *nv_cache = compaction->nv_cache;
	struct ftl_nv_cache_chunk *chunk = NULL;

	pthread_spin_lock(&nv_cache->lock);

	if (!TAILQ_EMPTY(&nv_cache->chunk_compacted_list)) {
		chunk = TAILQ_FIRST(&nv_cache->chunk_compacted_list);
		if (is_chunk_to_read(chunk)) {
			chunk = NULL;
		} else {
			goto END;
		}
	}

	if (!TAILQ_EMPTY(&nv_cache->chunk_full_list)) {
		chunk = TAILQ_FIRST(&nv_cache->chunk_full_list);
		TAILQ_REMOVE(&nv_cache->chunk_full_list, chunk, entry);
		TAILQ_INSERT_TAIL(&nv_cache->chunk_compacted_list,
				  chunk, entry);

		chunk->read_pointer = 0;
		chunk->blocks_compacted = 0;
	} else {
		assert(0);
	}

END:
	pthread_spin_unlock(&nv_cache->lock);
	return chunk;
}

static uint64_t _chunk_get_free_space(
	struct ftl_nv_cache *nv_cache,
	struct ftl_nv_cache_chunk *chunk)
{
	if (spdk_likely(chunk->write_pointer <= nv_cache->chunk_size)) {
		return nv_cache->chunk_size - chunk->write_pointer;
	} else {
		assert(0);
		return 0;
	}
}

static void _chunk_skip_blocks(
	struct ftl_nv_cache *nv_cache,
	struct ftl_nv_cache_chunk *chunk, uint64_t skipped_blocks)
{
	nv_cache->chunk_current = NULL;

	if (0 == skipped_blocks) {
		return;
	}

	chunk->blocks_skipped = skipped_blocks;
	uint64_t bytes_written = __atomic_add_fetch(&chunk->blocks_written,
				 skipped_blocks, __ATOMIC_SEQ_CST);

	if (bytes_written == nv_cache->chunk_size) {
		/* Chunk full move it on full list */
		TAILQ_INSERT_TAIL(&nv_cache->chunk_full_list, chunk, entry);
		nv_cache->chunk_full_count++;
	} else if (spdk_unlikely(bytes_written > nv_cache->chunk_size)) {
		assert(0);
	}
}

static void _chunk_advance_blocks(
	struct ftl_nv_cache *nv_cache,
	struct ftl_nv_cache_chunk *chunk, uint64_t advanced_blocks)
{
	uint64_t blocks_written = __atomic_add_fetch(&chunk->blocks_written,
				  advanced_blocks, __ATOMIC_SEQ_CST);

	if (blocks_written == nv_cache->chunk_size) {
		/* Chunk full move it on full list */
		pthread_spin_lock(&nv_cache->lock);
		TAILQ_INSERT_TAIL(&nv_cache->chunk_full_list, chunk, entry);
		nv_cache->chunk_full_count++;
		pthread_spin_unlock(&nv_cache->lock);
	} else if (spdk_unlikely(blocks_written > nv_cache->chunk_size)) {
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

	pthread_spin_lock(&nv_cache->lock);

AGAIN:
	chunk = nv_cache->chunk_current;
	if (!chunk) {
		if (!TAILQ_EMPTY(&nv_cache->chunk_free_list)) {
			chunk = TAILQ_FIRST(&nv_cache->chunk_free_list);
			TAILQ_REMOVE(&nv_cache->chunk_free_list, chunk, entry);
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
	pthread_spin_unlock(&nv_cache->lock);
	return address;
}

void ftl_nv_cache_commit_wr_buffer(struct ftl_nv_cache *nv_cache,
				   struct ftl_io *io)
{
	if (!io->nv_cache_chunk) {
		/* No chunk, nothing to do */
		return;
	}

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
			spdk_mempool_put(dev->nv_cache.md_pool,
					 compaction->batch->metadata);
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

static void
compaction_process_read_cb(struct spdk_bdev_io *bdev_io,
			   bool success,
			   void *cb_arg)
{

}

static struct ftl_nv_cache_block_metadata *
compaction_get_metadata(struct ftl_batch *batch, uint64_t md_size, uint64_t idx)
{
	off_t offset = md_size * idx;
	return (struct ftl_nv_cache_block_metadata *)(batch->metadata + offset);
}

static void
compaction_process_read(struct ftl_nv_cache_compaction *compaction)
{
	int rc;
	struct ftl_wbuf_entry *entry;
	struct ftl_nv_cache *nv_cache = compaction->nv_cache;
	uint64_t num_entries = compaction->batch->num_entries;

	struct spdk_bdev *bdev = spdk_bdev_desc_get_bdev(nv_cache->bdev_desc);
	uint64_t md_size = spdk_bdev_get_md_size(bdev);
	void *md = compaction->batch;

	struct spdk_io_channel *ioch = spdk_bdev_get_io_channel(
					       nv_cache->bdev_desc);


	if (compaction->iter.idx == num_entries) {
		compaction->iter.remaining = num_entries;
		compaction->iter.idx = 0;
		compaction->iter.entry = compaction->entries;
	}

	entry = compaction->iter.entry;
	while (compaction->iter.idx < num_entries) {
		/* TODO We can send one sequential IO in some cases */
		rc = spdk_bdev_read_blocks_with_md(nv_cache->bdev_desc,
						   ioch,
						   entry->payload, md,
						   entry->addr.cache_offset, 1,
						   compaction_process_read_cb, entry);

		if (-ENOMEM == rc) {
			/* Return and try again later */
		} else if (rc != 0) {
			/* TODO Handle this error */
			assert(0);
		}


		compaction->iter.idx++;
		entry = compaction->iter.entry++;
		md += md_size;
	}

	if (compaction->iter.idx == num_entries) {
		/* All IOs send, we'll continue from callbacks */
		compaction->process = NULL;
	}
}

static void
compaction_process_iter(struct ftl_nv_cache_compaction *compaction)
{
	struct ftl_nv_cache_chunk *chunk;
	struct ftl_wbuf_entry *entry;
	uint64_t num_entries = compaction->batch->num_entries;

	chunk = get_chunk_for_compaction(compaction);
	if (!chunk) {
		/* TODO Handle this error */
		assert(0);
	}

	while (compaction->iter.idx < num_entries) {
		entry = compaction->iter.entry;

		if (!is_chunk_to_read(chunk)) {
			chunk = get_chunk_for_compaction(compaction);
		}

		uint64_t read_ptr = chunk->read_pointer++;
		entry->addr.cache_offset = chunk->offset + read_ptr;


		compaction->iter.idx++;
		entry = compaction->iter.entry++;
	}

	if (compaction->iter.idx != num_entries) {
		/* TODO Handle this error */
		assert(0);
	}

	compaction->process = compaction_process_read;
}

static struct ftl_nv_cache_compaction *compaction_alloc(
	struct spdk_ftl_dev *dev)
{
	struct ftl_batch *batch;
	struct ftl_nv_cache_compaction *compaction;
	struct ftl_wbuf_entry *entry;
	uint64_t i;
	size_t size;

	size = sizeof(*compaction) + (sizeof(compaction->entries[0])
				      * dev->xfer_size);
	compaction = calloc(1, size);
	if (!compaction) {
		goto ERROR;
	}
	compaction->process = compaction_process_iter;
	compaction->nv_cache = &dev->nv_cache;

	if (TAILQ_EMPTY(&dev->free_batches)) {
		goto ERROR;
	}

	compaction->batch = batch = TAILQ_FIRST(&dev->free_batches);
	TAILQ_REMOVE(&dev->free_batches, batch, tailq);
	batch->num_entries = dev->xfer_size;

	batch->metadata = spdk_mempool_get(dev->nv_cache.md_pool);
	if (!batch->metadata) {
		goto ERROR;
	}

	entry = compaction->entries;
	for (i = 0; i < batch->num_entries; ++i, ++entry) {
		entry->payload = spdk_zmalloc(FTL_BLOCK_SIZE,
					      FTL_BLOCK_SIZE, NULL, SPDK_ENV_LCORE_ID_ANY,
					      SPDK_MALLOC_DMA);
		if (!entry->payload) {
			goto ERROR;
		}

		entry->addr.offset = FTL_ADDR_INVALID;
		entry->lba = FTL_LBA_INVALID;
		entry->addr.cached = true;
		entry->index = i;

		batch->iov[i].iov_base = entry->payload;
		batch->iov[i].iov_len = FTL_BLOCK_SIZE;

		TAILQ_INSERT_TAIL(&batch->entries, entry, tailq);
	}

	compaction->iter.entry = compaction->entries;
	compaction->iter.idx = 0;
	TAILQ_INIT(&compaction->chunk_list);

	return compaction;

ERROR:
	compaction_free(dev, compaction);
	return NULL;
}

void ftl_nv_cache_compact(struct spdk_ftl_dev *dev)
{
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;

	if (!_is_compaction_required(nv_cache)) {
		return;
	}

	if (nv_cache->compaction_process->process) {
		nv_cache->compaction_process->process(
			nv_cache->compaction_process);
	}
}


