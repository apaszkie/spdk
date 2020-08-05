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

	nv_cache->md_pool = spdk_mempool_create(pool_name, conf->nv_cache.max_request_cnt,
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

	nv_cache->chunk_size = FTL_NV_CACHE_CHUNK_SIZE_BYTES / FTL_BLOCK_SIZE;
	nv_cache->chunk_count = spdk_bdev_get_num_blocks(bdev) / nv_cache->chunk_size;
	nv_cache->chunk = calloc(nv_cache->chunk_count, sizeof(nv_cache->chunk[0]));
	for (i = 0; i < nv_cache->chunk_count; i++) {
		struct ftl_nv_cache_chunk *chunk = &nv_cache->chunk[i];

		chunk->id = i;
		chunk->offset = i * nv_cache->chunk_size;
		TAILQ_INSERT_TAIL(&nv_cache->chunk_free_list, chunk, entry);
	}

	return 0;
}

static inline uint64_t _chunk_get_free_space(
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

static inline void _chunk_skip_blocks(
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
	} else if (spdk_unlikely(bytes_written > nv_cache->chunk_size)) {
		assert(0);
	}
}

static inline void _chunk_advance_blocks(
	struct ftl_nv_cache *nv_cache,
	struct ftl_nv_cache_chunk *chunk, uint64_t advanced_blocks)
{
	uint64_t blocks_written = __atomic_add_fetch(&chunk->blocks_written,
				  advanced_blocks, __ATOMIC_SEQ_CST);

	if (blocks_written == nv_cache->chunk_size) {
		/* Chunk full move it on full list */
		pthread_spin_lock(&nv_cache->lock);
		TAILQ_INSERT_TAIL(&nv_cache->chunk_full_list, chunk, entry);
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
