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

#ifndef LIB_FTL_FTL_NV_CACHE_H_
#define LIB_FTL_FTL_NV_CACHE_H_

#include "spdk/stdinc.h"
#include "spdk/assert.h"

#include "ftl_io.h"
#include "ftl_rq.h"

#define FTL_NV_CACHE_HEADER_VERSION (1)
#define FTL_NV_CACHE_DATA_OFFSET    (1)
#define FTL_NV_CACHE_PHASE_OFFSET   (62)
#define FTL_NV_CACHE_PHASE_COUNT    (4)
#define FTL_NV_CACHE_PHASE_MASK     (3ULL << FTL_NV_CACHE_PHASE_OFFSET)
#define FTL_NV_CACHE_LBA_INVALID    (FTL_LBA_INVALID & ~FTL_NV_CACHE_PHASE_MASK)

struct spdk_ftl_dev;
struct ftl_nv_cache;

struct ftl_nv_cache_block_metadata {
	uint64_t lba;
	uint64_t sid;
};

struct ftl_nv_cache_chunk {
	uint64_t offset;
	uint64_t write_pointer;
	uint64_t read_pointer;
	uint64_t blocks_written;
	uint64_t blocks_skipped;
	uint64_t blocks_compacted;
	TAILQ_ENTRY(ftl_nv_cache_chunk) entry;
};

struct ftl_nv_cache_compaction {
	struct ftl_nv_cache *nv_cache;
	struct ftl_rq *wr;
	struct ftl_rq *rd;
	struct ftl_nv_cache_chunk *current_chunk;
	uint64_t rd_ptr;
	TAILQ_HEAD(, ftl_nv_cache_chunk) chunk_list;
	TAILQ_ENTRY(ftl_nv_cache_compaction) entry;
};

struct ftl_nv_cache {
	/* FTL device */
	struct spdk_ftl_dev *ftl_dev;

	/* Flag indicating halt request */
	bool halt;

	/* Write buffer cache bdev */
	struct spdk_bdev_desc *bdev_desc;

	/* Persistent cache IO channel */
	struct spdk_io_channel *cache_ioch;

	/* Flag indicating NV cache is read */
	bool ready;

	/* Metadata pool */
	struct spdk_mempool *md_pool;

	/* Metadata place holder for user's reads */
	void *md_rd;

	/* Block Metadata size */
	uint64_t md_size;

	/* Sum of block transfered from user to NV cache */
	uint64_t load_blocks;

	/* Number of data blocks */
	uint64_t num_data_blocks;

	/* Number of data blocks */
	uint64_t num_meta_blocks;

	uint64_t num_meta_compaction_blocks;
	uint64_t num_meta_chunk_blocks;

	struct ftl_nv_cache_chunk *chunk;
	uint64_t chunk_count;
	uint64_t chunk_size;
	uint64_t chunk_free_count;
	uint64_t chunk_full_count;
	uint64_t chunk_compaction_threshold;
	struct ftl_nv_cache_chunk *chunk_current;
	TAILQ_HEAD(, ftl_nv_cache_chunk) chunk_free_list;
	TAILQ_HEAD(, ftl_nv_cache_chunk) chunk_full_list;
	TAILQ_HEAD(, ftl_nv_cache_compaction) compaction_list;
	uint64_t compaction_active_count;

	struct ftl_nv_cache_block_metadata *vss;
	int64_t vss_size;
	int64_t vss_md_size;
};

int ftl_nv_cache_init(struct spdk_ftl_dev *dev, const char *bdev_name);

void ftl_nv_cache_deinit(struct spdk_ftl_dev *dev);

bool ftl_nv_cache_full(struct spdk_ftl_dev *dev);

static inline void ftl_nv_cache_halt(struct ftl_nv_cache *nv_cache)
{
	nv_cache->halt = true;
}

static inline void ftl_nv_cache_resume(struct ftl_nv_cache *nv_cache)
{
	nv_cache->halt = false;
}

static inline bool ftl_nv_cache_is_halted(struct ftl_nv_cache *nv_cache)
{
	return 0 == nv_cache->compaction_active_count;
}

uint64_t ftl_nv_cache_get_wr_buffer(struct ftl_nv_cache *nv_cache,
				    struct ftl_io *io);

void ftl_nv_cache_commit_wr_buffer(struct ftl_nv_cache *nv_cache,
				   struct ftl_io *io);

int ftl_nv_cache_read(struct ftl_io *io, struct ftl_addr addr,
		      uint32_t num_blocks,
		      spdk_bdev_io_completion_cb cb, void *cb_arg);

int ftl_nv_cache_write(struct ftl_io *io, struct ftl_addr addr,
		       uint32_t num_blocks,
		       void *md, spdk_bdev_io_completion_cb cb, void *cb_arg);

void ftl_nv_cache_compact(struct spdk_ftl_dev *dev);

static inline void
ftl_nv_cache_pack_lba(uint64_t lba, void *md_buf)
{
	struct ftl_nv_cache_block_metadata *md = md_buf;
	md->lba = lba;
}

static inline uint64_t
ftl_nv_cache_unpack_lba(void *md_buf)
{
	struct ftl_nv_cache_block_metadata *md = md_buf;
	return md->lba;
}

void ftl_nv_cache_fill_md(struct ftl_io *io);

void ftl_nv_cache_save_state(struct ftl_nv_cache *nv_cache,
		void (*cb)(void *cntx, bool status), void *cb_cntx);

void ftl_nv_cache_load_state(struct ftl_nv_cache *nv_cache,
		void (*cb)(void *cntx, bool status), void *cb_cntx);

#endif  /* LIB_FTL_FTL_NV_CACHE_H_ */
