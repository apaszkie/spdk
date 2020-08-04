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

#define FTL_NV_CACHE_HEADER_VERSION (1)
#define FTL_NV_CACHE_DATA_OFFSET    (1)
#define FTL_NV_CACHE_PHASE_OFFSET   (62)
#define FTL_NV_CACHE_PHASE_COUNT    (4)
#define FTL_NV_CACHE_PHASE_MASK     (3ULL << FTL_NV_CACHE_PHASE_OFFSET)
#define FTL_NV_CACHE_LBA_INVALID    (FTL_LBA_INVALID & ~FTL_NV_CACHE_PHASE_MASK)

struct spdk_ftl_dev;

struct ftl_nv_cache {
	/* Write buffer cache bdev */
	struct spdk_bdev_desc           *bdev_desc;
	/* Write pointer */
	uint64_t                current_addr;
	/* Number of available blocks left */
	uint64_t                num_available;
	/* Maximum number of blocks */
	uint64_t                num_data_blocks;
	/*
	 * Phase of the current cycle of writes. Each time whole cache area is filled, the phase is
	 * advanced. Current phase is saved in every IO's metadata, as well as in the header saved
	 * in the first sector. By looking at the phase of each block, it's possible to find the
	 * oldest block and replay the order of the writes when recovering the data from the cache.
	 */
	unsigned int                phase;
	/* Indicates that the data can be written to the cache */
	bool                    ready;
	/* Metadata pool */
	struct spdk_mempool         *md_pool;
	/* DMA buffer for writing the header */
	void                    *dma_buf;

	/* Cache lock */
	pthread_spinlock_t          lock;
};

int ftl_nv_cache_init(struct spdk_ftl_dev *dev, const char *bdev_name);

#endif  /* LIB_FTL_FTL_NV_CACHE_H_ */
