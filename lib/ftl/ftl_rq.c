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
#include "spdk/env.h"

#include "ftl_rq.h"
#include "ftl_core.h"
#include "ftl_band.h"

struct ftl_rq *ftl_rq_new(struct spdk_ftl_dev *dev, uint32_t num_blocks,
		uint32_t io_md_size)
{
	struct ftl_rq *rq;
	struct ftl_rq_entry *entry;
	struct iovec *io_vec;
	void *io_payload, *io_md;
	uint64_t i;
	size_t size;

	if (0 == num_blocks) {
		num_blocks = dev->xfer_size;
	}

	size = sizeof(*rq) + (sizeof(rq->entries[0]) * num_blocks);
	rq = calloc(1, size);
	if (!rq) {
		return NULL;
	}
	rq->dev = dev;
	rq->num_blocks = num_blocks;

	/* Allocate payload for IO and IO vector */
	io_payload = rq->io_payload = spdk_zmalloc(FTL_BLOCK_SIZE * num_blocks,
			FTL_BLOCK_SIZE, NULL, SPDK_ENV_LCORE_ID_ANY,
			SPDK_MALLOC_DMA);
	if (!io_payload) {
		goto ERROR;
	}
	rq->io_vec = calloc(num_blocks, sizeof(rq->io_vec[0]));
	if (!rq->io_vec) {
		goto ERROR;
	}
	rq->io_vec_size = num_blocks;

	/* Allocate extended metadata for IO */
	if (io_md_size) {
		rq->io_md_size = io_md_size;
		io_md = rq->io_md = spdk_zmalloc(io_md_size * num_blocks, FTL_BLOCK_SIZE,
				NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
		if (!io_md) {
			goto ERROR;
		}
	}

	entry = rq->entries;
	io_vec = rq->io_vec;
	for (i = 0; i < num_blocks; ++i) {
		entry->addr.offset = FTL_ADDR_INVALID;
		entry->lba = FTL_LBA_INVALID;
		entry->index = i;
		entry->io_payload = io_payload;
		entry->io_md = io_md;

		io_vec->iov_base = io_payload;
		io_vec->iov_len = FTL_BLOCK_SIZE;

		entry++;
		io_vec++;
		io_payload += FTL_BLOCK_SIZE;
		io_md += io_md_size;
	}

	return rq;
ERROR:
	ftl_rq_del(rq);
	return NULL;
}

void ftl_rq_del(struct ftl_rq *rq)
{
	if (!rq) {
		return;
	}

	if (rq->io_payload) {
		spdk_free(rq->io_payload);
	}

	if (rq->io_md) {
		spdk_free(rq->io_md);
	}

	if (rq->io_vec) {
		free(rq->io_vec);
	}

	free(rq);
}

void ftl_rq_update_l2p(struct ftl_rq *rq)
{
	struct spdk_ftl_dev *dev = rq->dev;
	struct ftl_rq_entry *iter = rq->entries;
	struct ftl_addr addr = rq->io.addr;
	struct ftl_band *band= rq->io.band;
	uint64_t i;

	for (i = 0; i < rq->num_blocks; i++, iter++) {
		ftl_update_l2p(dev, iter->lba, addr, iter->addr);
		addr = ftl_band_next_addr(band, addr, 1);
	}
}
