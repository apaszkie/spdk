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
#ifndef FTL_RQ_H
#define FTL_RQ_H

#include "spdk/stdinc.h"

#include "ftl_addr.h"

struct ftl_rq_entry {
	/* Data payload of single entry (block) */
	void *io_payload;

	void *io_md;

	/* Physical address of that particular block.  Valid once the data has
	 * been written out.
	 */
	struct ftl_addr addr;

	/* Logical block address */
	uint64_t lba;

	/* Index of this entry within FTL request */
	uint64_t index;

	struct {
		void *priv;
	} owner;

//	/* IO channel that owns the write bufer entry */
//	struct ftl_io_channel			*ioch;

//	/* Index within the IO channel's wbuf_entries array */
//	uint32_t				index;
//	uint32_t				io_flags;
//	/* Points at the band the data is copied from.  Only valid for internal
//	 * requests coming from reloc.
//	 */
//	struct ftl_band				*band;
//	/* Physical address of that particular block.  Valid once the data has
//	 * been written out.
//	 */
//	struct ftl_addr				addr;
//	/* Logical block address */
//	uint64_t				lba;
//
//	/* Trace ID of the requests the entry is part of */
//	uint64_t				trace;
//
//	/* Private data of the entry holder */
//	void					*priv_data;
//
//	/* Indicates that the entry was written out and is still present in the
//	 * L2P table.
//	 */
//	bool					valid;
//	/* Lock that protects the entry from being evicted from the L2P */
//	pthread_spinlock_t			lock;
//	TAILQ_ENTRY(ftl_wbuf_entry)		tailq;
};

struct ftl_rq {
	struct spdk_ftl_dev *dev;

	/* Request queue entry */
	TAILQ_ENTRY(ftl_rq) qentry;

	/* Number of block within the request */
	uint64_t num_blocks;

	/* Extended metadata for IO. Its size is io_md_size * num_blocks */
	void *io_md;

	/* Size of extended metadata size for one entry */
	uint64_t io_md_size;

	/* Array of IO vectors, its size equals to num_blocks  */
	struct iovec *io_vec;

	/* Payload for IO */
	void *io_payload;

	/* Fields for owner of this request */
	struct {
		/* End request callback */
		void (*cb)(struct ftl_rq *rq);

		/* Owner context */
		void *priv;
	} owner;

	/* Iterator fields for processing state of the request */
	struct {
		uint32_t idx;
		uint32_t count;
	} iter;

	struct ftl_rq_entry entries[];
};

/**
 * @brief Get new FTL request
 *
 * @param dev FTL device
 * @param num_blocks Number of block for newly created request.
 * @param io_md_size Extended metadata size for IO
 *
 * @note if num_blocks equals to 0 then FTL device transfer size will be used
 * for size
 *
 * @return New FTL request
 */
struct ftl_rq *ftl_rq_new(struct spdk_ftl_dev *dev, uint32_t num_blocks,
		uint32_t io_md_size);

/**
 * @brief Delete FTL request
 *
 * @param rq FTL request to be deleted
 */
void ftl_rq_del(struct ftl_rq *rq);


static inline void
ftl_rq_swap_payload(struct ftl_rq *a, uint32_t aidx,
		struct ftl_rq *b, uint32_t bidx)
{
	assert(aidx < a->num_blocks);
	assert(bidx < b->num_blocks);

	void *a_payload = a->io_vec[aidx].iov_base;
	void *b_payload = b->io_vec[bidx].iov_base;

	a->io_vec[aidx].iov_base = b_payload;
	a->entries[aidx].io_payload = b_payload;

	b->io_vec[bidx].iov_base = a_payload;
	b->entries[bidx].io_payload = a_payload;
}

#endif  // FTL_RQ_H
