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

#ifndef LIB_BDEV_COLLECTIONS_COMMON_H_
#define LIB_BDEV_COLLECTIONS_COMMON_H_

#include "spdk_internal/bdev.h"

#define ROUND_DOWN(a,b) (((a) * (b)) / (b))

struct sg_list {
	int iovcnt;
	struct iovec iov[];
};

static inline struct sg_list *alloc_sg_list(int iovcnt)
{
	struct sg_list *sgl = malloc(offsetof(struct sg_list, iov) + iovcnt * sizeof(struct iovec));
	if (!sgl) {
		SPDK_ERRLOG("Unable to allocate sg_list\n");
		return NULL;
	}
	sgl->iovcnt = iovcnt;
	return sgl;
}

static inline struct sg_list *copy_sg_list(const struct iovec *iov, int iovcnt)
{
	struct sg_list *sgl = alloc_sg_list(iovcnt);
	if (sgl) {
		memcpy(sgl->iov, iov, sgl->iovcnt * sizeof(*sgl->iov));
	}
	return sgl;
}

static inline struct sg_list *clone_sg_list(const struct sg_list *sgl)
{
	return copy_sg_list(sgl->iov, sgl->iovcnt);
}

struct sg_list *split_iov(const struct iovec *iov, int iovcnt, uint64_t offset, uint64_t len);

static inline struct sg_list *split_sg_list(const struct sg_list *sgl, uint64_t offset, uint64_t len)
{
	return split_iov(sgl->iov, sgl->iovcnt, offset, len);
}

static inline uint64_t sg_list_len(const struct sg_list *sgl)
{
	int i;
	uint64_t len = 0;
	for (i = 0; i < sgl->iovcnt; i++) {
		len += sgl->iov[i].iov_len;
	}
	return len;
}

struct sg_list *merge_sg_lists(const struct sg_list *sgl_1, const struct sg_list *sgl_2, uint64_t offset);

void xor_sgl(struct sg_list *sgl1, const struct sg_list *sgl2);
int memcmp_sgl(const struct sg_list *sgl1, const struct sg_list *sgl2);
void memcpy_sgl(struct sg_list *sgl1, const struct sg_list *sgl2);

int bdev_iov_submit_blocks(enum spdk_bdev_io_type io_type,
			   struct spdk_bdev_desc *desc,
			   struct spdk_io_channel *ch,
			   uint64_t offset_blocks,
			   uint64_t num_blocks,
			   struct sg_list *sgl,
			   spdk_bdev_io_completion_cb cb, void *cb_arg);

static inline char bdev_io_type_char(enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
		return 'R';
	case SPDK_BDEV_IO_TYPE_WRITE:
		return 'W';
	case SPDK_BDEV_IO_TYPE_UNMAP:
		return 'U';
	case SPDK_BDEV_IO_TYPE_FLUSH:
		return 'F';
	case SPDK_BDEV_IO_TYPE_RESET:
		return 'R';
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		return 'Z';
	default:
		return '?';
	}
}

//#undef SPDK_DEBUGLOG
//#define SPDK_DEBUGLOG(FLAG, ...) spdk_log(SPDK_LOG_NOTICE, __FILE__, __LINE__, __func__, __VA_ARGS__)

struct mempool_elem {
	struct sg_list *sgl;
	TAILQ_ENTRY(mempool_elem) link;
	struct mempool *mempool;
	bool extra;
};

struct mempool {
	size_t item_size;
	size_t item_align;
	TAILQ_HEAD(, mempool_elem) busy;
	TAILQ_HEAD(, mempool_elem) free;
	pthread_spinlock_t lock;
	TAILQ_HEAD(, event_cb) on_reclaimed;
};

struct event_cb {
	const struct spdk_thread *thread;
	spdk_thread_fn fn;
	void *ctx;
	TAILQ_ENTRY(event_cb) link;
};

void mempool_free(struct mempool *mempool);
struct mempool *mempool_alloc(unsigned int items, size_t item_size, size_t item_align);
struct mempool_elem *mempool_elem_get(struct mempool *mempool);
void mempool_elem_put(struct mempool_elem *e);
void mempool_on_reclaimed_add(struct mempool *mempool, spdk_thread_fn fn, void *ctx);

#define for_each_member(c, m) \
for ((m) = (c)->members; (m) < (c)->members + (c)->members_count; (m)++)

#endif /* LIB_BDEV_COLLECTIONS_COMMON_H_ */
