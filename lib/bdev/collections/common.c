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

#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/conf.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/io_channel.h"
#include "spdk/util.h"

#include "spdk/bdev.h"
#include "spdk_internal/log.h"

#include "common.h"

struct sg_list *split_iov(const struct iovec *iov, int iovcnt, uint64_t offset, uint64_t len)
{
	int i;
	size_t off = 0;
	int start_v = -1;
	size_t start_v_off;
	struct sg_list *sgl;
	int new_iovcnt = 0;

	for (i = 0; i < iovcnt; i++) {
		if (off + iov[i].iov_len > offset) {
			start_v = i;
			break;
		}
		off += iov[i].iov_len;
	}

	assert(start_v != -1);

	start_v_off = off;

	for (i = start_v; i < iovcnt; i++) {
		new_iovcnt++;

		if (off + iov[i].iov_len >= offset + len) {
			break;
		}
		off += iov[i].iov_len;
	}

	assert(start_v + new_iovcnt <= iovcnt);

	// TODO: don't alloc if new_iov is a simple subset of iov
	sgl = alloc_sg_list(new_iovcnt);
	if (!sgl) {
		return NULL;
	}

	off = start_v_off;
	iov += start_v;

	for (i = 0; i < new_iovcnt; i++) {
		sgl->iov[i].iov_base = iov->iov_base + (offset - off);
		sgl->iov[i].iov_len = spdk_min(len, iov->iov_len - (offset - off));

		off += iov->iov_len;
		iov++;
		offset += sgl->iov[i].iov_len;
		len -= sgl->iov[i].iov_len;
	}

	return sgl;
}

struct sg_list *merge_sg_lists(const struct sg_list *sgl_1, const struct sg_list *sgl_2, uint64_t offset)
{
	struct iovec *v;
	const struct iovec *v1, *v2;
	size_t off1 = 0;
	struct sg_list *sgl;

	if (offset + sg_list_len(sgl_2) > sg_list_len(sgl_1)) {
		SPDK_ERRLOG("sgl_2 too big\n");
		return NULL;
	}

	sgl = alloc_sg_list(sgl_1->iovcnt + sgl_2->iovcnt + 2);
	if (!sgl) {
		return NULL;
	}

	v = sgl->iov;
	v1 = sgl_1->iov;
	v2 = sgl_2->iov;

	while (off1 < offset) {
		off1 += v1->iov_len;
		*(v++) = *(v1++);
	}

	if (off1 > offset) {
		(v - 1)->iov_len -= (off1 - offset);
	}

	while (v2 < sgl_2->iov + sgl_2->iovcnt) {
		offset += v2->iov_len;
		*(v++) = *(v2++);
	}

	while (off1 < offset) {
		off1 += v1->iov_len;
		v1++;
	}

	if (off1 > offset) {
		v->iov_base = (v1 - 1)->iov_base + ((v1 - 1)->iov_len - (off1 - offset));
		v->iov_len = off1 - offset;
		offset += v->iov_len;
		v++;
	}

	while (v1 < sgl_1->iov + sgl_1->iovcnt) {
		*(v++) = *(v1++);
	}

	sgl->iovcnt = v - sgl->iov;

	return sgl;
}

static void *xor_buf(void *restrict to, const void *restrict from, size_t size)
{
	uint64_t *_to;
	const uint64_t *_from;
	size_t i;

	assert(size % sizeof(uint64_t) == 0);

	_to = to;
	_from = from;
	size /= sizeof(uint64_t);

	for (i = 0; i < size; i++) {
		_to[i] = _to[i] ^ _from[i];
	}

	return to;
}

static int iter_sgl(struct sg_list *sgl1, const struct sg_list *sgl2, void *(*fn1)(void *, const void *, size_t), int (*fn2)(const void *, const void *, size_t))
{
	struct iovec *v1;
	const struct iovec *v2;
	size_t off1, off2;
	int ret = 0;

	assert(fn1 || fn2);
	assert(sg_list_len(sgl1) == sg_list_len(sgl2));

	off1 = off2 = 0;
	v1 = sgl1->iov;
	v2 = sgl2->iov;

	while (v1 < sgl1->iov + sgl1->iovcnt &&
	       v2 < sgl2->iov + sgl2->iovcnt) {
		size_t len = spdk_min(v1->iov_len - off1, v2->iov_len - off2);

		if (fn1) {
			fn1(v1->iov_base + off1, v2->iov_base + off2, len);
		} else {
			ret = fn2(v1->iov_base + off1, v2->iov_base + off2, len);
			if (ret) {
				break;
			}
		}

		off1 += len;
		off2 += len;

		if (off1 == v1->iov_len) {
			off1 = 0;
			v1++;
		}

		if (off2 == v2->iov_len) {
			off2 = 0;
			v2++;
		}
	}

	return ret;
}

void xor_sgl(struct sg_list *sgl1, const struct sg_list *sgl2)
{
	iter_sgl(sgl1, sgl2, xor_buf, NULL);
}

int memcmp_sgl(const struct sg_list *sgl1, const struct sg_list *sgl2)
{
	return iter_sgl((struct sg_list *)sgl1, sgl2, NULL, memcmp);
}

void memcpy_sgl(struct sg_list *sgl1, const struct sg_list *sgl2)
{
	iter_sgl(sgl1, sgl2, memcpy, NULL);
}

int bdev_iov_submit_blocks(enum spdk_bdev_io_type io_type,
			   struct spdk_bdev_desc *desc,
			   struct spdk_io_channel *ch,
			   uint64_t offset_blocks,
			   uint64_t num_blocks,
			   struct sg_list *sgl,
			   spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	int (*rw_func)(struct spdk_bdev_desc *, struct spdk_io_channel *, struct iovec *, int,
		       uint64_t, uint64_t, spdk_bdev_io_completion_cb, void *) = NULL;

	if (io_type == SPDK_BDEV_IO_TYPE_READ)
		rw_func = spdk_bdev_readv_blocks;
	else if (io_type == SPDK_BDEV_IO_TYPE_WRITE)
		rw_func = spdk_bdev_writev_blocks;
	else {
		SPDK_ERRLOG("Unsupported io_type: %d\n", io_type);
		return -EINVAL;
	}

	return rw_func(desc, ch, sgl->iov, sgl->iovcnt, offset_blocks, num_blocks, cb, cb_arg);
}

static void mempool_elem_free(struct mempool_elem *e)
{
	spdk_dma_free(e->sgl->iov->iov_base);
	free(e->sgl);
	free(e);
}

void mempool_free(struct mempool *mempool)
{
	struct mempool_elem *e;

	assert(TAILQ_EMPTY(&mempool->busy));

	pthread_spin_destroy(&mempool->lock);

	while ((e = TAILQ_FIRST(&mempool->free))) {
		TAILQ_REMOVE(&mempool->free, e, link);

		mempool_elem_free(e);
	}
}

static struct mempool_elem *_mempool_elem_alloc(struct mempool *mempool)
{
	struct mempool_elem *e;
	struct sg_list *sgl;

	sgl = alloc_sg_list(1);
	if (!sgl) {
		return NULL;
	}

	sgl->iov->iov_base = spdk_dma_malloc(mempool->item_size, mempool->item_align, NULL);
	if (!sgl->iov->iov_base) {
		free(sgl);
		return NULL;
	}
	sgl->iov->iov_len = mempool->item_size;

	e = malloc(sizeof(*e));
	if (!e) {
		spdk_dma_free(sgl->iov->iov_base);
		free(sgl);
		return NULL;
	}

	e->mempool = mempool;
	e->sgl = sgl;

	return e;
}

struct mempool *mempool_alloc(unsigned int items, size_t item_size, size_t item_align)
{
	struct mempool_elem *e;
	struct mempool *mempool;

	mempool = calloc(1, sizeof(*mempool));
	if (!mempool) {
		return NULL;
	}

	mempool->item_size = item_size;
	mempool->item_align = item_align;

	TAILQ_INIT(&mempool->busy);
	TAILQ_INIT(&mempool->free);
	TAILQ_INIT(&mempool->on_reclaimed);
	pthread_spin_init(&mempool->lock, PTHREAD_PROCESS_PRIVATE);

	while (items--) {
		e = _mempool_elem_alloc(mempool);
		if (!e) {
			goto err;
		}
		e->extra = false;

		TAILQ_INSERT_TAIL(&mempool->free, e, link);
	}

	return mempool;
err:
	mempool_free(mempool);
	return NULL;
}

void mempool_elem_put(struct mempool_elem *e)
{
	struct mempool *mempool = e->mempool;
	struct event_cb *ev;

	if (e->extra) {
		mempool_elem_free(e);
		return;
	}
	pthread_spin_lock(&mempool->lock);
	TAILQ_REMOVE(&mempool->busy, e, link);
	TAILQ_INSERT_TAIL(&mempool->free, e, link);
	ev = TAILQ_FIRST(&mempool->on_reclaimed);
	if (ev) {
		TAILQ_REMOVE(&mempool->on_reclaimed, ev, link);
	}
	pthread_spin_unlock(&mempool->lock);
	if (ev) {
		spdk_thread_send_msg(ev->thread, ev->fn, ev->ctx);
		free(ev);
	}
}

void mempool_on_reclaimed_add(struct mempool *mempool, spdk_thread_fn fn, void *ctx)
{
	struct event_cb *e = malloc(sizeof(*e));
	assert(e); // TODO: this must not fail
	e->thread = spdk_get_thread();
	e->fn = fn;
	e->ctx = ctx;
	pthread_spin_lock(&mempool->lock);
	TAILQ_INSERT_TAIL(&mempool->on_reclaimed, e, link);
	pthread_spin_unlock(&mempool->lock);
}

struct mempool_elem *mempool_elem_get(struct mempool *mempool)
{
	struct mempool_elem *e;

	pthread_spin_lock(&mempool->lock);
	e = TAILQ_FIRST(&mempool->free);
	if (e) {
		TAILQ_REMOVE(&mempool->free, e, link);
		TAILQ_INSERT_TAIL(&mempool->busy, e, link);
	}
	pthread_spin_unlock(&mempool->lock);

	if (!e) {
		e = _mempool_elem_alloc(mempool);
		if (e) {
			e->extra = true;
		}
	}

	return e;
}
