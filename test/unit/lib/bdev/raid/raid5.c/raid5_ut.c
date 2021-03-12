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
 *   A PARTICULAR PURPOSE AiRE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"
#include "spdk_cunit.h"
#include "spdk/env.h"
#include "spdk_internal/mock.h"

#include "common/lib/test_env.c"
#include "bdev/raid/raid5.c"

#undef SPDK_CU_ASSERT_FATAL
#define SPDK_CU_ASSERT_FATAL(what) assert(what)

DEFINE_STUB_V(raid_bdev_module_list_add, (struct raid_bdev_module *raid_module));
DEFINE_STUB(rte_hash_hash, hash_sig_t, (const struct rte_hash *h, const void *key), 0);
DEFINE_STUB_V(rte_hash_free, (struct rte_hash *h));
DEFINE_STUB(spdk_bdev_get_buf_align, size_t, (const struct spdk_bdev *bdev), 0);
DEFINE_STUB(spdk_bdev_queue_io_wait, int, (struct spdk_bdev *bdev, struct spdk_io_channel *ch,
		struct spdk_bdev_io_wait_entry *entry), 0);
DEFINE_STUB_V(raid_bdev_queue_io_wait, (struct raid_bdev_io *raid_io, struct spdk_bdev *bdev,
		struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn));

void
raid_bdev_io_complete(struct raid_bdev_io *raid_io, enum spdk_bdev_io_status status)
{
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);

	if (bdev_io->internal.cb) {
		bdev_io->internal.cb(bdev_io, status == SPDK_BDEV_IO_STATUS_SUCCESS, bdev_io->internal.caller_ctx);
	}
}

struct hash_mock {
	uint64_t key;
	void *value;
};

int
rte_hash_lookup_with_hash_data(const struct rte_hash *_h, const void *_key, hash_sig_t sig, void **data)
{
	struct hash_mock *h = (struct hash_mock *)_h;
	uint64_t key = *((uint64_t *)_key);

	if (h->value == NULL || key != h->key) {
		return -ENOENT;
	} else {
		*data = h->value;
		return 0;
	}
}

int
rte_hash_add_key_with_hash_data(const struct rte_hash *_h, const void *_key, hash_sig_t sig, void *data)
{
	struct hash_mock *h = (struct hash_mock *)_h;

	h->key = *((uint64_t *)_key);
	h->value = data;

	return 0;
}

int32_t
rte_hash_del_key_with_hash(const struct rte_hash *_h, const void *_key, hash_sig_t sig)
{
	struct hash_mock *h = (struct hash_mock *)_h;
	uint64_t key = *((uint64_t *)_key);

	if (h->value == NULL || key != h->key) {
		return -ENOENT;
	} else {
		h->value = NULL;
		return 0;
	}
}

struct rte_hash *
rte_hash_create(const struct rte_hash_parameters *params)
{
	static struct hash_mock h;

	h.value = NULL;
	return (struct rte_hash *)&h;
}

int
spdk_thread_send_msg(const struct spdk_thread *thread, spdk_msg_fn fn, void *ctx)
{
	fn(ctx);
	return 0;
}

struct spdk_mempool
*spdk_mempool_create_ctor(const char *name, size_t count,
		size_t ele_size, size_t cache_size, int socket_id,
		spdk_mempool_obj_cb_t *obj_init, void *obj_init_arg)
{
	return spdk_mempool_create(name, count, ele_size, cache_size, socket_id);
}

struct raid5_params {
	uint8_t num_base_bdevs;
	uint64_t base_bdev_blockcnt;
	uint32_t base_bdev_blocklen;
	uint32_t strip_size;
};

static struct raid5_params *g_params;
static size_t g_params_count;

#define ARRAY_FOR_EACH(a, e) \
	for (e = a; e < a + SPDK_COUNTOF(a); e++)

#define RAID5_PARAMS_FOR_EACH(p) \
	for (p = g_params; p < g_params + g_params_count; p++)

static int
test_setup(void)
{
	uint8_t num_base_bdevs_values[] = { 3, 4, 5 };
	uint64_t base_bdev_blockcnt_values[] = { 1, 1024, 1024 * 1024 };
	uint32_t base_bdev_blocklen_values[] = { 512, 4096 };
	uint32_t strip_size_kb_values[] = { 1, 4, 128 };
	uint8_t *num_base_bdevs;
	uint64_t *base_bdev_blockcnt;
	uint32_t *base_bdev_blocklen;
	uint32_t *strip_size_kb;
	struct raid5_params *params;

	g_params_count = SPDK_COUNTOF(num_base_bdevs_values) *
			 SPDK_COUNTOF(base_bdev_blockcnt_values) *
			 SPDK_COUNTOF(base_bdev_blocklen_values) *
			 SPDK_COUNTOF(strip_size_kb_values);
	g_params = calloc(g_params_count, sizeof(*g_params));
	if (!g_params) {
		return -ENOMEM;
	}

	params = g_params;

	ARRAY_FOR_EACH(num_base_bdevs_values, num_base_bdevs) {
		ARRAY_FOR_EACH(base_bdev_blockcnt_values, base_bdev_blockcnt) {
			ARRAY_FOR_EACH(base_bdev_blocklen_values, base_bdev_blocklen) {
				ARRAY_FOR_EACH(strip_size_kb_values, strip_size_kb) {
					params->num_base_bdevs = *num_base_bdevs;
					params->base_bdev_blockcnt = *base_bdev_blockcnt;
					params->base_bdev_blocklen = *base_bdev_blocklen;
					params->strip_size = *strip_size_kb * 1024 / *base_bdev_blocklen;
					if (params->strip_size == 0 ||
					    params->strip_size > *base_bdev_blockcnt) {
						g_params_count--;
						continue;
					}
					params++;
				}
			}
		}
	}

	return 0;
}

static int
test_cleanup(void)
{
	free(g_params);
	return 0;
}

static struct raid_bdev *
create_raid_bdev(struct raid5_params *params)
{
	struct raid_bdev *raid_bdev;
	struct raid_base_bdev_info *base_info;

	raid_bdev = calloc(1, sizeof(*raid_bdev));
	SPDK_CU_ASSERT_FATAL(raid_bdev != NULL);

	raid_bdev->module = &g_raid5_module;
	raid_bdev->num_base_bdevs = params->num_base_bdevs;
	raid_bdev->base_bdev_info = calloc(raid_bdev->num_base_bdevs,
					   sizeof(struct raid_base_bdev_info));
	SPDK_CU_ASSERT_FATAL(raid_bdev->base_bdev_info != NULL);

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		base_info->bdev = calloc(1, sizeof(*base_info->bdev));
		SPDK_CU_ASSERT_FATAL(base_info->bdev != NULL);

		base_info->bdev->blockcnt = params->base_bdev_blockcnt;
		base_info->bdev->blocklen = params->base_bdev_blocklen;
	}

	raid_bdev->strip_size = params->strip_size;
	raid_bdev->strip_size_shift = spdk_u32log2(raid_bdev->strip_size);
	raid_bdev->bdev.blocklen = params->base_bdev_blocklen;

	return raid_bdev;
}

static void
delete_raid_bdev(struct raid_bdev *raid_bdev)
{
	struct raid_base_bdev_info *base_info;

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		free(base_info->bdev);
	}
	free(raid_bdev->base_bdev_info);
	free(raid_bdev);
}

static struct raid5_info *
create_raid5(struct raid5_params *params)
{
	struct raid_bdev *raid_bdev = create_raid_bdev(params);
	struct raid5_info *r5info;
	int ret;

	ret = raid5_start(raid_bdev);

	SPDK_CU_ASSERT_FATAL(ret == 0);

	r5info = raid_bdev->module_private;

	return r5info;
}

static void
delete_raid5(struct raid5_info *r5info)
{
	struct raid_bdev *raid_bdev = r5info->raid_bdev;

	raid5_stop(raid_bdev);

	delete_raid_bdev(raid_bdev);
}

static void
test_raid5_start(void)
{
	struct raid5_params *params;

	RAID5_PARAMS_FOR_EACH(params) {
		struct raid5_info *r5info;

		r5info = create_raid5(params);

		CU_ASSERT_EQUAL(r5info->stripe_blocks, params->strip_size * (params->num_base_bdevs - 1));
		CU_ASSERT_EQUAL(r5info->total_stripes, params->base_bdev_blockcnt / params->strip_size);
		CU_ASSERT_EQUAL(r5info->raid_bdev->bdev.blockcnt,
				(params->base_bdev_blockcnt - params->base_bdev_blockcnt % params->strip_size) *
				(params->num_base_bdevs - 1));
		CU_ASSERT_EQUAL(r5info->raid_bdev->bdev.optimal_io_boundary, params->strip_size);

		delete_raid5(r5info);
	}
}

static void
test_raid5_get_stripe_request(void)
{
	struct raid5_info *r5info;
	struct stripe_request *stripe_req;
	uint64_t i;
	struct raid5_io_channel r5ch;
	int ret;

	r5info = create_raid5(g_params);

	ret = raid5_io_channel_resource_init(r5info->raid_bdev, &r5ch);
	SPDK_CU_ASSERT_FATAL(ret == 0);

	for (i = 0; i < RAID5_MAX_STRIPES; i++) {
		stripe_req = raid5_get_stripe_request(&r5ch, i);
		SPDK_CU_ASSERT_FATAL(stripe_req != NULL);
		SPDK_CU_ASSERT_FATAL(stripe_req->stripe_index == i);
		free(stripe_req);
	}

	stripe_req = raid5_get_stripe_request(&r5ch, i);
	CU_ASSERT(stripe_req == NULL);

	raid5_io_channel_resource_deinit(&r5ch);

	delete_raid5(r5info);
}

struct stripe_io_info {
	enum spdk_bdev_io_status status;
	bool failed;
	int remaining;
	TAILQ_HEAD(, spdk_bdev_io) bdev_io_queue;
};

struct raid_io_info {
	char bdev_io_buf[sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io)];
	struct stripe_io_info *stripe_io_info;
	void *buf;
	void *parity_buf;
};

static struct raid_bdev_io *
get_raid_io(enum spdk_bdev_io_type io_type, struct raid5_info *r5info, struct stripe_io_info *stripe_io_info,
	    struct raid_bdev_io_channel *raid_ch, uint64_t offset_blocks, uint64_t num_blocks,
	    void *src_buf, void *dest_buf, void *parity_buf, spdk_bdev_io_completion_cb cb)
{
	struct spdk_bdev_io *bdev_io;
	struct raid_bdev_io *raid_io;
	struct raid_bdev *raid_bdev = r5info->raid_bdev;

	struct raid_io_info *io_info = calloc(1, sizeof(*io_info));
	SPDK_CU_ASSERT_FATAL(io_info != NULL);

	SPDK_CU_ASSERT_FATAL(io_info->bdev_io_buf == (char *)io_info);
	bdev_io = (struct spdk_bdev_io *)io_info->bdev_io_buf;
	bdev_io->bdev = &raid_bdev->bdev;
	bdev_io->type = io_type;
	bdev_io->u.bdev.offset_blocks = offset_blocks;
	bdev_io->u.bdev.num_blocks = num_blocks;
	bdev_io->internal.cb = cb;
	bdev_io->internal.caller_ctx = stripe_io_info;

	raid_io = (void *)bdev_io->driver_ctx;
	raid_io->raid_bdev = raid_bdev;
	raid_io->raid_ch = raid_ch;

	io_info->stripe_io_info = stripe_io_info;
	io_info->parity_buf = parity_buf;

	if (io_type == SPDK_BDEV_IO_TYPE_READ) {
		io_info->buf = src_buf;
		bdev_io->iov.iov_base = dest_buf;
	} else {
		io_info->buf = dest_buf;
		bdev_io->iov.iov_base = src_buf;
	}

	bdev_io->u.bdev.iovs = &bdev_io->iov;
	bdev_io->u.bdev.iovcnt = 1;
	bdev_io->iov.iov_len = num_blocks * bdev_io->bdev->blocklen;

	return raid_io;
}

void
spdk_bdev_free_io(struct spdk_bdev_io *bdev_io)
{
	free(bdev_io);
}

static void
submit_io(struct stripe_io_info *stripe_io_info, spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct spdk_bdev_io *bdev_io;

	bdev_io = calloc(1, sizeof(*bdev_io));
	SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
	bdev_io->internal.cb = cb;
	bdev_io->internal.caller_ctx = cb_arg;

	TAILQ_INSERT_TAIL(&stripe_io_info->bdev_io_queue, bdev_io, internal.link);
}

static void
process_io_completions(struct stripe_io_info *stripe_io_info)
{
	struct spdk_bdev_io *bdev_io;

	while ((bdev_io = TAILQ_FIRST(&stripe_io_info->bdev_io_queue))) {
		TAILQ_REMOVE(&stripe_io_info->bdev_io_queue, bdev_io, internal.link);

		bdev_io->internal.cb(bdev_io, true, bdev_io->internal.caller_ctx);
	}
}

int
spdk_bdev_writev_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			struct iovec *iov, int iovcnt,
			uint64_t offset_blocks, uint64_t num_blocks,
			spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct chunk *chunk = cb_arg;
	struct stripe_request *stripe_req;
	struct raid_io_info *io_info;
	void *dest_buf;

	SPDK_CU_ASSERT_FATAL(cb == raid5_chunk_write_complete);
	SPDK_CU_ASSERT_FATAL(iovcnt == 1);

	stripe_req = raid5_chunk_stripe_req(chunk);

	if (chunk == stripe_req->parity_chunk) {
		io_info = (struct raid_io_info *)spdk_bdev_io_from_ctx((__NEXT_DATA_CHUNK(stripe_req, stripe_req->chunks-1))->raid_io);
		dest_buf = io_info->parity_buf;

	} else {
		io_info = (struct raid_io_info *)spdk_bdev_io_from_ctx(chunk->raid_io);
		dest_buf = io_info->buf;
	}

	memcpy(dest_buf, iov->iov_base, iov->iov_len);

	submit_io(io_info->stripe_io_info, cb, cb_arg);

	return 0;
}

int
spdk_bdev_readv_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       struct iovec *iov, int iovcnt,
		       uint64_t offset_blocks, uint64_t num_blocks,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;
	struct raid_io_info *io_info;

	SPDK_CU_ASSERT_FATAL(cb == raid5_chunk_read_complete);
	SPDK_CU_ASSERT_FATAL(iovcnt == 1);

	io_info = (struct raid_io_info *)spdk_bdev_io_from_ctx(raid_io);

	memcpy(iov->iov_base, io_info->buf, iov->iov_len);

	submit_io(io_info->stripe_io_info, cb, cb_arg);

	return 0;
}

static void
xor_block(uint8_t *a, uint8_t *b, size_t size)
{
	while (size-- > 0) {
		a[size] ^= b[size];
	}
}

static void
raid_bdev_io_completion_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct stripe_io_info *stripe_io_info = cb_arg;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		stripe_io_info->failed = true;
	}

	if (--stripe_io_info->remaining == 0) {
		if (stripe_io_info->failed) {
			stripe_io_info->status = SPDK_BDEV_IO_STATUS_FAILED;
		} else {
			stripe_io_info->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		}
	}
}

static void
test_raid5_stripe_io(struct raid5_info *r5info, struct raid_bdev_io_channel *raid_ch, enum spdk_bdev_io_type io_type, uint64_t stripe_index, uint64_t stripe_offset_blocks, uint64_t num_blocks)
{
	void *src_buf, *dest_buf;
	void *parity_buf = NULL;
	void *reference_parity = NULL;
	uint32_t blocklen = r5info->raid_bdev->bdev.blocklen;
	size_t buf_size = num_blocks * blocklen;
	size_t parity_buf_size = r5info->raid_bdev->strip_size * blocklen;
	uint64_t block;
	struct stripe_request *stripe_req = NULL;
	struct raid5_io_channel *r5ch = raid_bdev_io_channel_get_resource(raid_ch);
	struct stripe_io_info stripe_io_info = { 0 };
	bool expected_success = true;

	TAILQ_INIT(&stripe_io_info.bdev_io_queue);

	if (io_type == SPDK_BDEV_IO_TYPE_WRITE &&
	    (num_blocks != r5info->stripe_blocks || stripe_offset_blocks > 0)) {
		expected_success = false;
	}

	src_buf = malloc(buf_size);
	SPDK_CU_ASSERT_FATAL(src_buf != NULL);

	dest_buf = malloc(buf_size);
	SPDK_CU_ASSERT_FATAL(dest_buf != NULL);

	memset(src_buf, 0xff, buf_size);
	for (block = 0; block < num_blocks; block++) {
		*((uint64_t *)(src_buf + block * blocklen)) = block;
	}

	if (io_type != SPDK_BDEV_IO_TYPE_READ) {
		stripe_req = raid5_get_stripe_request(r5ch, stripe_index);
		SPDK_CU_ASSERT_FATAL(stripe_req != NULL);
		raid5_stripe_req_ctor(r5ch->stripe_request_mempool, r5ch, stripe_req, 0);

		if (expected_success) {
			uint8_t i;
			void *src = src_buf;

			parity_buf = calloc(1, parity_buf_size);
			SPDK_CU_ASSERT_FATAL(parity_buf != NULL);

			reference_parity = calloc(1, parity_buf_size);
			SPDK_CU_ASSERT_FATAL(reference_parity != NULL);

			for (i = 0; i < r5info->raid_bdev->num_base_bdevs - 1; i++) {
				xor_block(reference_parity, src, parity_buf_size);
				src += parity_buf_size;
			}
		}
	}

	void *src = src_buf;
	void *dest = dest_buf;
	while (num_blocks) {
		struct raid_bdev_io *raid_io;
		uint64_t num_blocks_split;
		uint64_t chunk_offset = stripe_offset_blocks % r5info->raid_bdev->strip_size;
		uint64_t offset_blocks = stripe_index * r5info->stripe_blocks + stripe_offset_blocks;

		if (chunk_offset + num_blocks > r5info->raid_bdev->strip_size) {
			num_blocks_split = r5info->raid_bdev->strip_size - chunk_offset;
		} else {
			num_blocks_split = num_blocks;
		}

		stripe_io_info.remaining++;

		raid_io = get_raid_io(io_type, r5info, &stripe_io_info, raid_ch, offset_blocks, num_blocks_split, src, dest, parity_buf, raid_bdev_io_completion_cb);

		raid5_submit_rw_request(raid_io);

		num_blocks -= num_blocks_split;
		stripe_offset_blocks += num_blocks_split;
		src += num_blocks_split * blocklen;
		dest += num_blocks_split * blocklen;
	}

	process_io_completions(&stripe_io_info);

	if (stripe_io_info.status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		struct chunk *chunk;

		SPDK_CU_ASSERT_FATAL(stripe_req != NULL);
		FOR_EACH_CHUNK(stripe_req, chunk) {
			if (chunk->raid_io) {
				struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(chunk->raid_io);
				free(bdev_io);
				chunk->raid_io = NULL;
			}
		}
		stripe_req->remaining = 0;
		raid5_put_stripe_request(r5ch, stripe_req);
	}

	SPDK_CU_ASSERT_FATAL(expected_success == (stripe_io_info.status == SPDK_BDEV_IO_STATUS_SUCCESS));

	if (expected_success) {
		CU_ASSERT(memcmp(src_buf, dest_buf, buf_size) == 0);

		if (parity_buf != NULL) {
			CU_ASSERT(memcmp(parity_buf, reference_parity, parity_buf_size) == 0);
		}
	}

	free(src_buf);
	free(dest_buf);
	free(parity_buf);
	free(reference_parity);
}

static void
test_raid5_submit_rw_request(void)
{
	struct raid5_params *params;

	enum spdk_bdev_io_type io_types[] = {
		SPDK_BDEV_IO_TYPE_READ,
		SPDK_BDEV_IO_TYPE_WRITE,
	};

	RAID5_PARAMS_FOR_EACH(params) {
		struct raid5_info *r5info;
		struct {
			struct raid_bdev_io_channel raid_ch;
			struct raid5_io_channel r5ch;
		} ch;
		unsigned int i;
		int ret;

		r5info = create_raid5(params);
		SPDK_NOTICELOG("r5d%us%u block=%u\n", params->num_base_bdevs, params->strip_size, params->base_bdev_blocklen);

		ch.raid_ch.num_channels = params->num_base_bdevs;
		ch.raid_ch.base_channel = calloc(params->num_base_bdevs, sizeof(struct spdk_io_channel *));
		SPDK_CU_ASSERT_FATAL(ch.raid_ch.base_channel != NULL);

		/* mock buffers allocation */
		MOCK_SET(spdk_dma_malloc, (void *)1);
		ret = raid5_io_channel_resource_init(r5info->raid_bdev, &ch.r5ch);
		MOCK_CLEAR(spdk_dma_malloc);

		SPDK_CU_ASSERT_FATAL(ret == 0);

		/* allocate one stripe parity buffer and unset the rest */
		ch.r5ch.stripe_parity_buffers[0] = malloc(r5info->raid_bdev->strip_size * r5info->raid_bdev->bdev.blocklen);
		for (i = 1; i < RAID5_MAX_STRIPES; i++) {
			ch.r5ch.stripe_parity_buffers[i] = NULL;
		}

		struct {
			uint64_t stripe_offset_blocks;
			uint64_t num_blocks;
		} test_requests[] = {
			{ 0, 1 },
			{ 0, params->strip_size },
			{ 0, params->strip_size + 1 },
			{ 0, params->strip_size *(params->num_base_bdevs - 1) },
			{ 1, 1 },
			{ 1, params->strip_size },
			{ 1, params->strip_size + 1 },
			{ params->strip_size, 1 },
			{ params->strip_size, params->strip_size },
			{ params->strip_size, params->strip_size + 1 },
			{ params->strip_size - 1, 1 },
			{ params->strip_size - 1, params->strip_size },
			{ params->strip_size - 1, params->strip_size + 1 },
			{ params->strip_size - 1, 2 },
		};
		for (i = 0; i < SPDK_COUNTOF(test_requests); i++) {
			unsigned int ii;

			if (test_requests[i].stripe_offset_blocks + test_requests[i].num_blocks > r5info->stripe_blocks) {
				continue;
			}

			for (ii = 0; ii < SPDK_COUNTOF(io_types); ii++) {
				enum spdk_bdev_io_type io_type = io_types[ii];
				uint64_t stripe_index;

				for (stripe_index = 0; stripe_index < spdk_min(params->num_base_bdevs, r5info->total_stripes); stripe_index++) {
					SPDK_NOTICELOG("%s stripe: %lu stripe_offset_blocks: %lu num_blocks: %lu\n",
							(io_type == SPDK_BDEV_IO_TYPE_READ ? "read" : "write"),
							stripe_index,
							test_requests[i].stripe_offset_blocks,
							test_requests[i].num_blocks);

					test_raid5_stripe_io(r5info, &ch.raid_ch, io_type, stripe_index, test_requests[i].stripe_offset_blocks, test_requests[i].num_blocks);
				}
			}
		}

		raid5_io_channel_resource_deinit(&ch.r5ch);

		free(ch.raid_ch.base_channel);

		delete_raid5(r5info);
	}
}

int
main(int argc, char **argv)
{
	CU_pSuite suite = NULL;
	unsigned int num_failures;

	CU_set_error_action(CUEA_ABORT);
	CU_initialize_registry();

	suite = CU_add_suite("raid5", test_setup, test_cleanup);
	CU_ADD_TEST(suite, test_raid5_start);
	CU_ADD_TEST(suite, test_raid5_get_stripe_request);
	CU_ADD_TEST(suite, test_raid5_submit_rw_request);

	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	num_failures = CU_get_number_of_failures();
	CU_cleanup_registry();
	return num_failures;
}
