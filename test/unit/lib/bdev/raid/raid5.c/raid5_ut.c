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

DEFINE_STUB_V(raid_bdev_module_list_add, (struct raid_bdev_module *raid_module));
DEFINE_STUB_V(raid_bdev_io_complete, (struct raid_bdev_io *raid_io, enum spdk_bdev_io_status status));
DEFINE_STUB(raid_bdev_io_complete_part, bool,
	    (struct raid_bdev_io *raid_io, uint64_t completed,
	     enum spdk_bdev_io_status status),
	    true);
DEFINE_STUB(rte_hash_count, int32_t, (const struct rte_hash *h), 0);
DEFINE_STUB(rte_hash_del_key_with_hash, int32_t,
	    (const struct rte_hash *h,
	     const void *key, hash_sig_t sig),
	    0);
DEFINE_STUB(rte_hash_hash, hash_sig_t, (const struct rte_hash *h, const void *key), 0);
DEFINE_STUB_V(rte_hash_free, (struct rte_hash *h));
DEFINE_STUB(spdk_bdev_get_buf_align, size_t, (const struct spdk_bdev *bdev), 0);
DEFINE_STUB(spdk_bdev_queue_io_wait, int, (struct spdk_bdev *bdev, struct spdk_io_channel *ch,
		struct spdk_bdev_io_wait_entry *entry), 0);
DEFINE_STUB(spdk_io_channel_from_ctx, struct spdk_io_channel *, (void *ctx), NULL);
DEFINE_STUB(spdk_io_channel_get_thread, struct spdk_thread *, (struct spdk_io_channel *ch), NULL);

struct hash_mock {
	uint64_t key;
	void *value;
};

int
rte_hash_lookup_with_hash_data(const struct rte_hash *_h,
			       const void *_key, hash_sig_t sig, void **data)
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
rte_hash_add_key_with_hash_data(const struct rte_hash *_h,
				const void *_key, hash_sig_t sig, void *data)
{
	struct hash_mock *h = (struct hash_mock *)_h;

	h->key = *((uint64_t *)_key);
	h->value = data;

	return 0;
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
	unsigned int i;
	int ret;

	/* mock chunk buffers allocation */
	MOCK_SET(spdk_dma_malloc, (void *)1);

	ret = raid5_start(raid_bdev);

	MOCK_CLEAR(spdk_dma_malloc);

	SPDK_CU_ASSERT_FATAL(ret == 0);

	r5info = raid_bdev->module_private;

	for (i = 0; i < RAID5_MAX_STRIPES; i++) {
		uint8_t j;

		for (j = 0; j < params->num_base_bdevs; j++) {
			/* don't try to free the mock chunk buffers */
			r5info->stripes[i].chunk_buffers[j] = NULL;
		}
	}

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
		CU_ASSERT_EQUAL(r5info->raid_bdev->bdev.optimal_io_boundary, r5info->stripe_blocks);

		delete_raid5(r5info);
	}
}

static void
test_raid5_chunk_map_iov(void)
{
	struct iovec iov[] = {
		{ .iov_base = (void *)0x0ff0000, .iov_len = 4096 },
		{ .iov_base = (void *)0x1ff0000, .iov_len = 4096 },
		{ .iov_base = (void *)0x2ff0000, .iov_len = 512 },
		{ .iov_base = (void *)0x3ff0000, .iov_len = 512 },
	};
	struct chunk chunk;
	int ret;

	chunk.iovs = &chunk.iov;
	chunk.iovcnt = 1;

	/* whole first iov */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 0, 4096);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 1);
	CU_ASSERT_EQUAL(chunk.iov.iov_base, 0x0ff0000);
	CU_ASSERT_EQUAL(chunk.iov.iov_len, 4096);

	/* start of first iov */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 0, 1234);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 1);
	CU_ASSERT_EQUAL(chunk.iov.iov_base, 0x0ff0000);
	CU_ASSERT_EQUAL(chunk.iov.iov_len, 1234);

	/* end of first iov */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 1234, 4096 - 1234);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 1);
	CU_ASSERT_EQUAL(chunk.iov.iov_base, 0x0ff0000 + 1234);
	CU_ASSERT_EQUAL(chunk.iov.iov_len, 4096 - 1234);

	/* middle of first iov */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 1234, 128);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 1);
	CU_ASSERT_EQUAL(chunk.iov.iov_base, 0x0ff0000 + 1234);
	CU_ASSERT_EQUAL(chunk.iov.iov_len, 128);

	/* middle of second iov */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 4096 + 128, 1234);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 1);
	CU_ASSERT_EQUAL(chunk.iov.iov_base, 0x1ff0000 + 128);
	CU_ASSERT_EQUAL(chunk.iov.iov_len, 1234);

	/* end of first iov, whole second, start of third */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 128, 4096 * 2);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 3);
	CU_ASSERT_EQUAL(chunk.iovs[0].iov_base, 0x0ff0000 + 128);
	CU_ASSERT_EQUAL(chunk.iovs[0].iov_len, 4096 - 128);
	CU_ASSERT_EQUAL(chunk.iovs[1].iov_base, 0x1ff0000);
	CU_ASSERT_EQUAL(chunk.iovs[1].iov_len, 4096);
	CU_ASSERT_EQUAL(chunk.iovs[2].iov_base, 0x2ff0000);
	CU_ASSERT_EQUAL(chunk.iovs[2].iov_len, 128);

	/* end of second iov, whole third, start of fourth */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 4096 + 128, 4096 + 512);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 3);
	CU_ASSERT_EQUAL(chunk.iovs[0].iov_base, 0x1ff0000 + 128);
	CU_ASSERT_EQUAL(chunk.iovs[0].iov_len, 4096 - 128);
	CU_ASSERT_EQUAL(chunk.iovs[1].iov_base, 0x2ff0000);
	CU_ASSERT_EQUAL(chunk.iovs[1].iov_len, 512);
	CU_ASSERT_EQUAL(chunk.iovs[2].iov_base, 0x3ff0000);
	CU_ASSERT_EQUAL(chunk.iovs[2].iov_len, 128);

	/* 1:1 copy */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 0, 4096 * 2 + 512 * 2);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 4);
	CU_ASSERT(memcmp(iov, chunk.iovs, chunk.iovcnt * sizeof(*iov)) == 0);

	/* len == 0 */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 0, 0);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(chunk.iovcnt, 1);
	CU_ASSERT_EQUAL(chunk.iovs[0].iov_base, 0x0ff0000);
	CU_ASSERT_EQUAL(chunk.iovs[0].iov_len, 0);

	/* len exceededs size */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 0, 4096 * 2 + 512 * 2 + 1);
	CU_ASSERT_NOT_EQUAL(ret, 0);

	/* len exceededs size */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 1, 4096 * 2 + 512 * 2);
	CU_ASSERT_NOT_EQUAL(ret, 0);

	/* offset exceededs size */
	ret = raid5_chunk_map_iov(&chunk, iov, SPDK_COUNTOF(iov), 4096 * 2 + 512 * 2, 1);
	CU_ASSERT_NOT_EQUAL(ret, 0);

	if (chunk.iovs != &chunk.iov) {
		free(chunk.iovs);
	}
}

static void
test_raid5_get_stripe(void)
{
	struct raid5_info *r5info;
	static struct stripe *stripe;
	unsigned int count;

	r5info = create_raid5(g_params);

	for (count = 0; count < RAID5_MAX_STRIPES; count++) {
		stripe = raid5_get_stripe(r5info, count);
		CU_ASSERT(stripe != NULL);
		CU_ASSERT_EQUAL(stripe->index, count);
	}

	stripe = raid5_get_stripe(r5info, count);
	CU_ASSERT(stripe == NULL);

	delete_raid5(r5info);
}

static void
test_raid5_reclaim_stripes(void)
{
	struct raid5_info *r5info;
	static struct stripe *stripe;

	r5info = create_raid5(g_params);

	stripe = raid5_get_stripe(r5info, 0);
	CU_ASSERT(stripe != NULL);
	CU_ASSERT_EQUAL(stripe->refs, 1);

	CU_ASSERT(raid5_reclaim_stripes(r5info) == 0);

	stripe->refs--;

	CU_ASSERT(raid5_reclaim_stripes(r5info) == 1);

	delete_raid5(r5info);
}

struct raid_io_info {
	char bdev_io_buf[sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io)];
	struct raid5_info *r5info;
	struct raid_bdev_io *raid_io;
	uint64_t offset_blocks;
	uint64_t num_blocks;
	uint32_t blocklen;
	uint64_t stripe_idx;
	uint64_t stripe_offset_blocks;
	void *buf;
	size_t buf_pos;
	void *parity_buf;
	TAILQ_HEAD(, spdk_bdev_io) bdev_io_queue;
};

static void
init_io_info(struct raid_io_info *io_info, enum spdk_bdev_io_type type,
	     struct raid5_info *r5info, struct raid_bdev_io_channel *raid_ch,
	     uint64_t offset_blocks, uint64_t num_blocks, uint64_t stripe_idx,
	     uint64_t stripe_offset_blocks)
{
	struct spdk_bdev_io *bdev_io;
	struct raid_bdev_io *raid_io;
	struct raid_bdev *raid_bdev = r5info->raid_bdev;

	memset(io_info, 0, sizeof(*io_info));

	bdev_io = (struct spdk_bdev_io *)io_info->bdev_io_buf;
	bdev_io->bdev = &raid_bdev->bdev;
	bdev_io->type = type;
	bdev_io->u.bdev.offset_blocks = offset_blocks;
	bdev_io->u.bdev.num_blocks = num_blocks;

	raid_io = (void *)bdev_io->driver_ctx;
	raid_io->raid_bdev = raid_bdev;
	raid_io->raid_ch = raid_ch;

	io_info->r5info = r5info;
	io_info->raid_io = raid_io;
	io_info->offset_blocks = offset_blocks;
	io_info->num_blocks = num_blocks;
	io_info->blocklen = raid_bdev->bdev.blocklen;
	io_info->stripe_idx = stripe_idx;
	io_info->stripe_offset_blocks = stripe_offset_blocks;
	io_info->buf = NULL;
	io_info->buf_pos = 0;
	io_info->parity_buf = NULL;
	TAILQ_INIT(&io_info->bdev_io_queue);
}

static void
run_for_each_raid_io(void (*test_fn)(struct raid_io_info *))
{
	struct raid5_params *params;

	enum spdk_bdev_io_type io_types[] = {
		SPDK_BDEV_IO_TYPE_READ,
		SPDK_BDEV_IO_TYPE_WRITE,
	};

	RAID5_PARAMS_FOR_EACH(params) {
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
		struct raid5_info *r5info;
		struct {
			struct raid_bdev_io_channel raid_ch;
			struct raid5_io_channel r5ch;
		} ch;
		unsigned int i;

		r5info = create_raid5(params);

		ch.raid_ch.num_channels = params->num_base_bdevs;
		ch.raid_ch.base_channel = calloc(params->num_base_bdevs, sizeof(struct spdk_io_channel *));
		SPDK_CU_ASSERT_FATAL(ch.raid_ch.base_channel != NULL);

		raid5_io_channel_resource_init(r5info->raid_bdev, &ch.r5ch);

		for (i = 0; i < SPDK_COUNTOF(test_requests); i++) {
			uint64_t stripe_idx;

			for (stripe_idx = 0; stripe_idx < params->num_base_bdevs; stripe_idx++) {
				enum spdk_bdev_io_type *io_type;

				if (stripe_idx >= r5info->total_stripes ||
				    test_requests[i].stripe_offset_blocks + test_requests[i].num_blocks > r5info->stripe_blocks) {
					continue;
				}

				for (io_type = io_types; io_type < io_types + SPDK_COUNTOF(io_types); io_type++) {
					struct raid_io_info io_info;

					init_io_info(&io_info, *io_type,
						     r5info, &ch.raid_ch,
						     stripe_idx * r5info->stripe_blocks + test_requests[i].stripe_offset_blocks,
						     test_requests[i].num_blocks,
						     stripe_idx,
						     test_requests[i].stripe_offset_blocks);

					test_fn(&io_info);
				}
			}
		}

		raid5_io_channel_resource_deinit(r5info->raid_bdev, &ch.r5ch);

		free(ch.raid_ch.base_channel);

		delete_raid5(r5info);
	}
}

static void
__test_raid5_handle_stripe(struct raid_io_info *io_info)
{
	struct raid_bdev *raid_bdev = io_info->r5info->raid_bdev;
	static struct stripe *stripe;
	struct stripe_request dummy_req;
	struct stripe_request *stripe_req;
	uint8_t p_idx;
	uint8_t i;
	uint64_t offset_blocks = 0;
	uint64_t num_blocks = 0;
	uint64_t req_blocks_prev = 0;

	p_idx = raid_bdev->num_base_bdevs - (io_info->stripe_idx % raid_bdev->num_base_bdevs) - 1;

	stripe = raid5_get_stripe(io_info->r5info, io_info->stripe_idx);
	SPDK_CU_ASSERT_FATAL(stripe != NULL);

	TAILQ_INSERT_TAIL(&stripe->requests, &dummy_req, link);

	raid5_handle_stripe(io_info->raid_io, stripe, stripe->index,
			    io_info->stripe_offset_blocks,
			    io_info->num_blocks, 0);

	stripe_req = TAILQ_LAST(&stripe->requests, requests_head);
	CU_ASSERT(stripe_req != &dummy_req);

	TAILQ_INIT(&stripe->requests);

	CU_ASSERT_EQUAL(stripe_req->parity_chunk->index, p_idx);

	for (i = 0; i < raid_bdev->num_base_bdevs; i++) {
		struct chunk *chunk = &stripe_req->chunks[i];

		if (i == p_idx) {
			CU_ASSERT_EQUAL(chunk, stripe_req->parity_chunk);
			continue;
		}

		CU_ASSERT_FALSE(num_blocks > 0 && req_blocks_prev == 0 && chunk->req_blocks > 0);

		num_blocks += chunk->req_blocks;
		req_blocks_prev = chunk->req_blocks;

		if (chunk->req_offset > 0) {
			offset_blocks += chunk->req_offset;
		} else if (num_blocks == 0) {
			offset_blocks += raid_bdev->strip_size;
		}
	}

	CU_ASSERT_EQUAL(offset_blocks, io_info->stripe_offset_blocks);
	CU_ASSERT_EQUAL(num_blocks, io_info->num_blocks);

	raid5_stripe_request_put(stripe_req);
}

static void
test_raid5_handle_stripe(void)
{
	run_for_each_raid_io(__test_raid5_handle_stripe);
}

void
spdk_bdev_free_io(struct spdk_bdev_io *bdev_io)
{
	free(bdev_io);
}

static void
handle_submit_bdev_io(enum spdk_bdev_io_type io_type,
		      struct iovec *iov, int iovcnt,
		      uint64_t offset_blocks, uint64_t num_blocks,
		      spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct stripe_request *stripe_req;
	struct raid_io_info *io_info;
	struct spdk_bdev_io *bdev_io;
	struct chunk *chunk = cb_arg;

	SPDK_CU_ASSERT_FATAL(cb == raid5_complete_chunk_request);

	stripe_req = raid5_chunk_stripe_req(chunk);
	io_info = (struct raid_io_info *)spdk_bdev_io_from_ctx(stripe_req->raid_io);

	if (chunk->request_type == CHUNK_READ ||
	    chunk->request_type == CHUNK_PREREAD) {
		CU_ASSERT_EQUAL(io_type, SPDK_BDEV_IO_TYPE_READ);
	} else if (chunk->request_type == CHUNK_WRITE) {
		CU_ASSERT_EQUAL(io_type, SPDK_BDEV_IO_TYPE_WRITE);
	} else {
		CU_FAIL("Unknown chunk request_type");
	}

	if ((chunk != stripe_req->parity_chunk || chunk->request_type != CHUNK_READ) &&
	    (io_info->buf != NULL || io_info->parity_buf != NULL)) {
		size_t remaining = num_blocks * io_info->blocklen;
		void *buf;
		size_t *buf_offset;
		size_t parity_buf_offset;
		int i;

		if (chunk == stripe_req->parity_chunk) {
			buf = io_info->parity_buf;
			buf_offset = &parity_buf_offset;
			parity_buf_offset = (offset_blocks % io_info->r5info->raid_bdev->strip_size) * io_info->blocklen;
		} else {
			buf = io_info->buf;
			buf_offset = &io_info->buf_pos;
		}

		for (i = 0; i < iovcnt; i++) {
			if (chunk->request_type == CHUNK_PREREAD) {
				memset(iov[i].iov_base, 0, iov[i].iov_len);
			} else {
				if (io_type == SPDK_BDEV_IO_TYPE_READ) {
					memcpy(iov[i].iov_base, buf + *buf_offset, iov[i].iov_len);
				} else {
					memcpy(buf + *buf_offset, iov[i].iov_base, iov[i].iov_len);
				}
				*buf_offset += iov[i].iov_len;
			}
			remaining -= iov[i].iov_len;
		}
		CU_ASSERT_EQUAL(remaining, 0);
	}

	bdev_io = calloc(1, sizeof(*bdev_io));
	SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
	bdev_io->internal.cb = cb;
	bdev_io->internal.caller_ctx = cb_arg;

	TAILQ_INSERT_TAIL(&io_info->bdev_io_queue, bdev_io, internal.link);
}

int spdk_bdev_readv_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			   struct iovec *iov, int iovcnt,
			   uint64_t offset_blocks, uint64_t num_blocks,
			   spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	handle_submit_bdev_io(SPDK_BDEV_IO_TYPE_READ, iov, iovcnt, offset_blocks, num_blocks, cb, cb_arg);

	return 0;
}

int spdk_bdev_writev_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
			    struct iovec *iov, int iovcnt,
			    uint64_t offset_blocks, uint64_t num_blocks,
			    spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	handle_submit_bdev_io(SPDK_BDEV_IO_TYPE_WRITE, iov, iovcnt, offset_blocks, num_blocks, cb, cb_arg);

	return 0;
}

static void
process_bdev_io_completions(struct raid_io_info *io_info)
{
	struct spdk_bdev_io *bdev_io;

	while ((bdev_io = TAILQ_FIRST(&io_info->bdev_io_queue))) {
		TAILQ_REMOVE(&io_info->bdev_io_queue, bdev_io, internal.link);

		bdev_io->internal.cb(bdev_io, true, bdev_io->internal.caller_ctx);
	}
}

static void
xor_block(uint8_t *a, uint8_t *b, size_t size)
{
	while (size-- > 0) {
		a[size] ^= b[size];
	}
}

static void
__test_raid5_submit_rw_request(struct raid_io_info *io_info)
{
	struct raid5_info *r5info = io_info->r5info;
	void *src_buf, *dest_buf;
	void *parity_buf = NULL;
	size_t buf_size = io_info->num_blocks * io_info->blocklen;
	size_t parity_buf_size = r5info->raid_bdev->strip_size * io_info->blocklen;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(io_info->raid_io);
	uint64_t block;

	src_buf = malloc(buf_size);
	SPDK_CU_ASSERT_FATAL(src_buf != NULL);

	dest_buf = malloc(buf_size);
	SPDK_CU_ASSERT_FATAL(dest_buf != NULL);

	memset(src_buf, 0xff, buf_size);
	for (block = 0; block < io_info->num_blocks; block++) {
		*((uint64_t *)(src_buf + block * io_info->blocklen)) = block;
	}

	bdev_io->u.bdev.iovs = &bdev_io->iov;
	bdev_io->u.bdev.iovcnt = 1;
	bdev_io->iov.iov_len = buf_size;

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
		io_info->buf = src_buf;
		bdev_io->iov.iov_base = dest_buf;
	} else {
		struct stripe *stripe;
		uint8_t i;
		uint64_t data_offset;
		uint64_t parity_offset;

		io_info->buf = dest_buf;
		bdev_io->iov.iov_base = src_buf;

		stripe = raid5_get_stripe(r5info, io_info->stripe_idx);
		SPDK_CU_ASSERT_FATAL(stripe != NULL);
		for (i = 0; i < r5info->raid_bdev->num_base_bdevs; i++) {
			if (stripe->chunk_buffers[i] == NULL) {
				/* will be freed by raid5_free() */
				stripe->chunk_buffers[i] = malloc(r5info->raid_bdev->strip_size * io_info->blocklen);
				SPDK_CU_ASSERT_FATAL(stripe->chunk_buffers[i] != NULL);
			}
		}

		parity_buf = calloc(1, parity_buf_size);
		SPDK_CU_ASSERT_FATAL(parity_buf != NULL);

		io_info->parity_buf = calloc(1, parity_buf_size);
		SPDK_CU_ASSERT_FATAL(io_info->parity_buf != NULL);

		data_offset = 0;
		parity_offset = io_info->stripe_offset_blocks;
		while (data_offset < io_info->num_blocks) {
			if (parity_offset == r5info->raid_bdev->strip_size) {
				parity_offset = 0;
			}
			xor_block(parity_buf + (parity_offset * io_info->blocklen),
				  src_buf + (data_offset * io_info->blocklen),
				  io_info->blocklen);
			data_offset++;
			parity_offset++;
		}
	}

	raid5_submit_rw_request(io_info->raid_io);

	process_bdev_io_completions(io_info);

	CU_ASSERT_EQUAL(io_info->buf_pos, buf_size);
	CU_ASSERT(memcmp(src_buf, dest_buf, buf_size) == 0);

	if (parity_buf != NULL) {
		CU_ASSERT(memcmp(parity_buf, io_info->parity_buf, parity_buf_size) == 0);

		free(parity_buf);
		free(io_info->parity_buf);
	}

	free(src_buf);
	free(dest_buf);
}

static void
test_raid5_submit_rw_request(void)
{
	run_for_each_raid_io(__test_raid5_submit_rw_request);
}

static void
__test_raid5_rmw(struct raid_io_info *io_info)
{
	struct raid5_info *r5info = io_info->r5info;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(io_info->raid_io);
	struct stripe *stripe;
	struct stripe_request *stripe_req;
	struct spdk_bdev_io *chunk_bdev_io, *tmp;
	struct chunk *chunk;
	uint64_t to_preread = 0;
	uint64_t to_write = 0;

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ ||
	    io_info->num_blocks == r5info->stripe_blocks) {
		return;
	}

	stripe = raid5_get_stripe(r5info, io_info->stripe_idx);
	SPDK_CU_ASSERT_FATAL(stripe != NULL);

	raid5_submit_rw_request(io_info->raid_io);

	TAILQ_FOREACH_SAFE(chunk_bdev_io, &io_info->bdev_io_queue, internal.link, tmp) {
		chunk = chunk_bdev_io->internal.caller_ctx;

		CU_ASSERT_EQUAL(chunk->request_type, CHUNK_PREREAD);

		to_preread += chunk->preread_blocks;

		free(chunk_bdev_io);
	}

	CU_ASSERT(to_preread > 0);

	while ((stripe_req = TAILQ_LAST(&stripe->requests, requests_head))) {
		CU_ASSERT_EQUAL(stripe_req->raid_io, io_info->raid_io);

		FOR_EACH_CHUNK(stripe_req, chunk) {
			to_write += chunk->req_blocks;
		}

		raid5_complete_stripe_request(stripe_req);
	}

	CU_ASSERT(to_write > 0);
	CU_ASSERT(to_preread <= to_write);
}

static void
test_raid5_rmw(void)
{
	run_for_each_raid_io(__test_raid5_rmw);
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
	CU_ADD_TEST(suite, test_raid5_chunk_map_iov);
	CU_ADD_TEST(suite, test_raid5_get_stripe);
	CU_ADD_TEST(suite, test_raid5_reclaim_stripes);
	CU_ADD_TEST(suite, test_raid5_handle_stripe);
	CU_ADD_TEST(suite, test_raid5_submit_rw_request);
	CU_ADD_TEST(suite, test_raid5_rmw);

	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	num_failures = CU_get_number_of_failures();
	CU_cleanup_registry();
	return num_failures;
}
