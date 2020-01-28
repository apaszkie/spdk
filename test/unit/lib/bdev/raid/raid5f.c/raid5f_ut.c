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

#include "common/lib/test_env.c"
#include "bdev/raid/raid5f.c"

DEFINE_STUB_V(raid_bdev_module_list_add, (struct raid_bdev_module *raid_module));
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

struct raid5f_params {
	uint8_t num_base_bdevs;
	uint64_t base_bdev_blockcnt;
	uint32_t base_bdev_blocklen;
	uint32_t strip_size;
};

static struct raid5f_params *g_params;
static size_t g_params_count;

#define ARRAY_FOR_EACH(a, e) \
	for (e = a; e < a + SPDK_COUNTOF(a); e++)

#define RAID5F_PARAMS_FOR_EACH(p) \
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
	struct raid5f_params *params;

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
create_raid_bdev(struct raid5f_params *params)
{
	struct raid_bdev *raid_bdev;
	struct raid_base_bdev_info *base_info;

	raid_bdev = calloc(1, sizeof(*raid_bdev));
	SPDK_CU_ASSERT_FATAL(raid_bdev != NULL);

	raid_bdev->module = &g_raid5f_module;
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

static struct raid5f_info *
create_raid5f(struct raid5f_params *params)
{
	struct raid_bdev *raid_bdev = create_raid_bdev(params);
	struct raid5f_info *r5f_info;
	int ret;

	ret = raid5f_start(raid_bdev);

	SPDK_CU_ASSERT_FATAL(ret == 0);

	r5f_info = raid_bdev->module_private;

	return r5f_info;
}

static void
delete_raid5f(struct raid5f_info *r5f_info)
{
	struct raid_bdev *raid_bdev = r5f_info->raid_bdev;

	raid5f_stop(raid_bdev);

	delete_raid_bdev(raid_bdev);
}

static void
test_raid5f_start(void)
{
	struct raid5f_params *params;

	RAID5F_PARAMS_FOR_EACH(params) {
		struct raid5f_info *r5f_info;

		r5f_info = create_raid5f(params);

		CU_ASSERT_EQUAL(r5f_info->stripe_blocks, params->strip_size * (params->num_base_bdevs - 1));
		CU_ASSERT_EQUAL(r5f_info->total_stripes, params->base_bdev_blockcnt / params->strip_size);
		CU_ASSERT_EQUAL(r5f_info->raid_bdev->bdev.blockcnt,
				(params->base_bdev_blockcnt - params->base_bdev_blockcnt % params->strip_size) *
				(params->num_base_bdevs - 1));
		CU_ASSERT_EQUAL(r5f_info->raid_bdev->bdev.optimal_io_boundary, params->strip_size);
		CU_ASSERT_TRUE(r5f_info->raid_bdev->bdev.split_on_optimal_io_boundary);

		delete_raid5f(r5f_info);
	}
}

struct raid_io_info {
	enum spdk_bdev_io_status status;
	bool failed;
	int remaining;
	TAILQ_HEAD(, spdk_bdev_io) bdev_io_queue;
};

struct test_raid_bdev_io {
	char bdev_io_buf[sizeof(struct spdk_bdev_io) + sizeof(struct raid_bdev_io)];
	struct raid_io_info *io_info;
	void *buf;
};

static void
raid_bdev_io_completion_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid_io_info *io_info = cb_arg;

	spdk_bdev_free_io(bdev_io);

	if (!success) {
		io_info->failed = true;
	}

	if (--io_info->remaining == 0) {
		if (io_info->failed) {
			io_info->status = SPDK_BDEV_IO_STATUS_FAILED;
		} else {
			io_info->status = SPDK_BDEV_IO_STATUS_SUCCESS;
		}
	}
}

static struct raid_bdev_io *
get_raid_io(enum spdk_bdev_io_type io_type, struct raid5f_info *r5f_info,
	    struct raid_io_info *io_info,
	    struct raid_bdev_io_channel *raid_ch, uint64_t offset_blocks, uint64_t num_blocks,
	    void *src_buf, void *dest_buf)
{
	struct spdk_bdev_io *bdev_io;
	struct raid_bdev_io *raid_io;
	struct raid_bdev *raid_bdev = r5f_info->raid_bdev;

	struct test_raid_bdev_io *test_raid_bdev_io = calloc(1, sizeof(*test_raid_bdev_io));
	SPDK_CU_ASSERT_FATAL(test_raid_bdev_io != NULL);

	SPDK_CU_ASSERT_FATAL(test_raid_bdev_io->bdev_io_buf == (char *)test_raid_bdev_io);
	bdev_io = (struct spdk_bdev_io *)test_raid_bdev_io->bdev_io_buf;
	bdev_io->bdev = &raid_bdev->bdev;
	bdev_io->type = io_type;
	bdev_io->u.bdev.offset_blocks = offset_blocks;
	bdev_io->u.bdev.num_blocks = num_blocks;
	bdev_io->internal.cb = raid_bdev_io_completion_cb;
	bdev_io->internal.caller_ctx = io_info;

	raid_io = (void *)bdev_io->driver_ctx;
	raid_io->raid_bdev = raid_bdev;
	raid_io->raid_ch = raid_ch;

	test_raid_bdev_io->io_info = io_info;

	if (io_type == SPDK_BDEV_IO_TYPE_READ) {
		test_raid_bdev_io->buf = src_buf;
		bdev_io->iov.iov_base = dest_buf;
	} else {
		test_raid_bdev_io->buf = dest_buf;
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
submit_io(struct raid_io_info *io_info, spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct spdk_bdev_io *bdev_io;

	bdev_io = calloc(1, sizeof(*bdev_io));
	SPDK_CU_ASSERT_FATAL(bdev_io != NULL);
	bdev_io->internal.cb = cb;
	bdev_io->internal.caller_ctx = cb_arg;

	TAILQ_INSERT_TAIL(&io_info->bdev_io_queue, bdev_io, internal.link);
}

static void
process_io_completions(struct raid_io_info *io_info)
{
	struct spdk_bdev_io *bdev_io;

	while ((bdev_io = TAILQ_FIRST(&io_info->bdev_io_queue))) {
		TAILQ_REMOVE(&io_info->bdev_io_queue, bdev_io, internal.link);

		bdev_io->internal.cb(bdev_io, true, bdev_io->internal.caller_ctx);
	}
}

int
spdk_bdev_readv_blocks(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
		       struct iovec *iov, int iovcnt,
		       uint64_t offset_blocks, uint64_t num_blocks,
		       spdk_bdev_io_completion_cb cb, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;
	struct test_raid_bdev_io *test_raid_bdev_io;

	SPDK_CU_ASSERT_FATAL(cb == raid5f_chunk_read_complete);
	SPDK_CU_ASSERT_FATAL(iovcnt == 1);

	test_raid_bdev_io = (struct test_raid_bdev_io *)spdk_bdev_io_from_ctx(raid_io);

	memcpy(iov->iov_base, test_raid_bdev_io->buf, iov->iov_len);

	submit_io(test_raid_bdev_io->io_info, cb, cb_arg);

	return 0;
}

static void
test_raid5f_submit_rw_request(struct raid5f_info *r5f_info, struct raid_bdev_io_channel *raid_ch,
			      enum spdk_bdev_io_type io_type, uint64_t stripe_index, uint64_t stripe_offset_blocks,
			      uint64_t num_blocks)
{
	uint64_t offset_blocks = stripe_index * r5f_info->stripe_blocks + stripe_offset_blocks;
	void *src_buf, *dest_buf;
	struct raid_bdev *raid_bdev = r5f_info->raid_bdev;
	uint32_t blocklen = raid_bdev->bdev.blocklen;
	size_t buf_size = num_blocks * blocklen;
	uint64_t block;
	struct raid_io_info io_info = { 0 };

	SPDK_CU_ASSERT_FATAL(io_type == SPDK_BDEV_IO_TYPE_READ);

	TAILQ_INIT(&io_info.bdev_io_queue);

	src_buf = spdk_dma_malloc(buf_size, 4096, NULL);
	SPDK_CU_ASSERT_FATAL(src_buf != NULL);

	dest_buf = spdk_dma_malloc(buf_size, 4096, NULL);
	SPDK_CU_ASSERT_FATAL(dest_buf != NULL);

	memset(src_buf, 0xff, buf_size);
	for (block = 0; block < num_blocks; block++) {
		*((uint64_t *)(src_buf + block * blocklen)) = block;
	}

	void *src = src_buf;
	void *dest = dest_buf;
	while (num_blocks) {
		struct raid_bdev_io *raid_io;
		uint64_t chunk_offset = offset_blocks % raid_bdev->strip_size;
		uint64_t num_blocks_split = spdk_min(num_blocks, raid_bdev->strip_size - chunk_offset);

		io_info.remaining++;
		raid_io = get_raid_io(io_type, r5f_info, &io_info, raid_ch, offset_blocks, num_blocks_split,
				      src, dest);

		raid5f_submit_rw_request(raid_io);

		num_blocks -= num_blocks_split;
		offset_blocks += num_blocks_split;
		src += num_blocks_split * blocklen;
		dest += num_blocks_split * blocklen;
	}

	process_io_completions(&io_info);

	CU_ASSERT(io_info.status == SPDK_BDEV_IO_STATUS_SUCCESS);

	CU_ASSERT(memcmp(src_buf, dest_buf, buf_size) == 0);

	free(src_buf);
	free(dest_buf);
}

static void
run_for_each_raid5f_config(void (*test_fn)(struct raid_bdev *raid_bdev,
			   struct raid_bdev_io_channel *raid_ch))
{
	struct raid5f_params *params;

	RAID5F_PARAMS_FOR_EACH(params) {
		struct raid5f_info *r5f_info;
		struct raid_bdev_io_channel raid_ch = { 0 };

		r5f_info = create_raid5f(params);

		raid_ch.num_channels = params->num_base_bdevs;
		raid_ch.base_channel = calloc(params->num_base_bdevs, sizeof(struct spdk_io_channel *));
		SPDK_CU_ASSERT_FATAL(raid_ch.base_channel != NULL);

		test_fn(r5f_info->raid_bdev, &raid_ch);

		free(raid_ch.base_channel);

		delete_raid5f(r5f_info);
	}
}

struct test_request_conf {
	uint64_t stripe_offset_blocks;
	uint64_t num_blocks;
};

static void
__test_raid5f_submit_read_request(struct raid_bdev *raid_bdev, struct raid_bdev_io_channel *raid_ch)
{
	struct raid5f_info *r5f_info = raid_bdev->module_private;
	uint32_t strip_size = raid_bdev->strip_size;
	unsigned int i;

	struct test_request_conf test_requests[] = {
		{ 0, 1 },
		{ 0, strip_size },
		{ 0, strip_size + 1 },
		{ 0, r5f_info->stripe_blocks },
		{ 1, 1 },
		{ 1, strip_size },
		{ 1, strip_size + 1 },
		{ strip_size, 1 },
		{ strip_size, strip_size },
		{ strip_size, strip_size + 1 },
		{ strip_size - 1, 1 },
		{ strip_size - 1, strip_size },
		{ strip_size - 1, strip_size + 1 },
		{ strip_size - 1, 2 },
	};
	for (i = 0; i < SPDK_COUNTOF(test_requests); i++) {
		struct test_request_conf *t = &test_requests[i];
		uint64_t stripe_index;

		for (stripe_index = 0; stripe_index < spdk_min(raid_bdev->num_base_bdevs, r5f_info->total_stripes);
		     stripe_index++) {
			test_raid5f_submit_rw_request(r5f_info, raid_ch, SPDK_BDEV_IO_TYPE_READ,
						      stripe_index, t->stripe_offset_blocks, t->num_blocks);
		}
	}
}
static void
test_raid5f_submit_read_request(void)
{
	run_for_each_raid5f_config(__test_raid5f_submit_read_request);
}

int
main(int argc, char **argv)
{
	CU_pSuite suite = NULL;
	unsigned int num_failures;

	CU_set_error_action(CUEA_ABORT);
	CU_initialize_registry();

	suite = CU_add_suite("raid5f", test_setup, test_cleanup);
	CU_ADD_TEST(suite, test_raid5f_start);
	CU_ADD_TEST(suite, test_raid5f_submit_read_request);

	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	num_failures = CU_get_number_of_failures();
	CU_cleanup_registry();
	return num_failures;
}
