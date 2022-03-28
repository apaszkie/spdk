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

#include "spdk_cunit.h"

#include "util/xor.c"
#include "common/lib/test_env.c"

#define BUF_COUNT 8
#define SRC_BUF_COUNT (BUF_COUNT - 1)
#define BUF_SIZE 4096

static void
test_xor_gen(void)
{
	void *bufs[BUF_COUNT];
	void *bufs2[2];
	uint8_t *ref, *dest;
	int ret;
	size_t i, j;
	uint32_t *tmp;

	/* alloc and fill the buffers with a pattern */
	for (i = 0; i < BUF_COUNT; i++) {
		ret = posix_memalign(&bufs[i], spdk_xor_get_buf_alignment(), BUF_SIZE);
		SPDK_CU_ASSERT_FATAL(ret == 0);

		tmp = bufs[i];
		for (j = 0; j < BUF_SIZE / sizeof(*tmp); j++) {
			tmp[j] = (i << 16) + j;
		}
	}
	dest = bufs[SRC_BUF_COUNT];

	/* prepare the reference buffer */
	ref = malloc(BUF_SIZE);
	SPDK_CU_ASSERT_FATAL(ref != NULL);

	memset(ref, 0, BUF_SIZE);
	for (i = 0; i < SRC_BUF_COUNT; i++) {
		for (j = 0; j < BUF_SIZE; j++) {
			ref[j] ^= ((uint8_t *)bufs[i])[j];
		}
	}

	/* generate xor, compare the dest and reference buffers */
	ret = spdk_xor_gen(dest, bufs, SRC_BUF_COUNT, BUF_SIZE);
	CU_ASSERT(ret == 0);
	ret = memcmp(ref, dest, BUF_SIZE);
	CU_ASSERT(ret == 0);

	/* unaligned buffer */
	bufs2[0] = bufs[0];
	bufs2[1] = bufs[1] + 1;

	ret = spdk_xor_gen(dest, bufs2, SPDK_COUNTOF(bufs2), BUF_SIZE);
	CU_ASSERT(ret == -EINVAL);

	/* len not multiple of alignment */
	memset(dest, 0xba, BUF_SIZE);
	ret = spdk_xor_gen(dest, bufs, SRC_BUF_COUNT, BUF_SIZE - 1);
	CU_ASSERT(ret == 0);
	ret = memcmp(ref, dest, BUF_SIZE - 1);
	CU_ASSERT(ret == 0);

	/* xoring a buffer with itself should result in all zeros */
	memset(ref, 0, BUF_SIZE);
	bufs2[0] = bufs[0];
	bufs2[1] = bufs[0];
	dest = bufs[0];

	ret = spdk_xor_gen(dest, bufs2, SPDK_COUNTOF(bufs2), BUF_SIZE);
	CU_ASSERT(ret == 0);
	ret = memcmp(ref, dest, BUF_SIZE);
	CU_ASSERT(ret == 0);

	/* cleanup */
	for (i = 0; i < BUF_COUNT; i++) {
		free(bufs[i]);
	}
	free(ref);
}

int
main(int argc, char **argv)
{
	CU_pSuite	suite = NULL;
	unsigned int	num_failures;

	CU_set_error_action(CUEA_ABORT);
	CU_initialize_registry();

	suite = CU_add_suite("xor", NULL, NULL);

	CU_ADD_TEST(suite, test_xor_gen);

	CU_basic_set_mode(CU_BRM_VERBOSE);

	CU_basic_run_tests();

	num_failures = CU_get_number_of_failures();
	CU_cleanup_registry();

	return num_failures;
}
