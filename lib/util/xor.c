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

#include "spdk/xor.h"
#include "spdk/config.h"
#include "spdk/assert.h"
#include "spdk/util.h"

#ifdef SPDK_CONFIG_ISAL
#include "isa-l/include/raid.h"

#define SPDK_XOR_BUF_ALIGN 32

static int
do_xor_gen(void *dest, void **sources, int n, int len)
{
	void *buffers[n + 1];

	memcpy(buffers, sources, n * sizeof(buffers[0]));
	buffers[n] = dest;

	if (xor_gen(n + 1, len, buffers)) {
		return -EINVAL;
	}

	return 0;
}

#else

#define SPDK_XOR_BUF_ALIGN sizeof(long)

static int
do_xor_gen(void *dest, void **sources, int n, int len)
{
	uint32_t shift = spdk_u32log2(SPDK_XOR_BUF_ALIGN);
	int len_div = len >> shift;
	int i, j;

	for (i = 0; i < len_div; i++) {
		long w = 0;

		for (j = 0; j < n; j++) {
			w ^= ((long *)sources[j])[i];
		}
		((long *)dest)[i] = w;
	}

	for (i = len_div << shift; i < len; i++) {
		uint8_t b = 0;

		for (j = 0; j < n; j++) {
			b ^= ((uint8_t *)sources[j])[i];
		}
		((uint8_t *)dest)[i] = b;
	}

	return 0;
}

#endif

static inline bool
is_unaligned(void *ptr)
{
	return (uintptr_t)ptr & (SPDK_XOR_BUF_ALIGN - 1);
}

int
spdk_xor_gen(void *dest, void **sources, int n, int len)
{
	int i;

	if (n < 2 || is_unaligned(dest)) {
		return -EINVAL;
	}
	for (i = 0; i < n; i++) {
		if (is_unaligned(sources[i])) {
			return -EINVAL;
		}
	}

	return do_xor_gen(dest, sources, n, len);
}

size_t
spdk_xor_get_buf_alignment(void)
{
	return SPDK_XOR_BUF_ALIGN;
}

SPDK_STATIC_ASSERT(SPDK_XOR_BUF_ALIGN > 0 && !(SPDK_XOR_BUF_ALIGN & (SPDK_XOR_BUF_ALIGN - 1)),
		   "Must be power of 2");
