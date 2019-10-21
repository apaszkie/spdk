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

#ifndef LIB_BDEV_COLLECTIONS_VBDEV_COLLECTIONS_H_
#define LIB_BDEV_COLLECTIONS_VBDEV_COLLECTIONS_H_

#include "spdk_internal/bdev.h"
#include "common.h"

struct collection_member {
	struct collection *collection;
	char name[128];
	unsigned int idx;
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
};

struct collection {
	unsigned int n;
	struct spdk_bdev *vbdev;
	struct collection_member *members;
	unsigned int members_count;
	uint64_t chunk_bytes;
	uint64_t chunk_blocks;
	uint64_t member_size_mb;
	uint64_t member_size_blocks;
	TAILQ_ENTRY(collection) link;
	struct collection_fn_table *fn_table;
	void *private;
};

struct collections_io_channel {
	struct spdk_io_channel **members_ch;
};

struct collection_fn_table {
	const char *name;
	int (*start)(struct collection *collection);
	void (*stop)(struct collection *collection);
	int (*submit_request)(struct collection *collection, struct collections_io_channel *ch, struct spdk_bdev_io *bdev_io);
	int (*custom_op)(struct collection *collection, const char *op_name, void *arg);
	TAILQ_ENTRY(collection_fn_table) link;
};

void collection_module_list_add(struct collection_fn_table *collection_module);

#define COLLECTION_MODULE_REGISTER_FN_NAME(line) COLLECTION_MODULE_REGISTER_FN_NAME_(line)
#define COLLECTION_MODULE_REGISTER_FN_NAME_(line) collection_module_register_##line

#define COLLECTION_MODULE_REGISTER(_module)							\
	__attribute__((constructor)) static void						\
	COLLECTION_MODULE_REGISTER_FN_NAME(__LINE__)  (void)					\
	{											\
	    collection_module_list_add(_module);						\
	}

void collection_complete_request_part(struct spdk_bdev_io *orig_io, int error, uint64_t num_blocks);

#endif /* LIB_BDEV_COLLECTIONS_VBDEV_COLLECTIONS_H_ */
