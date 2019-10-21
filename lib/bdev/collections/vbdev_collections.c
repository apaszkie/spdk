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
#include "spdk/event.h"
#include "spdk/string.h"
#include "spdk/io_channel.h"
#include "spdk/util.h"

#include "spdk_internal/bdev.h"
#include "spdk_internal/log.h"

#include "vbdev_collections.h"
#include "common.h"

struct collections_bdev_io {
	uint64_t blocks_remaining;
};

static TAILQ_HEAD(, collection_fn_table) g_collection_modules = TAILQ_HEAD_INITIALIZER(g_collection_modules);

void collection_module_list_add(struct collection_fn_table *collection_module)
{
	TAILQ_INSERT_TAIL(&g_collection_modules, collection_module, link);
}

static TAILQ_HEAD(, collection) g_collections = TAILQ_HEAD_INITIALIZER(g_collections);

static int collection_bdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct collections_io_channel *ch = ctx_buf;
	struct collection *collection = io_device;
	unsigned int i;

	ch->members_ch = calloc(collection->members_count, sizeof(*ch->members_ch));
	if (!ch->members_ch) {
		SPDK_ERRLOG("could not calloc members_ch\n");
		return -ENOMEM;
	}

	for (i = 0; i < collection->members_count; i++) {
		ch->members_ch[i] = spdk_bdev_get_io_channel(collection->members[i].bdev_desc);
	}

	return 0;
}

static void collection_bdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct collections_io_channel *ch = ctx_buf;
	struct collection *collection = io_device;
	unsigned int i;

	for (i = 0; i < collection->members_count; i++) {
		spdk_put_io_channel(ch->members_ch[i]);
	}
	free(ch->members_ch);
}

static int vbdev_collection_destruct(void *ctx)
{
	struct collection *collection = ctx;
	struct collection_member *member;

	TAILQ_REMOVE(&g_collections, collection, link);

	if (collection->fn_table->stop) {
		collection->fn_table->stop(collection);
	}

	for_each_member(collection, member) {
		if (member->bdev)
			spdk_bdev_module_release_bdev(member->bdev);
		if (member->bdev_desc)
			spdk_bdev_close(member->bdev_desc);
	}

	if (collection->vbdev) {
		free(collection->vbdev->name);
		free(collection->vbdev);
	}
	free(collection->members);
	free(collection);

	return 0;
}

static int vbdev_collection_submit_rw_request(struct collection *collection, struct collections_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct collections_bdev_io *io_ctx = (struct collections_bdev_io *)bdev_io->driver_ctx;

	io_ctx->blocks_remaining = bdev_io->u.bdev.num_blocks;

	return collection->fn_table->submit_request(collection, ch, bdev_io);
}

static void vbdev_collection_submit_request(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io)
{
	struct collection *collection = bdev_io->bdev->ctxt;
	struct collections_io_channel *ch = spdk_io_channel_get_ctx(_ch);
	int ret;

	SPDK_DEBUGLOG(SPDK_LOG_VBDEV_COLLECTIONS, "bdev_io: %p (%c %lu+%lu)\n", bdev_io, bdev_io_type_char(bdev_io->type), bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		ret = vbdev_collection_submit_rw_request(collection, ch, bdev_io);
		break;
	// TODO: handle discard, flush etc.
	default:
		SPDK_ERRLOG("unsupported I/O type %d\n", bdev_io->type);
		ret = -EINVAL;
		break;
	}

	if (ret) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool vbdev_collection_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	return (io_type == SPDK_BDEV_IO_TYPE_READ || io_type == SPDK_BDEV_IO_TYPE_WRITE);
}

static struct spdk_io_channel *vbdev_collection_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(ctx);
}

static const struct spdk_bdev_fn_table vbdev_collections_fn_table = {
	.destruct		= vbdev_collection_destruct,
	.submit_request		= vbdev_collection_submit_request,
	.io_type_supported	= vbdev_collection_io_type_supported,
	.get_io_channel		= vbdev_collection_get_io_channel,
};

static int collection_parse_members(const char *members, struct collection *collection)
{
	char *s = " ";
	char *t;
	int i;
	char *str;
	size_t str_size;
	int ret = 0;
	struct collection_member *member;

	str_size = strlen(members) + 1;
	str = malloc(str_size);
	if (!str) {
		return -ENOMEM;
	}
	strcpy(str, members);

	collection->members_count = 0;
	for (t = strtok(str, s); t; t = strtok(NULL, s)) {
		if (strlen(t) >= sizeof(member->name)) {
			SPDK_ERRLOG("Collection%u: member name '%s' too long\n", collection->n, t);
			free(str);
			return -EINVAL;
		}
		collection->members_count++;
	}

	collection->members = calloc(collection->members_count, sizeof(struct collection_member));
	if (!collection->members) {
		free(str);
		return -ENOMEM;
	}

	strcpy(str, members);

	for (t = strtok(str, s), i = 0; t; t = strtok(NULL, s), i++) {
		member = &collection->members[i];
		member->collection = collection;
		strcpy(member->name, t);
		member->idx = i;
	}

	free(str);

	return ret;
}

static struct collection_fn_table *collection_find_type(const char *type)
{
	struct collection_fn_table *ret;

	TAILQ_FOREACH(ret, &g_collection_modules, link) {
		if (strcasecmp(ret->name, type) == 0)
			return ret;
	}

	return NULL;
}

static int vbdev_collections_init(void)
{
	struct spdk_conf_section *sp;
	struct collection *collection;
	int tmp;
	const char *members;
	const char *type;
	int ret;

	for (sp = spdk_conf_first_section(NULL); sp != NULL; sp = spdk_conf_next_section(sp)) {
		if (!spdk_conf_section_match_prefix(sp, "Collection")) {
			continue;
		}

		collection = calloc(1, sizeof(*collection));
		if (!collection) {
			return -ENOMEM;
		}

		if (sscanf(spdk_conf_section_get_name(sp), "Collection%u", &collection->n) != 1) {
			SPDK_ERRLOG("Section '%s' has non-numeric suffix.\n",
				    spdk_conf_section_get_name(sp));
			free(collection);
			return -EINVAL;
		}

		type = spdk_conf_section_get_val(sp, "Type");
		if (!type) {
			SPDK_ERRLOG("Collection%u: missing Type\n", collection->n);
			free(collection);
			return -EINVAL;
		}

		collection->fn_table = collection_find_type(type);
		if (!collection->fn_table) {
			SPDK_ERRLOG("Collection%u: unknown Type '%s'\n", collection->n, type);
			free(collection);
			return -EINVAL;
		}

		tmp = spdk_conf_section_get_intval(sp, "ChunkSizeKB");
		if (tmp < 1) {
			SPDK_ERRLOG("Collection%u: missing or invalid ChunkSizeKB\n", collection->n);
			free(collection);
			return -EINVAL;
		}
		collection->chunk_bytes = tmp * 1024;

		tmp = spdk_conf_section_get_intval(sp, "MemberSizeMB");
		if (tmp > 0) {
			collection->member_size_mb = tmp;
		}

		members = spdk_conf_section_get_val(sp, "Members");
		if (!members) {
			SPDK_ERRLOG("Collection%u: missing Members\n", collection->n);
			free(collection);
			return -EINVAL;
		}

		ret = collection_parse_members(members, collection);
		if (ret) {
			free(collection);
			return ret;
		}

		TAILQ_INSERT_TAIL(&g_collections, collection, link);
	}

	return 0;
}

static void vbdev_collections_finish(void)
{
	struct collection *collection, *tmp;

	TAILQ_FOREACH_SAFE(collection, &g_collections, link, tmp) {
		vbdev_collection_destruct(collection);
	}
}

static int vbdev_collections_get_ctx_size(void)
{
	return sizeof(struct collections_bdev_io);
}

static void collection_member_hotremove_cb(void *ctx)
{
	struct spdk_bdev *bdev_find = ctx;
	struct collection *collection;
	struct collection_member *member;

	TAILQ_FOREACH(collection, &g_collections, link) {
		if (!collection->vbdev) {
			continue;
		}

		for_each_member(collection, member) {
			if (member->bdev == bdev_find) {
				spdk_bdev_unregister(collection->vbdev, NULL, NULL);
				break;
			}
		}
	}
}

static void collection_bdev_io_complete(struct spdk_bdev_io *orig_io, enum spdk_bdev_io_status status)
{
	SPDK_DEBUGLOG(SPDK_LOG_VBDEV_COLLECTIONS, "bdev_io: %p (%c %lu+%lu)\n", orig_io, bdev_io_type_char(orig_io->type), orig_io->u.bdev.offset_blocks, orig_io->u.bdev.num_blocks);

	spdk_bdev_io_complete(orig_io, status);
}

void collection_complete_request_part(struct spdk_bdev_io *orig_io, int error, uint64_t num_blocks)
{
	if (!error) {
		struct collections_bdev_io *io_ctx = (struct collections_bdev_io *)orig_io->driver_ctx;

		io_ctx->blocks_remaining -= num_blocks;
		if (io_ctx->blocks_remaining == 0) {
			collection_bdev_io_complete(orig_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		}
	} else {
		collection_bdev_io_complete(orig_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static struct spdk_bdev_module collections_if;

static int collection_start(struct collection *collection)
{
	int ret;
	unsigned int i;
	struct spdk_bdev **base_bdevs;

	collection->vbdev = calloc(1, sizeof(*collection->vbdev));
	collection->vbdev->name = spdk_sprintf_alloc("Collection%u", collection->n);
	if (!collection->vbdev->name) {
		SPDK_ERRLOG("could not allocate bdev name\n");
		ret = -ENOMEM;
		goto end;
	}

	collection->vbdev->product_name = "Collections";
	collection->vbdev->ctxt = collection;
	collection->vbdev->fn_table = &vbdev_collections_fn_table;
	collection->vbdev->module = &collections_if;

	if (collection->fn_table->start) {
		ret = collection->fn_table->start(collection);
		if (ret) {
			goto end;
		}
	}

	spdk_io_device_register(collection, collection_bdev_ch_create_cb, collection_bdev_ch_destroy_cb,
				sizeof(struct collections_io_channel));

	base_bdevs = calloc(collection->members_count, sizeof(*base_bdevs));
	if (!base_bdevs) {
		ret = -ENOMEM;
		goto end;
	}

	for (i = 0; i < collection->members_count; i++) {
		base_bdevs[i] = collection->members[i].bdev;
	}

	ret = spdk_vbdev_register(collection->vbdev, base_bdevs, collection->members_count);
	free(base_bdevs);
	if (ret) {
		SPDK_ERRLOG("could not register vbdev\n");
		goto end;
	}

	SPDK_NOTICELOG("vbdev %s created\n", collection->vbdev->name);

	spdk_bdev_module_examine_done(&collections_if);
end:
	if (ret) {
		free(collection->vbdev->name);
		free(collection->vbdev);
		collection->vbdev = NULL;
	}
	return ret;
}

static void vbdev_collections_examine(struct spdk_bdev *bdev)
{
	int ret;
	struct collection *collection;

	TAILQ_FOREACH(collection, &g_collections, link) {
		struct collection_member *member;
		unsigned int active_members_count = 0;

		if (collection->vbdev) {
			continue;
		}

		for_each_member(collection, member) {
			if (strcmp(member->name, bdev->name) != 0 || member->bdev) {
				continue;
			}

			ret = spdk_bdev_open(bdev, false, collection_member_hotremove_cb,
					     bdev, &member->bdev_desc);
			if (ret) {
				SPDK_ERRLOG("could not open bdev %s\n", bdev->name);
				break;
			}

			ret = spdk_bdev_module_claim_bdev(bdev, member->bdev_desc, &collections_if);
			if (ret) {
				SPDK_ERRLOG("could not claim bdev %s\n", bdev->name);
				spdk_bdev_close(member->bdev_desc);
				break;
			}
			SPDK_NOTICELOG("bdev %s claimed\n", bdev->name);

			member->bdev = bdev;
		}

		for_each_member(collection, member) {
			if (member->bdev) {
				active_members_count++;
			}
		}
		if (active_members_count == collection->members_count) {
			if (collection_start(collection) != 0) {
				SPDK_ERRLOG("failed to start vbdev %s\n", bdev->name);
			} else {
				// spdk_bdev_module_examine_done is called from collection_start_continued
				return;
			}
		}
	}

	spdk_bdev_module_examine_done(&collections_if);
}

static struct spdk_bdev_module collections_if = {
	.name = "collections",
	.module_init = vbdev_collections_init,
	.get_ctx_size = vbdev_collections_get_ctx_size,
	.examine = vbdev_collections_examine,
	.module_fini = vbdev_collections_finish,
};

SPDK_BDEV_MODULE_REGISTER(&collections_if)

SPDK_LOG_REGISTER_COMPONENT("vbdev_collections", SPDK_LOG_VBDEV_COLLECTIONS)
