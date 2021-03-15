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
#include "spdk/thread.h"
#include "spdk/util.h"

#include "spdk/bdev_module.h"
#include "spdk_internal/log.h"

struct bdev_rmw {
	struct spdk_bdev vbdev;
	struct spdk_bdev *base_bdev;
	struct spdk_bdev_desc *base_desc;
	uint32_t write_unit_blocks;
	TAILQ_ENTRY(bdev_rmw) link;
	struct write_unit *write_units;
};
static TAILQ_HEAD(, bdev_rmw) g_vbdevs = TAILQ_HEAD_INITIALIZER(g_vbdevs);

struct bdev_rmw_io {
	struct spdk_bdev_io *orig_io;
	struct bdev_rmw_io_channel *ch;
	struct write_unit *write_unit;
	uint64_t write_unit_offset;
	TAILQ_ENTRY(bdev_rmw_io) link;
	bool skip_preread;
};

struct write_unit {
	void *buf;
	TAILQ_HEAD(, bdev_rmw_io) ios;
	pthread_spinlock_t lock;
};

struct bdev_rmw_io_channel {
	struct bdev_rmw *bdev_rmw;
	struct spdk_io_channel *base_ch;
};

static int vbdev_cache_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct bdev_rmw_io_channel *ch = ctx_buf;
	struct bdev_rmw *bdev_rmw = io_device;

	ch->bdev_rmw = bdev_rmw;
	ch->base_ch = spdk_bdev_get_io_channel(bdev_rmw->base_desc);

	return 0;
}

static void vbdev_cache_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct bdev_rmw_io_channel *ch = ctx_buf;

	spdk_put_io_channel(ch->base_ch);
}

static int bdev_rmw_destruct(void *ctx)
{
	struct bdev_rmw *bdev_rmw = (struct bdev_rmw *)ctx;
	unsigned int num_units = bdev_rmw->base_bdev->blockcnt / bdev_rmw->write_unit_blocks;
	unsigned int i;

	spdk_io_device_unregister(bdev_rmw, NULL);
	spdk_bdev_module_release_bdev(bdev_rmw->base_bdev);
	spdk_bdev_close(bdev_rmw->base_desc);

	for (i = 0; i < num_units; i++) {
		struct write_unit *write_unit = &bdev_rmw->write_units[i];

		pthread_spin_destroy(&write_unit->lock);
	}

	free(bdev_rmw->write_units);
	free(bdev_rmw->vbdev.name);
	free(bdev_rmw);

	return 0;
}

static void bdev_rmw_complete_io(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *parent_bdev_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	spdk_bdev_io_complete(parent_bdev_io, (success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED));
}

static int bdev_rmw_do_submit_write_request(struct bdev_rmw_io *rmw_io);

static void bdev_rmw_complete_write(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct bdev_rmw_io *rmw_io = cb_arg;
	struct bdev_rmw_io *next_io;
	struct write_unit *write_unit = rmw_io->write_unit;
	void *buf;

	pthread_spin_lock(&write_unit->lock);
	next_io = TAILQ_NEXT(rmw_io, link);
	TAILQ_REMOVE(&write_unit->ios, rmw_io, link);
	if (!next_io) {
		buf = write_unit->buf;
		write_unit->buf = NULL;
	}
	pthread_spin_unlock(&write_unit->lock);

	bdev_rmw_complete_io(bdev_io, success, rmw_io->orig_io);

	if (next_io) {
		int ret = bdev_rmw_do_submit_write_request(next_io);
		if (ret) {
			SPDK_WARNLOG("failed to submit io: %d\n", ret);
		}
	} else {
		spdk_dma_free(buf);
	}
}

static void bdev_rmw_complete_preread(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct bdev_rmw_io *rmw_io = cb_arg;

	if (success) {
		struct bdev_rmw *bdev_rmw = rmw_io->ch->bdev_rmw;
		void *buf = rmw_io->write_unit->buf + (rmw_io->write_unit_offset * bdev_rmw->base_bdev->blocklen);
		int ret;
		int i;

		for (i = 0; i < rmw_io->orig_io->u.bdev.iovcnt; i++) {
			struct iovec *iov = &rmw_io->orig_io->u.bdev.iovs[i];
			memcpy(buf, iov->iov_base, iov->iov_len);
			buf += iov->iov_len;
		}

		ret = spdk_bdev_write_blocks(bdev_rmw->base_desc, rmw_io->ch->base_ch, rmw_io->write_unit->buf,
				bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
				bdev_rmw_complete_write, rmw_io);
		if (ret) {
			bdev_rmw_complete_write(bdev_io, false, rmw_io);
		} else {
			spdk_bdev_free_io(bdev_io);
		}
	} else {
		bdev_rmw_complete_io(bdev_io, false, rmw_io->orig_io);
	}
}

static int bdev_rmw_do_submit_write_request(struct bdev_rmw_io *rmw_io)
{
	struct write_unit *write_unit = rmw_io->write_unit;
	struct spdk_bdev_io *bdev_io = rmw_io->orig_io;
	struct bdev_rmw *bdev_rmw = rmw_io->ch->bdev_rmw;
	int ret;

	if (rmw_io->skip_preread) {
		ret = spdk_bdev_writev_blocks(bdev_rmw->base_desc, rmw_io->ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
				bdev_rmw_complete_write, rmw_io);
	} else {
		uint64_t write_unit_index = write_unit - bdev_rmw->write_units;

		if (write_unit->buf == NULL) {
			write_unit->buf = spdk_dma_malloc(bdev_rmw->write_unit_blocks * bdev_rmw->base_bdev->blocklen, 4096, NULL);
			if (!write_unit->buf) {
				return -ENOMEM;
			}
		}
		ret = spdk_bdev_read_blocks(bdev_rmw->base_desc, rmw_io->ch->base_ch, write_unit->buf,
				write_unit_index * bdev_rmw->write_unit_blocks, bdev_rmw->write_unit_blocks,
				bdev_rmw_complete_preread, rmw_io);
	}

	return ret;
}

static int bdev_rmw_submit_write_request(struct bdev_rmw *bdev_rmw, struct bdev_rmw_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct bdev_rmw_io *rmw_io = (struct bdev_rmw_io *)bdev_io->driver_ctx;
	uint64_t write_unit_index = bdev_io->u.bdev.offset_blocks / bdev_rmw->write_unit_blocks;
	struct write_unit *write_unit;
	bool submit;

	write_unit = &bdev_rmw->write_units[write_unit_index];

	rmw_io->write_unit = write_unit;
	rmw_io->write_unit_offset = bdev_io->u.bdev.offset_blocks % bdev_rmw->write_unit_blocks;
	rmw_io->orig_io = bdev_io;
	rmw_io->ch = ch;
	rmw_io->skip_preread = (rmw_io->write_unit_offset == 0 &&
				bdev_io->u.bdev.num_blocks == bdev_rmw->write_unit_blocks);

	pthread_spin_lock(&write_unit->lock);
	submit = TAILQ_EMPTY(&write_unit->ios);
	TAILQ_INSERT_TAIL(&write_unit->ios, rmw_io, link);
	pthread_spin_unlock(&write_unit->lock);

	if (!submit) {
		return 0;
	}

	return bdev_rmw_do_submit_write_request(rmw_io);
}

static void bdev_rmw_submit_request(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io)
{
	struct bdev_rmw *bdev_rmw = bdev_io->bdev->ctxt;
	struct bdev_rmw_io_channel *ch = spdk_io_channel_get_ctx(_ch);
	int ret;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		ret = spdk_bdev_readv_blocks(bdev_rmw->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
				bdev_rmw_complete_io, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		ret = bdev_rmw_submit_write_request(bdev_rmw, ch, bdev_io);
		break;
	// TODO: handle discard, flush etc.
	default:
		SPDK_ERRLOG("unsupported I/O type %d\n", bdev_io->type);
		ret = 1;
		break;
	}

	if (ret != 0) {
		SPDK_ERRLOG("ERROR on bdev_io submission!\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool bdev_rmw_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	return (io_type == SPDK_BDEV_IO_TYPE_READ || io_type == SPDK_BDEV_IO_TYPE_WRITE);
}

static struct spdk_io_channel *bdev_rmw_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(ctx);
}

static const struct spdk_bdev_fn_table bdev_rmw_fn_table = {
	.destruct		= bdev_rmw_destruct,
	.submit_request		= bdev_rmw_submit_request,
	.io_type_supported	= bdev_rmw_io_type_supported,
	.get_io_channel		= bdev_rmw_get_io_channel,
};

static int bdev_rmw_get_ctx_size(void)
{
	return sizeof(struct bdev_rmw_io);
}

static void bdev_rmw_hotremove_cb(void *ctx)
{
	struct spdk_bdev *bdev_find = ctx;
	struct bdev_rmw *bdev_rmw, *tmp;

	TAILQ_FOREACH_SAFE(bdev_rmw, &g_vbdevs, link, tmp) {
		if (bdev_rmw->base_bdev == bdev_find) {
			TAILQ_REMOVE(&g_vbdevs, bdev_rmw, link);
			spdk_bdev_unregister(&bdev_rmw->vbdev, NULL, NULL);
		}
	}
}

static struct spdk_bdev_module bdev_rmw_if;

static struct bdev_rmw *bdev_rmw_create(const char *name, struct spdk_bdev *base_bdev)
{
	struct bdev_rmw *bdev_rmw;
	int ret;
	unsigned int num_write_units;
	unsigned int i;

	TAILQ_FOREACH(bdev_rmw, &g_vbdevs, link) {
		if (strcmp(bdev_rmw->vbdev.name, name) == 0) {
			SPDK_ERRLOG("rmw bdev %s already exists\n", name);
			return NULL;
		}
	}

	if (!base_bdev->write_unit_size) {
		SPDK_ERRLOG("Base bdev write_unit_size = 0\n");
		return NULL;
	}

	num_write_units = base_bdev->blockcnt / base_bdev->write_unit_size;

	bdev_rmw = calloc(1, sizeof(*bdev_rmw));
	if (!bdev_rmw) {
		SPDK_ERRLOG("Failed to allocate bdev_rmw\n");
		return NULL;
	}

	bdev_rmw->vbdev.name = strdup(name);
	if (!bdev_rmw->vbdev.name) {
		SPDK_ERRLOG("Failed to allocate bdev_rmw name\n");
		goto err1;
	}

	ret = spdk_bdev_open(base_bdev, false, bdev_rmw_hotremove_cb, base_bdev, &bdev_rmw->base_desc);
	if (ret) {
		SPDK_ERRLOG("Could not open bdev %s\n", base_bdev->name);
		goto err2;
	}

	ret = spdk_bdev_module_claim_bdev(base_bdev, bdev_rmw->base_desc, &bdev_rmw_if);
	if (ret) {
		SPDK_ERRLOG("Could not claim bdev %s\n", base_bdev->name);
		goto err3;
	}

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_RMW, "bdev %s claimed as %s base dev\n", base_bdev->name, bdev_rmw->vbdev.name);

	bdev_rmw->base_bdev = base_bdev;
	bdev_rmw->write_unit_blocks = base_bdev->write_unit_size;

	bdev_rmw->vbdev.product_name = "rmw";
	bdev_rmw->vbdev.write_cache = true;
	bdev_rmw->vbdev.optimal_io_boundary = bdev_rmw->write_unit_blocks;
	bdev_rmw->vbdev.split_on_optimal_io_boundary = true;
	bdev_rmw->vbdev.blocklen = bdev_rmw->base_bdev->blocklen;
	bdev_rmw->vbdev.blockcnt = num_write_units * bdev_rmw->write_unit_blocks;
	bdev_rmw->vbdev.ctxt = bdev_rmw;
	bdev_rmw->vbdev.fn_table = &bdev_rmw_fn_table;
	bdev_rmw->vbdev.module = &bdev_rmw_if;

	bdev_rmw->write_units = calloc(num_write_units, sizeof(*bdev_rmw->write_units));
	if (!bdev_rmw->write_units) {
		goto err4;
	}

	for (i = 0; i < num_write_units; i++) {
		struct write_unit *write_unit = &bdev_rmw->write_units[i];

		TAILQ_INIT(&write_unit->ios);
		pthread_spin_init(&write_unit->lock, PTHREAD_PROCESS_PRIVATE);
	}

	spdk_io_device_register(bdev_rmw, vbdev_cache_ch_create_cb, vbdev_cache_ch_destroy_cb,
				sizeof(struct bdev_rmw_io_channel), "rmw_bdev");

	ret = spdk_bdev_register(&bdev_rmw->vbdev);
	if (ret) {
		SPDK_ERRLOG("Could not register vbdev\n");
		goto err5;
	}

	TAILQ_INSERT_TAIL(&g_vbdevs, bdev_rmw, link);

	SPDK_DEBUGLOG(SPDK_LOG_BDEV_RMW, "vbdev %s created\n", bdev_rmw->vbdev.name);

	return bdev_rmw;
err5:
	spdk_io_device_unregister(bdev_rmw, NULL);
	free(bdev_rmw->write_units);
err4:
	spdk_bdev_module_release_bdev(bdev_rmw->base_bdev);
err3:
	spdk_bdev_close(bdev_rmw->base_desc);
err2:
	free(bdev_rmw->vbdev.name);
err1:
	free(bdev_rmw);
	return NULL;
}

static int bdev_rmw_init(void)
{
	return 0;
}

static void bdev_rmw_examine_section(struct spdk_conf_section *conf_section, struct spdk_bdev *bdev)
{
	const char *name;
	const char *base_bdev_name;

	name = spdk_conf_section_get_val(conf_section, "Name");
	if (!name) {
		SPDK_ERRLOG("rmw name missing\n");
		return;
	}

	base_bdev_name = spdk_conf_section_get_val(conf_section, "BaseBdev");
	if (!base_bdev_name) {
		SPDK_ERRLOG("rmw base bdev name missing\n");
		return;
	}

	if (strcmp(base_bdev_name, bdev->name) != 0) {
		return;
	}

	bdev_rmw_create(name, bdev);
}

static void bdev_rmw_examine(struct spdk_bdev *bdev)
{
	struct spdk_conf_section *conf_section;

	conf_section = spdk_conf_first_section(NULL);
	while (conf_section != NULL) {
		if (spdk_conf_section_match_prefix(conf_section, "RMW")) {
			bdev_rmw_examine_section(conf_section, bdev);
		}
		conf_section = spdk_conf_next_section(conf_section);
	}

	spdk_bdev_module_examine_done(&bdev_rmw_if);
}

static struct spdk_bdev_module bdev_rmw_if = {
	.name = "rmw",
	.module_init = bdev_rmw_init,
	.get_ctx_size = bdev_rmw_get_ctx_size,
	.examine_config = bdev_rmw_examine,
};

SPDK_BDEV_MODULE_REGISTER(rmw, &bdev_rmw_if)

SPDK_LOG_REGISTER_COMPONENT("bdev_rmw", SPDK_LOG_BDEV_RMW)
