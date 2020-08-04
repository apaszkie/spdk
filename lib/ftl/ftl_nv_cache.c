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

#include "spdk/ftl.h"
#include "spdk/log.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

#include "ftl_nv_cache.h"
#include "ftl_core.h"

static void _nv_cache_bdev_event_cb(enum spdk_bdev_event_type type,
				    struct spdk_bdev *bdev, void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		assert(0);
		break;
	default:
		break;
	}
}

/* Dummy bdev module used to to claim bdevs. */
static struct spdk_bdev_module _ftl_bdev_nv_cache_module = {
	.name   = "ftl_lib_nv_cache",
};

int
ftl_nv_cache_init(struct spdk_ftl_dev *dev, const char *bdev_name)
{
	struct spdk_bdev *bdev;
	struct ftl_nv_cache *nv_cache = &dev->nv_cache;
	struct spdk_ftl_conf *conf = &dev->conf;
	char pool_name[128];
	int rc;

	if (!bdev_name) {
		return 0;
	}

	bdev = spdk_bdev_get_by_name(bdev_name);
	if (!bdev) {
		SPDK_ERRLOG("Unable to find bdev: %s\n", bdev_name);
		return -1;
	}

	if (spdk_bdev_open_ext(bdev_name, true, _nv_cache_bdev_event_cb,
			       dev, &nv_cache->bdev_desc)) {
		SPDK_ERRLOG("Unable to open bdev: %s\n", bdev_name);
		return -1;
	}

	if (spdk_bdev_module_claim_bdev(bdev, nv_cache->bdev_desc,
					&_ftl_bdev_nv_cache_module)) {
		spdk_bdev_close(nv_cache->bdev_desc);
		nv_cache->bdev_desc = NULL;
		SPDK_ERRLOG("Unable to claim bdev %s\n", bdev_name);
		return -1;
	}

	SPDK_INFOLOG(SPDK_LOG_FTL_INIT, "Using %s as write buffer cache\n",
		     spdk_bdev_get_name(bdev));

	if (spdk_bdev_get_block_size(bdev) != FTL_BLOCK_SIZE) {
		SPDK_ERRLOG("Unsupported block size (%d)\n", spdk_bdev_get_block_size(bdev));
		return -1;
	}

	if (!spdk_bdev_is_md_separate(bdev)) {
		SPDK_ERRLOG("Bdev %s doesn't support separate metadata buffer IO\n",
			    spdk_bdev_get_name(bdev));
		return -1;
	}

	if (spdk_bdev_get_md_size(bdev) < sizeof(uint64_t)) {
		SPDK_ERRLOG("Bdev's %s metadata is too small (%"PRIu32")\n",
			    spdk_bdev_get_name(bdev), spdk_bdev_get_md_size(bdev));
		return -1;
	}

	if (spdk_bdev_get_dif_type(bdev) != SPDK_DIF_DISABLE) {
		SPDK_ERRLOG("Unsupported DIF type used by bdev %s\n",
			    spdk_bdev_get_name(bdev));
		return -1;
	}

	/* The cache needs to be capable of storing at least two full bands. This requirement comes
	 * from the fact that cache works as a protection against power loss, so before the data
	 * inside the cache can be overwritten, the band it's stored on has to be closed. Plus one
	 * extra block is needed to store the header.
	 */
	if (spdk_bdev_get_num_blocks(bdev) < ftl_get_num_blocks_in_band(dev) * 2 + 1) {
		SPDK_ERRLOG("Insufficient number of blocks for write buffer cache (available: %"
			    PRIu64", required: %"PRIu64")\n", spdk_bdev_get_num_blocks(bdev),
			    ftl_get_num_blocks_in_band(dev) * 2 + 1);
		return -1;
	}

	rc = snprintf(pool_name, sizeof(pool_name), "ftl-nvpool-%p", dev);
	if (rc < 0 || rc >= 128) {
		return -1;
	}

	nv_cache->md_pool = spdk_mempool_create(pool_name, conf->nv_cache.max_request_cnt,
						spdk_bdev_get_md_size(bdev) *
						conf->nv_cache.max_request_size,
						SPDK_MEMPOOL_DEFAULT_CACHE_SIZE,
						SPDK_ENV_SOCKET_ID_ANY);
	if (!nv_cache->md_pool) {
		SPDK_ERRLOG("Failed to initialize non-volatile cache metadata pool\n");
		return -1;
	}

	nv_cache->dma_buf = spdk_dma_zmalloc(FTL_BLOCK_SIZE, spdk_bdev_get_buf_align(bdev), NULL);
	if (!nv_cache->dma_buf) {
		SPDK_ERRLOG("Memory allocation failure\n");
		return -1;
	}

	if (pthread_spin_init(&nv_cache->lock, PTHREAD_PROCESS_PRIVATE)) {
		SPDK_ERRLOG("Failed to initialize cache lock\n");
		return -1;
	}

	nv_cache->current_addr = FTL_NV_CACHE_DATA_OFFSET;
	nv_cache->num_data_blocks = spdk_bdev_get_num_blocks(bdev) - 1;
	nv_cache->num_available = nv_cache->num_data_blocks;
	nv_cache->ready = false;

	return 0;
}

