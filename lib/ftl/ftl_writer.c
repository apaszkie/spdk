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

#include <spdk/likely.h>

#include "ftl_writer.h"
#include "ftl_band.h"
#include "ftl_band_ops.h"

void ftl_writer_init(struct spdk_ftl_dev *dev, struct ftl_writer *writer,
		uint64_t limit)
{
	memset(writer, 0, sizeof(*writer));
	writer->dev = dev;
	TAILQ_INIT(&writer->rq_queue);
	LIST_INIT(&writer->full_bands);
	writer->limit = limit;
	writer->halt = true;
}

static bool _can_write(struct ftl_writer *writer) {
	return writer->band->state == FTL_BAND_STATE_OPEN;
}

static void _band_state_change(struct ftl_band *band)
{
	struct ftl_writer *writer = band->owner.priv;

	switch(band->state) {
	case FTL_BAND_STATE_FULL:
		assert(writer->band == band);
		LIST_INSERT_HEAD(&writer->full_bands, band, list_entry);
		writer->band = NULL;
		break;

	default:
		break;
	}
}

static void _close_full_bands(struct ftl_writer *writer)
{
	struct ftl_band *band, *next;

	LIST_FOREACH_SAFE(band, &writer->full_bands, list_entry, next) {
		if (band->iter.queue_depth) {
			continue;
		}

		LIST_REMOVE(band, list_entry);
		ftl_band_close(band);
		ftl_band_clear_owner(band, _band_state_change, writer);
	}
}

static bool _is_active(struct ftl_writer *writer) {
	if (spdk_unlikely(writer->halt)) {
		return false;
	}

	if (writer->dev->limit < writer->limit) {
		return false;
	} else {
		return true;
	}
}

static struct ftl_band *_get_band(struct ftl_writer *writer)
{
	if (spdk_likely(writer->next_band)) {
		if (spdk_unlikely(writer->next_band->state == FTL_BAND_STATE_PREP)) {
			struct ftl_zone *zone = CIRCLEQ_FIRST(&writer->next_band->zones);

			if (ftl_zone_is_writable(writer->dev, zone)) {
				/*
				 * In the mean time open new band.
				 */
				ftl_band_open(writer->next_band);
			}
		}
	} else if (_is_active(writer)) {
		/* Get in the mean time next band */
		writer->next_band = ftl_band_get_next_free(writer->dev);
		if (writer->next_band) {
			ftl_band_set_owner(writer->next_band,
					_band_state_change, writer);

			if (ftl_band_write_prep(writer->next_band)) {
				/* TODO Handle Error */
				abort();
			}
		}
	}

	if (spdk_unlikely(!writer->band)) {
		if (writer->next_band) {
			if (writer->next_band->state == FTL_BAND_STATE_OPEN) {
				writer->band = writer->next_band;
				writer->next_band = NULL;
			}
		}
	}

	return writer->band;
}


void ftl_writer_run(struct ftl_writer *writer)
{
	int rc;
	struct ftl_band *band;

	if (spdk_unlikely(!LIST_EMPTY(&writer->full_bands))) {
		_close_full_bands(writer);
	}

	band = _get_band(writer);
	if (spdk_unlikely(!band)) {
		return;
	}

	if (!_can_write(writer)) {
		return;
	}

	if (!TAILQ_EMPTY(&writer->rq_queue)) {
		struct ftl_rq *rq = TAILQ_FIRST(&writer->rq_queue);
		TAILQ_REMOVE(&writer->rq_queue, rq, qentry);
		rc = ftl_band_rq_write(writer->band, rq);
		if (spdk_unlikely(rc)) {
			rq->owner.cb(rq);
		}
	}
}

static void _pad_cb(struct ftl_rq *rq)
{
	struct ftl_writer *writer = rq->owner.priv;
	if (rq->success) {
		struct ftl_band *band = rq->io.band;
		if (ftl_band_full(band, band->iter.offset)) {
			/* We finished band padding and can free request*/
			assert(writer->pad_rq == rq);
			ftl_rq_del(writer->pad_rq);
			writer->pad_rq = NULL;
		} else {
			/* Continue band padding */
			ftl_writer_queue_rq(writer, rq);
		}
	} else {
		/* TODO Handle error */
		abort();
	}
}

bool ftl_writer_is_halted(struct ftl_writer *writer)
{
	if (writer->band) {
		if (writer->halt & !writer->pad_rq) {
			/*
			 * Halt procedure is in progress, we need to close
			 * active band by padding them
			 */

			struct spdk_ftl_dev *dev = writer->dev;
			struct ftl_rq *pad = ftl_rq_new(dev, dev->xfer_size,
					dev->md_size);
			if (pad) {
				writer->pad_rq = pad;
				pad->owner.priv = writer;
				pad->owner.cb = _pad_cb;
				memset(pad->io_payload, 0,
					pad->num_blocks * FTL_BLOCK_SIZE);
				ftl_writer_queue_rq(writer, pad);
			}
		}

		return false;
	}

	if (writer->next_band) {
		return false;
	}

	if (!LIST_EMPTY(&writer->full_bands)) {
		return false;
	}

	return true;
}
