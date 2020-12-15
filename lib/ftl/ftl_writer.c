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
	TAILQ_INIT(&writer->basic_rq_queue);
	LIST_INIT(&writer->full_bands);
	writer->limit = limit;
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

static bool _is_limit(struct ftl_writer *writer) {
	return writer->dev->limit < writer->limit;
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
	} else if (!_is_limit(writer)) {
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
	struct ftl_band *band = _get_band(writer);
	if (spdk_unlikely(!band)) {
		return;
	}

	if (!_can_write(writer)) {
		return;
	}

	if (!TAILQ_EMPTY(&writer->rq_queue)) {
		struct ftl_rq *rq = TAILQ_FIRST(&writer->rq_queue);
		TAILQ_REMOVE(&writer->rq_queue, rq, qentry);
		ftl_band_rq_write(writer->band, rq);
	}


	if (!TAILQ_EMPTY(&writer->basic_rq_queue)) {
		struct ftl_basic_rq *brq = TAILQ_FIRST(&writer->basic_rq_queue);
		TAILQ_REMOVE(&writer->basic_rq_queue, brq, qentry);
		ftl_band_basic_rq_write(writer->band, brq);
	}

	if (spdk_unlikely(!LIST_EMPTY(&writer->full_bands))) {
		_close_full_bands(writer);
	}
}
