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

#ifndef FTL_WRITER_H
#define FTL_WRITER_H

#include "spdk/stdinc.h"
#include "spdk/queue.h"

#include "ftl_rq.h"

struct ftl_writer {
	struct spdk_ftl_dev *dev;

	TAILQ_HEAD(, ftl_rq) rq_queue;

	TAILQ_HEAD(, ftl_basic_rq) basic_rq_queue;

	uint64_t queue_depth;

	/* Band currently being written to */
	struct ftl_band	*band;

	/* Band next being written to */
	struct ftl_band *next_band;

	/* List of full bands */
	LIST_HEAD(, ftl_band) full_bands;

	/* FTL band limit which blocks writes */
	int limit;
};

void ftl_writer_init(struct spdk_ftl_dev *dev, struct ftl_writer *writer,
		uint64_t limit);

void ftl_writer_run(struct ftl_writer *writer);

static inline void
ftl_writer_queue_rq(struct ftl_writer *writer, struct ftl_rq *rq)
{
	TAILQ_INSERT_TAIL(&writer->rq_queue, rq, qentry);
}

static inline void
ftl_writer_queue_basic_rq(struct ftl_writer *writer, struct ftl_basic_rq *brq)
{
	TAILQ_INSERT_TAIL(&writer->basic_rq_queue, brq, qentry);
}

#endif  // FTL_WRITER_H
