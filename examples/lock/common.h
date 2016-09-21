/* Copyright (c) 2013-2016, Robert Escriva
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Replicant nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef replicant_lock_example_h_
#define replicant_lock_example_h_

/* C */
#include <stdint.h>
#include <stdio.h>

/* replicant */
#include <replicant.h>

inline char*
pack64be(uint64_t number, char* buffer)
{
    buffer[0] = (number >> 56) & 0xffU;
    buffer[1] = (number >> 48) & 0xffU;
    buffer[2] = (number >> 40) & 0xffU;
    buffer[3] = (number >> 32) & 0xffU;
    buffer[4] = (number >> 24) & 0xffU;
    buffer[5] = (number >> 16) & 0xffU;
    buffer[6] = (number >> 8) & 0xffU;
    buffer[7] = number & 0xffU;
    return buffer + sizeof(uint64_t);
}

inline const char*
unpack64be(const char* buffer, uint64_t* number)
{
    *number = 0;
    *number |= (((uint64_t)buffer[0]) & 0xffU) << 56;
    *number |= (((uint64_t)buffer[1]) & 0xffU) << 48;
    *number |= (((uint64_t)buffer[2]) & 0xffU) << 40;
    *number |= (((uint64_t)buffer[3]) & 0xffU) << 32;
    *number |= (((uint64_t)buffer[4]) & 0xffU) << 24;
    *number |= (((uint64_t)buffer[5]) & 0xffU) << 16;
    *number |= (((uint64_t)buffer[6]) & 0xffU) << 8;
    *number |= (((uint64_t)buffer[7]) & 0xffU);
    return buffer + sizeof(uint64_t);
}

#ifdef REPLICANT_FINISH
void
replicant_finish(struct replicant_client* repl, int64_t id, enum replicant_returncode* rc)
{
    enum replicant_returncode lrc;
    int64_t lid;

    if (id < 0)
    {
        goto error;
    }

    lid = replicant_client_wait(repl, id, -1, &lrc);

    if (lid < 0)
    {
        *rc = lrc;
        goto error;
    }

    assert(id == lid);

    if (*rc != REPLICANT_SUCCESS)
    {
        goto error;
    }

    return;

error:
    fprintf(stderr, "error: %s\n", replicant_client_error_message(repl));
    exit(EXIT_FAILURE);
}
#endif

#endif /* replicant_lock_example_h_ */
