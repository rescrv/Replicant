/* Copyright (c) 2013, Robert Escriva
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

/* C */
#include <assert.h>
#include <stdint.h>
#include <string.h>

/* Replicant */
#include <replicant_state_machine.h>

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

void*
counter_create(struct replicant_state_machine_context* ctx)
{
    void* x = NULL;
    uint64_t count = 0;
    (void)(ctx); /* unusued */

    x = malloc(sizeof(uint64_t));
    count = 0;
    pack64be(count, x);
    return x;
}

void*
counter_recreate(struct replicant_state_machine_context* ctx,
                 const char* data, size_t data_sz)
{
    FILE* log = NULL;
    void* x = NULL;
    uint64_t count = 0;
    (void)(ctx); /* unusued */

    log = replicant_state_machine_log_stream(ctx);

    if (data_sz != sizeof(uint64_t))
    {
        fprintf(log, "recreation failed: corrupt snapshot\n");
        return NULL;
    }

    x = malloc(sizeof(uint64_t));
    memmove(x, data, sizeof(uint64_t));
    count = 0;
    unpack64be(x, &count);
    fprintf(log, "recreated counter object at %lu\n", count);
    return x;
}

void
counter_destroy(struct replicant_state_machine_context* ctx,
                void* f)
{
    (void)(ctx); /* unusued */

    if (f)
    {
        free(f);
    }
}

void
counter_snapshot(struct replicant_state_machine_context* ctx,
                 void* obj,
                 const char** data, size_t* data_sz)
{
    char* ptr;
    (void)(ctx); /* unusued */

    ptr = malloc(sizeof(uint64_t));
    *data = ptr;
    *data_sz = sizeof(uint64_t);

    if (ptr)
    {
        memmove(ptr, obj, sizeof(uint64_t));
    }
}

void
counter_counter(struct replicant_state_machine_context* ctx,
                void* obj,
                const char* data, size_t data_sz)
{
    uint64_t count;
    (void)(data); /* unused */
    (void)(data_sz); /* unused */

    count = 0;
    unpack64be(obj, &count);
    count += 1;
    pack64be(count, obj);

    if (count % 10000 == 0)
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "counter hit %lu\n", count);
    }

    replicant_state_machine_set_response(ctx, obj, sizeof(uint64_t));
}

struct replicant_state_machine rsm = {
    counter_create,
    counter_recreate,
    counter_destroy,
    counter_snapshot,
    {{"counter", counter_counter},
     {NULL, NULL}}
};
