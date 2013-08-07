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
#include <stdint.h>
#include <string.h>

/* Replicant */
#include <replicant_state_machine.h>

inline char*
pack64be(uint64_t number, char* buffer)
{
    buffer[0] = number >> 56;
    buffer[1] = (number >> 48) & 0xff;
    buffer[2] = (number >> 40) & 0xff;
    buffer[3] = (number >> 32) & 0xff;
    buffer[4] = (number >> 24) & 0xff;
    buffer[5] = (number >> 16) & 0xff;
    buffer[6] = (number >> 8) & 0xff;
    buffer[7] = number & 0xff;
    return buffer + sizeof(uint64_t);
}

inline const char*
unpack64be(const char* buffer, uint64_t* number)
{
    *number = ((uint64_t)buffer[0]) << 56
            | ((uint64_t)buffer[1]) << 48
            | ((uint64_t)buffer[2]) << 40
            | ((uint64_t)buffer[3]) << 32
            | ((uint64_t)buffer[4]) << 24
            | ((uint64_t)buffer[5]) << 16
            | ((uint64_t)buffer[6]) << 8
            | ((uint64_t)buffer[7]);
    return buffer + sizeof(uint64_t);
}

void*
counter_create(struct replicant_state_machine_context* ctx)
{
    void* x = malloc(sizeof(uint64_t));
    memset(x, 0, sizeof(uint64_t));
    return x;
}

void*
counter_recreate(struct replicant_state_machine_context* ctx,
                 const char* data, size_t data_sz)
{
    if (data_sz != sizeof(uint64_t))
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "recreation failed: corrupt snapshot");
        return NULL;
    }

    void* x = malloc(sizeof(uint64_t));
    memmove(x, data, sizeof(uint64_t));
    return x;
}

void
counter_destroy(struct replicant_state_machine_context* ctx,
                void* f)
{
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
    if (!obj)
    {
        *data = NULL;
        *data_sz = 0;
    }
    else
    {
        *data = malloc(sizeof(uint64_t));
        *data_sz = sizeof(uint64_t);

        if (data)
        {
            memmove(data, obj, sizeof(uint64_t));
        }
    }
}

void
counter_counter(struct replicant_state_machine_context* ctx,
                void* obj,
                const char* data, size_t data_sz)
{
    if (obj)
    {
        uint64_t count = 0;
        unpack64be(obj, &count);
        count += 1;
        pack64be(count, obj);
        replicant_state_machine_set_response(ctx, obj, sizeof(uint64_t));
    }
}

struct replicant_state_machine rsm = {
    counter_create,
    counter_recreate,
    counter_destroy,
    counter_snapshot,
    {{"counter", counter_counter},
     {NULL, NULL}}
};
