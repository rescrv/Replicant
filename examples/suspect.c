
/* Copyright (c) 2012, Robert Escriva
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
suspect_create(struct replicant_state_machine_context* ctx)
{
    (void) ctx;
    return malloc(sizeof(int));
}

void*
suspect_recreate(struct replicant_state_machine_context* ctx,
                 const char* data, size_t data_sz)
{
    (void) ctx;
    (void) data;
    (void) data_sz;
    return malloc(sizeof(int));
}

void
suspect_destroy(struct replicant_state_machine_context* ctx,
                void* f)
{
    (void) ctx;
    free(f);
}

void
suspect_snapshot(struct replicant_state_machine_context* ctx,
                 void* obj,
                 const char** data, size_t* data_sz)
{
    (void) ctx;
    (void) obj;
    *data = malloc(sizeof(int));
    *data_sz = sizeof(int);
}

void
suspect_start(struct replicant_state_machine_context* ctx,
              void* obj,
              const char* data, size_t data_sz)
{
    uint64_t client = replicant_state_machine_get_client(ctx);
    FILE* log = replicant_state_machine_log_stream(ctx);
    char buf[sizeof(uint64_t)];

    pack64be(client, buf);
    fprintf(log, "requesting failure tracking of client %lu\n", client);
    replicant_state_machine_suspect(ctx, client, "suspect",  buf, sizeof(uint64_t));
    (void) obj;
    (void) data;
    (void) data_sz;
}

void
suspect_suspect(struct replicant_state_machine_context* ctx,
                void* obj,
                const char* data, size_t data_sz)
{
    uint64_t client;
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (data_sz != sizeof(uint64_t))
    {
        fprintf(log, "\"suspect\" callback called with data_sz=%lu, not data_sz=%lu\n",
                data_sz, sizeof(uint64_t));
        return;
    }

    unpack64be(data, &client);
    fprintf(log, "replicant suspects client %lu\n", client);
    (void) obj;
}

struct replicant_state_machine rsm = {
    suspect_create,
    suspect_recreate,
    suspect_destroy,
    suspect_snapshot,
    {{"start", suspect_start},
     {"suspect", suspect_suspect},
     {NULL, NULL}}
};
