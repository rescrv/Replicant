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

void*
nop_create(struct replicant_state_machine_context* ctx)
{
    return malloc(sizeof(int));
}

void*
nop_recreate(struct replicant_state_machine_context* ctx,
             const char* data, size_t sz)
{
    return malloc(sizeof(int));
}

void
nop_destroy(struct replicant_state_machine_context* ctx,
            void* f)
{
    free(f);
}

void
nop_snapshot(struct replicant_state_machine_context* ctx,
             void* obj,
             const char** data, size_t* sz)
{
    *data = malloc(sizeof(int));
    *sz = sizeof(int);
}

void
nop_nop(struct replicant_state_machine_context* ctx,
        void* obj,
        const char* data, size_t data_sz)
{
}

struct replicant_state_machine rsm = {
    nop_create,
    nop_recreate,
    nop_destroy,
    nop_snapshot,
    {{"nop", nop_nop},
     {NULL, NULL}}
};
