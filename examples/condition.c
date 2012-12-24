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

/* C */
#include <ctype.h>
#include <stdio.h>
#include <string.h>

/* Replicant */
#include <replicant_state_machine.h>

void*
condition_create(struct replicant_state_machine_context* ctx)
{
    void* x = malloc(sizeof(int64_t) + 21);
    uint64_t cond;

    if (replicant_state_machine_condition_create(ctx, &cond) < 0)
    {
        return NULL;
    }

    memmove(x, &cond, sizeof(int64_t));
    return x;
}

void*
condition_recreate(struct replicant_state_machine_context* ctx,
                   const char* data, size_t data_sz)
{
    void* x = malloc(sizeof(int64_t) + 21);

    if (data_sz == sizeof(int64_t))
    {
        memmove(x, data, data_sz);
        return x;
    }

    return NULL;
}

void
condition_destroy(struct replicant_state_machine_context* ctx,
                  void* f)
{
    free(f);
}

void
condition_snapshot(struct replicant_state_machine_context* ctx,
                   void* obj, const char** data, size_t* data_sz)
{
    char* ret = malloc(sizeof(int64_t));
    *data = ret;
    *data_sz = sizeof(int64_t);
    memmove(ret, obj, sizeof(int64_t));
}

void
condition_wakeup(struct replicant_state_machine_context* ctx,
                 void* obj, const char* data, size_t data_sz)
{
    int64_t cond;
    memmove(&cond, obj, sizeof(uint64_t));
    replicant_state_machine_condition_broadcast(ctx, cond, NULL);
    int num = sprintf(obj + sizeof(uint64_t), "%ld", cond);
    replicant_state_machine_set_response(ctx, obj + sizeof(uint64_t), num + 1);
}

struct replicant_state_machine rsm = {
    condition_create,
    condition_recreate,
    condition_destroy,
    condition_snapshot,
    {{"wakeup", condition_wakeup},
     {NULL, NULL}}
};
