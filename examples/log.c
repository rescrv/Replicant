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

/* Replicant */
#include <rsm.h>

void*
log_create(struct rsm_context* ctx)
{
    return (void*) -1;
}

void*
log_recreate(struct rsm_context* ctx,
             const char* data, size_t data_sz)
{
    return (void*) -1;
}

int
log_snapshot(struct rsm_context* ctx,
             void* obj,
             char** data, size_t* data_sz)
{
    *data = NULL;
    *data_sz = 0;
    return 0;
}

void
log_log(struct rsm_context* ctx,
        void* obj,
        const char* data, size_t data_sz)
{
    rsm_log(ctx, "begin logging of function");
    int is_print = 1;
    int saw_null = 0;
    size_t i = 0;

    for (i = 0; i < data_sz; ++i)
    {
        if (data[i] == '\0')
        {
            saw_null = 1;
            break;
        }
        else if (!isprint(data[i]))
        {
            is_print = 0;
        }
    }

    if (is_print && !saw_null)
    {
        rsm_log(ctx, "log was asked to log \"%.*s\" and it is %lld bytes long", data_sz, data, i);
    }
    else if (is_print && saw_null)
    {
        rsm_log(ctx, "log was asked to log \"%s\" and it is %lld bytes long", data, i);
    }
    else
    {
        rsm_log(ctx, "will not log unprintable characters");
    }

    rsm_log(ctx, "end logging of function");
}

struct state_machine rsm = {
    log_create,
    log_recreate,
    log_snapshot,
    {{"log", log_log},
     {NULL, NULL}}
};
