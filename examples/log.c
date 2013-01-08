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
#include <replicant_state_machine.h>

void*
log_create(struct replicant_state_machine_context* ctx)
{
    return malloc(sizeof(int));
}

void*
log_recreate(struct replicant_state_machine_context* ctx,
             const char* data, size_t data_sz)
{
    return malloc(sizeof(int));
}

void
log_destroy(struct replicant_state_machine_context* ctx,
            void* f)
{
    free(f);
}

void
log_snapshot(struct replicant_state_machine_context* ctx,
             void* obj,
             const char** data, size_t* data_sz)
{
    *data = NULL;
    *data_sz = 0;
}

void
log_log(struct replicant_state_machine_context* ctx,
        void* obj,
        const char* data, size_t data_sz)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    fprintf(log, "begin logging of slot\n");
    int is_print = 1;
    int saw_null = 0;
    size_t i = 0;

    for (i = 0; i < data_sz; ++i)
    {
        if (data[i] == '\0')
        {
            saw_null = 1;
        }
        else if (!isprint(data[i]))
        {
            is_print = 0;
        }
    }

    if (is_print && saw_null)
    {
        fprintf(log, "log was asked to log \"%s\" and it is %lld bytes long\n", data, i);
    }
    else
    {
        fprintf(log, "will not log unprintable characters\n");
    }

    fprintf(log, "end logging of slot\n");
}

struct replicant_state_machine rsm = {
    log_create,
    log_recreate,
    log_destroy,
    log_snapshot,
    {{"log", log_log},
     {NULL, NULL}}
};
