// Copyright (c) 2015, Robert Escriva
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Replicant nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#define __STDC_LIMIT_MACROS

#if HAVE_CONFIG_H
#include "config.h"
#endif

// C
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

// Replicant
#include <rsm.h>
#include "visibility.h"
#include "daemon/object_interface.h"
#include "daemon/rsm.h"

#pragma GCC diagnostic ignored "-Wsuggest-attribute=format"

extern "C"
{

REPLICANT_API void
rsm_log(rsm_context* ctx, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    object_command_log(ctx->obj_int, format, args);
    va_end(args);
}

REPLICANT_API void
rsm_set_output(rsm_context* ctx, const char* output, size_t output_sz)
{
    char* ptr = NULL;

    if (output_sz)
    {
        ptr = static_cast<char*>(realloc(ctx->output, output_sz));

        if (ptr == NULL)
        {
            ctx->status = -1;
            return;
        }

        ctx->output = ptr;
        memmove(ctx->output, output, output_sz);
    }

    ctx->output_sz = output_sz;
}

REPLICANT_API void
rsm_cond_create(rsm_context* ctx, const char* cond)
{
    return object_cond_create(ctx->obj_int, cond);
}

REPLICANT_API void
rsm_cond_destroy(rsm_context* ctx, const char* cond)
{
    return object_cond_destroy(ctx->obj_int, cond);
}

REPLICANT_API int
rsm_cond_broadcast(rsm_context* ctx, const char* cond)
{
    return object_cond_broadcast(ctx->obj_int, cond);
}

REPLICANT_API int
rsm_cond_broadcast_data(rsm_context* ctx,
                        const char* cond,
                        const char* data, size_t data_sz)
{
    return object_cond_broadcast_data(ctx->obj_int, cond, data, data_sz);
}

REPLICANT_API int
rsm_cond_current_value(rsm_context* ctx,
                       const char* cond, uint64_t* state,
                       const char** data, size_t* data_sz)
{
    return object_cond_current_value(ctx->obj_int, cond, state, data, data_sz);
}

REPLICANT_API void
rsm_tick_interval(struct rsm_context* ctx, const char* func, uint64_t seconds)
{
    return object_tick_interval(ctx->obj_int, func, seconds);
}

REPLICANT_API void
rsm_context_init(rsm_context* ctx, object_interface* obj_int)
{
    ctx->obj_int = obj_int;
    ctx->status = 0;
    ctx->output = NULL;
    ctx->output_sz = 0;
}

REPLICANT_API void
rsm_context_finish(rsm_context* ctx)
{
    if (ctx->output)
    {
        free(ctx->output);
        ctx->output = NULL;
        ctx->output_sz = 0;
    }
}

} // extern "C"
