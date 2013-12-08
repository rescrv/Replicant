// Copyright (c) 2012, Robert Escriva
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
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#if HAVE_CONFIG_H
#include "config.h"
#endif

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>
#include <e/time.h>

// Replicant
#if !HAVE_DECL_OPEN_MEMSTREAM
#include "daemon/memstream.h"
#endif
#include "daemon/replicant_state_machine.h"
#include "daemon/replicant_state_machine_context.h"

#define COND_STR2NUM(STR, NUM) \
    do \
    { \
        if (strlen(STR) > sizeof(uint64_t)) \
        { \
            return -1; \
        } \
        char cond_buf[sizeof(uint64_t)]; \
        memset(cond_buf, 0, sizeof(cond_buf)); \
        memmove(cond_buf, STR, strlen(STR)); \
        e::unpack64be(cond_buf, &NUM); \
    } while (0)

replicant_state_machine_context :: replicant_state_machine_context(uint64_t s,
                                                                   uint64_t o,
                                                                   uint64_t c,
                                                                   replicant::object_manager* om,
                                                                   replicant::object_manager::object* ob)
    : slot(s)
    , object(o)
    , client(c)
    , log_output(NULL)
    , log_output_sz(0)
    , output(open_memstream(&log_output, &log_output_sz))
    , obj_man(om)
    , obj(ob)
    , response(NULL)
    , response_sz(0)
    , alarm_func("")
    , alarm_when(0)
{
}

replicant_state_machine_context :: ~replicant_state_machine_context() throw ()
{
    if (output)
    {
        fclose(output);
    }

    if (log_output)
    {
        free(log_output);
    }
}

void
replicant_state_machine_context :: close_log_output()
{
    if (output)
    {
        fclose(output);
    }

    output = NULL;
}

extern "C"
{

uint64_t
replicant_state_machine_get_client(struct replicant_state_machine_context* ctx)
{
    return ctx->client;
}

FILE*
replicant_state_machine_log_stream(struct replicant_state_machine_context* ctx)
{
    assert(ctx->output);
    return ctx->output;
}

void
replicant_state_machine_set_response(struct replicant_state_machine_context* ctx,
                                     const char* data, size_t data_sz)
{
    ctx->response = data;
    ctx->response_sz = data_sz;
}

int
replicant_state_machine_condition_create(struct replicant_state_machine_context* ctx,
                                         const char* cond)
{
    uint64_t _cond;
    COND_STR2NUM(cond, _cond);
    return ctx->obj_man->condition_create(ctx->obj, _cond);
}

int
replicant_state_machine_condition_destroy(struct replicant_state_machine_context* ctx,
                                          const char* cond)
{
    uint64_t _cond;
    COND_STR2NUM(cond, _cond);
    return ctx->obj_man->condition_destroy(ctx->obj, _cond);
}

int
replicant_state_machine_condition_broadcast(struct replicant_state_machine_context* ctx,
                                            const char* cond,
                                            uint64_t* state)
{
    uint64_t _cond;
    COND_STR2NUM(cond, _cond);
    return ctx->obj_man->condition_broadcast(ctx->obj, _cond, state);
}

void
replicant_state_machine_alarm(struct replicant_state_machine_context* ctx,
                              const char* func, uint64_t seconds)
{
    ctx->alarm_func = func;
    ctx->alarm_when = seconds;
}

} // extern "C"
