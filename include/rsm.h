/* Copyright (c) 2015, Robert Escriva
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
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef replicant_rsm_h_
#define replicant_rsm_h_

/* C */
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct rsm_context;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-pedantic"

struct state_machine_transition
{
    const char* name;
    void (*func)(struct rsm_context* ctx, void* obj, const char* data, size_t data_sz);
};

struct state_machine
{
    void* (*ctor)(struct rsm_context* ctx);
    void* (*rtor)(struct rsm_context* ctx, const char* data, size_t data_sz);
    int (*snap)(struct rsm_context* ctx, void* obj, char** data, size_t* data_sz);
    struct state_machine_transition transitions[];
};

#pragma GCC diagnostic pop

void rsm_log(struct rsm_context* ctx, const char* format, ...);
void rsm_set_output(struct rsm_context* ctx, const char* output, size_t output_sz);

void rsm_cond_create(struct rsm_context* ctx, const char* cond);
void rsm_cond_destroy(struct rsm_context* ctx, const char* cond);
int rsm_cond_broadcast(struct rsm_context* ctx, const char* cond);
int rsm_cond_broadcast_data(struct rsm_context* ctx,
                            const char* cond,
                            const char* data, size_t data_sz);
int rsm_cond_current_value(struct rsm_context* ctx,
                           const char* cond, uint64_t* state,
                           const char** data, size_t* data_sz);

void rsm_tick_interval(struct rsm_context* ctx, const char* func, uint64_t seconds);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* replicant_rsm_h_ */
