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

#ifndef replicant_state_machine_h_
#define replicant_state_machine_h_
#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* C */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

struct replicant_state_machine_context;

uint64_t
replicant_state_machine_get_client(struct replicant_state_machine_context* ctx);
FILE*
replicant_state_machine_log_stream(struct replicant_state_machine_context* ctx);
void
replicant_state_machine_set_response(struct replicant_state_machine_context* ctx,
                                     const char* data, size_t data_sz);

int
replicant_state_machine_condition_create(struct replicant_state_machine_context* ctx,
                                         const char* cond);
int
replicant_state_machine_condition_destroy(struct replicant_state_machine_context* ctx,
                                          const char* cond);
int
replicant_state_machine_condition_broadcast(struct replicant_state_machine_context* ctx,
                                            const char* cond,
                                            uint64_t* state);

struct replicant_state_machine_step
{
    const char* name;
    void (*func)(struct replicant_state_machine_context* ctx, void* obj, const char* data, size_t data_sz);
};

struct replicant_state_machine
{
    void* (*ctor)(struct replicant_state_machine_context* ctx);
    void* (*rtor)(struct replicant_state_machine_context* ctx, const char* data, size_t data_sz);
    void (*dtor)(struct replicant_state_machine_context* ctx, void* obj);
    void (*snap)(struct replicant_state_machine_context* ctx, void* obj, const char** data, size_t* data_sz);
    struct replicant_state_machine_step steps[];
};

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* replicant_state_machine_h_ */
