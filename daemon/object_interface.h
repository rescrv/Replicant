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

#ifndef replicant_daemon_object_interface_h_
#define replicant_daemon_object_interface_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

/* Replicant */
#include <replicant.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct object_interface;

struct object_interface* object_interface_create(int fd);
void object_interface_destroy(struct object_interface* obj_int);

void object_permanent_error(struct object_interface* obj_int, const char* format, ...) __attribute__ ((noreturn));

enum action_t
{
    ACTION_CTOR = 1,
    ACTION_RTOR = 2,
    ACTION_COMMAND  = 3,
    ACTION_SNAPSHOT = 4,
    ACTION_SHUTDOWN = 16
};

int object_next_action(struct object_interface* obj_int, enum action_t* action);

struct command
{
    const char* func;
    const char* input;
    size_t input_sz;
};

enum command_response_t
{
    COMMAND_RESPONSE_LOG                    = 1,
    COMMAND_RESPONSE_COND_CREATE            = 2,
    COMMAND_RESPONSE_COND_DESTROY           = 3,
    COMMAND_RESPONSE_COND_BROADCAST         = 4,
    COMMAND_RESPONSE_COND_BROADCAST_DATA    = 5,
    COMMAND_RESPONSE_COND_CURRENT_VALUE     = 6,
    COMMAND_RESPONSE_TICK_INTERVAL          = 7,
    COMMAND_RESPONSE_OUTPUT                 = 16
};

void object_read_snapshot(struct object_interface* obj_int, const char** data, size_t* data_sz);

void object_read_command(struct object_interface* obj_int, struct command* cmd);
void object_command_log(struct object_interface* obj_int,
                        const char *format, va_list ap);
void object_command_output(struct object_interface* obj_int,
                           enum replicant_returncode status,
                           const char* data, size_t data_sz);

void object_cond_create(struct object_interface* obj_int, const char* cond);
void object_cond_destroy(struct object_interface* obj_int, const char* cond);
int object_cond_broadcast(struct object_interface* obj_int, const char* cond);
int object_cond_broadcast_data(struct object_interface* obj_int,
                               const char* cond,
                               const char* data, size_t data_sz);
int object_cond_current_value(struct object_interface* obj_int,
                              const char* cond, uint64_t* state,
                              const char** data, size_t* data_sz);

void object_tick_interval(struct object_interface* obj_int,
                          const char* func,
                          uint64_t seconds);

void object_snapshot(struct object_interface* obj_int,
                     const char* data, size_t data_sz);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* replicant_daemon_object_interface_h_ */
