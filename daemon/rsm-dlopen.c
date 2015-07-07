/* Copyright (c) 2015, Cornell University
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

/* C */
#include <string.h>

/* POSIX */
#include <dlfcn.h>
#include <errno.h>
#include <unistd.h>

/* Replicant */
#include <rsm.h>
#include "daemon/rsm.h"
#include "daemon/object_interface.h"

static void
action_ctor(struct state_machine* rsm,
            void** state,
            struct object_interface* obj_int);

static void
action_rtor(struct state_machine* rsm,
            void** state,
            struct object_interface* obj_int);

static void
action_command(struct state_machine* rsm,
               void* state,
               struct object_interface* obj_int);

static void
action_snapshot(struct state_machine* rsm,
                void* state,
                struct object_interface* obj_int);

static void
action_nop(struct state_machine* rsm,
           void* state,
           struct object_interface* obj_int);

int
main(int argc, const char* argv[])
{
    const long max_open_files = sysconf(_SC_OPEN_MAX);
    char* socket_str = NULL;
    long socket_fd = -1;
    long li = 0;
    char* end = NULL;
    void* lib = NULL;
    struct state_machine* rsm = NULL;
    void* state = NULL;
    struct object_interface* obj_int = NULL;
    enum action_t action;

    if (argc != 2)
    {
        return EXIT_FAILURE;
    }

    socket_str = getenv("FD");

    if (!socket_str)
    {
        return EXIT_FAILURE;
    }

    errno = 0;
    socket_fd = strtol(socket_str, &end, 10);

    if (socket_fd < 0 || errno != 0 || *end != '\0')
    {
        return EXIT_FAILURE;
    }

    if (dup2(socket_fd, 0) < 0)
    {
        return EXIT_FAILURE;
    }

    for (li = 1; li < max_open_files; ++li)
    {
        close(li);
    }

    obj_int = object_interface_create(0);

    if (!obj_int)
    {
        return EXIT_FAILURE;
    }

    lib = dlopen(argv[1], RTLD_NOW|RTLD_LOCAL);

    if (!lib)
    {
        object_permanent_error(obj_int, "could not load library: %s", dlerror());
        return EXIT_FAILURE;
    }

    rsm = (struct state_machine*)dlsym(lib, "rsm");

    if (!rsm)
    {
        object_permanent_error(obj_int, "could not find \"rsm\" symbol in library");
        return EXIT_FAILURE;
    }

    while (object_next_action(obj_int, &action) == 0)
    {
        switch (action)
        {
            case ACTION_CTOR:
                action_ctor(rsm, &state, obj_int);
                break;
            case ACTION_RTOR:
                action_rtor(rsm, &state, obj_int);
                break;
            case ACTION_COMMAND:
                action_command(rsm, state, obj_int);
                break;
            case ACTION_SNAPSHOT:
                action_snapshot(rsm, state, obj_int);
                break;
            case ACTION_NOP:
                action_nop(rsm, state, obj_int);
                break;
            case ACTION_SHUTDOWN:
                break;
            default:
                object_permanent_error(obj_int, "bad action %d", action);
                abort();
        }
    }

    object_interface_destroy(obj_int);
    return EXIT_SUCCESS;
}

void
action_ctor(struct state_machine* rsm,
            void** state,
            struct object_interface* obj_int)
{
    struct rsm_context ctx;
    rsm_context_init(&ctx, obj_int);
    *state = rsm->ctor(&ctx);

    if (ctx.status != 0)
    {
        object_permanent_error(obj_int, "ctor failed");
    }

    object_command_output(ctx.obj_int, REPLICANT_SUCCESS, ctx.output, ctx.output_sz);
    rsm_context_finish(&ctx);
}

void
action_rtor(struct state_machine* rsm,
            void** state,
            struct object_interface* obj_int)
{
    struct rsm_context ctx;
    const char* data = NULL;
    size_t data_sz = 0;

    object_read_snapshot(obj_int, &data, &data_sz);

    rsm_context_init(&ctx, obj_int);
    *state = rsm->rtor(&ctx, data, data_sz);

    if (ctx.status != 0)
    {
        object_permanent_error(obj_int, "rtor failed");
    }

    object_command_output(ctx.obj_int, REPLICANT_SUCCESS, ctx.output, ctx.output_sz);
    rsm_context_finish(&ctx);
}

void
action_command(struct state_machine* rsm,
               void* state,
               struct object_interface* obj_int)
{
    struct command cmd;
    struct rsm_context ctx;
    struct state_machine_transition* transitions = rsm->transitions;
    struct state_machine_transition* transition = NULL;

    object_read_command(obj_int, &cmd);

    while (transitions->name)
    {
        if (strcmp(cmd.func, transitions->name) == 0)
        {
            transition = transitions;
            break;
        }

        ++transitions;
    }

    if (transition)
    {
        rsm_context_init(&ctx, obj_int);
        transition->func(&ctx, state, cmd.input, cmd.input_sz);

        if (ctx.status != 0)
        {
            object_permanent_error(obj_int, "execution failed");
        }

        object_command_output(ctx.obj_int, REPLICANT_SUCCESS, ctx.output, ctx.output_sz);
        rsm_context_finish(&ctx);
    }
    else
    {
        object_command_output(obj_int, REPLICANT_FUNC_NOT_FOUND, NULL, 0);
    }
}

void
action_snapshot(struct state_machine* rsm,
                void* state,
                struct object_interface* obj_int)
{
    char* data = NULL;
    size_t data_sz = 0;
    struct rsm_context ctx;
    rsm_context_init(&ctx, obj_int);

    if (rsm->snap(&ctx, state, &data, &data_sz) < 0)
    {
        object_permanent_error(obj_int, "snapshot failed");
    }

    object_snapshot(obj_int, data, data_sz);

    if (data)
    {
        free(data);
    }
}

static void
action_nop(struct state_machine* rsm,
           void* state,
           struct object_interface* obj_int)
{
    object_nop_response(obj_int);
    (void) rsm;
    (void) state;
}
