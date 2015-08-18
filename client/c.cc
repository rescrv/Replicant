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

// POSIX
#include <errno.h>
#include <signal.h>

// e
#include <e/guard.h>

// Replicant
#include <replicant.h>
#include "visibility.h"
#include "common/macros.h"
#include "client/client.h"
#include "client/server_status.h"

#define FAKE_STATUS replicant_returncode _status; replicant_returncode* status = &_status

#define SIGNAL_PROTECT_ERR(X) \
    sigset_t old_sigs; \
    sigset_t all_sigs; \
    sigfillset(&all_sigs); \
    if (pthread_sigmask(SIG_BLOCK, &all_sigs, &old_sigs) < 0) \
    { \
        *status = REPLICANT_INTERNAL; \
        return (X); \
    } \
    e::guard g = e::makeguard(pthread_sigmask, SIG_SETMASK, (sigset_t*)&old_sigs, (sigset_t*)NULL)

#define SIGNAL_PROTECT SIGNAL_PROTECT_ERR(-1);
inline void return_void() {}
#define SIGNAL_PROTECT_VOID SIGNAL_PROTECT_ERR(return_void());

#define C_WRAP_EXCEPT(X) \
    replicant::client* cl = reinterpret_cast<replicant::client*>(_cl); \
    SIGNAL_PROTECT; \
    try \
    { \
        X \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = REPLICANT_SEE_ERRNO; \
        cl->set_error_message("out of memory"); \
        return -1; \
    } \
    catch (...) \
    { \
        *status = REPLICANT_EXCEPTION; \
        cl->set_error_message("unhandled exception was thrown"); \
        return -1; \
    }

#define C_WRAP_EXCEPT_VOID(X) \
    replicant::client* cl = reinterpret_cast<replicant::client*>(_cl); \
    SIGNAL_PROTECT_VOID; \
    try \
    { \
        X \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = REPLICANT_SEE_ERRNO; \
        cl->set_error_message("out of memory"); \
    } \
    catch (...) \
    { \
        *status = REPLICANT_EXCEPTION; \
        cl->set_error_message("unhandled exception was thrown"); \
    }

extern "C"
{

REPLICANT_API replicant_client*
replicant_client_create(const char* coordinator, uint16_t port)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);

    try
    {
        return reinterpret_cast<replicant_client*>(new replicant::client(coordinator, port));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

REPLICANT_API replicant_client*
replicant_client_create_conn_str(const char* conn_str)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);

    try
    {
        return reinterpret_cast<replicant_client*>(new replicant::client(conn_str));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

REPLICANT_API void
replicant_client_destroy(replicant_client* client)
{
    delete reinterpret_cast<replicant::client*>(client);
}

REPLICANT_API int64_t
replicant_client_poke(struct replicant_client* _cl,
                      replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->poke(status);
    );
}

REPLICANT_API int64_t
replicant_client_generate_unique_number(struct replicant_client* _cl,
                                        replicant_returncode* status,
                                        uint64_t* number)
{
    C_WRAP_EXCEPT(
    return cl->generate_unique_number(status, number);
    );
}

REPLICANT_API int64_t
replicant_client_new_object(struct replicant_client* _cl,
                            const char* object,
                            const char* path,
                            replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->new_object(object, path, status);
    );
}

REPLICANT_API int64_t
replicant_client_del_object(struct replicant_client* _cl,
                            const char* object,
                            replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->del_object(object, status);
    );
}

REPLICANT_API int64_t
replicant_client_kill_object(struct replicant_client* _cl,
                             const char* object,
                             replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->kill_object(object, status);
    );
}

REPLICANT_API int64_t
replicant_client_backup_object(struct replicant_client* _cl,
                               const char* object,
                               enum replicant_returncode* status,
                               char** state, size_t* state_sz)
{
    C_WRAP_EXCEPT(
    return cl->backup_object(object, status, state, state_sz);
    );
}

REPLICANT_API int64_t
replicant_client_restore_object(struct replicant_client* _cl,
                                const char* object,
                                const char* backup, size_t backup_sz,
                                enum replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->restore_object(object, backup, backup_sz, status);
    );
}

REPLICANT_API int64_t
replicant_client_list_objects(struct replicant_client* _cl,
                              enum replicant_returncode* status,
                              char** objects)
{
    C_WRAP_EXCEPT(
    return cl->list_objects(status, objects);
    );
}

REPLICANT_API int64_t
replicant_client_call(struct replicant_client* _cl,
                      const char* object,
                      const char* func,
                      const char* input, size_t input_sz,
                      unsigned flags,
                      replicant_returncode* status,
                      char** output, size_t* output_sz)
{
    C_WRAP_EXCEPT(
    return cl->call(object, func, input, input_sz, flags, status, output, output_sz);
    );
}

REPLICANT_API int64_t
replicant_client_cond_wait(struct replicant_client* _cl,
                           const char* object,
                           const char* cond,
                           uint64_t state,
                           replicant_returncode* status,
                           char** data, size_t* data_sz)
{
    C_WRAP_EXCEPT(
    return cl->cond_wait(object, cond, state, status, data, data_sz);
    );
}

REPLICANT_API int64_t
replicant_client_cond_follow(struct replicant_client* _cl,
                             const char* object,
                             const char* cond,
                             enum replicant_returncode* status,
                             uint64_t* state,
                             char** data, size_t* data_sz)
{
    C_WRAP_EXCEPT(
    return cl->cond_follow(object, cond, status, state, data, data_sz);
    );
}

REPLICANT_API int64_t
replicant_client_defended_call(struct replicant_client* _cl,
                               const char* object,
                               const char* enter_func,
                               const char* enter_input, size_t enter_input_sz,
                               const char* exit_func,
                               const char* exit_input, size_t exit_input_sz,
                               enum replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->defended_call(object, enter_func, enter_input, enter_input_sz,
                             exit_func, exit_input, exit_input_sz, status);
    );
}

REPLICANT_API int
replicant_client_conn_str(struct replicant_client* _cl,
                          enum replicant_returncode* status,
                          char** servers)
{
    C_WRAP_EXCEPT(
    return cl->conn_str(status, servers);
    );
}

REPLICANT_API int64_t
replicant_client_kill_server(struct replicant_client* _cl,
                             uint64_t token,
                             enum replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->kill_server(token, status);
    );
}

REPLICANT_API int64_t
replicant_client_loop(struct replicant_client* _cl, int timeout,
                      enum replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    int64_t ret = cl->loop(timeout, status);
    cl->adjust_flagfd();
    return ret;
    );
}

REPLICANT_API int64_t
replicant_client_wait(struct replicant_client* _cl,
                      int64_t specific_op, int timeout,
                      enum replicant_returncode* status)
{
    C_WRAP_EXCEPT(
    int64_t ret = cl->wait(specific_op, timeout, status);
    cl->adjust_flagfd();
    return ret;
    );
}

REPLICANT_API int
replicant_client_kill(struct replicant_client* _cl, int64_t id)
{
    FAKE_STATUS;
    C_WRAP_EXCEPT(
    cl->kill(id);
    return 0;
    );
}

REPLICANT_API int
replicant_client_poll_fd(replicant_client* _cl)
{
    FAKE_STATUS;
    C_WRAP_EXCEPT(
    return cl->poll_fd();
    );
}

REPLICANT_API int
replicant_client_block(replicant_client* _cl, int timeout)
{
    FAKE_STATUS;
    C_WRAP_EXCEPT(
    return cl->block(timeout);
    );
}

REPLICANT_API const char*
replicant_client_error_message(replicant_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    replicant::client* cl = reinterpret_cast<replicant::client*>(_cl);
    return cl->error_message();
}

REPLICANT_API const char*
replicant_client_error_location(replicant_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    replicant::client* cl = reinterpret_cast<replicant::client*>(_cl);
    return cl->error_location();
}

REPLICANT_API const char*
replicant_returncode_to_string(enum replicant_returncode stat)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    switch (stat)
    {
        CSTRINGIFY(REPLICANT_SUCCESS);
        CSTRINGIFY(REPLICANT_MAYBE);
        CSTRINGIFY(REPLICANT_SEE_ERRNO);
        CSTRINGIFY(REPLICANT_CLUSTER_JUMP);
        CSTRINGIFY(REPLICANT_COMM_FAILED);
        CSTRINGIFY(REPLICANT_OBJ_NOT_FOUND);
        CSTRINGIFY(REPLICANT_OBJ_EXIST);
        CSTRINGIFY(REPLICANT_FUNC_NOT_FOUND);
        CSTRINGIFY(REPLICANT_COND_NOT_FOUND);
        CSTRINGIFY(REPLICANT_COND_DESTROYED);
        CSTRINGIFY(REPLICANT_SERVER_ERROR);
        CSTRINGIFY(REPLICANT_TIMEOUT);
        CSTRINGIFY(REPLICANT_INTERRUPTED);
        CSTRINGIFY(REPLICANT_NONE_PENDING);
        CSTRINGIFY(REPLICANT_INTERNAL);
        CSTRINGIFY(REPLICANT_EXCEPTION);
        CSTRINGIFY(REPLICANT_GARBAGE);
        default:
            return "unknown replicant_returncode";
    }
}

REPLICANT_API int
replicant_server_status(const char* host, uint16_t port, int timeout,
                        enum replicant_returncode* status,
                        char** human_readable)
{
    SIGNAL_PROTECT_ERR(-1);

    try
    {
        return replicant::server_status(host, port, timeout, status, human_readable);
    }
    catch (std::bad_alloc& ba)
    {
        *status = REPLICANT_SEE_ERRNO;
        errno = ENOMEM;
        return -1;
    }
    catch (...)
    {
        *status = REPLICANT_INTERNAL;
        return -1;
    }
}

} // extern "C"
