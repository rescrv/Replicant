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

#ifndef replicant_h_
#define replicant_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct replicant_client;

/* replicant_returncode occupies [5120, 5376) */
enum replicant_returncode
{
    REPLICANT_SUCCESS        = 5120,
    /* Maybe the operation happened, maybe it didn't; we cannot say */
    REPLICANT_MAYBE          = 5121,
    /* System errors; consult errno */
    REPLICANT_SEE_ERRNO      = 5122,
    /* The operation failed because the client connected to a different cluster */
    REPLICANT_CLUSTER_JUMP   = 5123,
    /* Total communication breakdown.  Usually:
     *
     * - The client could not connect to any server in the bootstrap string
     * - The client could not connect to any server in the cluster
     *
     * this should not happen, so long as at least one server in the majority is
     * reachable by the client
     */
    REPLICANT_COMM_FAILED    = 5124,
    /* Errors performing operations on the object */
    REPLICANT_OBJ_NOT_FOUND  = 5184,
    REPLICANT_OBJ_EXIST      = 5185,
    REPLICANT_FUNC_NOT_FOUND = 5186,
    REPLICANT_COND_NOT_FOUND = 5187,
    REPLICANT_COND_DESTROYED = 5188,
    /* A server behaving abnormally */
    REPLICANT_SERVER_ERROR   = 5248,
    /* Loop-specific errors */
    REPLICANT_TIMEOUT        = 5312,
    REPLICANT_INTERRUPTED    = 5313,
    REPLICANT_NONE_PENDING   = 5314,
    /* This should never happen.  It indicates a bug */
    REPLICANT_INTERNAL       = 5373,
    REPLICANT_EXCEPTION      = 5374,
    REPLICANT_GARBAGE        = 5375
};

char* replicant_client_validate_conn_str(const char* conn_str);
char* replicant_client_host_to_conn_str(const char* host, uint16_t port);
char* replicant_client_add_to_conn_str(const char* conn_str, const char* host, uint16_t port);

struct replicant_client*
replicant_client_create(const char* host, uint16_t port);
struct replicant_client*
replicant_client_create_conn_str(const char* conn_str);
void
replicant_client_destroy(struct replicant_client* client);

int64_t
replicant_client_poke(struct replicant_client* client,
                      enum replicant_returncode* status);

int64_t
replicant_client_generate_unique_number(struct replicant_client* client,
                                        enum replicant_returncode* status,
                                        uint64_t* number);

int64_t
replicant_client_new_object(struct replicant_client* client,
                            const char* object,
                            const char* path,
                            enum replicant_returncode* status);

int64_t
replicant_client_del_object(struct replicant_client* client,
                            const char* object,
                            enum replicant_returncode* status);

int64_t
replicant_client_kill_object(struct replicant_client* client,
                             const char* object,
                             enum replicant_returncode* status);

int64_t
replicant_client_backup_object(struct replicant_client* client,
                               const char* object,
                               enum replicant_returncode* status,
                               char** state, size_t* state_sz);

int64_t
replicant_client_restore_object(struct replicant_client* client,
                                const char* object,
                                const char* backup, size_t backup_sz,
                                enum replicant_returncode* status);

int64_t
replicant_client_list_objects(struct replicant_client* client,
                              enum replicant_returncode* status,
                              char** objects);

#define REPLICANT_CALL_IDEMPOTENT 1
#define REPLICANT_CALL_ROBUST 2

int64_t
replicant_client_call(struct replicant_client* client,
                      const char* object,
                      const char* func,
                      const char* input, size_t input_sz,
                      unsigned flags,
                      enum replicant_returncode* status,
                      char** output, size_t* output_sz);

int64_t
replicant_client_cond_wait(struct replicant_client* client,
                           const char* object,
                           const char* cond,
                           uint64_t state,
                           enum replicant_returncode* status,
                           char** data, size_t* data_sz);

int64_t
replicant_client_cond_follow(struct replicant_client* client,
                             const char* object,
                             const char* cond,
                             enum replicant_returncode* status,
                             uint64_t* state,
                             char** data, size_t* data_sz);

int64_t
replicant_client_defended_call(struct replicant_client* client,
                               const char* object,
                               const char* enter_func,
                               const char* enter_input, size_t enter_input_sz,
                               const char* exit_func,
                               const char* exit_input, size_t exit_input_sz,
                               enum replicant_returncode* status);

int
replicant_client_conn_str(struct replicant_client* client,
                          enum replicant_returncode* status,
                          char** servers);

int64_t
replicant_client_kill_server(struct replicant_client* client,
                             uint64_t token,
                             enum replicant_returncode* status);

int64_t
replicant_client_loop(struct replicant_client* client, int timeout,
                      enum replicant_returncode* status);

int64_t
replicant_client_wait(struct replicant_client* client,
                      int64_t specific_op, int timeout,
                      enum replicant_returncode* status);

int
replicant_client_kill(struct replicant_client* client, int64_t id);

int
replicant_client_poll_fd(struct replicant_client* client);

int
replicant_client_block(struct replicant_client* client, int timeout);

const char*
replicant_client_error_message(struct replicant_client* client);

const char*
replicant_client_error_location(struct replicant_client* client);

const char*
replicant_returncode_to_string(enum replicant_returncode);

int
replicant_server_status(const char* host, uint16_t port, int timeout,
                        enum replicant_returncode* status,
                        char** human_readable);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* replicant_h_ */
