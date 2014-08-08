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

#ifndef replicant_h_
#define replicant_h_

// STL
#include <map>
#include <memory>

// po6
#include <po6/net/hostname.h>

// e
#include <e/buffer.h>
#include <e/error.h>
#include <e/garbage_collector.h>
#include <e/intrusive_ptr.h>

// replicant_returncode occupies [4864, 5120)
enum replicant_returncode
{
    REPLICANT_SUCCESS   = 4864,
    /* send/wait-specific values */
    REPLICANT_NAME_TOO_LONG = 4880,
    /* loop-specific values */
    REPLICANT_NONE_PENDING  = 4896,
    /* loop/send/wait-specific values */
    REPLICANT_BACKOFF               = 4912,
    REPLICANT_INTERNAL_ERROR        = 4913,
    REPLICANT_INTERRUPTED           = 4914,
    REPLICANT_MISBEHAVING_SERVER    = 4915,
    REPLICANT_NEED_BOOTSTRAP        = 4916,
    REPLICANT_TIMEOUT               = 4917,
    REPLICANT_CLUSTER_JUMP          = 4918,
    /* command-specific values */
    REPLICANT_BAD_LIBRARY       = 4928,
    REPLICANT_COND_DESTROYED    = 4929,
    REPLICANT_COND_NOT_FOUND    = 4930,
    REPLICANT_FUNC_NOT_FOUND    = 4931,
    REPLICANT_OBJ_EXIST         = 4932,
    REPLICANT_OBJ_NOT_FOUND     = 4933,
    REPLICANT_SERVER_ERROR      = 4934,
    REPLICANT_CTOR_FAILED       = 4935,
    /* predictable uninitialized value */
    REPLICANT_GARBAGE   = 5119
};

void
replicant_destroy_output(const char* output, size_t output_sz);

namespace replicant
{
class chain_node;
class configuration;
class mapper;
} // namespace replicant

class replicant_client
{
    public:
        replicant_client(const char* connection_string);
        replicant_client(const char* host, in_port_t port);
        replicant_client(po6::net::hostname* bootstrap, size_t bootstrap_sz);
        ~replicant_client() throw ();

    public:
        e::error last_error() { return m_last_error; }
        int64_t new_object(const char* object,
                           const char* path,
                           replicant_returncode* status,
                           const char** errmsg, size_t* errmsg_sz);
        int64_t del_object(const char* object,
                           replicant_returncode* status,
                           const char** errmsg, size_t* errmsg_sz);
        int64_t backup_object(const char* object,
                              replicant_returncode* status,
                              const char** output, size_t* output_sz);
        int64_t restore_object(const char* object,
                               const char* path,
                               const char* backup,
                               replicant_returncode* status,
                               const char** errmsg, size_t* errmsg_sz);
        int64_t send(const char* object,
                     const char* func,
                     const char* data, size_t data_sz,
                     replicant_returncode* status,
                     const char** output, size_t* output_sz);
        int64_t wait(const char* object,
                     const char* cond,
                     uint64_t state,
                     replicant_returncode* status);
        replicant_returncode disconnect();
        int64_t loop(int timeout, replicant_returncode* status);
        int64_t loop(int64_t id, int timeout, replicant_returncode* status);
        void kill(int64_t id);
        // string allocated with malloc; free with free
        // returns 0 on success; -1 on error; not async
        int64_t list_servers(replicant_returncode* status,
                             std::string* servers);
        // string allocated with malloc; free with free
        // returns 0 on success; -1 on error; not async
        int64_t connect_str(replicant_returncode* status,
                            std::string* servers);
#ifdef _MSC_VER
        fd_set* poll_fd();
#else
        int poll_fd();
#endif

    private:
        class command;
        typedef std::map<uint64_t, e::intrusive_ptr<command> > command_map;

    private:
        replicant_client(const replicant_client& other);

    private:
        int64_t inner_loop(replicant_returncode* status);
        // Work the state machine to keep connected to the replicated service
        int64_t maintain_connection(replicant_returncode* status);
        int64_t perform_bootstrap(replicant_returncode* status);
        int64_t send_token_registration(replicant_returncode* status);
        int64_t wait_for_token_registration(replicant_returncode* status);
        int64_t handle_inform(const replicant::chain_node& node,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up,
                              replicant_returncode* status);
        int64_t handle_ping(const replicant::chain_node& node,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up,
                            replicant_returncode* status);
        // Send commands and receive responses
        int64_t send_to_specific_node(const replicant::chain_node* node,
                                      std::auto_ptr<e::buffer> msg,
                                      replicant_returncode* status);
        int64_t send_to_chain_head(std::auto_ptr<e::buffer> msg,
                                   replicant_returncode* status);
        int64_t send_to_preferred_chain_position(e::intrusive_ptr<command> cmd,
                                                 replicant_returncode* status);
        int64_t send_nops_to_preferred_quorum(replicant_returncode* status);
        void handle_disruption(const replicant::chain_node& node,
                               replicant_returncode* status);
        int64_t handle_command_response(const replicant::chain_node& node,
                                        std::auto_ptr<e::buffer> msg,
                                        e::unpacker up,
                                        replicant_returncode* status);
        // Cluster jumps
        int64_t report_cluster_jump(replicant_returncode* status);
        // Utilities
        uint64_t generate_token();
        void reset_to_disconnected();
        void killall(replicant_returncode status, e::error err);

    private:
        replicant_client& operator = (const replicant_client& rhs);

    private:
        std::auto_ptr<e::garbage_collector> m_gc;
        std::auto_ptr<replicant::mapper> m_busybee_mapper;
        std::auto_ptr<class busybee_st> m_busybee;
        std::auto_ptr<replicant::configuration> m_config;
        std::vector<po6::net::hostname> m_bootstrap;
        uint64_t m_token;
        uint64_t m_nonce;
        uint64_t m_cluster;
        enum { REPLCL_DISCONNECTED, REPLCL_BOOTSTRAPPED,
               REPLCL_REGISTER_SENT, REPLCL_REGISTERED } m_state;
        command_map m_commands;
        command_map m_complete;
        command_map m_resend;
        e::error m_last_error;
        bool m_cluster_jump;
        e::garbage_collector::thread_state m_gc_ts;
};

std::ostream&
operator << (std::ostream& lhs, replicant_returncode rhs);

#endif /* replicant_h_ */
