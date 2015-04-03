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
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifndef replicant_daemon_client_h_
#define replicant_daemon_client_h_

// C
#include <stdint.h>

// STL
#include <list>
#include <map>
#include <memory>

// e
#include <e/error.h>
#include <e/flagfd.h>
#include <e/intrusive_ptr.h>

// Replicant
#include <replicant.h>
#include "namespace.h"
#include "common/bootstrap.h"
#include "common/ids.h"
#include "client/mapper.h"

BEGIN_REPLICANT_NAMESPACE
class pending;
class pending_call_robust;

class client
{
    public:
        client(const char* coordinator, uint16_t port);
        client(const char* conn_str);
        ~client() throw ();

    public:
        int64_t poke(replicant_returncode* status);
        int64_t generate_unique_number(replicant_returncode* status,
                                       uint64_t* number);
        int64_t new_object(const char* object,
                           const char* path,
                           replicant_returncode* status);
        int64_t del_object(const char* object,
                           replicant_returncode* status);
        int64_t kill_object(const char* object,
                            replicant_returncode* status);
        int64_t backup_object(const char* object,
                              replicant_returncode* status,
                              char** state, size_t* state_sz);
        int64_t restore_object(const char* object,
                               const char* backup, size_t backup_sz,
                               replicant_returncode* status);
        int64_t list_objects(replicant_returncode* status, char** objects);
        int64_t call(const char* object,
                     const char* func,
                     const char* input, size_t input_sz,
                     unsigned flags,
                     replicant_returncode* status,
                     char** output, size_t* output_sz);
        int64_t cond_wait(const char* object,
                          const char* cond,
                          uint64_t state,
                          replicant_returncode* status,
                          char** data, size_t* data_sz);
        int conn_str(replicant_returncode* status, char** servers);
        int64_t kill_server(uint64_t token, replicant_returncode* status);
        // looping/polling
        int64_t loop(int timeout, replicant_returncode* status);
        int64_t wait(int64_t id, int timeout, replicant_returncode* status);
        void kill(int64_t id);
        // Return the fildescriptor that replicant uses for networking
        int poll_fd();
        // Block unitl there is incoming data or the timeout is reached
        int block(int timeout);
        // error handling
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);

    public:
        void bump_config_cond_state(uint64_t x) { m_config_cond_state = std::max(m_config_cond_state, x); }

    private:
        int64_t inner_loop(replicant_returncode* status);
        bool maintain_connection(replicant_returncode* status);
        void handle_disruption(server_id si);
        bool handle_bootstrap(server_id si, e::unpacker up, replicant_returncode* status);
        int64_t send(pending* p);
        int64_t send_robust(pending_call_robust* p);
        bool send(server_id si, std::auto_ptr<e::buffer> msg, replicant_returncode* status);

    private:
        typedef std::map<std::pair<server_id, uint64_t>, e::intrusive_ptr<pending> > pending_map_t;
        typedef std::map<std::pair<server_id, uint64_t>, e::intrusive_ptr<pending_call_robust> > pending_robust_map_t;
        typedef std::list<e::intrusive_ptr<pending> > pending_list_t;
        typedef std::list<e::intrusive_ptr<pending_call_robust> > pending_robust_list_t;
        bootstrap m_bootstrap;
        mapper m_busybee_mapper;
        std::auto_ptr<class busybee_st> m_busybee;
        uint64_t m_random_token;
        uint64_t m_config_cond_state;
        configuration m_config;
        uint64_t m_bootstrap_count;
        int64_t m_next_client_id;
        uint64_t m_next_nonce;
        pending_map_t m_pending;
        pending_robust_map_t m_pending_robust;
        pending_list_t m_pending_retry;
        pending_robust_list_t m_pending_robust_retry;
        pending_list_t m_complete;
        e::error m_last_error;
        e::flagfd m_flagfd;

    private:
        client(const client&);
        client& operator = (const client&);
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_client_h_
