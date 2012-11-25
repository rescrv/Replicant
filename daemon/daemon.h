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

#ifndef replicant_daemon_h_
#define replicant_daemon_h_

// STL
#include <queue>
#include <functional>
#include <set>
#include <utility>
#include <vector>

// Google SparseHash
#include <sparsehash/sparse_hash_map>

// po6
#include <po6/net/hostname.h>
#include <po6/net/ipaddr.h>

// BusyBee
#include <busybee_mta.h>

// Replicant
#include "common/chain_node.h"
#include "common/configuration.h"
#include "common/mapper.h"
#include "daemon/configuration_manager.h"
#include "daemon/connection.h"
#include "daemon/fact_store.h"
#include "daemon/failure_manager.h"
#include "daemon/heal_next.h"
#include "daemon/object_manager.h"
#include "daemon/settings.h"

class replicant_daemon
{
    public:
        replicant_daemon();
        ~replicant_daemon() throw ();

    public:
        int run(bool daemonize,
                po6::pathname data,
                bool set_bind_to,
                po6::net::location bind_to,
                bool set_existing,
                po6::net::hostname existing);

    // Configure the chain membership via (re)configuration
    private:
        void process_bootstrap(const replicant::connection& conn,
                               std::auto_ptr<e::buffer> msg,
                               e::buffer::unpacker up);
        void process_inform(const replicant::connection& conn,
                            std::auto_ptr<e::buffer> msg,
                            e::buffer::unpacker up);
        void process_join(const replicant::connection& conn,
                          std::auto_ptr<e::buffer> msg,
                          e::buffer::unpacker up);
        void process_config_propose(const replicant::connection& conn,
                                    std::auto_ptr<e::buffer> msg,
                                    e::buffer::unpacker up);
        void process_config_accept(const replicant::connection& conn,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void process_config_reject(const replicant::connection& conn,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void propose_config(const configuration& config);
        void accept_config(const configuration& config);
        void accept_config_inform_spares(const configuration& old_config);
        void periodic_join_cluster(uint64_t now);
        void periodic_maintain_cluster(uint64_t now);
        void periodic_describe_cluster(uint64_t now);

    // Client-related functions
    private:
        void process_client_register(const replicant::connection& conn,
                                     std::auto_ptr<e::buffer> msg,
                                     e::buffer::unpacker up);
        void process_client_disconnect(const replicant::connection& conn,
                                       std::auto_ptr<e::buffer> msg,
                                       e::buffer::unpacker up);
        void accept_config_inform_clients(const configuration& old_config);

    // Normal-case chain-replication-related goodness.
    private:
        void process_command_submit(const replicant::connection& conn,
                                    std::auto_ptr<e::buffer> msg,
                                    e::buffer::unpacker up);
        void process_command_issue(const replicant::connection& conn,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void process_command_ack(const replicant::connection& conn,
                                 std::auto_ptr<e::buffer> msg,
                                 e::buffer::unpacker up);
        void issue_command(uint64_t slot, uint64_t object,
                           uint64_t client, uint64_t nonce,
                           const e::slice& data);
        void acknowledge_command(uint64_t slot);
        void record_execution(uint64_t slot, uint64_t client, uint64_t nonce, replicant::response_returncode rc, const e::slice& data);

    // Error-case chain functions
    private:
        void process_heal_req(const replicant::connection& conn,
                              std::auto_ptr<e::buffer> msg,
                              e::buffer::unpacker up);
        void process_heal_resp(const replicant::connection& conn,
                               std::auto_ptr<e::buffer> msg,
                               e::buffer::unpacker up);
        void process_heal_done(const replicant::connection& conn,
                               std::auto_ptr<e::buffer> msg,
                               e::buffer::unpacker up);
        void transfer_more_state();
        void accept_config_reset_healing(const configuration& old_config);
        void periodic_heal_next(uint64_t now);
        void handle_disruption_reset_healing(uint64_t token);

    // Check for faults
    public:
        void process_ping(const replicant::connection& conn,
                          std::auto_ptr<e::buffer> msg,
                          e::buffer::unpacker up);
        void process_pong(const replicant::connection& conn,
                          std::auto_ptr<e::buffer> msg,
                          e::buffer::unpacker up);
        void periodic_exchange(uint64_t now);

    // Manage communication
    private:
        bool recv(replicant::connection* conn, std::auto_ptr<e::buffer>* msg);
        bool send(const replicant::connection& conn, std::auto_ptr<e::buffer> msg);
        bool send(const chain_node& node, std::auto_ptr<e::buffer> msg);
        bool send(uint64_t token, std::auto_ptr<e::buffer> msg);

    // Handle communication disruptions
    private:
        void handle_disruption(uint64_t token);
        void periodic_handle_disruption(uint64_t now);
        void periodic_retry_disruption(uint64_t now);

    // Periodically run certain functions
    private:
        typedef void (replicant_daemon::*periodic_fptr)(uint64_t now);
        typedef std::pair<uint64_t, periodic_fptr> periodic;
        void trip_periodic(uint64_t when, periodic_fptr fp);
        void run_periodic();
        void periodic_nop(uint64_t now);

    // Utilities
    private:
        bool generate_token(uint64_t* token);

    private:
        settings m_s;
        replicant::mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        chain_node m_us;
        configuration_manager m_config_manager;
        replicant::failure_manager m_failure_manager;
        replicant::object_manager m_object_manager;
        std::vector<periodic> m_periodic;
        heal_next m_heal_next;
        std::queue<uint64_t> m_disrupted_unhandled;
        std::set<uint64_t> m_disrupted_backoff;
        std::vector<std::pair<uint64_t, uint64_t> > m_disrupted_times;
        replicant::fact_store m_fs;
};

#endif // replicant_daemon_h_
