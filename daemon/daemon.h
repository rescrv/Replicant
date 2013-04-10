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
#include <tr1/memory>
#include <utility>
#include <vector>

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
#include "daemon/failure_detector.h"
#include "daemon/heal_next.h"
#include "daemon/object_manager.h"
#include "daemon/settings.h"

namespace replicant
{

class daemon
{
    public:
        daemon();
        ~daemon() throw ();

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
                               e::unpacker up);
        void process_inform(const replicant::connection& conn,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up);
        void process_server_register(const replicant::connection& conn,
                                     std::auto_ptr<e::buffer> msg,
                                     e::unpacker up);
        void process_config_propose(const replicant::connection& conn,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up);
        void process_config_accept(const replicant::connection& conn,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void process_config_reject(const replicant::connection& conn,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        // send accept message for proposal
        void accept_proposal(const chain_node& dest,
                             uint64_t proposal_id,
                             uint64_t proposal_time);
        // send reject message for proposal
        void reject_proposal(const chain_node& dest,
                             uint64_t proposal_id,
                             uint64_t proposal_time);
        // create an INFORM message ready to pass to "send"
        std::auto_ptr<e::buffer> create_inform_message();
        // invoke all hooks, presumably because the configuration changed
        void post_reconfiguration_hooks();
        // propose a new configuration.  the caller must enforce all invariants
        // about this configuration w.r.t. previously issued ones and this call
        // will assert that
        void propose_config(const configuration& config);
        void periodic_describe_cluster(uint64_t now);
        void periodic_maintain_cluster(uint64_t now);

    // Client-related functions
    private:
        void process_client_register(const replicant::connection& conn,
                                     std::auto_ptr<e::buffer> msg,
                                     e::unpacker up);
        void process_client_disconnect(const replicant::connection& conn,
                                       std::auto_ptr<e::buffer> msg,
                                       e::unpacker up);

    // Normal-case chain-replication-related goodness.
    private:
        void process_command_submit(const replicant::connection& conn,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up);
        void process_command_issue(const replicant::connection& conn,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void process_command_ack(const replicant::connection& conn,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up);
        void issue_command(uint64_t slot, uint64_t object,
                           uint64_t client, uint64_t nonce,
                           const e::slice& data);
        void acknowledge_command(uint64_t slot);
        void record_execution(uint64_t slot, uint64_t client, uint64_t nonce, replicant::response_returncode rc, const e::slice& data);

    // Error-case chain functions
    private:
        void process_heal_req(const replicant::connection& conn,
                              std::auto_ptr<e::buffer> msg,
                              e::unpacker up);
        void process_heal_resp(const replicant::connection& conn,
                               std::auto_ptr<e::buffer> msg,
                               e::unpacker up);
        void process_heal_done(const replicant::connection& conn,
                               std::auto_ptr<e::buffer> msg,
                               e::unpacker up);
        void transfer_more_state();
        void periodic_heal_next(uint64_t now);

    // Notify/wait-style conditions
    private:
        void process_condition_wait(const replicant::connection& conn,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up);
        void send_notify(uint64_t client, uint64_t nonce, replicant::response_returncode rc, const e::slice& data);

    // Check for faults
    private:
        void process_ping(const replicant::connection& conn,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void process_pong(const replicant::connection& conn,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void periodic_exchange(uint64_t now);

    // Manage communication
    private:
        bool recv(replicant::connection* conn, std::auto_ptr<e::buffer>* msg);
        bool send(const replicant::connection& conn, std::auto_ptr<e::buffer> msg);
        bool send(const chain_node& node, std::auto_ptr<e::buffer> msg);
        bool send_no_disruption(uint64_t token, std::auto_ptr<e::buffer> msg);

    // Handle communication disruptions
    private:
        void handle_disruption(uint64_t token);
        void periodic_handle_disruption(uint64_t now);
        void handle_disruption_reset_healing(uint64_t token);
        void handle_disruption_reset_reconfiguration(uint64_t token);

    // Periodically run certain functions
    private:
        typedef void (daemon::*periodic_fptr)(uint64_t now);
        typedef std::pair<uint64_t, periodic_fptr> periodic;
        void trip_periodic(uint64_t when, periodic_fptr fp);
        void run_periodic();
        void periodic_nop(uint64_t now);

    // Utilities
    private:
        bool generate_token(uint64_t* token);

    private:
        typedef std::map<uint64_t, std::tr1::shared_ptr<failure_detector> > failure_detector_map_t;

    private:
        settings m_s;
        replicant::mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        chain_node m_us;
        configuration_manager m_config_manager;
        replicant::object_manager m_object_manager;
        failure_detector_map_t m_failure_detectors;
        std::vector<periodic> m_periodic;
        std::map<uint64_t, uint64_t> m_temporary_servers;
        heal_next m_heal_next;
        std::set<uint64_t> m_disrupted_backoff;
        bool m_disrupted_retry_scheduled;
        replicant::fact_store m_fs;
};

} // namespace replicant

#endif // replicant_daemon_h_
