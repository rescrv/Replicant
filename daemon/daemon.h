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
#include <set>
#include <utility>
#include <vector>

// po6
#include <po6/net/hostname.h>
#include <po6/net/ipaddr.h>

// BusyBee
#include <busybee_sta.h>

// Replicant
#include "common/chain_node.h"
#include "common/configuration.h"
#include "daemon/client_manager.h"
#include "daemon/command_manager.h"
#include "daemon/configuration_manager.h"
#include "daemon/heal_next.h"
#include "daemon/object_manager.h"
#include "daemon/settings.h"

class replicant_daemon
{
    public:
        replicant_daemon(po6::net::ipaddr bind_to,
                         in_port_t incoming,
                         in_port_t outgoing);
        ~replicant_daemon() throw ();

    // Public calls to run the daemon
    public:
        // Start a new cluster
        int run(bool daemonize);
        // Join an existing cluster
        int run(bool daemonize, po6::net::hostname existing);

    // Setup and run the daemon
    private:
        bool install_signal_handlers();
        bool install_signal_handler(int signum, void (*func)(int));
        bool generate_identifier_token();
        bool start_new_cluster();
        bool connect_to_cluster(po6::net::hostname hn);
        bool daemonize();
        bool loop();
        void process_message(const po6::net::location& from,
                             std::auto_ptr<e::buffer> msg);

    // Give others the cluster config and process new configs (out of band)
    private:
        void process_join(const po6::net::location& from,
                          std::auto_ptr<e::buffer> msg,
                          e::buffer::unpacker up);
        void process_inform(const po6::net::location& from,
                            std::auto_ptr<e::buffer> msg,
                            e::buffer::unpacker up);

    // Configure the chain membership via (re)configuration
    private:
        void process_become_spare(const po6::net::location& from,
                                  std::auto_ptr<e::buffer> msg,
                                  e::buffer::unpacker up);
        void process_become_standby(const po6::net::location& from,
                                    std::auto_ptr<e::buffer> msg,
                                    e::buffer::unpacker up);
        void process_become_member(const po6::net::location& from,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void process_config_propose(const po6::net::location& from,
                                    std::auto_ptr<e::buffer> msg,
                                    e::buffer::unpacker up);
        void process_config_accept(const po6::net::location& from,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void process_config_reject(const po6::net::location& from,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void propose_config(const configuration& config);
        void accept_config(const configuration& config);
        void accept_config_inform_spares(const configuration& old_config,
                                         const configuration& new_config);
        void accept_config_inform_clients(const configuration& old_config,
                                          const configuration& new_config);
        void send_config_accept(const configuration& config);
        void send_config_reject(const configuration& config);
        void periodic_become_spare(uint64_t now);
        void periodic_become_standby(uint64_t now);
        void periodic_become_chain_member(uint64_t now);

    // Normal-case chain-replication-related goodness.
    private:
        void process_command_submit(const po6::net::location& from,
                                    std::auto_ptr<e::buffer> msg,
                                    e::buffer::unpacker up);
        void process_command_issue(const po6::net::location& from,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker up);
        void process_command_ack(const po6::net::location& from,
                                 std::auto_ptr<e::buffer> msg,
                                 e::buffer::unpacker up);
        void issue_command(const po6::net::location& from,
                           uint64_t client, uint64_t nonce,
                           uint64_t slot, uint64_t object,
                           std::auto_ptr<e::buffer> msg);
        void acknowledge_command(const po6::net::location& from, uint64_t slot);
        void apply_command(e::intrusive_ptr<command> cmd);
        void send_command_ack(uint64_t slot, const po6::net::location& dst);

    // Error-case chain functions
    private:
        void process_req_state(const po6::net::location& from,
                               std::auto_ptr<e::buffer> msg,
                               e::buffer::unpacker up);
        void process_resp_state(const po6::net::location& from,
                                std::auto_ptr<e::buffer> msg,
                                e::buffer::unpacker up);
        void process_snapshot(const po6::net::location& from,
                              std::auto_ptr<e::buffer> msg,
                              e::buffer::unpacker up);
        void process_healed(const po6::net::location& from,
                            std::auto_ptr<e::buffer> msg,
                            e::buffer::unpacker up);
        void transfer_more_state();
        void accept_config_reset_healing(const configuration& old_config,
                                         const configuration& new_config);
        void handle_disconnect_heal(const po6::net::location& host);
        void periodic_heal_next(uint64_t now);

    // Client-related functions
    private:
        void process_identify(const po6::net::location& from,
                              std::auto_ptr<e::buffer> msg,
                              e::buffer::unpacker up);
        void process_client_list(const po6::net::location& from,
                                 std::auto_ptr<e::buffer> msg,
                                 e::buffer::unpacker up);
        void handle_disconnect_client(const po6::net::location& host);
        void periodic_list_clients(uint64_t now);
        void periodic_detach_clients(uint64_t now);
        void apply_command_clients(e::intrusive_ptr<command> cmd);
        void apply_command_depart(e::intrusive_ptr<command> cmd);

    // Garbage Collection
    private:
        void periodic_garbage_collect_commands_start(uint64_t now);
        void periodic_garbage_collect_commands_collect(uint64_t now);
        void apply_command_garbage(e::intrusive_ptr<command> cmd);
        void apply_command_snapshot(e::intrusive_ptr<command> cmd);

    // Handle objects
    private:
        void apply_command_create(e::intrusive_ptr<command> cmd);

    // Handle TCP disconnects
    private:
        void handle_disconnect(const po6::net::location& host);
        void periodic_handle_disconnect(uint64_t now);
        void handle_disconnect_propose_new_config(const po6::net::location& host);

    // Random utility functions
    private:
        bool send(const po6::net::location& to,
                  std::auto_ptr<e::buffer> msg);

    // Periodically run certain functions
    private:
        typedef void (replicant_daemon::*periodic_fptr)(uint64_t now);
        typedef std::pair<uint64_t, periodic_fptr> periodic;
        void trip_periodic(uint64_t when, periodic_fptr fp);
        void run_periodic();
        void periodic_nop(uint64_t now);
        void periodic_set_timeout(uint64_t now);
        void periodic_dump_config(uint64_t now);

    //XXX: expose send response to object_manager threads
    public:
        void send_command_response(e::intrusive_ptr<command> cmd);

    private:
        settings m_s;
        busybee_sta m_busybee;
        chain_node m_us;
        configuration_manager m_confman;
        command_manager m_commands;
        client_manager m_clients;
        object_manager m_objects;
        std::vector<periodic> m_periodic;
        heal_next m_heal_next;
        bool m_heal_prev;
        uint64_t m_disconnected_iteration;
        std::queue<std::pair<uint64_t, po6::net::location> > m_disconnected;
        std::map<po6::net::location, uint64_t> m_disconnected_times;
        uint64_t m_marked_for_gc;
        typedef std::list<std::pair<uint64_t, std::tr1::shared_ptr<e::buffer> > > snapshot_list;
        snapshot_list m_snapshots;
};

#endif // replicant_daemon_h_
