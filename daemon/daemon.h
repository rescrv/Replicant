// Copyright (c) 2012-2015, Robert Escriva
// Copyright (c) 2017, Robert Escriva, Cornell University
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
#include <list>
#include <memory>
#include <set>
#include <utility>
#include <vector>

// Google SparseHash
#include <google/dense_hash_map>

// po6
#include <po6/net/hostname.h>
#include <po6/net/ipaddr.h>
#include <po6/path.h>
#include <po6/threads/thread.h>

// BusyBee
#include <busybee.h>

// Replicant
#include "namespace.h"
#include "common/bootstrap.h"
#include "common/configuration.h"
#include "daemon/acceptor.h"
#include "daemon/ballot.h"
#include "daemon/controller.h"
#include "daemon/deferred_msg.h"
#include "daemon/failure_tracker.h"
#include "daemon/pvalue.h"
#include "daemon/replica.h"
#include "daemon/settings.h"
#include "daemon/slot_type.h"
#include "daemon/unordered_command.h"

BEGIN_REPLICANT_NAMESPACE
class scout;
class leader;

class daemon
{
    public:
        daemon();
        ~daemon() throw ();

    public:
        int run(bool daemonize,
                std::string data,
                std::string log,
                std::string pidfile,
                bool has_pidfile,
                bool set_bind_to,
                po6::net::location bind_to,
                bool set_existing,
                const bootstrap& bs,
                const char* init_obj,
                const char* init_lib,
                const char* init_str,
                const char* init_rst);
        const server_id id() const { return m_us.id; }

    // getting to steady state
    public:
        void become_cluster_member(bootstrap bs);
        void setup_replica_from_bootstrap(bootstrap bs,
                                          std::auto_ptr<replica>* rep);
        void send_bootstrap(server_id si);
        void process_bootstrap(server_id si,
                               std::auto_ptr<e::buffer> msg,
                               e::unpacker up);
        void process_state_transfer(server_id si,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up);
        void process_who_are_you(server_id si,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up);

    // core Paxos protocol in steady state
    public:
        void send_paxos_phase1a(server_id to, const ballot& b);
        void process_paxos_phase1a(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void send_paxos_phase1b(server_id to);
        void process_paxos_phase1b(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void send_paxos_phase2a(server_id to, const pvalue& pval);
        void process_paxos_phase2a(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void send_paxos_phase2b(server_id to, const pvalue& pval);
        void process_paxos_phase2b(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void send_paxos_learn(server_id to, const pvalue& pval);
        void process_paxos_learn(server_id si,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up);
        void send_paxos_submit(uint64_t slot_start, uint64_t slot_limit, const e::slice& command);
        void process_paxos_submit(server_id si,
                                  std::auto_ptr<e::buffer> msg,
                                  e::unpacker up);
        void enqueue_paxos_command(slot_type t,
                                   const std::string& command);
        void enqueue_paxos_command(server_id on_behalf_of,
                                   uint64_t request_nonce,
                                   slot_type t,
                                   const std::string& command);
        void enqueue_robust_paxos_command(server_id on_behalf_of,
                                          uint64_t request_nonce,
                                          uint64_t command_nonce,
                                          uint64_t min_slot,
                                          slot_type t,
                                          const std::string& command);
        void flush_enqueued_commands_with_stale_leader();
        void periodic_flush_enqueued_commands(uint64_t now);
        void convert_unassigned_to_unordered();
        void send_unordered_command(unordered_command* uc);
        void periodic_maintain(uint64_t now);
        void periodic_maintain_scout();
        void periodic_maintain_leader();
        void periodic_start_scout();
        void periodic_warn_scout_stuck(uint64_t now);
        bool post_config_change_hook(); // true if good; false if need to exit

    // Manage cluster membership
    public:
        std::string construct_become_member_command(const server& s);
        void process_server_become_member(server_id si,
                                          std::auto_ptr<e::buffer> msg,
                                          e::unpacker up);
        void periodic_check_address(uint64_t now);

    // Nonce-oriented stuff
    public:
        void process_unique_number(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void periodic_generate_nonce_sequence(uint64_t now);
        void callback_nonce_sequence(server_id si, uint64_t token, uint64_t counter);
        bool generate_nonce(uint64_t* nonce);
        void process_when_nonces_available(server_id si,
                                           std::auto_ptr<e::buffer> msg);

    // Dead objects?
    public:
        void process_object_failed(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void periodic_maintain_objects(uint64_t now);

    // Callbacks from the replica
    public:
        void callback_condition(server_id si,
                                uint64_t nonce,
                                uint64_t state,
                                const std::string& data);
        void callback_enqueued(uint64_t command_nonce,
                               server_id* si,
                               uint64_t* request_nonce);
        void callback_client(server_id si, uint64_t nonce,
                             replicant_returncode status,
                             const std::string& result);

    // Client-library calls
    public:
        void process_poke(server_id si,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void process_cond_wait(server_id si,
                               std::auto_ptr<e::buffer> msg,
                               e::unpacker up);
        void process_call(server_id si,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void process_get_robust_params(server_id si,
                                       std::auto_ptr<e::buffer> msg,
                                       e::unpacker up);
        void process_call_robust(server_id si,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up);
        void periodic_tick(uint64_t now);

    // Pinging to overthrow the leaders
    public:
        void send_ping(server_id si);
        void process_ping(server_id si,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void send_pong(server_id si);
        void process_pong(server_id si,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void periodic_ping_servers(uint64_t now);

    public:
        void rebootstrap(bootstrap b);

    public:
        bool send(server_id si, std::auto_ptr<e::buffer> msg);
        bool send_from_non_main_thread(server_id si, std::auto_ptr<e::buffer> msg);
        bool send_when_acceptor_persistent(server_id si, std::auto_ptr<e::buffer> msg);
        void flush_acceptor_messages();

    public:
        void debug_dump();

    private:
        typedef void (daemon::*periodic_fptr)(uint64_t now);
        struct periodic;
        daemon(const daemon&);
        daemon& operator = (const daemon&);
        void register_periodic(unsigned interval_ms, periodic_fptr fp);
        void run_periodic();

    private:
        e::garbage_collector m_gc;
        e::garbage_collector::thread_state m_gc_ts;
        server m_us;
        // This mutex must be held whenever non-const operations are performed
        // on the configuration (mainly assignment), or whenever the config is
        // accessed outside the main thread that loops from "run".
        po6::threads::mutex m_config_mtx;
        configuration m_config;
        // This controller uses the above lock to access the config (potentially
        // from other threads).
        controller m_busybee_controller;
        busybee_server* m_busybee;
        failure_tracker m_ft;
        std::vector<periodic> m_periodic;

        // Bootstrap when every node in the cluster has changed its address
        std::auto_ptr<po6::threads::thread> m_bootstrap_thread;
        uint32_t m_bootstrap_stop;

        // generate unique numbers, using a counter in the replica
        uint64_t m_unique_token;
        uint64_t m_unique_base;
        uint64_t m_unique_offset;

        // unordered commands; received from clients, and awaiting consensus
        typedef google::dense_hash_map<uint64_t, unordered_command*> unordered_map_t;
        typedef std::list<unordered_command*> unordered_list_t;
        po6::threads::mutex m_unordered_mtx;
        unordered_map_t m_unordered_cmds;
        unordered_list_t m_unassigned_cmds;

        // messages enqueued to wait for persistence
        std::list<deferred_msg> m_msgs_waiting_for_persistence;

        // messages waiting for nonces are sent to this server by a client.
        // They require a nonce to be available to be properly processed.  When
        // nonces become available, these messages will be re-delivered via
        // busybee.
        std::list<deferred_msg> m_msgs_waiting_for_nonces;

        // paxos state
        acceptor m_acceptor;
        std::auto_ptr<scout> m_scout;
        uint64_t m_scout_wait_cycles;
        std::auto_ptr<leader> m_leader;
        std::auto_ptr<replica> m_replica;
        uint64_t m_last_replica_snapshot; // XXX remove
        uint64_t m_last_gc_slot; // XXX remove
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_h_
