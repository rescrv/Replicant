// Copyright (c) 2012-2015, Robert Escriva
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
#include <po6/pathname.h>
#include <po6/threads/thread.h>

// BusyBee
#include <busybee_mta.h>

// Replicant
#include "namespace.h"
#include "common/bootstrap.h"
#include "common/configuration.h"
#include "daemon/acceptor.h"
#include "daemon/ballot.h"
#include "daemon/mapper.h"
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
                po6::pathname data,
                po6::pathname log,
                po6::pathname pidfile,
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

    public:
        void setup_replica_from_bootstrap(const bootstrap& bs,
                                          std::auto_ptr<replica>* rep);
        void become_cluster_member(const bootstrap& bs);
        void process_bootstrap(server_id si,
                               std::auto_ptr<e::buffer> msg,
                               e::unpacker up);
        void process_silent_bootstrap(server_id si,
                                      std::auto_ptr<e::buffer> msg,
                                      e::unpacker up);
        void send_bootstrap(server_id si);
        void process_state_transfer(server_id si,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up);
        void process_suggest_rejoin(server_id si,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up);
        void process_who_are_you(server_id si,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up);
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
        void observe_ballot(const ballot& b);
        void periodic_scout(uint64_t now);
        void periodic_abdicate(uint64_t now);
        void periodic_warn_scout_stuck(uint64_t now);
        bool post_config_change_hook(); // true if good; false if need to exit
        void bootstrap_thread();

    public:
        std::string construct_become_member_command(const server& s);
        void process_server_become_member(server_id si,
                                          std::auto_ptr<e::buffer> msg,
                                          e::unpacker up);
        void periodic_check_address(uint64_t now);

    public:
        void process_unique_number(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void periodic_generate_nonce_sequence(uint64_t now);
        void callback_nonce_sequence(server_id si, uint64_t token, uint64_t counter);
        bool generate_nonce(uint64_t* nonce);
        void process_when_nonces_available(server_id si,
                                           std::auto_ptr<e::buffer> msg);

    public:
        void process_object_failed(server_id si,
                                   std::auto_ptr<e::buffer> msg,
                                   e::unpacker up);
        void periodic_maintain_objects(uint64_t now);

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

    public:
        void send_ping(server_id si);
        void process_ping(server_id si,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void send_pong(server_id si);
        void process_pong(server_id si,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void periodic_ping_acceptors(uint64_t now);
        bool suspect_failed(server_id si);
        void periodic_submit_dead_nodes(uint64_t now);

    public:
        bool send(server_id si, std::auto_ptr<e::buffer> msg);
        bool send_from_non_main_thread(server_id si, std::auto_ptr<e::buffer> msg);
        bool send_when_acceptor_persistent(server_id si, std::auto_ptr<e::buffer> msg);
        void flush_acceptor_messages();
        void handle_disruption(server_id si);

    public:
        void debug_dump();

    private:
        typedef void (daemon::*periodic_fptr)(uint64_t now);
        struct periodic;
        void register_periodic(unsigned interval_ms, periodic_fptr fp);
        void run_periodic();

    private:
        e::garbage_collector m_gc;
        e::garbage_collector::thread_state m_gc_ts;
        mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        uint32_t m_busybee_init;
        server m_us;
        po6::threads::mutex m_config_mtx;
        configuration m_config;
        po6::threads::thread m_bootstrap_thread;
        bootstrap m_saved_bootstrap;
        bootstrap m_bootstrap;
        std::vector<periodic> m_periodic;

        // generate unique numbers, using a counter in the replica
        uint64_t m_unique_token;
        uint64_t m_unique_base;
        uint64_t m_unique_offset;

        // failure detection
        std::vector<uint64_t> m_last_seen;
        std::vector<uint64_t> m_suspect_counts;

        // unordered commands; received from clients, and awaiting consensus
        po6::threads::mutex m_unordered_mtx;
        typedef google::dense_hash_map<uint64_t, unordered_command*> unordered_map_t;
        unordered_map_t m_unordered__cmds;
        typedef std::list<unordered_command*> unordered_list_t;
        unordered_list_t m_unassigned_cmds;

        // messages enqueued to wait for persistence
        struct deferred_msg
        {
            deferred_msg(uint64_t w, server_id t, e::buffer* m)
                : when(w), si(t), msg(m) {}
            deferred_msg(const deferred_msg& other)
                : when(other.when), si(other.si), msg(other.msg) {}
            ~deferred_msg() throw () {}

            deferred_msg& operator = (const deferred_msg&);

            uint64_t when;
            server_id si;
            e::buffer* msg;
        };
        std::list<deferred_msg> m_deferred_msgs;

        // messages waiting to send when BusyBee comes online
        po6::threads::mutex m_busybee_queue_mtx;
        std::list<deferred_msg> m_busybee_queue;

        // messages waiting for nonces
        std::list<deferred_msg> m_msgs_waiting_for_nonces;

        // paxos state
        acceptor m_acceptor;
        ballot m_highest_ballot;
        bool m_first_scout;
        std::auto_ptr<scout> m_scout;
        uint64_t m_scouts_since_last_leader;
        uint64_t m_scout_wait_cycles;
        std::auto_ptr<leader> m_leader;
        std::auto_ptr<replica> m_replica;
        uint64_t m_last_replica_snapshot;
        uint64_t m_last_gc_slot;
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_h_
