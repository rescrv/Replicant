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

#ifndef replicant_daemon_replica_h_
#define replicant_daemon_replica_h_

// POSIX
#include <poll.h>

// STL
#include <list>
#include <map>
#include <memory>
#include <queue>

// Google SparseHash
#include <google/dense_hash_set>

// e
#include <e/flagfd.h>
#include <e/intrusive_ptr.h>

// Replicant
#include "namespace.h"
#include "common/configuration.h"
#include "common/constants.h"
#include "daemon/condition.h"
#include "daemon/object.h"
#include "daemon/pvalue.h"
#include "daemon/snapshot.h"

BEGIN_REPLICANT_NAMESPACE
class daemon;

class replica
{
    public:
        replica(daemon* d, const configuration& config);
        ~replica() throw ();

    // only call from main daemon thread
    public:
        void learn(const pvalue& p);

    // only call from main daemon thread
    public:
        const configuration& config() const { return m_configs.front(); }
        const std::list<configuration>& configs() const { return m_configs; }
        bool any_config_has(server_id si) const;
        bool any_config_has(const po6::net::location& bind_to) const;
        bool discontinuous() const { return !m_pvalues.empty() && m_slot < m_pvalues.front().s; }
        void window(uint64_t* start, uint64_t* limit) const;
        bool fill_window() const { return m_configs.size() > 1; }
        uint64_t gc_up_to() const;
        void cond_wait(server_id si, uint64_t nonce,
                       const e::slice& obj,
                       const e::slice& cond,
                       uint64_t state);
        bool has_output(uint64_t nonce,
                        uint64_t min_slot,
                        replicant_returncode* status,
                        std::string* output);
        void clean_dead_objects();
        uint64_t last_tick() { return m_cond_tick.peek_state(); }
        uint64_t strike_number(server_id si) const;
        void set_defense_threshold(uint64_t tick);

    // snapshots
    public:
        void take_blocking_snapshot(uint64_t* snapshot_slot,
                                    e::slice* snapshot,
                                    std::auto_ptr<e::buffer>* snapshot_backing);
        void initiate_snapshot();
        void snapshot_barrier();
        uint64_t last_snapshot_num();
        void get_last_snapshot(uint64_t* snapshot_slot,
                               e::slice* snapshot,
                               std::auto_ptr<e::buffer>* snapshot_backing);
        void snapshot_finished();
        static replica* from_snapshot(daemon* d, const e::slice& snap);

    // recovering from object failures
    public:
        void enqueue_failed_objects();

    public:
        struct history;
        struct repair_info;
        struct defender;
        friend class object;
        typedef std::map<std::string, e::intrusive_ptr<object> > object_map_t;
        typedef std::list<e::intrusive_ptr<object> > object_list_t;
        typedef std::map<std::string, repair_info> failure_map_t;

    private:
        void execute(const pvalue& p);
        void execute_server_become_member(const pvalue& p, e::unpacker up);
        void execute_server_set_gc_thresh(e::unpacker up);
        void execute_server_change_address(const pvalue& p, e::unpacker up);
        void execute_server_record_strike(e::unpacker up);
        void execute_increment_counter(e::unpacker up);
        void execute_object_failed(const pvalue& p, e::unpacker up);
        void execute_kill_object(const pvalue& p,
                                 unsigned flags,
                                 uint64_t command_nonce,
                                 server_id si,
                                 uint64_t request_nonce,
                                 const e::slice& input);
        void execute_list_objects(const pvalue& p,
                                  unsigned flags,
                                  uint64_t command_nonce,
                                  server_id si,
                                  uint64_t request_nonce,
                                  const e::slice& input);
        void post_fail_action(object* obj, repair_info* ri);
        void execute_object_repair(e::unpacker up);
        void execute_poke(const e::slice& cmd);
        void execute_tick(const pvalue& p,
                          unsigned flags,
                          uint64_t command_nonce,
                          server_id si,
                          uint64_t request_nonce,
                          e::unpacker up);
        void execute_call(const pvalue& p,
                          unsigned flags,
                          uint64_t command_nonce,
                          server_id si,
                          uint64_t request_nonce,
                          e::unpacker up);
        void execute_new_object(const pvalue& p,
                                unsigned flags,
                                uint64_t command_nonce,
                                server_id si,
                                uint64_t request_nonce,
                                const e::slice& input);
        void execute_del_object(const pvalue& p,
                                unsigned flags,
                                uint64_t command_nonce,
                                server_id si,
                                uint64_t request_nonce,
                                const e::slice& input);
        void execute_backup_object(const pvalue& p,
                                   unsigned flags,
                                   uint64_t command_nonce,
                                   server_id si,
                                   uint64_t request_nonce,
                                   const e::slice& input);
        void execute_restore_object(const pvalue& p,
                                    unsigned flags,
                                    uint64_t command_nonce,
                                    server_id si,
                                    uint64_t request_nonce,
                                    const e::slice& input);
        void execute_kill_server(const pvalue& p,
                                 unsigned flags,
                                 uint64_t command_nonce,
                                 server_id si,
                                 uint64_t request_nonce,
                                 const e::slice& input);
        void execute_defended(const pvalue& p,
                              unsigned flags,
                              uint64_t command_nonce,
                              server_id si,
                              uint64_t request_nonce,
                              const e::slice& input);
        void execute_defend(const pvalue& p,
                            unsigned flags,
                            uint64_t command_nonce,
                            server_id si,
                            uint64_t request_nonce,
                            const e::slice& input);
        void execute_takedown(const pvalue& p,
                              unsigned flags,
                              uint64_t command_nonce,
                              server_id si,
                              uint64_t request_nonce,
                              const e::slice& input);
        void executed(const pvalue& p,
                      unsigned flags,
                      uint64_t command_nonce,
                      server_id si,
                      uint64_t request_nonce,
                      replicant_returncode status,
                      const std::string& result);
        bool launch(object* obj, const char* executable, const char* const * args);
        object* launch_library(const std::string& name, uint64_t slot, const std::string& lib);
        bool relaunch(const e::slice& name, uint64_t slot, const e::slice& state);

    private:
        daemon* m_daemon;
        uint64_t m_slot;
        std::list<pvalue> m_pvalues;
        std::list<configuration> m_configs;
        std::vector<uint64_t> m_slots;
        condition m_cond_config;
        condition m_cond_tick;
        condition m_cond_strikes[REPLICANT_MAX_REPLICAS];
        std::map<uint64_t, defender> m_defended;
        uint64_t m_counter;
        std::deque<uint64_t> m_command_nonces;
        google::dense_hash_set<uint64_t> m_command_nonces_lookup;
        object_map_t m_objects;
        object_list_t m_dying_objects;
        failure_map_t m_failed_objects;

        // manipulate snapshots
        po6::threads::mutex m_snapshots_mtx;
        std::list<e::intrusive_ptr<snapshot> > m_snapshots;

        // protect the latest snapshot
        po6::threads::mutex m_latest_snapshot_mtx;
        uint64_t m_latest_snapshot_slot;
        std::auto_ptr<e::buffer> m_latest_snapshot_backing;

        // protect the robust history
        po6::threads::mutex m_robust_mtx;
        std::list<history> m_robust_history;
        google::dense_hash_set<uint64_t> m_robust_history_lookup;

    private:
        replica(const replica&);
        replica& operator = (const replica&);
};

e::packer
operator << (e::packer lhs, const replica::history& rhs);
e::unpacker
operator >> (e::unpacker lhs, replica::history& rhs);
size_t
pack_size(const replica::history& rhs);

e::packer
operator << (e::packer lhs, const replica::defender& rhs);
e::unpacker
operator >> (e::unpacker lhs, replica::defender& rhs);
size_t
pack_size(const replica::defender& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_replica_h_
