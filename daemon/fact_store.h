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

#ifndef replicant_daemon_fact_store_h_
#define replicant_daemon_fact_store_h_

// po6
#include <po6/pathname.h>

// STL
#include <map>

// Lightning MDB
#include <lmdb.h>

// Replicant
#include "common/configuration.h"
#include "common/response_returncode.h"
#include "daemon/configuration_manager.h"

namespace replicant
{

class fact_store
{
    public:
        fact_store();
        ~fact_store() throw ();

    // Setup/teardown
    public:
        bool open(const po6::pathname& path,
                  bool* saved,
                  chain_node* saved_us,
                  configuration_manager* saved_config_manager);
        bool repair(const po6::pathname& path);
        bool save(const chain_node& saved_us);

    // Manage configurations
    public:
        bool is_proposed_configuration(uint64_t proposal_id, uint64_t proposal_time);
        bool is_accepted_configuration(uint64_t proposal_id, uint64_t proposal_time);
        bool is_rejected_configuration(uint64_t proposal_id, uint64_t proposal_time);
        void propose_configuration(uint64_t proposal_id, uint64_t proposal_time,
                                   const configuration* configs, size_t configs_sz);
        void accept_configuration(uint64_t proposal_id, uint64_t proposal_time);
        void reject_configuration(uint64_t proposal_id, uint64_t proposal_time);
        void inform_configuration(const configuration& config);

    // Manage client information
    public:
        bool is_client(uint64_t client);
        bool is_live_client(uint64_t client);
        void get_all_clients(std::vector<uint64_t>* clients);
        void reg_client(uint64_t client);
        void die_client(uint64_t client);

    // Deal with slot assignments
    public:
        bool get_slot(uint64_t slot,
                      uint64_t* object,
                      uint64_t* client,
                      uint64_t* nonce,
                      e::slice* data,
                      std::string* backing);
        bool get_slot(uint64_t client, uint64_t nonce, uint64_t* slot);
        bool get_exec(uint64_t slot, replicant::response_returncode* rc, e::slice* data, std::string* backing);
        bool is_acknowledged_slot(uint64_t slot);
        bool is_issued_slot(uint64_t slot);
        uint64_t next_slot_to_issue();
        uint64_t next_slot_to_ack();
        void issue_slot(uint64_t slot,
                        uint64_t object,
                        uint64_t client,
                        uint64_t nonce,
                        const e::slice& data); /// also sets (client, nonce)->slot mapping
        void ack_slot(uint64_t slot);
        void exec_slot(uint64_t slot,
                       replicant::response_returncode rc,
                       const e::slice& data);
        void clear_unacked_slots();

    private:
        bool check_key_exists(MDB_val *key);
        bool retrieve_value(MDB_val *key, MDB_val *value);
        void store_key_value(MDB_val *key, MDB_val *value);
        void delete_key(MDB_val *key);
        bool only_key_is_replicant_key();

    private:
        bool fsck(bool verbose,
                  bool destructive,
                  configuration_manager* config_manager);
        bool fsck_meta_state(bool verbose,
                             bool destructive,
                             configuration_manager* config_manager);
        bool scan_accepted_proposals(bool verbose,
                                     bool destructive,
                                     std::vector<std::pair<uint64_t, uint64_t> >* accepted_proposals);
        bool scan_rejected_proposals(bool verbose,
                                     bool destructive,
                                     std::vector<std::pair<uint64_t, uint64_t> >* rejected_proposals);
        bool scan_informed_configurations(bool verbose,
                                          bool destructive,
                                          std::map<uint64_t, configuration>* configurations);
        bool scan_proposals(bool verbose,
                            bool destructive,
                            const std::vector<std::pair<uint64_t, uint64_t> >& accepted_proposals,
                            const std::vector<std::pair<uint64_t, uint64_t> >& rejected_proposals,
                            std::vector<configuration_manager::proposal>* proposals,
                            std::map<uint64_t, configuration>* proposed,
                            std::map<uint64_t, configuration>* accepted);
        bool fsck_clients(bool verbose,
                          bool destructive);
        bool fsck_slots(bool verbose,
                        bool destructive);
        bool do_open(const po6::pathname& path,
                  bool* saved,
                  chain_node* saved_us,
                  configuration_manager* saved_config_manager,
				  bool isOpen);

    private:
        fact_store(const fact_store&);
        fact_store& operator = (const fact_store&);

    private:
        MDB_env *m_db;
		MDB_txn *m_rtxn;
		MDB_cursor *m_rcsr;
        MDB_dbi m_dbi;
        uint64_t m_cache_next_slot_issue;
        uint64_t m_cache_next_slot_ack;
};

} // namespace replicant

#endif // replicant_daemon_fact_store_h_
