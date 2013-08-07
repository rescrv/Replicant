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

#ifndef replicant_configuration_manager_h_
#define replicant_configuration_manager_h_

// STL
#include <list>

// Replicant
#include "common/configuration.h"

namespace replicant
{

class configuration_manager
{
    public:
        class proposal;

    public:
        configuration_manager();
        configuration_manager(const configuration_manager&);
        ~configuration_manager() throw ();

    public:
        const configuration& stable() const;
        const configuration& latest() const;
        void get_all_nodes(std::vector<chain_node>* nodes) const;
        void get_config_chain(std::vector<configuration>* config_chain) const;
        bool get_proposal(uint64_t proposal_id,
                          uint64_t proposal_time,
                          configuration* config) const;
        void get_proposals(std::vector<proposal>* proposals) const;
        bool is_compatible(const configuration* configs,
                           size_t configs_sz) const;
        bool contains_quorum_of_all(const configuration& config) const;
        bool any(bool (configuration::*func)(const chain_node&) const, const chain_node& node) const;
        bool any(bool (configuration::*func)(uint64_t) const, uint64_t token) const;
        bool contains(const configuration& config) const;

    public:
        void advance(const configuration& config);
        void merge(uint64_t proposal_id,
                   uint64_t proposal_time,
                   const configuration* configs,
                   size_t configs_sz);
        void reject(uint64_t proposal_id, uint64_t proposal_time);
        void reset(const configuration& config);

    public:
        configuration_manager& operator = (const configuration_manager&);

    private:
        friend std::ostream& operator << (std::ostream& lhs, const configuration_manager& rhs);
        friend e::buffer::packer operator << (e::buffer::packer lhs, const configuration_manager& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, configuration_manager& rhs);
        friend size_t pack_size(const configuration_manager& rhs);

    private:
        std::list<configuration> m_configs;
        std::list<proposal> m_proposals;
};

struct configuration_manager::proposal
{
    proposal() : id(), time(), version() {}
    proposal(uint64_t i, uint64_t t, uint64_t v) : id(i), time(t), version(v) {}
    ~proposal() throw () {}
    uint64_t id;
    uint64_t time;
    uint64_t version;
};

bool
operator < (const configuration_manager::proposal& lhs, const configuration_manager::proposal& rhs);

std::ostream&
operator << (std::ostream& lhs, const configuration_manager& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const configuration_manager& rhs);

e::unpacker
operator >> (e::unpacker lhs, configuration_manager& rhs);

size_t
pack_size(const configuration_manager& cm);

} // namespace replicant

#endif // replicant_configuration_manager_h_
