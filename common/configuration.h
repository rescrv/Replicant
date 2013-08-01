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

#ifndef replicant_configuration_h_
#define replicant_configuration_h_

// Replicant
#include "common/chain_node.h"

namespace replicant
{

class configuration
{
    public:
        configuration();
        configuration(uint64_t cluster,
                      uint64_t prev_token,
                      uint64_t this_token,
                      uint64_t version,
                      const chain_node& head);
        ~configuration() throw ();

    // metadata
    public:
        uint64_t cluster() const { return m_cluster; }
        uint64_t version() const { return m_version; }
        uint64_t prev_token() const { return m_prev_token; }
        uint64_t this_token() const { return m_this_token; }

    // invariants
    public:
        bool validate() const;
        bool quorum_of(const configuration& other) const;
        uint64_t fault_tolerance() const;
        uint64_t servers_needed_for(uint64_t f) const;

    // membership
    public:
        bool has_token(uint64_t token) const;
        bool is_member(const chain_node& node) const;
        const chain_node* node_from_token(uint64_t token) const;

    // chaining
    public:
        const chain_node* head() const;
        const chain_node* command_tail() const;
        const chain_node* config_tail() const;
        const chain_node* prev(uint64_t token) const;
        const chain_node* next(uint64_t token) const;
        bool in_command_chain(uint64_t token) const;
        bool in_config_chain(uint64_t token) const;
        uint64_t command_size() const;
        uint64_t config_size() const;

    // iterators
    public:
        const chain_node* members_begin() const;
        const chain_node* members_end() const;
        const uint64_t* chain_begin() const;
        const uint64_t* chain_end() const;

    // modify the configuration
    public:
        void bump_version();
        void add_member(const chain_node& node);
        void add_to_chain(uint64_t token);
        void remove_from_chain(uint64_t token);
        void grow_command_chain();

    private:
        friend bool operator == (const configuration& lhs, const configuration& rhs);
        friend std::ostream& operator << (std::ostream& lhs, const configuration& rhs);
        friend e::buffer::packer operator << (e::buffer::packer lhs, const configuration& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, configuration& rhs);
        friend size_t pack_size(const configuration& rhs);

    private:
        uint64_t m_cluster;
        uint64_t m_prev_token;
        uint64_t m_this_token;
        uint64_t m_version;
        std::vector<chain_node> m_members;
        std::vector<uint64_t> m_chain;
        uint64_t m_command_sz;
};

// < compares version only
bool
operator < (const configuration& lhs, const configuration& rhs);

bool
operator == (const configuration& lhs, const configuration& rhs);
inline bool
operator != (const configuration& lhs, const configuration& rhs) { return !(lhs == rhs); }

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const configuration& rhs);

e::unpacker
operator >> (e::unpacker lhs, configuration& rhs);

char*
pack_config(const configuration& config, char* ptr);

size_t
pack_size(const configuration& rhs);

size_t
pack_size(const std::vector<configuration>& rhs);

} // namespace replicant

#endif // replicant_configuration_h_
