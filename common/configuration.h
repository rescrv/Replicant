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

#define REPL_CONFIG_SZ 255

class configuration
{
    public:
        configuration();
        configuration(uint64_t version, const chain_node& head);
        ~configuration() throw ();

    public:
        uint64_t version() const;
        uint64_t prev_token() const;
        uint64_t this_token() const;
        bool quorum_of(const configuration& other) const;
        bool validate() const;
        const chain_node& get(uint64_t token) const;
        uint64_t fault_tolerance() const;
        uint64_t servers_needed_for(uint64_t f) const;

    // Chain facts
    public:
        const chain_node& head() const;
        const chain_node& command_tail() const;
        const chain_node& config_tail() const;
        bool has_prev(const chain_node& node) const;
        const chain_node& prev(const chain_node& node) const;
        bool has_next(const chain_node& node) const;
        const chain_node& next(const chain_node& node) const;

    // Iterate over differnt types of nodes
    public:
        bool in_cluster(const chain_node& node) const;
        bool is_member(const chain_node& node) const;
        const chain_node* members_begin() const;
        const chain_node* members_end() const;
        bool is_standby(const chain_node& node) const;
        const chain_node* standbys_begin() const;
        const chain_node* standbys_end() const;
        bool is_spare(const chain_node& node) const;
        const chain_node* spares_begin() const;
        const chain_node* spares_end() const;

    // Modify the configuration
    public:
        bool may_add_spare() const;
        void add_spare(const chain_node& node);
        bool may_promote_spare() const;
        void promote_spare(const chain_node& node);
        bool may_promote_standby() const;
        void promote_standby();

    private:
        friend bool operator == (const configuration& lhs, const configuration& rhs);
        friend std::ostream& operator << (std::ostream& lhs, const configuration& rhs);
        friend e::buffer::packer operator << (e::buffer::packer lhs, const configuration& rhs);
        friend e::buffer::unpacker operator >> (e::buffer::unpacker lhs, configuration& rhs);
        friend size_t pack_size(const configuration& rhs);

    private:
        uint64_t m_version;
        chain_node m_chain[REPL_CONFIG_SZ + 1];
        uint8_t m_member_sz;
        uint8_t m_standby_sz;
        chain_node m_spare[REPL_CONFIG_SZ];
        uint8_t m_spare_sz;
};

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

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, configuration& rhs);

char*
pack_config(const configuration& config, char* ptr);

size_t
pack_size(const configuration& rhs);

size_t
pack_size(const std::vector<configuration>& rhs);

#endif // replicant_configuration_h_
