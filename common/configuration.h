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

#ifndef replicant_common_configuration_h_
#define replicant_common_configuration_h_

// Replicant
#include "namespace.h"
#include "common/bootstrap.h"
#include "common/configuration.h"
#include "common/server.h"

BEGIN_REPLICANT_NAMESPACE

// All configuration methods must be "const" (and no mutable keyword or const
// cast).

class configuration
{
    public:
        configuration();
        configuration(cluster_id cluster,
                      version_id version,
                      uint64_t first_slot,
                      server* servers,
                      size_t servers_sz);
        configuration(const configuration& c, const server& s, uint64_t first_slot);
        configuration(const configuration&);
        ~configuration() throw ();

    public:
        bool validate() const;

    // metadata
    public:
        cluster_id cluster() const { return m_cluster; }
        version_id version() const { return m_version; }
        uint64_t first_slot() const { return m_first_slot; }

    // membership
    public:
        bool has(server_id si) const;
        bool has(const po6::net::location& bind_to) const;
        unsigned index(server_id si) const;
        const std::vector<server>& servers() const { return m_servers; }
        std::vector<server_id> server_ids() const;
        const server* get(server_id si) const;
        bootstrap current_bootstrap() const;

    private:
        friend e::packer operator << (e::packer lhs, const configuration& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, configuration& rhs);
        friend size_t pack_size(const configuration& rhs);

    private:
        cluster_id m_cluster;
        version_id m_version;
        uint64_t m_first_slot;
        std::vector<server> m_servers;
};

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs);

e::packer
operator << (e::packer lhs, const configuration& rhs);
e::unpacker
operator >> (e::unpacker lhs, configuration& rhs);
size_t
pack_size(const configuration& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_common_configuration_h_
