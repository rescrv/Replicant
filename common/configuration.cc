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

// C
#include <assert.h>

// STL
#include <algorithm>

// e
#include <e/serialization.h>

// Replicant
#include "common/configuration.h"
#include "common/packing.h"

using replicant::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_first_slot()
    , m_servers()
{
}

configuration :: configuration(cluster_id c,
                               version_id v,
                               uint64_t f,
                               server* s,
                               size_t s_sz)
    : m_cluster(c)
    , m_version(v)
    , m_first_slot(f)
    , m_servers(s, s + s_sz)
{
}

configuration :: configuration(const configuration& c, const server& s, uint64_t f)
    : m_cluster(c.m_cluster)
    , m_version(c.m_version.get() + 1)
    , m_first_slot(f)
    , m_servers(c.m_servers)
{
    assert(c.first_slot() < f);
    assert(!has(s.id));
    assert(!has(s.bind_to));
    m_servers.push_back(s);
}

configuration :: configuration(const configuration& other)
    : m_cluster(other.m_cluster)
    , m_version(other.m_version)
    , m_first_slot(other.m_first_slot)
    , m_servers(other.m_servers)
{
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: validate() const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == server_id() ||
            m_servers[i].bind_to == po6::net::location())
        {
            return false;
        }

        for (size_t j = i + 1; j < m_servers.size(); ++j)
        {
            if (m_servers[i].id == m_servers[j].id ||
                m_servers[i].bind_to == m_servers[j].bind_to)
            {
                return false;
            }
        }
    }

    return !m_servers.empty();
}

bool
configuration :: has(server_id si) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == si)
        {
            return true;
        }
    }

    return false;
}

bool
configuration :: has(const po6::net::location& loc) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].bind_to == loc)
        {
            return true;
        }
    }

    return false;
}

unsigned
configuration :: index(server_id si) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == si)
        {
            return i;
        }
    }

    return m_servers.size();
}

std::vector<replicant::server_id>
configuration :: server_ids() const
{
    std::vector<server_id> s;

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        s.push_back(m_servers[i].id);
    }

    return s;
}

const replicant::server*
configuration :: get(server_id si) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == si)
        {
            return &m_servers[i];
        }
    }

    return NULL;
}

const replicant::server*
configuration :: get(const po6::net::location& bind_to) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].bind_to == bind_to)
        {
            return &m_servers[i];
        }
    }

    return NULL;
}

replicant::bootstrap
configuration :: current_bootstrap() const
{
    std::vector<po6::net::hostname> hns;

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        hns.push_back(po6::net::hostname(m_servers[i].bind_to));
    }

    return bootstrap(hns);
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const configuration& rhs)
{
    lhs << "configuration(cluster=" << rhs.cluster().get()
        << ", version=" << rhs.version().get()
        << ", first_slot=" << rhs.first_slot()
        << ", [";
    const std::vector<server>& servers(rhs.servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (i > 0)
        {
            lhs << ", ";
        }

        lhs << servers[i];
    }

    lhs << "])";
    return lhs;
}

e::packer
replicant :: operator << (e::packer lhs, const configuration& rhs)
{
    return lhs << rhs.m_cluster << rhs.m_version << rhs.m_first_slot << rhs.m_servers;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, configuration& rhs)
{
    return lhs >> rhs.m_cluster >> rhs.m_version >> rhs.m_first_slot >> rhs.m_servers;
}

size_t
replicant :: pack_size(const configuration& rhs)
{
    return 3 * sizeof(uint64_t) + pack_size(rhs.m_servers);
}
