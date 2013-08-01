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

// STL
#include <algorithm>
#include <memory>

// Google Log
#include <glog/logging.h>

// Replicant
#include "common/configuration.h"

using replicant::chain_node;
using replicant::configuration;

configuration :: configuration()
    : m_cluster()
    , m_prev_token()
    , m_this_token()
    , m_version()
    , m_members()
    , m_chain()
    , m_command_sz()
{
}

configuration :: configuration(uint64_t c,
                               uint64_t pt,
                               uint64_t tt,
                               uint64_t v,
                               const chain_node& h)
    : m_cluster(c)
    , m_prev_token(pt)
    , m_this_token(tt)
    , m_version(v)
    , m_members()
    , m_chain()
    , m_command_sz()
{
    m_members.push_back(h);
    m_chain.push_back(h.token);
    m_command_sz = 1;
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: validate() const
{
    for (size_t i = 0; i < m_members.size(); ++i)
    {
        for (size_t j = i + 1; j < m_members.size(); ++j)
        {
            if (m_members[i].token == m_members[j].token ||
                m_members[i].address == m_members[j].address)
            {
                return false;
            }
        }
    }

    if (m_command_sz > m_chain.size())
    {
        return false;
    }

    for (size_t i = 0; i < m_chain.size(); ++i)
    {
        const chain_node* n = node_from_token(m_chain[i]);

        if (n == NULL)
        {
            return false;
        }

        for (size_t j = i + 1; j < m_chain.size(); ++j)
        {
            if (m_chain[i] == m_chain[j])
            {
                return false;
            }
        }
    }

    return true;
}

bool
configuration :: quorum_of(const configuration& other) const
{
    assert(validate());
    assert(other.validate());
    assert(cluster() == other.cluster());
    size_t count = 0;

    for (size_t i = 0; i < m_chain.size(); ++i)
    {
        for (size_t j = 0; j < other.m_chain.size(); ++j)
        {
            if (m_chain[i] == other.m_chain[j])
            {
                ++count;
                break;
            }
        }
    }

    size_t quorum = other.m_chain.size() / 2 + 1;
    return count >= quorum;
}

uint64_t
configuration :: fault_tolerance() const
{
    if (m_chain.empty())
    {
        return 0;
    }

    return (m_chain.size() - 1) / 2;
}

uint64_t
configuration :: servers_needed_for(uint64_t f) const
{
    uint64_t needed = f * 2 + 1;

    if (needed < m_chain.size())
    {
        return 0;
    }
    else
    {
        return needed - m_chain.size();
    }
}

bool
configuration :: has_token(uint64_t token) const
{
    return node_from_token(token) != NULL;
}

bool
configuration :: is_member(const chain_node& node) const
{
    const chain_node* n = node_from_token(node.token);
    return n && *n == node;
}

const chain_node*
configuration :: node_from_token(uint64_t token) const
{
    for (size_t i = 0; i < m_members.size(); ++i)
    {
        if (m_members[i].token == token)
        {
            return &m_members[i];
        }
    }

    return NULL;
}

const chain_node*
configuration :: head() const
{
    if (m_chain.empty())
    {
        return NULL;
    }

    const chain_node* n = node_from_token(m_chain[0]);
    assert(n);
    return n;
}

const chain_node*
configuration :: command_tail() const
{
    assert(m_command_sz <= m_chain.size());

    if (m_command_sz == 0)
    {
        return NULL;
    }

    const chain_node* n = node_from_token(m_chain[m_command_sz - 1]);
    assert(n);
    return n;
}

const chain_node*
configuration :: config_tail() const
{
    if (m_chain.empty())
    {
        return NULL;
    }

    const chain_node* n = node_from_token(m_chain[m_chain.size() - 1]);
    assert(n);
    return n;
}

const chain_node*
configuration :: prev(uint64_t token) const
{
    const uint64_t* p = NULL;
    const uint64_t* cur = &m_chain.front();
    const uint64_t* end = &m_chain.front() + m_chain.size();

    for (; cur < end; ++cur)
    {
        if (*cur == token)
        {
            const chain_node* node = NULL;

            if (p)
            {
                node = node_from_token(*p);
                assert(node);
            }

            return node;
        }

        p = cur;
    }

    return NULL;
}

const chain_node*
configuration :: next(uint64_t token) const
{
    const uint64_t* n = NULL;
    const uint64_t* cur = &m_chain.front() + m_chain.size() - 1;
    const uint64_t* end = &m_chain.front();

    for (; cur >= end; --cur)
    {
        if (*cur == token)
        {
            const chain_node* node = NULL;

            if (n)
            {
                node = node_from_token(*n);
                assert(node);
            }

            return node;
        }

        n = cur;
    }

    return NULL;
}

bool
configuration :: in_command_chain(uint64_t token) const
{
    assert(m_command_sz <= m_chain.size());

    for (size_t i = 0; i < m_command_sz; ++i)
    {
        if (m_chain[i] == token)
        {
            return true;
        }
    }

    return false;
}

bool
configuration :: in_config_chain(uint64_t token) const
{
    for (size_t i = 0; i < m_chain.size(); ++i)
    {
        if (m_chain[i] == token)
        {
            return true;
        }
    }

    return false;
}

uint64_t
configuration :: command_size() const
{
    return m_command_sz;
}

uint64_t
configuration :: config_size() const
{
    return m_chain.size();
}

const chain_node*
configuration :: members_begin() const
{
    return &m_members.front();
}

const chain_node*
configuration :: members_end() const
{
    return &m_members.front() + m_members.size();
}

const uint64_t*
configuration :: chain_begin() const
{
    return &m_chain.front();
}

const uint64_t*
configuration :: chain_end() const
{
    return &m_chain.front() + m_chain.size();
}

void
configuration :: bump_version()
{
    m_prev_token = m_this_token;
    // XXX m_this_token = token
    ++m_version;
}

void
configuration :: add_member(const chain_node& node)
{
    assert(node_from_token(node.token) == NULL);
    m_members.push_back(node);
    std::sort(m_members.begin(), m_members.end());
}

void
configuration :: add_to_chain(uint64_t token)
{
    assert(!in_config_chain(token));
    m_chain.push_back(token);
}

void
configuration :: remove_from_chain(uint64_t token)
{
    assert(in_config_chain(token));

    for (size_t i = 0; i < m_chain.size(); ++i)
    {
        if (m_chain[i] == token)
        {
            if (i < m_command_sz)
            {
                --m_command_sz;
            }

            for (size_t j = i; j + 1 < m_chain.size(); ++j)
            {
                m_chain[j] = m_chain[j + 1];
            }

            m_chain.pop_back();
            return;
        }
    }
}

void
configuration :: grow_command_chain()
{
    assert(m_command_sz < m_chain.size());
    ++m_command_sz;
}

bool
replicant :: operator < (const configuration& lhs, const configuration& rhs)
{
    return lhs.version() < rhs.version();
}

bool
replicant :: operator == (const configuration& lhs, const configuration& rhs)
{
    if (lhs.m_cluster != rhs.m_cluster ||
        lhs.m_prev_token != rhs.m_prev_token ||
        lhs.m_this_token != rhs.m_this_token ||
        lhs.m_version != rhs.m_version ||
        lhs.m_members.size() != rhs.m_members.size() ||
        lhs.m_chain.size() != rhs.m_chain.size() ||
        lhs.m_command_sz != rhs.m_command_sz)
    {
        return false;
    }

    for (size_t i = 0; i < lhs.m_members.size(); ++i)
    {
        if (lhs.m_members[i] != rhs.m_members[i])
        {
            return false;
        }
    }

    for (size_t i = 0; i < lhs.m_chain.size(); ++i)
    {
        if (lhs.m_chain[i] != rhs.m_chain[i])
        {
            return false;
        }
    }

    return true;
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const configuration& rhs)
{
    lhs << "configuration(cluster=" << rhs.m_cluster
        << ", prev_token=" << rhs.m_prev_token
        << ", this_token=" << rhs.m_this_token
        << ", version=" << rhs.m_version
        << ", command=[";

    for (size_t i = 0; i < std::min(rhs.m_command_sz, rhs.m_chain.size()); ++i)
    {
        lhs << rhs.m_chain[i] << (i + 1 < std::min(rhs.m_command_sz, rhs.m_chain.size()) ? ", " : "");
    }

    lhs << "], config=[";

    for (size_t i = 0; i < rhs.m_chain.size(); ++i)
    {
        lhs << rhs.m_chain[i] << (i + 1 < rhs.m_chain.size() ? ", " : "");
    }

    lhs << "], members=[";

    for (size_t i = 0; i < rhs.m_members.size(); ++i)
    {
        lhs << rhs.m_members[i] << (i + 1 < rhs.m_members.size() ? ", " : "");
    }

    lhs << "])";
    return lhs;
}

e::buffer::packer
replicant :: operator << (e::buffer::packer lhs, const configuration& rhs)
{
    lhs = lhs << rhs.m_cluster
              << rhs.m_prev_token
              << rhs.m_this_token
              << rhs.m_version
              << uint64_t(rhs.m_members.size())
              << rhs.m_command_sz
              << uint64_t(rhs.m_chain.size());

    for (uint64_t i = 0; i < rhs.m_members.size(); ++i)
    {
        lhs = lhs << rhs.m_members[i];
    }

    for (uint64_t i = 0; i < rhs.m_chain.size(); ++i)
    {
        lhs = lhs << rhs.m_chain[i];
    }

    return lhs;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, configuration& rhs)
{
    uint64_t members_sz;
    uint64_t chain_sz;
    lhs = lhs >> rhs.m_cluster
              >> rhs.m_prev_token
              >> rhs.m_this_token
              >> rhs.m_version
              >> members_sz
              >> rhs.m_command_sz
              >> chain_sz;
    rhs.m_members.resize(members_sz);
    rhs.m_chain.resize(chain_sz);

    for (uint64_t i = 0; i < rhs.m_members.size(); ++i)
    {
        lhs = lhs >> rhs.m_members[i];
    }

    for (uint64_t i = 0; i < rhs.m_chain.size(); ++i)
    {
        lhs = lhs >> rhs.m_chain[i];
    }

    return lhs;
}


char*
replicant :: pack_config(const configuration& config, char* ptr)
{
    // XXX inefficient, lazy hack
    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(config)));
    msg->pack_at(0) << config;
    memmove(ptr, msg->data(), msg->size());
    return ptr + msg->size();
}

size_t
replicant :: pack_size(const configuration& rhs)
{
    size_t sz = sizeof(uint64_t) // rhs.m_cluster
              + sizeof(uint64_t) // rhs.m_prev_token
              + sizeof(uint64_t) // rhs.m_this_token
              + sizeof(uint64_t) // rhs.m_version
              + sizeof(uint64_t) // rhs.m_members.size()
              + sizeof(uint64_t) // rhs.m_command_sz
              + sizeof(uint64_t); // rhs.m_chain.size()

    for (size_t i = 0; i < rhs.m_members.size(); ++i)
    {
        sz += pack_size(rhs.m_members[i]);
    }

    sz += sizeof(uint64_t) * rhs.m_chain.size();
    return sz;
}

size_t
replicant :: pack_size(const std::vector<configuration>& rhs)
{
    size_t sz = sizeof(uint32_t);

    for (size_t i = 0; i < rhs.size(); ++i)
    {
        sz += pack_size(rhs[i]);
    }

    return sz;
}
