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
#include <memory>

// Google Log
#include <glog/logging.h>

// Replicant
#include "common/configuration.h"

configuration :: configuration()
    : m_version(0)
    , m_chain()
    , m_member_sz(0)
    , m_standby_sz(0)
    , m_spare()
    , m_spare_sz(0)
{
}

configuration :: configuration(uint64_t v, const chain_node& h)
    : m_version(v)
    , m_chain()
    , m_member_sz(0)
    , m_standby_sz(0)
    , m_spare()
    , m_spare_sz(0)
{
    m_chain[0] = h;
    m_member_sz = 1;
    m_standby_sz = 1;
}

configuration :: ~configuration() throw ()
{
}

uint64_t
configuration :: version() const
{
    return m_version;
}

uint64_t
configuration :: prev_token() const
{
    return 0xdeadbeefcafebabeULL;
}

uint64_t
configuration :: this_token() const
{
    return 0xdeadbeefcafebabeULL;
}

bool
configuration :: quorum_of(const configuration& other) const
{
    assert(validate());
    assert(other.validate());
    size_t count = 0;

    for (const chain_node* n = members_begin(); n < members_end(); ++n)
    {
        bool found = false;

        for (const chain_node* o = other.members_begin(); !found && o < other.members_end(); ++o)
        {
            if (*n == *o)
            {
                ++count;
                found = true;
            }
        }

        for (const chain_node* o = other.standbys_begin(); !found && o < other.standbys_end(); ++o)
        {
            if (*n == *o)
            {
                ++count;
                found = true;
            }
        }
    }

    for (const chain_node* n = standbys_begin(); n < standbys_end(); ++n)
    {
        bool found = false;

        for (const chain_node* o = other.members_begin(); !found && o < other.members_end(); ++o)
        {
            if (*n == *o)
            {
                ++count;
                found = true;
            }
        }

        for (const chain_node* o = other.standbys_begin(); !found && o < other.standbys_end(); ++o)
        {
            if (*n == *o)
            {
                ++count;
                found = true;
            }
        }
    }

    size_t quorum = other.m_standby_sz / 2 + 1;
    return count >= quorum;
}

bool
configuration :: validate() const
{
    for (const chain_node* n = members_begin(); n < members_end(); ++n)
    {
        for (const chain_node* m = n + 1; m < members_end(); ++m)
        {
            if (n->address == m->address || n->token == m->token)
            {
                return false;
            }
        }

        for (const chain_node* s = standbys_begin(); s < standbys_end(); ++s)
        {
            if (n->address == s->address || n->token == s->token)
            {
                return false;
            }
        }

        for (const chain_node* s = spares_begin(); s < spares_end(); ++s)
        {
            if (n->address == s->address || n->token == s->token)
            {
                return false;
            }
        }
    }

    for (const chain_node* n = standbys_begin(); n < standbys_end(); ++n)
    {
        for (const chain_node* s = n + 1; s < standbys_end(); ++s)
        {
            if (n->address == s->address || n->token == s->token)
            {
                return false;
            }
        }

        for (const chain_node* s = spares_begin(); s < spares_end(); ++s)
        {
            if (n->address == s->address || n->token == s->token)
            {
                return false;
            }
        }
    }

    for (const chain_node* n = spares_begin(); n < spares_end(); ++n)
    {
        for (const chain_node* s = n + 1; s < spares_end(); ++s)
        {
            if (n->address == s->address || n->token == s->token)
            {
                return false;
            }
        }
    }

    return m_member_sz <= m_standby_sz;
}

const chain_node&
configuration :: get(uint64_t token) const
{
    for (const chain_node* n = members_begin(); n < members_end(); ++n)
    {
        if (n->token == token)
        {
            return *n;
        }
    }

    for (const chain_node* n = standbys_begin(); n < standbys_end(); ++n)
    {
        if (n->token == token)
        {
            return *n;
        }
    }

    for (const chain_node* n = spares_begin(); n < spares_end(); ++n)
    {
        if (n->token == token)
        {
            return *n;
        }
    }

    return m_chain[REPL_CONFIG_SZ];
}

uint64_t
configuration :: fault_tolerance() const
{
    return (m_standby_sz - 1) / 2;
}

uint64_t
configuration :: servers_needed_for(uint64_t f) const
{
    uint64_t needed = f * 2 + 1;

    if (needed < m_standby_sz)
    {
        return 0;
    }
    else
    {
        return needed - m_standby_sz;
    }
}

const chain_node&
configuration :: head() const
{
    if (m_member_sz == 0)
    {
        return m_chain[REPL_CONFIG_SZ];
    }

    return m_chain[0];
}

const chain_node&
configuration :: command_tail() const
{
    if (m_member_sz == 0)
    {
        return m_chain[REPL_CONFIG_SZ];
    }

    return m_chain[m_member_sz - 1];
}

const chain_node&
configuration :: config_tail() const
{
    if (m_standby_sz == 0)
    {
        return m_chain[REPL_CONFIG_SZ];
    }

    return m_chain[m_standby_sz - 1];
}

bool
configuration :: has_prev(const chain_node& node) const
{
    return prev(node) != chain_node();
}

const chain_node&
configuration :: prev(const chain_node& node) const
{
    const chain_node* ret = m_chain + REPL_CONFIG_SZ;
    const chain_node* cur = m_chain;
    const chain_node* end = m_chain + m_standby_sz;

    for (; cur < end; ++cur)
    {
        if (*cur == node)
        {
            return *ret;
        }

        ret = cur;
    }

    return m_chain[REPL_CONFIG_SZ];
}

bool
configuration :: has_next(const chain_node& node) const
{
    return next(node) != chain_node();
}

const chain_node&
configuration :: next(const chain_node& node) const
{
    const chain_node* ret = m_chain + REPL_CONFIG_SZ;
    const chain_node* cur = m_chain + m_standby_sz - 1;
    const chain_node* end = m_chain;

    for (; cur >= end; --cur)
    {
        if (*cur == node)
        {
            return *ret;
        }

        ret = cur;
    }

    return m_chain[REPL_CONFIG_SZ];
}

bool
configuration :: in_cluster(const chain_node& node) const
{
    return is_member(node) || is_standby(node);
}

bool
configuration :: is_member(const chain_node& node) const
{
    for (const chain_node* m = members_begin(); m != members_end(); ++m)
    {
        if (*m == node)
        {
            return true;
        }
    }

    return false;
}

const chain_node*
configuration :: members_begin() const
{
    return m_chain + 0;
}

const chain_node*
configuration :: members_end() const
{
    return m_chain + m_member_sz;
}

bool
configuration :: is_standby(const chain_node& node) const
{
    for (const chain_node* s = standbys_begin(); s != standbys_end(); ++s)
    {
        if (*s == node)
        {
            return true;
        }
    }

    return false;
}

const chain_node*
configuration :: standbys_begin() const
{
    return m_chain + m_member_sz;
}

const chain_node*
configuration :: standbys_end() const
{
    return m_chain + m_standby_sz;
}

bool
configuration :: is_spare(const chain_node& node) const
{
    for (const chain_node* s = spares_begin(); s != spares_end(); ++s)
    {
        if (*s == node)
        {
            return true;
        }
    }

    return false;
}

const chain_node*
configuration :: spares_begin() const
{
    return m_spare + 0;
}

const chain_node*
configuration :: spares_end() const
{
    return m_spare + m_spare_sz;
}

bool
configuration :: may_add_spare() const
{
    return m_spare_sz < REPL_CONFIG_SZ;
}

void
configuration :: add_spare(const chain_node& node)
{
    assert(may_add_spare());
    assert(!in_cluster(node));
    assert(!is_spare(node));
    m_spare[m_spare_sz] = node;
    ++m_spare_sz;
    ++m_version;
}

bool
configuration :: may_promote_spare() const
{
    return m_standby_sz < REPL_CONFIG_SZ && m_spare_sz > 0;
}

void
configuration :: promote_spare(const chain_node& node)
{
    assert(may_promote_spare());
    assert(!in_cluster(node));
    assert(is_spare(node));

    for (size_t i = 0; i < m_spare_sz; ++i)
    {
        if (m_spare[i] == node)
        {
            for (size_t j = i + 1; j < m_spare_sz; ++j)
            {
                m_spare[j - 1] = m_spare[j];
            }

            --m_spare_sz;
            m_chain[m_standby_sz] = node;
            ++m_standby_sz;
        }
    }

    ++m_version;
}

bool
configuration :: may_promote_standby() const
{
    return m_member_sz < m_standby_sz;
}

void
configuration :: promote_standby()
{
    assert(may_promote_standby());
    ++m_member_sz;
    ++m_version;
}

void
configuration :: remove(const chain_node& node)
{
    size_t idx = 0;

    while (idx < m_standby_sz)
    {
        if (m_chain[idx] == node)
        {
            for (size_t i = idx; i + 1 < m_standby_sz; ++i)
            {
                m_chain[i] = m_chain[i + 1];
            }

            --m_standby_sz;
            m_member_sz = std::min(m_member_sz, m_standby_sz);
        }
        else
        {
            ++idx;
        }
    }

    idx = 0;

    while (idx < m_spare_sz)
    {
        if (m_spare[idx] == node)
        {
            for (size_t i = idx; i + 1 < m_spare_sz; ++i)
            {
                m_spare[i] = m_spare[i + 1];
            }

            --m_spare_sz;
        }
        else
        {
            ++idx;
        }
    }
}

void
configuration :: bump_version()
{
    ++m_version;
}

bool
operator == (const configuration& lhs, const configuration& rhs)
{
    if (lhs.m_version != rhs.m_version ||
        lhs.m_member_sz != rhs.m_member_sz ||
        lhs.m_standby_sz != rhs.m_standby_sz ||
        lhs.m_spare_sz != rhs.m_spare_sz)
    {
        return false;
    }

    for (size_t i = 0; i < lhs.m_member_sz; ++i)
    {
        if (lhs.m_chain[i] != rhs.m_chain[i])
        {
            return false;
        }
    }

    for (size_t i = lhs.m_member_sz; i < lhs.m_standby_sz; ++i)
    {
        if (lhs.m_chain[i] != rhs.m_chain[i])
        {
            return false;
        }
    }

    for (size_t i = 0; i < lhs.m_spare_sz; ++i)
    {
        if (lhs.m_spare[i] != rhs.m_spare[i])
        {
            return false;
        }
    }

    return true;
}

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs)
{
    lhs << "configuration(version="
        << rhs.version() << ", members=";

    for (const chain_node* m = rhs.members_begin(); m < rhs.members_end(); ++m)
    {
        lhs << *m << (m + 1 < rhs.members_end() ? "->" : "");
    }

    lhs << ", standbys=";

    for (const chain_node* s = rhs.standbys_begin(); s != rhs.standbys_end(); ++s)
    {
        lhs << *s << (s + 1 < rhs.standbys_end() ? "->" : "");
    }

    lhs << ", spares=";

    for (const chain_node* s = rhs.spares_begin(); s != rhs.spares_end(); ++s)
    {
        lhs << *s << (s + 1 < rhs.spares_end() ? "->" : "");
    }

    lhs << ")";
    return lhs;
}

e::buffer::packer
operator << (e::buffer::packer lhs, const configuration& rhs)
{
    lhs = lhs << rhs.m_version
              << rhs.m_member_sz
              << rhs.m_standby_sz
              << rhs.m_spare_sz;

    for (const chain_node* m = rhs.members_begin(); m != rhs.members_end(); ++m)
    {
        lhs = lhs << *m;
    }

    for (const chain_node* s = rhs.standbys_begin(); s != rhs.standbys_end(); ++s)
    {
        lhs = lhs << *s;
    }

    for (const chain_node* s = rhs.spares_begin(); s != rhs.spares_end(); ++s)
    {
        lhs = lhs << *s;
    }

    return lhs;
}

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, configuration& rhs)
{
    lhs = lhs >> rhs.m_version
              >> rhs.m_member_sz
              >> rhs.m_standby_sz
              >> rhs.m_spare_sz;

    for (size_t i = 0; i < rhs.m_member_sz; ++i)
    {
        lhs = lhs >> rhs.m_chain[i];
    }

    for (size_t i = rhs.m_member_sz; i < rhs.m_standby_sz; ++i)
    {
        lhs = lhs >> rhs.m_chain[i];
    }

    for (size_t i = 0; i < rhs.m_spare_sz; ++i)
    {
        lhs = lhs >> rhs.m_spare[i];
    }

    return lhs;
}

char*
pack_config(const configuration& config, char* ptr)
{
    // XXX inefficient, lazy hack
    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(config)));
    msg->pack_at(0) << config;
    memmove(ptr, msg->data(), msg->size());
    return ptr + msg->size();
}

size_t
pack_size(const configuration& rhs)
{
    size_t sz = 0;

    for (const chain_node* m = rhs.members_begin(); m != rhs.members_end(); ++m)
    {
        sz += pack_size(*m);
    }

    for (const chain_node* s = rhs.standbys_begin(); s != rhs.standbys_end(); ++s)
    {
        sz += pack_size(*s);
    }

    for (const chain_node* s = rhs.spares_begin(); s != rhs.spares_end(); ++s)
    {
        sz += pack_size(*s);
    }

    return sizeof(uint64_t) + 3 * sizeof(uint8_t) + sz;
}

size_t
pack_size(const std::vector<configuration>& rhs)
{
    size_t sz = sizeof(uint32_t);

    for (size_t i = 0; i < rhs.size(); ++i)
    {
        sz += pack_size(rhs[i]);
    }

    return sz;
}
