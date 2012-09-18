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

bool
configuration :: quorum_of(const configuration& other) const
{
    size_t count = 0;
    const chain_node* seq = other.members_begin();

    for (const chain_node* n = members_begin(); n < members_end(); ++n)
    {
        bool match_in_chain = false;

        for (const chain_node* o = seq; o < other.members_end(); ++o)
        {
            if (*n == *o)
            {
                ++count;
                match_in_chain = true;
                seq = o;
                break;
            }
        }

        // The first member of our chain not in other's chain prevents us from
        // counting any other members of our chain in the quorum if they exist
        // in other's chain.  This guarantees that we can only remove nodes from
        // other's chain and add them to standby, not insert them at arbitrary
        // locations.
        if (!match_in_chain)
        {
            seq = other.members_end();

            if (other.is_standby(*n))
            {
                ++count;
            }
        }
    }

    for (const chain_node* n = standbys_begin(); n < standbys_end(); ++n)
    {
        if (other.in_chain(*n))
        {
            ++count;
        }
    }

    for (const chain_node* s = spares_begin(); s != spares_end(); ++s)
    {
        if (other.in_chain(*s))
        {
            ++count;
        }
    }

    size_t quorum = other.m_standby_sz / 2 + 1;
    return count >= quorum;
}

bool
configuration :: validate() const
{
    if (m_member_sz > m_standby_sz)
    {
        return false;
    }

    for (const chain_node* n = members_begin(); n < standbys_end(); ++n)
    {
        for (const chain_node* m = n + 1; m < standbys_end(); ++m)
        {
            if (*n == *m)
            {
                return false;
            }
        }

        for (const chain_node* s = spares_begin(); s < spares_end(); ++s)
        {
            if (*n == *s)
            {
                return false;
            }
        }
    }

    return true;
}

const chain_node&
configuration :: head() const
{
    return m_chain[0];
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

const chain_node&
configuration :: first_standby() const
{
    assert(standbys_begin() != standbys_end());
    return *standbys_begin();
}

bool
configuration :: in_chain(const chain_node& node) const
{
    for (const chain_node* n = members_begin(); n != standbys_end(); ++n)
    {
        if (*n == node)
        {
            return true;
        }
    }

    return false;
}

bool
configuration :: in_chain_sender(const po6::net::location& host) const
{
    for (const chain_node* n = members_begin(); n != standbys_end(); ++n)
    {
        if (n->sender() == host)
        {
            return true;
        }
    }

    return false;
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

bool
configuration :: chain_full() const
{
    return m_standby_sz >= REPL_CONFIG_SZ;
}

bool
configuration :: spares_full() const
{
    return m_spare_sz >= REPL_CONFIG_SZ;
}

const chain_node*
configuration :: members_begin() const
{
    return &m_chain[0];
}

const chain_node*
configuration :: members_end() const
{
    return &m_chain[m_member_sz];
}

const chain_node*
configuration :: standbys_begin() const
{
    return &m_chain[m_member_sz];
}

const chain_node*
configuration :: standbys_end() const
{
    return &m_chain[m_standby_sz];
}

const chain_node*
configuration :: spares_begin() const
{
    return &m_spare[0];
}

const chain_node*
configuration :: spares_end() const
{
    return &m_spare[m_spare_sz];
}

void
configuration :: add_spare(const chain_node& node)
{
    assert(m_spare_sz < REPL_CONFIG_SZ);
    assert(!is_spare(node));
    m_spare[m_spare_sz] = node;
    ++m_spare_sz;
}

void
configuration :: add_standby(const chain_node& node)
{
    assert(m_standby_sz < REPL_CONFIG_SZ);
    assert(!is_standby(node));
    m_chain[m_standby_sz] = node;
    ++m_standby_sz;
    remove_spare(node);
}

void
configuration :: bump_version()
{
    m_version = m_version + 1;
}

void
configuration :: convert_standby(const chain_node& node)
{
    assert(m_standby_sz < REPL_CONFIG_SZ);
    assert(m_member_sz < m_standby_sz);
    ++m_member_sz;
    remove_spare(node);
}

void
configuration :: remove_spare(chain_node node)
{
    size_t i = 0;

    while (i < m_spare_sz)
    {
        if (m_spare[i] == node)
        {
            m_spare[i] = m_spare[m_spare_sz - 1];
            --m_spare_sz;
        }
        else
        {
            ++i;
        }
    }
}

void
configuration :: remove_standby(chain_node node)
{
    size_t i = m_member_sz;

    while (i < m_standby_sz)
    {
        if (m_chain[i] == node)
        {
            for (size_t j = i; j + 1 < m_standby_sz; ++j)
            {
                m_chain[j] = m_chain[j + 1];
            }

            --m_standby_sz;
        }
        else
        {
            ++i;
        }
    }
}

void
configuration :: remove_member(chain_node node)
{
    size_t i = 0;

    while (i < m_member_sz)
    {
        if (m_chain[i] == node)
        {
            for (size_t j = i; j + 1 < m_standby_sz; ++j)
            {
                m_chain[j] = m_chain[j + 1];
            }

            --m_standby_sz;
            --m_member_sz;
        }
        else
        {
            ++i;
        }
    }
}

bool
operator < (const configuration& lhs, const configuration& rhs)
{
    return lhs.version() < rhs.version();
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

    for (size_t i = 0; i < lhs.m_standby_sz; ++i)
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

    for (size_t i = 0; i < rhs.m_standby_sz; ++i)
    {
        lhs = lhs >> rhs.m_chain[i];
    }

    for (size_t i = 0; i < rhs.m_spare_sz; ++i)
    {
        lhs = lhs >> rhs.m_spare[i];
    }

    return lhs;
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
