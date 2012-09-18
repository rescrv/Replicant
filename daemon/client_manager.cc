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

#define __STDC_LIMIT_MACROS

// C
#include <cassert>
#include <stdint.h>

// STL
#include <algorithm>

// Replicant
#include "daemon/client_manager.h"

client_manager :: client_manager()
    : m_clients()
{
}

client_manager :: ~client_manager() throw ()
{
}

const client_manager::client_details*
client_manager :: get(uint64_t client) const
{
    client_map::const_iterator it = m_clients.find(client);

    if (it != m_clients.end())
    {
        return &it->second;
    }
    else
    {
        return NULL;
    }
}

void
client_manager :: get(std::vector<po6::net::location>* clients) const
{
    for (client_map::const_iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        clients->push_back(it->second.location());
    }
}

void
client_manager :: get(std::vector<uint64_t>* clients) const
{
    for (client_map::const_iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        clients->push_back(it->first);
    }
}

void
client_manager :: get_live_clients(std::vector<uint64_t>* clients) const
{
    for (client_map::const_iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        if (it->second.location() != po6::net::location())
        {
            clients->push_back(it->first);
        }
    }
}

uint64_t
client_manager :: lowest_slot_in_use() const
{
    uint64_t slot = UINT64_MAX;

    for (client_map::const_iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        slot = std::min(slot, it->second.lower_bound_slot());
    }

    return slot;
}

client_manager::client_details*
client_manager :: create(uint64_t client)
{
    std::pair<client_map::iterator, bool> inserted;
    inserted = m_clients.insert(std::make_pair(client, client_details()));
    return &inserted.first->second;
}

void
client_manager :: disassociate(const po6::net::location& host)
{
    po6::net::location loc;

    for (client_map::iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        if (it->second.location() == host)
        {
            it->second.set_location(loc);
        }
    }
}

void
client_manager :: garbage_collect(uint64_t slot)
{
    for (client_map::iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        it->second.garbage_collect(slot);
    }
}

client_manager::client_details*
client_manager :: get(uint64_t client)
{
    client_map::iterator it = m_clients.find(client);

    if (it != m_clients.end())
    {
        return &it->second;
    }
    else
    {
        return NULL;
    }
}

void
client_manager :: get_old_clients(uint64_t time, std::vector<uint64_t>* clients)
{
    for (client_map::iterator it = m_clients.begin();
            it != m_clients.end(); ++it)
    {
        if (it->second.last_seen() < time && it->second.last_seen() == 0)
        {
            it->second.set_last_seen(time);
        }
        else if (it->second.last_seen() < time)
        {
            clients->push_back(it->first);
        }
    }
}

void
client_manager :: remove(uint64_t client)
{
    m_clients.erase(client);
}

client_manager :: client_details :: client_details()
    : m_location()
    , m_lower_bound(0)
    , m_last_seen(0)
    , m_slots()
{
}

client_manager :: client_details :: ~client_details() throw ()
{
}

const po6::net::location&
client_manager :: client_details :: location() const
{
    return m_location;
}

uint64_t
client_manager :: client_details :: last_seen() const
{
    return m_last_seen;
}

uint64_t
client_manager :: client_details :: slot_for(uint64_t nonce) const
{
    std::pair<uint64_t, uint64_t> lb(nonce, 0);
    slot_list::const_iterator it = std::lower_bound(m_slots.begin(), m_slots.end(), lb);

    if (it != m_slots.end())
    {
        return it->second;
    }
    else
    {
        return 0;
    }
}

uint64_t
client_manager :: client_details :: lower_bound_nonce() const
{
    if (m_slots.empty())
    {
        return 0;
    }

    return m_slots.front().first;
}

uint64_t
client_manager :: client_details :: upper_bound_nonce() const
{
    if (m_slots.empty())
    {
        return 0;
    }

    return m_slots.back().first;
}

uint64_t
client_manager :: client_details :: lower_bound_slot() const
{
    if (m_slots.empty())
    {
        return 0;
    }

    return m_slots.front().second;
}

uint64_t
client_manager :: client_details :: upper_bound_slot() const
{
    if (m_slots.empty())
    {
        return 0;
    }

    return m_slots.back().second;
}

void
client_manager :: client_details :: garbage_collect(uint64_t slot)
{
    size_t first_to_keep = 0;

    while (first_to_keep < m_slots.size() && m_slots[first_to_keep].second < slot)
    {
        ++first_to_keep;
    }

    size_t i = 0;
    size_t j = first_to_keep;

    while (j < m_slots.size())
    {
        m_slots[i] = m_slots[j];
        ++i;
        ++j;
    }

    m_slots.resize(m_slots.size() - first_to_keep);
}

void
client_manager :: client_details :: set_last_seen(uint64_t time)
{
    m_last_seen = time;
}

void
client_manager :: client_details :: set_location(const po6::net::location& host)
{
    m_location = host;
}

void
client_manager :: client_details :: set_lower_bound(uint64_t nonce)
{
    m_lower_bound = nonce;
    size_t first_to_keep = 0;

    while (first_to_keep < m_slots.size() && m_slots[first_to_keep].first < nonce)
    {
        ++first_to_keep;
    }

    size_t i = 0;
    size_t j = first_to_keep;

    while (j < m_slots.size())
    {
        m_slots[i] = m_slots[j];
        ++i;
        ++j;
    }

    m_slots.resize(m_slots.size() - first_to_keep);
}

void
client_manager :: client_details :: set_slot(uint64_t nonce, uint64_t slot)
{
    m_slots.push_back(std::make_pair(nonce, slot));

    for (size_t i = m_slots.size() - 1; i > 0; --i)
    {
        if (m_slots[i] < m_slots[i - 1])
        {
            std::swap(m_slots[i], m_slots[i - 1]);
        }
        else
        {
            break;
        }
    }
}
