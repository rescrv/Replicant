// Copyright (c) 2014, Robert Escriva
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
#include <cassert>
#include <cstdlib>

// STL
#include <algorithm>

// Replicant
#include "daemon/client_manager.h"

using replicant::client_manager;

struct client_manager::client_metadata
{
    client_metadata() : id(0), life(0){}
    client_metadata(uint64_t i) : id(i), life(0) {}
    uint64_t id;
    uint64_t life;
};

client_manager :: client_manager()
    : m_clients()
{
}

client_manager :: ~client_manager() throw ()
{
}

void
client_manager :: register_client(uint64_t client)
{
    m_clients.push_back(client_metadata(client));

    for (size_t i = m_clients.size() - 1; i > 0; --i)
    {
        if (m_clients[i - 1].id < m_clients[i].id)
        {
            break;
        }

        assert(m_clients[i - 1].id > m_clients[i].id);
        std::swap(m_clients[i - 1], m_clients[i]);
    }
}

void
client_manager :: deregister_client(uint64_t client)
{
    for (size_t i = 0; i < m_clients.size(); ++i)
    {
        if (m_clients[i].id < client)
        {
            continue;
        }
        else if (m_clients[i].id > client)
        {
            break;
        }

        for (size_t j = i + 1; j < m_clients.size(); ++j)
        {
            m_clients[j - 1] = m_clients[j];
        }

        m_clients.pop_back();
        break;
    }
}

void
client_manager :: list_clients(std::vector<uint64_t>* clients)
{
    clients->resize(m_clients.size());

    for (size_t i = 0; i < m_clients.size(); ++i)
    {
        (*clients)[i] = m_clients[i].id;
    }
}

void
client_manager :: owned_clients(uint64_t chain_index,
                                uint64_t chain_length,
                                std::vector<uint64_t>* clients)
{
    if (chain_index == chain_length)
    {
        return;
    }
    else if (chain_length <= 1)
    {
        return list_clients(clients);
    }

    uint64_t increment = ((1ULL << 32) / chain_length) << 32;
    uint64_t lower_bound = chain_index * increment;
    uint64_t upper_bound = (lower_bound - 1) + increment;

    for (size_t i = 0; i < m_clients.size(); ++i)
    {
        if (lower_bound <= m_clients[i].id &&
            m_clients[i].id <= upper_bound)
        {
            clients->push_back(m_clients[i].id);
        }
    }
}

void
client_manager :: last_seen_before(uint64_t when, std::vector<uint64_t>* clients)
{
    for (size_t i = 0; i < m_clients.size(); ++i)
    {
        if (m_clients[i].life < when)
        {
            clients->push_back(m_clients[i].id);
        }
    }
}

void
client_manager :: proof_of_life(uint64_t now)
{
    for (size_t i = 0; i < m_clients.size(); ++i)
    {
        m_clients[i].life = now;
    }
}

void
client_manager :: proof_of_life(uint64_t id, uint64_t now)
{
    for (size_t i = 0; i < m_clients.size(); ++i)
    {
        if (m_clients[i].id == id)
        {
            m_clients[i].life = now;
            break;
        }
        else if (m_clients[i].id > id)
        {
            break;
        }
    }
}
