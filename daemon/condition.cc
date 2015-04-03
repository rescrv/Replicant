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

// STL
#include <algorithm>

// Replicant
#include "common/ids.h"
#include "daemon/condition.h"
#include "daemon/daemon.h"

using replicant::condition;

struct condition::waiter
{
    waiter();
    waiter(uint64_t wait_for, server_id client, uint64_t nonce);
    waiter(const waiter&);
    ~waiter() throw ();

    static bool compare_for_heap(const waiter& lhs, const waiter& rhs);

    uint64_t wait_for;
    server_id client;
    uint64_t nonce;
};

condition :: waiter :: waiter()
    : wait_for(0)
    , client()
    , nonce(0)
{
}

condition :: waiter :: waiter(uint64_t w, server_id c, uint64_t n)
    : wait_for(w)
    , client(c)
    , nonce(n)
{
}

condition :: waiter :: waiter(const waiter& other)
    : wait_for(other.wait_for)
    , client(other.client)
    , nonce(other.nonce)
{
}

condition :: waiter :: ~waiter() throw ()
{
}

bool
condition :: waiter :: compare_for_heap(const waiter& lhs, const waiter& rhs)
{
    return lhs.wait_for > rhs.wait_for;
}

condition :: condition()
    : m_state(0)
    , m_data()
    , m_waiters()
{
}

condition :: condition(uint64_t initial)
    : m_state(initial)
    , m_data()
    , m_waiters()
{
}

condition :: ~condition() throw ()
{
}

void
condition :: wait(daemon* d, server_id si, uint64_t nonce, uint64_t state)
{
    if (state <= m_state)
    {
        d->callback_condition(si, nonce, m_state, m_data);
    }
    else
    {
        m_waiters.push_back(waiter(state, si, nonce));
        std::push_heap(m_waiters.begin(), m_waiters.end(), waiter::compare_for_heap);
    }
}

void
condition :: broadcast(daemon* d)
{
    ++m_state;

    while (!m_waiters.empty() && m_waiters[0].wait_for <= m_state)
    {
        d->callback_condition(m_waiters[0].client, m_waiters[0].nonce, m_state, m_data);
        std::pop_heap(m_waiters.begin(), m_waiters.end(), waiter::compare_for_heap);
        m_waiters.pop_back();
    }
}

void
condition :: broadcast(daemon* d, const char* data, size_t data_sz)
{
    ++m_state;
    m_data.assign(data, data_sz);

    while (!m_waiters.empty() && m_waiters[0].wait_for <= m_state)
    {
        d->callback_condition(m_waiters[0].client, m_waiters[0].nonce, m_state, m_data);
        std::pop_heap(m_waiters.begin(), m_waiters.end(), waiter::compare_for_heap);
        m_waiters.pop_back();
    }
}

uint64_t
condition :: peek_state() const
{
    return m_state;
}

void
condition :: peek_state(uint64_t* state, const char** data, size_t* data_sz) const
{
    *state = m_state;
    *data = m_data.data();
    *data_sz = m_data.size();
}

e::packer
replicant :: operator << (e::packer lhs, const condition& rhs)
{
    return lhs << rhs.m_state;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, condition& rhs)
{
    return lhs >> rhs.m_state;
}

size_t
replicant :: pack_size(const condition&)
{
    return sizeof(uint64_t);
}
