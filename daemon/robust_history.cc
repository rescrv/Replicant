// Copyright (c) 2016, Robert Escriva
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

// e
#include <e/serialization.h>

// Replicant
#include "common/constants.h"
#include "common/packing.h"
#include "daemon/robust_history.h"

using replicant::robust_history;

struct robust_history::entry
{
    entry()
        : slot(), nonce(), status(), output() {}
    entry(uint64_t s, uint64_t n, replicant_returncode st, const std::string& o)
        : slot(s), nonce(n), status(st), output(o) {}
    entry(const entry& other)
        : slot(other.slot), nonce(other.nonce), status(other.status), output(other.output) {}
    ~entry() throw () {}
    uint64_t slot;
    uint64_t nonce;
    replicant_returncode status;
    std::string output;
};

robust_history :: robust_history()
    : m_mtx()
    , m_history()
    , m_lookup()
    , m_inhibit_gc(false)
{
    m_lookup.set_empty_key(UINT64_MAX);
    m_lookup.set_deleted_key(UINT64_MAX - 1);
}

robust_history :: ~robust_history() throw ()
{
}

bool
robust_history :: has_output(uint64_t nonce,
                             uint64_t min_slot,
                             replicant_returncode* status,
                             std::string* output)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!m_history.empty() && min_slot < m_history.front().slot &&
        m_lookup.find(nonce) == m_lookup.end())
    {
        *status = REPLICANT_MAYBE;
        *output = "";
        return true;
    }

    if (m_lookup.find(nonce) == m_lookup.end())
    {
        return false;
    }

    for (std::list<entry>::iterator it = m_history.begin();
            it != m_history.end(); ++it)
    {
        if (it->nonce == nonce)
        {
            *status = it->status;
            *output = it->output;
            return true;
        }
    }

    abort();
}

void
robust_history :: executed(const pvalue& p,
                           uint64_t command_nonce,
                           replicant_returncode status,
                           const std::string& result)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (m_history.empty())
    {
        m_history.push_back(entry(p.s, command_nonce, status, result));
    }
    // in practice, we'll never hit the next two cases because the RSMs will be
    // scheduled to never overrun the command_nonce history, but it's here as a
    // safety measure.
    else if (m_history.front().slot > p.s)
    {
        m_history.push_front(entry(p.s, command_nonce, status, result));
    }
    else if (m_history.front().slot == p.s)
    {
        return;
    }
    else
    {
        std::list<entry>::iterator it = m_history.end();
        --it;

        while (it != m_history.begin() && it->slot >= p.s)
        {
            --it;
        }

        // it points to the history entry for the highest slot less than p.s
        ++it; // move forward one
        m_history.insert(it, entry(p.s, command_nonce, status, result));
    }

    m_lookup.insert(command_nonce);
    cleanup();
}

void
robust_history :: copy_up_to(robust_history* other, uint64_t slot)
{
    po6::threads::mutex::hold hold(&m_mtx);
    po6::threads::mutex::hold hold2(&other->m_mtx);
    other->m_history.clear();
    other->m_lookup.clear();

    for (std::list<entry>::iterator it = m_history.begin();
            it != m_history.end(); ++it)
    {
        if (it->slot < slot)
        {
            other->m_history.push_back(*it);
            other->m_lookup.insert(it->nonce);
        }
    }
}

void
robust_history :: inhibit_gc()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_inhibit_gc = true;
}

void
robust_history :: allow_gc()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_inhibit_gc = false;
    cleanup();
}

void
robust_history :: cleanup()
{
    if (m_inhibit_gc)
    {
        return;
    }

    while (m_history.size() > REPLICANT_SERVER_DRIVEN_NONCE_HISTORY)
    {
        m_lookup.erase(m_history.front().nonce);
        m_history.pop_front();
    }
}

e::packer
replicant :: operator << (e::packer lhs, robust_history& rhs)
{
    po6::threads::mutex::hold hold(&rhs.m_mtx);
    return lhs << rhs.m_history;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, robust_history& rhs)
{
    po6::threads::mutex::hold hold(&rhs.m_mtx);
	rhs.m_history.clear();
    lhs = lhs >> rhs.m_history;
    rhs.m_lookup.clear();

    for (std::list<robust_history::entry>::iterator it = rhs.m_history.begin();
            it != rhs.m_history.end(); ++it)
    {
        rhs.m_lookup.insert(it->nonce);
    }

    return lhs;
}

e::packer
replicant :: operator << (e::packer lhs, const robust_history::entry& rhs)
{
    return lhs << rhs.slot << rhs.nonce << rhs.status << e::slice(rhs.output);
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, robust_history::entry& rhs)
{
    e::slice o;
    lhs = lhs >> rhs.slot >> rhs.nonce >> rhs.status >> o;
    rhs.output.assign(o.cdata(), o.size());
    return lhs;
}

size_t
replicant :: pack_size(const robust_history::entry& rhs)
{
    e::slice o(rhs.output);
    return 2 * sizeof(uint64_t) + pack_size(rhs.status) + pack_size(o);
}
