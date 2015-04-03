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

// Replicant
#include "common/constants.h"
#include "common/quorum_calc.h"
#include "daemon/daemon.h"
#include "daemon/leader.h"
#include "daemon/scout.h"
#include "daemon/commander.h"

using replicant::leader;

leader :: leader(const scout& s)
    : m_ballot(s.current_ballot())
    , m_acceptors(s.taken_up())
    , m_quorum(quorum_calc(s.acceptors().size()))
    , m_commanders()
    , m_start(s.window_start())
    , m_limit(s.window_limit())
{
    for (size_t i = 0; i < s.pvals().size(); ++i)
    {
        const pvalue& p(s.pvals()[i]);

        if (p.s < m_start)
        {
            continue;
        }

        commander_map_t::iterator it = m_commanders.find(p.s);

        if (it == m_commanders.end())
        {
            m_commanders.insert(std::make_pair(p.s, commander(p)));
        }
        else
        {
            if (it->second.pval().b < p.b)
            {
                it->second = commander(p);
            }
        }
    }

    for (commander_map_t::iterator it = m_commanders.begin();
            it != m_commanders.end(); ++it)
    {
        it->second.set_ballot(current_ballot());
    }

    const uint64_t start = m_commanders.empty() ? 0 : m_commanders.begin()->first;
    const uint64_t limit = m_commanders.empty() ? 0 : m_commanders.rbegin()->first;

    for (size_t slot = start; slot < limit; ++slot)
    {
        commander_map_t::iterator it = m_commanders.find(slot);

        if (it == m_commanders.end())
        {
            pvalue pval(current_ballot(), slot, std::string());
            m_commanders.insert(std::make_pair(slot, pval));
        }
    }

    uint64_t next = limit;
    const std::vector<scout::enqueued_proposal>& enqueued(s.enqueued());

    for (size_t i = 0; i < enqueued.size(); ++i)
    {
        if (enqueued[i].start <= next && next < enqueued[i].limit)
        {
            pvalue pval(current_ballot(), next, enqueued[i].command);
            m_commanders.insert(std::make_pair(next, pval));
            ++next;
        }
    }
}

leader :: ~leader() throw ()
{
}

void
leader :: send_all_proposals(daemon* d)
{
    for (commander_map_t::iterator it = m_commanders.begin();
            it != m_commanders.end(); ++it)
    {
        send_proposal(d, &it->second);
    }
}

bool
leader :: accept(server_id si, const pvalue& p)
{
    if (std::find(m_acceptors.begin(), m_acceptors.end(), si) == m_acceptors.end())
    {
        return false;
    }

    commander_map_t::iterator it = m_commanders.find(p.s);

    if (it == m_commanders.end())
    {
        return false;
    }

    if (it->second.pval() != p)
    {
        return false;
    }

    it->second.accept(si);
    return it->second.accepted() >= m_quorum;
}

void
leader :: nop_fill(daemon* d, uint64_t limit)
{
    for (size_t slot = m_start; slot < limit; ++slot)
    {
        commander_map_t::iterator it = m_commanders.find(slot);

        if (it == m_commanders.end())
        {
            pvalue pval(current_ballot(), slot, std::string());
            std::pair<commander_map_t::iterator, bool> ins;
            ins = m_commanders.insert(std::make_pair(slot, pval));
            assert(ins.second);
            send_proposal(d, &ins.first->second);
        }
    }
}

void
leader :: propose(daemon* d, uint64_t slot_start, uint64_t slot_limit, const std::string& c)
{
    const uint64_t search_start = std::max(slot_start, m_start);
    uint64_t slot = 0;

    for (slot = search_start; slot < slot_limit; ++slot)
    {
        commander_map_t::iterator it = m_commanders.find(slot);

        if (it == m_commanders.end())
        {
            break;
        }
    }

    if (slot >= slot_limit)
    {
        return;
    }

    assert(m_commanders.find(slot) == m_commanders.end());
    pvalue pval(current_ballot(), slot, c);
    commander_map_t::iterator it = m_commanders.insert(std::make_pair(slot, commander(pval))).first;
    send_proposal(d, &it->second);
}

void
leader :: set_window(daemon* d, uint64_t start, uint64_t limit)
{
    assert(start >= m_start);
    assert(limit >= m_limit);
    const uint64_t max_slot = m_commanders.empty() ? m_start : m_commanders.rbegin()->first;
    const uint64_t old_limit = m_limit;
    m_start = start;
    m_limit = limit;

    for (uint64_t i = old_limit; i < m_limit; ++i)
    {
        commander_map_t::iterator it = m_commanders.find(i);

        if (it == m_commanders.end())
        {
            continue;
        }

        send_proposal(d, &it->second);
    }

    nop_fill(d, max_slot);
}

void
leader :: garbage_collect(uint64_t below)
{
    while (!m_commanders.empty() && m_commanders.begin()->first < below)
    {
        m_commanders.erase(m_commanders.begin());
    }
}

void
leader :: send_proposal(daemon* d, commander* c)
{
    if (c->pval().s < m_start ||
        c->pval().s >= m_limit)
    {
        return;
    }

    for (size_t i = 0; i < m_acceptors.size(); ++i)
    {
        if (!c->accepted_by(m_acceptors[i]))
        {
            d->send_paxos_phase2a(m_acceptors[i], c->pval());
        }
    }
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const leader& rhs)
{
    return lhs << "leader(" << rhs.current_ballot() << ")";
}
