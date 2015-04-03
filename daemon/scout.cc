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
#include "common/constants.h"
#include "daemon/scout.h"

using replicant::scout;

scout :: enqueued_proposal :: enqueued_proposal()
    : start(0)
    , limit(0)
    , command()
{
}

scout :: enqueued_proposal :: enqueued_proposal(uint64_t s, uint64_t l, const e::slice& c)
    : start(s)
    , limit(l)
    , command(c.cdata(), c.size())
{
}


scout :: enqueued_proposal :: ~enqueued_proposal() throw ()
{
}

scout :: scout(const ballot& b, server_id* a, size_t a_sz)
    : m_ballot(b)
    , m_acceptors(a, a + a_sz)
    , m_taken_up()
    , m_pvals()
    , m_start()
    , m_limit(REPLICANT_SLOTS_WINDOW)
    , m_enqueued()
{
    assert(a_sz > 0);
}

scout :: ~scout() throw ()
{
}

bool
scout :: adopted() const
{
    return m_taken_up.size() > m_acceptors.size() - m_taken_up.size();
}

std::vector<replicant::server_id>
scout :: missing() const
{
    std::vector<server_id> ret;

    for (size_t i = 0; i < m_acceptors.size(); ++i)
    {
        if (std::find(m_taken_up.begin(), m_taken_up.end(), m_acceptors[i]) == m_taken_up.end())
        {
            ret.push_back(m_acceptors[i]);
        }
    }

    return ret;
}

bool
scout :: take_up(server_id si, const pvalue* p, size_t p_sz)
{
    if (std::find(m_taken_up.begin(), m_taken_up.end(), si) != m_taken_up.end() ||
        std::find(m_acceptors.begin(), m_acceptors.end(), si) == m_acceptors.end())
    {
        return false;
    }

    m_taken_up.push_back(si);

    for (size_t i = 0; i < p_sz; ++i)
    {
        m_pvals.push_back(p[i]);
    }

    std::sort(m_pvals.begin(), m_pvals.end());
    std::vector<pvalue>::iterator it = std::unique(m_pvals.begin(), m_pvals.end());
    m_pvals.resize(it - m_pvals.begin());
    return true;
}

void
scout :: enqueue(uint64_t start, uint64_t limit, const e::slice& command)
{
    m_enqueued.push_back(enqueued_proposal(start, limit, command));
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const scout& rhs)
{
    return lhs << "scout(" << rhs.current_ballot() << ")";
}
