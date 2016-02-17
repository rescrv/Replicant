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
#include "daemon/commander.h"

using replicant::commander;

commander :: commander(const pvalue& p)
    : m_pval(p)
    , m_accepted_by()
{
    for (size_t i = 0; i < REPLICANT_MAX_REPLICAS; ++i)
    {
        m_timestamps[i] = 0;
    }
}

commander :: commander(const commander& other)
    : m_pval(other.m_pval)
    , m_accepted_by(other.m_accepted_by)
{
    for (size_t i = 0; i < REPLICANT_MAX_REPLICAS; ++i)
    {
        m_timestamps[i] = other.m_timestamps[i];
    }
}

commander :: ~commander() throw ()
{
}

bool
commander :: accepted_by(server_id si)
{
    return std::find(m_accepted_by.begin(), m_accepted_by.end(), si) != m_accepted_by.end();
}

void
commander :: accept(server_id si)
{
    if (!accepted_by(si))
    {
        m_accepted_by.push_back(si);
    }
}

size_t
commander :: accepted()
{
    return m_accepted_by.size();
}

uint64_t
commander :: timestamp(unsigned idx)
{
    assert(idx < REPLICANT_MAX_REPLICAS);
    return m_timestamps[idx];
}

void
commander :: timestamp(unsigned idx, uint64_t ts)
{
    assert(idx < REPLICANT_MAX_REPLICAS);
    m_timestamps[idx] = ts;
}

commander&
commander :: operator = (const commander& rhs)
{
    m_pval = rhs.m_pval;
    m_accepted_by = rhs.m_accepted_by;

    for (size_t i = 0; i < REPLICANT_MAX_REPLICAS; ++i)
    {
        m_timestamps[i] = rhs.m_timestamps[i];
    }

    return *this;
}
