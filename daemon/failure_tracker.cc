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

// po6
#include <po6/time.h>

// Replicant
#include "daemon/failure_tracker.h"

using replicant::failure_tracker;

failure_tracker :: failure_tracker(configuration* config)
    : m_config(config)
    , m_us()
{
    assume_all_alive();
}

failure_tracker :: ~failure_tracker() throw ()
{
}

void
failure_tracker :: set_server_id(server_id us)
{
    m_us = us;
}

void
failure_tracker :: assume_all_alive()
{
    const uint64_t now = po6::monotonic_time();

    for (unsigned i = 0; i < REPLICANT_MAX_REPLICAS; ++i)
    {
        m_last_seen[i] = now;
    }
}

void
failure_tracker :: proof_of_life(server_id si)
{
    const std::vector<server>& servers(m_config->servers());

    for (unsigned i = 0; i < servers.size() && i < REPLICANT_MAX_REPLICAS; ++i)
    {
        if (servers[i].id == si)
        {
            m_last_seen[i] = po6::monotonic_time();
        }
    }
}

bool
failure_tracker :: suspect_failed(server_id si, uint64_t timeout)
{
    if (si == m_us)
    {
        return false;
    }

    const std::vector<server>& servers(m_config->servers());
    assert(servers.size() <= REPLICANT_MAX_REPLICAS);
    const uint64_t max_seen = *std::max_element(m_last_seen, m_last_seen + servers.size());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id == m_us)
        {
            m_last_seen[i] = max_seen;
        }
    }

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id == si)
        {
            const uint64_t now = po6::monotonic_time();
            const uint64_t diff = now - m_last_seen[i];
            const uint64_t self_suspicion = now - max_seen;
            const uint64_t susp = diff - self_suspicion;
            return susp > timeout;
        }
    }

    return true;
}
