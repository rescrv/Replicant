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
#include <algorithm>

// Replicant
#include "daemon/failure_manager.h"

using replicant::failure_manager;

failure_manager :: failure_manager()
    : m_fds()
{
}

failure_manager :: ~failure_manager() throw ()
{
}

void
failure_manager :: heartbeat(uint64_t token, uint64_t now)
{
    failure_detector_map_t::iterator it = m_fds.find(token);

    if (it != m_fds.end())
    {
        it->second->heartbeat(now);
    }
}

void
failure_manager :: get_all_suspicions(uint64_t now, std::vector<std::pair<uint64_t, double> >* suspicions)
{
    suspicions->clear();

    for (failure_detector_map_t::iterator it = m_fds.begin(); it != m_fds.end(); ++it)
    {
        suspicions->push_back(std::make_pair(it->first, it->second->suspicion(now)));
    }
}

void
failure_manager :: record_suspicions(uint64_t seqno, const std::vector<std::pair<uint64_t, double> >& suspicions)
{
    // XXX
}

void
failure_manager :: reset(const std::vector<chain_node>& _nodes)
{
    std::vector<uint64_t> nodes;

    for (size_t i = 0; i < _nodes.size(); ++i)
    {
        nodes.push_back(_nodes[i].token);
    }

    std::sort(nodes.begin(), nodes.end());

    for (size_t i = 0; i < nodes.size(); ++i)
    {
        if (m_fds.find(nodes[i]) == m_fds.end())
        {
            std::tr1::shared_ptr<failure_detector> ptr(new failure_detector());
            m_fds.insert(std::make_pair(nodes[i], ptr));
        }
    }

    failure_detector_map_t::iterator it = m_fds.begin();

    while (it != m_fds.end())
    {
        if (!std::binary_search(nodes.begin(), nodes.end(), it->first))
        {
            m_fds.erase(it);
            it = m_fds.begin();
        }
        else
        {
            ++it;
        }
    }
}
