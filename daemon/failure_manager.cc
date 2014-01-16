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
#include <cmath>

// BusyBee
#include "busybee_constants.h"

// Replicant
#include "common/network_msgtype.h"
#include "daemon/failure_detector.h"
#include "daemon/failure_manager.h"

using replicant::failure_detector;
using replicant::failure_manager;

failure_manager :: failure_manager()
    : m_fds()
{
}

failure_manager :: ~failure_manager() throw ()
{
}

void
failure_manager :: track(const std::vector<uint64_t>& tokens,
                         uint64_t interval, uint64_t window_sz)
{
    for (size_t i = 1; i < tokens.size(); ++i)
    {
        assert(tokens[i - 1] < tokens[i]);
    }

    size_t old_idx = 0;
    size_t new_idx = 0;
    fd_vector_t replacement;
    replacement.reserve(tokens.size());

    while (old_idx < m_fds.size() && new_idx < tokens.size())
    {
        if (m_fds[old_idx].first == tokens[new_idx])
        {
            replacement.push_back(m_fds[old_idx]);
            ++old_idx;
            ++new_idx;
        }
        else if (m_fds[old_idx].first < tokens[new_idx])
        {
            ++old_idx;
        }
        else if (m_fds[old_idx].first > tokens[new_idx])
        {
            fd_ptr fd = new failure_detector(interval, window_sz);
            replacement.push_back(std::make_pair(tokens[new_idx], fd));
            ++new_idx;
        }
    }

    while (new_idx < tokens.size())
    {
        fd_ptr fd = new failure_detector(interval, window_sz);
        replacement.push_back(std::make_pair(tokens[new_idx], fd));
        ++new_idx;
    }

    m_fds.swap(replacement);
}

void
failure_manager :: ping(daemon* d,
                        bool (daemon::*s)(uint64_t, std::auto_ptr<e::buffer>),
                        uint64_t version)
{
    for (size_t i = 0; i < m_fds.size(); ++i)
    {
        uint64_t seqno = m_fds[i].second->seqno();
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_PING)
                  + sizeof(uint64_t)
                  + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PING << version << seqno;
        (*d.*s)(m_fds[i].first, msg);
    }
}

void
failure_manager :: pong(uint64_t token, uint64_t seqno, uint64_t now)
{
    failure_detector* fd = find(token);

    if (!fd)
    {
        return;
    }

    fd->heartbeat(seqno, now);
}

double
failure_manager :: suspicion(uint64_t token, uint64_t now) const
{
    failure_detector* fd = find(token);

    if (!fd)
    {
        return -1;
    }

    return fd->suspicion(now);
}

namespace
{

struct suspicion_stats
{
    uint64_t token;
    double suspicion;
    double mean;
    double stdev;
};

bool
compare_suspicions(const suspicion_stats& lhs, const suspicion_stats& rhs)
{
    return lhs.suspicion < rhs.suspicion;
}

}

void
failure_manager :: get_suspicions(uint64_t now, std::vector<uint64_t>* tokens, size_t* cutoff)
{
    std::vector<suspicion_stats> stats;
    stats.reserve(tokens->size());

    for (size_t i = 0; i < tokens->size(); ++i)
    {
        double d = suspicion((*tokens)[i], now);

        if (d < 0)
        {
            d = HUGE_VAL;
        }

        stats.push_back(suspicion_stats());
        stats.back().token = (*tokens)[i];
        stats.back().suspicion = d;
    }

    std::sort(stats.begin(), stats.end(), compare_suspicions);
    double n = 0;
    double mean = 0;
    double M2 = 0;

    for (size_t i = 0; i < stats.size(); ++i)
    {
        if (std::isinf(stats[i].suspicion))
        {
            break;
        }

        ++n;
        double delta = stats[i].suspicion - mean;
        mean = mean + delta / n;
        M2 = M2 + delta * (stats[i].suspicion - mean);
        stats[i].mean = mean;
        stats[i].stdev = n > 1 ? sqrt(M2 / (n - 1)) : 0;
    }

    for (size_t i = 0; i < stats.size(); ++i)
    {
        (*tokens)[i] = stats[i].token;
    }

    *cutoff = 0;

    for (; *cutoff < stats.size(); ++*cutoff)
    {
        if (std::isinf(stats[*cutoff].suspicion))
        {
            break;
        }
        else if (stats[*cutoff].suspicion > 10.0)
        {
            break;
        }
        else if (*cutoff > 0)
        {
            double threshold = stats[*cutoff - 1].mean
                             + 6 * std::max(stats[*cutoff - 1].stdev, 0.25);

            if (stats[*cutoff].suspicion > threshold)
            {
                break;
            }
        }
    }
}

failure_detector*
failure_manager :: find(uint64_t token) const
{
    fd_vector_t::const_iterator it = std::lower_bound(m_fds.begin(), m_fds.end(),
                                                      std::make_pair(token, fd_ptr()));

    if (it == m_fds.end() || it->first != token)
    {
        return NULL;
    }

    return it->second.get();
}
