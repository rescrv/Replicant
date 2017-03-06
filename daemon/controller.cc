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

// Replicant
#include "daemon/controller.h"

using replicant::controller;

controller :: controller(po6::threads::mutex* mtx, configuration* c)
    : m_mtx(mtx)
    , m_c(c)
    , m_aux()
{
}

controller :: ~controller() throw ()
{
}

po6::net::location
controller :: lookup(uint64_t si)
{
    po6::threads::mutex::hold hold(m_mtx);

    for (size_t i = 0; i < m_aux.size(); ++i)
    {
        if (m_aux[i].id.get() == si)
        {
            return m_aux[i].bind_to;
        }
    }

    for (size_t i = 0; i < m_c->servers().size(); ++i)
    {
        if (m_c->servers()[i].id.get() == si)
        {
            return m_c->servers()[i].bind_to;
        }
    }

    return po6::net::location();
}

void
controller :: add_aux(const server& s)
{
    po6::threads::mutex::hold hold(m_mtx);
    const std::vector<server>& servers(m_c->servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id == s.id && servers[i].bind_to == s.bind_to)
        {
            return;
        }
    }

    for (size_t i = 0; i < m_aux.size(); ++i)
    {
        if (m_aux[i].id == s.id)
        {
            m_aux[i] = s;
            return;
        }
    }

    m_aux.push_back(s);
}

void
controller :: clear_aux()
{
    po6::threads::mutex::hold hold(m_mtx);
    m_aux.clear();
}
