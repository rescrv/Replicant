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

// e
#include <e/endian.h>

// Replicant
#include "daemon/snapshot.h"

#include <glog/logging.h>
using replicant::snapshot;

snapshot :: snapshot(uint64_t up_to)
    : m_ref(0)
    , m_up_to(up_to)
    , m_mtx()
    , m_cond(&m_mtx)
    , m_failed()
    , m_objects()
    , m_snapshot()
    , m_packer(&m_snapshot)
{
}

snapshot :: ~snapshot() throw ()
{
}

void
snapshot :: wait()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (!done_condition())
    {
        m_cond.wait();
    }
}

void
snapshot :: replica_internals(const e::slice& replica)
{
    m_packer = m_packer << e::pack_memmove(replica.data(), replica.size());
}

void
snapshot :: start_object(const std::string& name)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_objects.insert(name);
}

void
snapshot :: finish_object(const std::string& name, const std::string& snap)
{
    po6::threads::mutex::hold hold(&m_mtx);
    std::set<std::string>::iterator it = m_objects.find(name);

    if (it != m_objects.end())
    {
        m_objects.erase(it);
        m_packer << e::slice(name) << e::slice(snap);
    }

    m_cond.broadcast();
}

void
snapshot :: abort_snapshot()
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!m_objects.empty())
    {
        m_failed = true;
    }

    m_cond.broadcast();
}

bool
snapshot :: done()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return done_condition();
}

const std::string&
snapshot :: contents()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_snapshot;
}

bool
snapshot :: done_condition()
{
    return m_failed || m_objects.empty();
}
