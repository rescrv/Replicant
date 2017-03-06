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

// BusyBee
#include "busybee.h"

// Replicant
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "client/pending_cond_wait.h"

using replicant::pending_cond_wait;

pending_cond_wait :: pending_cond_wait(int64_t id,
                                       const char* object, const char* cond,
                                       uint64_t state,
                                       replicant_returncode* st,
                                       char** data, size_t* data_sz)
    : pending(id, st)
    , m_object(object)
    , m_cond(cond)
    , m_state(state)
    , m_data(data)
    , m_data_sz(data_sz)
{
    if (m_data)
    {
        *m_data = NULL;
        *m_data_sz = 0;
    }
}

pending_cond_wait :: ~pending_cond_wait() throw ()
{
}

std::auto_ptr<e::buffer>
pending_cond_wait :: request(uint64_t nonce)
{
    e::slice obj(m_object);
    e::slice cond(m_cond);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_COND_WAIT)
                    + 2 * sizeof(uint64_t)
                    + pack_size(obj)
                    + pack_size(cond);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_COND_WAIT << nonce << obj << cond << m_state;
    return msg;
}

bool
pending_cond_wait :: resend_on_failure()
{
    return true;
}

void
pending_cond_wait :: handle_response(client*, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t state;
    e::slice data;
    replicant_returncode st;
    up = up >> st >> state >> data;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "received bad cond_wait response";
    }
    else
    {
        this->set_status(st);

        if (m_data)
        {
            *m_data = static_cast<char*>(malloc(data.size()));
            *m_data_sz = data.size();
            memmove(*m_data, data.data(), data.size());
        }
    }
}
