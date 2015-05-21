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
#include "busybee_constants.h"

// Replicant
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "client/client.h"
#include "client/pending_cond_follow.h"

using replicant::pending_cond_follow;

pending_cond_follow :: pending_cond_follow(int64_t id,
                                           const char* object, const char* cond,
                                           replicant_returncode* st,
                                           uint64_t* state,
                                           char** data, size_t* data_sz)
    : pending(id, st)
    , m_object(object)
    , m_cond(cond)
    , m_state(state)
    , m_data(data)
    , m_data_sz(data_sz)
    , m_has_callback(false)
    , m_callback()
{
    *m_state = 0;

    if (m_data)
    {
        *m_data = NULL;
        *m_data_sz = 0;
    }
}

pending_cond_follow :: pending_cond_follow(int64_t id,
                                           const char* object, const char* cond,
                                           replicant_returncode* st,
                                           uint64_t* state,
                                           char** data, size_t* data_sz,
                                           void (client::*callback)())
    : pending(id, st)
    , m_object(object)
    , m_cond(cond)
    , m_state(state)
    , m_data(data)
    , m_data_sz(data_sz)
    , m_has_callback(true)
    , m_callback(callback)
{
    *m_state = 0;

    if (m_data)
    {
        *m_data = NULL;
        *m_data_sz = 0;
    }
}

pending_cond_follow :: ~pending_cond_follow() throw ()
{
}

std::auto_ptr<e::buffer>
pending_cond_follow :: request(uint64_t nonce)
{
    e::slice obj(m_object);
    e::slice cond(m_cond);
    uint64_t state = *m_state + 1;
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_COND_WAIT)
                    + 2 * sizeof(uint64_t)
                    + pack_size(obj)
                    + pack_size(cond);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_COND_WAIT << nonce << obj << cond << state;
    return msg;
}

bool
pending_cond_follow :: resend_on_failure()
{
    return true;
}

void
pending_cond_follow :: handle_response(client* cl, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t state;
    e::slice data;
    replicant_returncode st;
    up = up >> st >> state >> data;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "received bad cond_follow response";
    }
    else
    {
        this->set_status(st);

        if (st == REPLICANT_SUCCESS)
        {
            *m_state = state;

            if (m_data && data.size() == 0)
            {
                *m_data = NULL;
                *m_data_sz = 0;
            }
            else if (m_data)
            {
                if (*m_data)
                {
                    *m_data = static_cast<char*>(realloc(*m_data, data.size()));
                }
                else
                {
                    *m_data = static_cast<char*>(malloc(data.size()));
                }

                if (!*m_data)
                {
                    throw std::bad_alloc();
                }

                *m_data_sz = data.size();
                memmove(*m_data, data.data(), data.size());
            }

            if (m_has_callback)
            {
                (cl->*m_callback)();
            }
        }
    }

    cl->send(this);
}
