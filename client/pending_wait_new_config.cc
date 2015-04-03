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
#include <busybee_constants.h>

// Replicant
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "client/client.h"
#include "client/pending_wait_new_config.h"

using replicant::pending_wait_new_config;

pending_wait_new_config :: pending_wait_new_config(client* cl, version_id current)
    : pending(-1, &m_status)
    , m_client(cl)
    , m_current(current)
    , m_status()
{
}

pending_wait_new_config :: ~pending_wait_new_config() throw ()
{
}

std::auto_ptr<e::buffer>
pending_wait_new_config :: request(uint64_t nonce)
{
    e::slice obj("replicant");
    e::slice cond("configuration");
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_COND_WAIT)
                    + 2 * sizeof(uint64_t)
                    + pack_size(obj)
                    + pack_size(cond);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_COND_WAIT << nonce << obj << cond << (m_current.get() + 1);
    return msg;
}

bool
pending_wait_new_config :: resend_on_failure()
{
    return true;
}

void
pending_wait_new_config :: handle_response(std::auto_ptr<e::buffer>,
                                           e::unpacker)
{
    m_client->bump_config_cond_state(m_current.get() + 1);
}
