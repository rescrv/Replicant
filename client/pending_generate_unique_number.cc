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
#include <busybee.h>

// Replicant
#include "common/network_msgtype.h"
#include "client/pending_generate_unique_number.h"

using replicant::pending_generate_unique_number;

pending_generate_unique_number :: pending_generate_unique_number(int64_t id,
                                                                 replicant_returncode* st,
                                                                 uint64_t* number)
    : pending(id, st)
    , m_number(number)
{
}

pending_generate_unique_number :: ~pending_generate_unique_number() throw ()
{
}

std::auto_ptr<e::buffer>
pending_generate_unique_number :: request(uint64_t nonce)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_UNIQUE_NUMBER)
                    + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_UNIQUE_NUMBER << nonce;
    return msg;
}

bool
pending_generate_unique_number :: resend_on_failure()
{
    return true;
}

void
pending_generate_unique_number :: handle_response(client*,
                                                  std::auto_ptr<e::buffer>,
                                                  e::unpacker up)
{
    up = up >> *m_number;

    if (up.error() || up.remain())
    {
        PENDING_ERROR(SERVER_ERROR) << "received bad unique number response";
    }
    else
    {
        this->success();
    }
}
