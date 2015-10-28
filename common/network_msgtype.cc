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

// C++
#include <iostream>

// Replicant
#include "common/macros.h"
#include "common/network_msgtype.h"

std::ostream&
replicant :: operator << (std::ostream& lhs, network_msgtype rhs)
{
    switch (rhs)
    {
        STRINGIFY(REPLNET_NOP);
        STRINGIFY(REPLNET_BOOTSTRAP);
        STRINGIFY(REPLNET_PING);
        STRINGIFY(REPLNET_PONG);
        STRINGIFY(REPLNET_STATE_TRANSFER);
        STRINGIFY(REPLNET_WHO_ARE_YOU);
        STRINGIFY(REPLNET_IDENTITY);
        STRINGIFY(REPLNET_PAXOS_PHASE1A);
        STRINGIFY(REPLNET_PAXOS_PHASE1B);
        STRINGIFY(REPLNET_PAXOS_PHASE2A);
        STRINGIFY(REPLNET_PAXOS_PHASE2B);
        STRINGIFY(REPLNET_PAXOS_LEARN);
        STRINGIFY(REPLNET_PAXOS_SUBMIT);
        STRINGIFY(REPLNET_SERVER_BECOME_MEMBER);
        STRINGIFY(REPLNET_UNIQUE_NUMBER);
        STRINGIFY(REPLNET_OBJECT_FAILED);
        STRINGIFY(REPLNET_POKE);
        STRINGIFY(REPLNET_COND_WAIT);
        STRINGIFY(REPLNET_CALL);
        STRINGIFY(REPLNET_GET_ROBUST_PARAMS);
        STRINGIFY(REPLNET_CALL_ROBUST);
        STRINGIFY(REPLNET_CLIENT_RESPONSE);
        STRINGIFY(REPLNET_GARBAGE);
        default:
            lhs << "unknown msgtype";
    }

    return lhs;
}

e::packer
replicant :: operator << (e::packer lhs, const network_msgtype& rhs)
{
    uint8_t mt = static_cast<uint8_t>(rhs);
    return lhs << mt;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, network_msgtype& rhs)
{
    uint8_t mt;
    lhs = lhs >> mt;
    rhs = static_cast<network_msgtype>(mt);
    return lhs;
}

size_t
replicant :: pack_size(const network_msgtype&)
{
    return sizeof(uint8_t);
}
