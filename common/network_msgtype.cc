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
replicant :: operator << (std::ostream& lhs, replicant_network_msgtype rhs)
{
    switch (rhs)
    {
        STRINGIFY(REPLNET_NOP);
        STRINGIFY(REPLNET_BOOTSTRAP);
        STRINGIFY(REPLNET_INFORM);
        STRINGIFY(REPLNET_SERVER_REGISTER);
        STRINGIFY(REPLNET_SERVER_REGISTER_FAILED);
        STRINGIFY(REPLNET_SERVER_CHANGE_ADDRESS);
        STRINGIFY(REPLNET_SERVER_IDENTIFY);
        STRINGIFY(REPLNET_SERVER_IDENTITY);
        STRINGIFY(REPLNET_CONFIG_PROPOSE);
        STRINGIFY(REPLNET_CONFIG_ACCEPT);
        STRINGIFY(REPLNET_CONFIG_REJECT);
        STRINGIFY(REPLNET_CLIENT_REGISTER);
        STRINGIFY(REPLNET_CLIENT_DISCONNECT);
        STRINGIFY(REPLNET_CLIENT_TIMEOUT);
        STRINGIFY(REPLNET_CLIENT_UNKNOWN);
        STRINGIFY(REPLNET_CLIENT_DECEASED);
        STRINGIFY(REPLNET_COMMAND_SUBMIT);
        STRINGIFY(REPLNET_COMMAND_ISSUE);
        STRINGIFY(REPLNET_COMMAND_ACK);
        STRINGIFY(REPLNET_COMMAND_RESPONSE);
        STRINGIFY(REPLNET_HEAL_REQ);
        STRINGIFY(REPLNET_HEAL_RETRY);
        STRINGIFY(REPLNET_HEAL_RESP);
        STRINGIFY(REPLNET_HEAL_DONE);
        STRINGIFY(REPLNET_STABLE);
        STRINGIFY(REPLNET_CONDITION_WAIT);
        STRINGIFY(REPLNET_CONDITION_NOTIFY);
        STRINGIFY(REPLNET_PING);
        STRINGIFY(REPLNET_PONG);
        default:
            lhs << "unknown msgtype";
    }

    return lhs;
}

e::buffer::packer
replicant :: operator << (e::buffer::packer lhs, const replicant_network_msgtype& rhs)
{
    uint8_t mt = static_cast<uint8_t>(rhs);
    return lhs << mt;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, replicant_network_msgtype& rhs)
{
    uint8_t mt;
    lhs = lhs >> mt;
    rhs = static_cast<replicant_network_msgtype>(mt);
    return lhs;
}

size_t
replicant :: pack_size(const replicant_network_msgtype&)
{
    return sizeof(uint8_t);
}
