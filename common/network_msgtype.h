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

#ifndef replicant_network_msgtype_h_
#define replicant_network_msgtype_h_

// e
#include <e/buffer.h>

namespace replicant
{

// next avail 29
enum replicant_network_msgtype
{
    REPLNET_NOP                     = 0,
    REPLNET_BOOTSTRAP               = 1,
    REPLNET_INFORM                  = 2,
    REPLNET_SERVER_REGISTER         = 3,
    REPLNET_SERVER_REGISTER_FAILED  = 4,
    REPLNET_SERVER_CHANGE_ADDRESS   = 26,
    REPLNET_SERVER_IDENTIFY         = 27,
    REPLNET_SERVER_IDENTITY         = 28,
    REPLNET_CONFIG_PROPOSE          = 5,
    REPLNET_CONFIG_ACCEPT           = 6,
    REPLNET_CONFIG_REJECT           = 7,
    REPLNET_CLIENT_REGISTER         = 8,
    REPLNET_CLIENT_DISCONNECT       = 9,
    REPLNET_CLIENT_TIMEOUT          = 10,
    REPLNET_CLIENT_UNKNOWN          = 11,
    REPLNET_CLIENT_DECEASED         = 12,
    REPLNET_COMMAND_SUBMIT          = 13,
    REPLNET_COMMAND_ISSUE           = 14,
    REPLNET_COMMAND_ACK             = 15,
    REPLNET_COMMAND_RESPONSE        = 16,
    REPLNET_HEAL_REQ                = 17,
    REPLNET_HEAL_RETRY              = 18,
    REPLNET_HEAL_RESP               = 19,
    REPLNET_HEAL_DONE               = 20,
    REPLNET_STABLE                  = 21,
    REPLNET_CONDITION_WAIT          = 22,
    REPLNET_CONDITION_NOTIFY        = 23,
    REPLNET_PING                    = 24,
    REPLNET_PONG                    = 25
};

std::ostream&
operator << (std::ostream& lhs, replicant_network_msgtype rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const replicant_network_msgtype& rhs);

e::unpacker
operator >> (e::unpacker lhs, replicant_network_msgtype& rhs);

size_t
pack_size(const replicant_network_msgtype& rhs);

} // namespace replicant

#endif // replicant_network_msgtype_h_
