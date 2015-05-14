// Copyright (c) 2012-2015, Robert Escriva
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

#ifndef replicant_common_network_msgtype_h_
#define replicant_common_network_msgtype_h_

// e
#include <e/buffer.h>

// Replicant
#include "namespace.h"

BEGIN_REPLICANT_NAMESPACE

enum network_msgtype
{
    REPLNET_NOP                     = 0,

    REPLNET_BOOTSTRAP               = 28,
    REPLNET_SILENT_BOOTSTRAP        = 27,
    REPLNET_PING                    = 29,
    REPLNET_PONG                    = 30,
    REPLNET_STATE_TRANSFER          = 31,
    REPLNET_SUGGEST_REJOIN          = 26,
    REPLNET_WHO_ARE_YOU             = 25,
    REPLNET_IDENTITY                = 24,

    REPLNET_PAXOS_PHASE1A           = 32,
    REPLNET_PAXOS_PHASE1B           = 33,
    REPLNET_PAXOS_PHASE2A           = 34,
    REPLNET_PAXOS_PHASE2B           = 35,
    REPLNET_PAXOS_LEARN             = 36,
    REPLNET_PAXOS_SUBMIT            = 37,

    REPLNET_SERVER_BECOME_MEMBER    = 48,
    REPLNET_UNIQUE_NUMBER           = 63,
    REPLNET_OBJECT_FAILED           = 62,
    REPLNET_POKE                    = 64,
    REPLNET_COND_WAIT               = 69,
    REPLNET_CALL                    = 70,
    REPLNET_GET_ROBUST_PARAMS       = 72,
    REPLNET_CALL_ROBUST             = 73,

    REPLNET_CLIENT_RESPONSE         = 224,

    REPLNET_GARBAGE                 = 255
};

std::ostream&
operator << (std::ostream& lhs, network_msgtype rhs);

e::packer
operator << (e::packer lhs, const network_msgtype& rhs);
e::unpacker
operator >> (e::unpacker lhs, network_msgtype& rhs);
size_t
pack_size(const network_msgtype& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_common_network_msgtype_h_
