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
#include "common/chain_node.h"
#include "packing.h"

chain_node :: chain_node()
    : address()
    , incoming_port(0)
    , outgoing_port(0)
    , token(0)
{
}

chain_node :: ~chain_node() throw ()
{
}

po6::net::location
chain_node :: receiver() const
{
    return po6::net::location(address, incoming_port);
}

po6::net::location
chain_node :: sender() const
{
    return po6::net::location(address, outgoing_port);
}

bool
operator == (const chain_node& lhs, const chain_node& rhs)
{
    return lhs.address == rhs.address &&
           lhs.incoming_port == rhs.incoming_port &&
           lhs.outgoing_port == rhs.outgoing_port &&
           lhs.token == rhs.token;
}

std::ostream&
operator << (std::ostream& lhs, const chain_node& rhs)
{
    return lhs << "chain_node(ip=" << rhs.address
               << ", incoming_port=" << rhs.incoming_port
               << ", outgoing_port=" << rhs.outgoing_port
               << ", token=" << rhs.token << ")";
}

e::buffer::packer
operator << (e::buffer::packer lhs, const chain_node& rhs)
{
    return lhs << rhs.address
               << rhs.incoming_port << rhs.outgoing_port
               << rhs.token;
}

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, chain_node& rhs)
{
    return lhs >> rhs.address
               >> rhs.incoming_port >> rhs.outgoing_port
               >> rhs.token;
} 

size_t
pack_size(const chain_node& rhs)
{
    return pack_size(rhs.address) + 2 * sizeof(uint16_t) + sizeof(uint64_t);
}
