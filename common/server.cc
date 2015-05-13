// Copyright (c) 2013-2015, Cornell University
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
#include <e/serialization.h>

// Replicant
#include "common/packing.h"
#include "common/server.h"

using replicant::server;

server :: server()
    : id()
    , bind_to()
{
}

server :: server(server_id sid, const po6::net::location& bt)
    : id(sid)
    , bind_to(bt)
{
}

bool
replicant :: operator < (const server& lhs, const server& rhs)
{
    return lhs.id < rhs.id;
}

bool
replicant :: operator == (const server& lhs, const server& rhs)
{
    return lhs.id == rhs.id;
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const server& rhs)
{
    return lhs << "server(id=" << rhs.id.get() << ", bind_to=" << rhs.bind_to << ")";
}

e::packer
replicant :: operator << (e::packer lhs, const server& rhs)
{
    return lhs << rhs.id << rhs.bind_to;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, server& rhs)
{
    return lhs >> rhs.id >> rhs.bind_to;
}

size_t
replicant :: pack_size(const server& rhs)
{
    return pack_size(rhs.id) + pack_size(rhs.bind_to);
}
