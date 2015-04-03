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

// e
#include <e/tuple_compare.h>

// Replicant
#include "daemon/ballot.h"

using replicant::ballot;

ballot :: ballot()
    : number(0)
    , leader()
{
}

ballot :: ballot(uint64_t n, server_id l)
    : number(n)
    , leader(l)
{
}

ballot :: ballot(const ballot& other)
    : number(other.number)
    , leader(other.leader)
{
}

ballot :: ~ballot() throw ()
{
}

ballot&
ballot :: operator = (const ballot& rhs)
{
    number = rhs.number;
    leader = rhs.leader;
    return *this;
}

int
replicant :: compare(const ballot& lhs, const ballot& rhs)
{
    return e::tuple_compare(lhs.number, lhs.leader,
                            rhs.number, rhs.leader);
}

bool
replicant :: operator < (const ballot& lhs, const ballot& rhs)
{
    return compare(lhs, rhs) < 0;
}

bool
replicant :: operator <= (const ballot& lhs, const ballot& rhs)
{
    return compare(lhs, rhs) <= 0;
}

bool
replicant :: operator == (const ballot& lhs, const ballot& rhs)
{
    return compare(lhs, rhs) == 0;
}

bool
replicant :: operator != (const ballot& lhs, const ballot& rhs)
{
    return compare(lhs, rhs) != 0;
}

bool
replicant :: operator >= (const ballot& lhs, const ballot& rhs)
{
    return compare(lhs, rhs) >= 0;
}

bool
replicant :: operator > (const ballot& lhs, const ballot& rhs)
{
    return compare(lhs, rhs) > 0;
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const ballot& rhs)
{
    return lhs << "ballot(" << rhs.number << ", " << rhs.leader << ")";
}

e::packer
replicant :: operator << (e::packer lhs, const ballot& rhs)
{
    return lhs << rhs.number << rhs.leader;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, ballot& rhs)
{
    return lhs >> rhs.number >> rhs.leader;
}

size_t
replicant :: pack_size(const ballot& p)
{
    return sizeof(uint64_t) + pack_size(p.leader);
}
