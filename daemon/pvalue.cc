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
#include <e/strescape.h>
#include <e/tuple_compare.h>
#include <e/varint.h>

// Replicant
#include "common/packing.h"
#include "daemon/pvalue.h"

using replicant::pvalue;

pvalue :: pvalue()
    : b()
    , s()
    , c()
{
}

pvalue :: pvalue(const ballot& _b, uint64_t _s, const std::string& _c)
    : b(_b)
    , s(_s)
    , c(_c)
{
}

pvalue :: pvalue(const pvalue& other)
    : b(other.b)
    , s(other.s)
    , c(other.c)
{
}

pvalue :: ~pvalue() throw ()
{
}

pvalue&
pvalue :: operator = (const pvalue& rhs)
{
    // no self check
    b = rhs.b;
    s = rhs.s;
    c = rhs.c;
    return *this;
}

int
replicant :: compare(const pvalue& lhs, const pvalue& rhs)
{
    return e::tuple_compare(lhs.b, lhs.s, rhs.b, rhs.s);
}

bool
replicant :: operator < (const pvalue& lhs, const pvalue& rhs)
{
    return compare(lhs, rhs) < 0;
}

bool
replicant :: operator <= (const pvalue& lhs, const pvalue& rhs)
{
    return compare(lhs, rhs) <= 0;
}

bool
replicant :: operator == (const pvalue& lhs, const pvalue& rhs)
{
    return compare(lhs, rhs) == 0;
}

bool
replicant :: operator != (const pvalue& lhs, const pvalue& rhs)
{
    return compare(lhs, rhs) != 0;
}

bool
replicant :: operator >= (const pvalue& lhs, const pvalue& rhs)
{
    return compare(lhs, rhs) >= 0;
}

bool
replicant :: operator > (const pvalue& lhs, const pvalue& rhs)
{
    return compare(lhs, rhs) > 0;
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const pvalue& rhs)
{
    return lhs << "pvalue(" << rhs.b << ", slot=" << rhs.s << ", command=" << e::strescape(rhs.c) << ")";
}

size_t
replicant :: pack_size(const pvalue& p)
{
    return pack_size(p.b) + pack_size(p.s) + pack_size(e::slice(p.c));
}

e::packer
replicant :: operator << (e::packer lhs, const pvalue& rhs)
{
    return lhs << rhs.b << rhs.s << e::slice(rhs.c);
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, pvalue& rhs)
{
    e::slice c;
    lhs = lhs >> rhs.b >> rhs.s >> c;

    if (lhs.error())
    {
        return lhs;
    }

    rhs.c.assign(c.cdata(), c.size());
    return lhs;
}
