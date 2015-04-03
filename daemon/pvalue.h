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

#ifndef replicant_daemon_pvalue_h_
#define replicant_daemon_pvalue_h_

// Replicant
#include "namespace.h"
#include "common/ids.h"
#include "daemon/ballot.h"

BEGIN_REPLICANT_NAMESPACE

class pvalue
{
    public:
        pvalue();
        pvalue(const ballot& b, uint64_t s, const std::string& c);
        pvalue(const pvalue& other);
        ~pvalue() throw ();

    public:
        pvalue& operator = (const pvalue& rhs);

    public:
        ballot b;
        uint64_t s;
        std::string c;
};

int
compare(const pvalue& lhs, const pvalue& rhs);

bool operator < (const pvalue& lhs, const pvalue& rhs);
bool operator <= (const pvalue& lhs, const pvalue& rhs);
bool operator == (const pvalue& lhs, const pvalue& rhs);
bool operator != (const pvalue& lhs, const pvalue& rhs);
bool operator >= (const pvalue& lhs, const pvalue& rhs);
bool operator > (const pvalue& lhs, const pvalue& rhs);

std::ostream&
operator << (std::ostream& lhs, const pvalue& rhs);

size_t
pack_size(const pvalue& p);
e::packer
operator << (e::packer lhs, const pvalue& rhs);
e::unpacker
operator >> (e::unpacker lhs, pvalue& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_pvalue_h_
