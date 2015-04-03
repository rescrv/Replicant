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

#ifndef replicant_daemon_ballot_h_
#define replicant_daemon_ballot_h_

// Replicant
#include "namespace.h"
#include "common/ids.h"

BEGIN_REPLICANT_NAMESPACE

class ballot
{
    public:
        ballot();
        ballot(uint64_t number, server_id leader);
        ballot(const ballot& other);
        ~ballot() throw ();

    public:
        ballot& operator = (const ballot& rhs);

    public:
        uint64_t number;
        server_id leader;
};

int
compare(const ballot& lhs, const ballot& rhs);

bool operator < (const ballot& lhs, const ballot& rhs);
bool operator <= (const ballot& lhs, const ballot& rhs);
bool operator == (const ballot& lhs, const ballot& rhs);
bool operator != (const ballot& lhs, const ballot& rhs);
bool operator >= (const ballot& lhs, const ballot& rhs);
bool operator > (const ballot& lhs, const ballot& rhs);

std::ostream&
operator << (std::ostream& lhs, const ballot& rhs);

e::packer
operator << (e::packer lhs, const ballot& rhs);
e::unpacker
operator >> (e::unpacker lhs, ballot& rhs);
size_t
pack_size(const ballot& p);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_ballot_h_
