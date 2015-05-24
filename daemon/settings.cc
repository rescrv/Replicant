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
#include "common/packing.h"
#include "daemon/settings.h"

#define NANOS 1ULL
#define MICROS (1000ULL * NANOS)
#define MILLIS (1000ULL * MICROS)
#define SECONDS (1000ULL * MILLIS)

using replicant::settings;

settings :: settings()
    : SUSPECT_TIMEOUT(50 * MILLIS)
    , SUSPECT_STRIKES(5)
    , DEFEND_TIMEOUT(10)
{
}

e::packer
replicant :: operator << (e::packer lhs, const settings& rhs)
{
    return lhs << rhs.SUSPECT_TIMEOUT
               << rhs.SUSPECT_STRIKES
               << rhs.DEFEND_TIMEOUT;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, settings& rhs)
{
    return lhs >> rhs.SUSPECT_TIMEOUT
               >> rhs.SUSPECT_STRIKES
               >> rhs.DEFEND_TIMEOUT;
}

size_t
replicant :: pack_size(const settings&)
{
    return 3 * pack_size(uint64_t());
}
