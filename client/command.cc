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

#define __STDC_LIMIT_MACROS

// e
#include <e/endian.h>

// Replicant
#include "client/command.h"

using replicant::chain_node;

replicant_client :: command :: command(replicant_returncode* st,
                                       uint64_t n,
                                       std::auto_ptr<e::buffer> m,
                                       const char** output,
                                       size_t* output_sz)
    : m_ref(0)
    , m_nonce(n)
    , m_clientid(n)
    , m_request(m)
    , m_status(st)
    , m_output(output)
    , m_output_sz(output_sz)
    , m_sent_to()
    , m_last_error_desc()
    , m_last_error_file()
    , m_last_error_line()
{
    *st = REPLICANT_GARBAGE;
}

replicant_client :: command :: ~command() throw ()
{
}

void
replicant_client :: command :: set_nonce(uint64_t n)
{
    m_nonce = n;
}

void
replicant_client :: command :: set_sent_to(const chain_node& s)
{
    m_sent_to = s;
}

void
replicant_client :: command :: fail(replicant_returncode status)
{
    *m_status = status;
}

void
replicant_client :: command :: succeed(std::auto_ptr<e::buffer> backing,
                                       const e::slice& resp,
                                       replicant_returncode status)
{
    if (m_output)
    {
        char* base = reinterpret_cast<char*>(backing.get());
        const char* data = reinterpret_cast<const char*>(resp.data());
        assert(data >= base);
        assert(data - base < UINT16_MAX);
        uint16_t diff = data - base;
        assert(diff >= 2);
        e::pack16le(diff, base + diff - 2);
        *m_output = data;
        *m_output_sz = resp.size();
        backing.release();
    }

    *m_status = status;
}
