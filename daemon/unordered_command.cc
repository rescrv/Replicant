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

// Replicant
#include "daemon/unordered_command.h"

using replicant::unordered_command;

unordered_command :: unordered_command(server_id obo,
                                       uint64_t rn,
                                       slot_type t,
                                       const std::string& c)
    : m_on_behalf_of(obo)
    , m_request_nonce(rn)
    , m_type(t)
    , m_command(c)
    , m_command_nonce(0)
    , m_last_used_ballot()
    , m_lowest_possible_slot(0)
    , m_robust(false)
{
}

unordered_command :: ~unordered_command() throw ()
{
}

void
unordered_command :: set_command_nonce(uint64_t cn)
{
    assert(m_command_nonce == 0);
    m_command_nonce = cn;
}

void
unordered_command :: set_last_used_ballot(const ballot& b)
{
    m_last_used_ballot = b;
}

void
unordered_command :: set_lowest_possible_slot(uint64_t slot)
{
    assert(!m_robust);
    assert(m_lowest_possible_slot <= slot);
    m_lowest_possible_slot = slot;
}
