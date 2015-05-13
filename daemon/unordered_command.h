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

#ifndef replicant_daemon_unordered_command_h_
#define replicant_daemon_unordered_command_h_

// Replicant
#include "namespace.h"
#include "common/ids.h"
#include "daemon/ballot.h"
#include "daemon/slot_type.h"

BEGIN_REPLICANT_NAMESPACE

class unordered_command
{
    public:
        unordered_command(server_id on_behalf_of,
                          uint64_t request_nonce,
                          slot_type t,
                          const std::string& command);
        virtual ~unordered_command() throw ();

    public:
        server_id on_behalf_of() const { return m_on_behalf_of; }
        uint64_t request_nonce() const { return m_request_nonce; }
        slot_type type() const { return m_type; }
        const std::string& command() const { return m_command; }
        uint64_t command_nonce() const { return m_command_nonce; }
        void set_command_nonce(uint64_t command_nonce);

        const ballot& last_used_ballot() const { return m_last_used_ballot; }
        void set_last_used_ballot(const ballot& b);

        uint64_t lowest_possible_slot() const { return m_lowest_possible_slot; }
        void set_lowest_possible_slot(uint64_t slot);

        bool robust() const { return m_robust; }
        void set_robust() { m_robust = true; }

    private:
        const server_id m_on_behalf_of;
        const uint64_t m_request_nonce;
        const slot_type m_type;
        const std::string m_command;
        uint64_t m_command_nonce;
        ballot m_last_used_ballot;
        uint64_t m_lowest_possible_slot;
        bool m_robust;

    // noncopyable
    private:
        unordered_command(const unordered_command&);
        unordered_command& operator = (const unordered_command&);
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_unordered_command_h_
