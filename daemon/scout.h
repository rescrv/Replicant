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

#ifndef replicant_daemon_scout_h_
#define replicant_daemon_scout_h_

// Replicant
#include "namespace.h"
#include "common/ids.h"
#include "daemon/ballot.h"
#include "daemon/pvalue.h"

BEGIN_REPLICANT_NAMESPACE

class scout
{
    public:
        struct enqueued_proposal
        {
            enqueued_proposal();
            enqueued_proposal(uint64_t s, uint64_t l, const e::slice& c);
            ~enqueued_proposal() throw ();

            uint64_t start;
            uint64_t limit;
            std::string command;
        };

    public:
        scout(const ballot& b, server_id* acceptors, size_t acceptors_sz);
        ~scout() throw ();

    public:
        bool adopted() const;
        const ballot& current_ballot() const { return m_ballot; }
        const std::vector<server_id>& acceptors() const { return m_acceptors; }
        const std::vector<server_id>& taken_up() const { return m_taken_up; }
        std::vector<server_id> missing() const;
        const std::vector<pvalue>& pvals() const { return m_pvals; }
        bool take_up(server_id si, const pvalue* pvals, size_t pvals_sz);
        void set_window(uint64_t s, uint64_t l) { m_start = s; m_limit = l; }
        uint64_t window_start() const { return m_start; }
        uint64_t window_limit() const { return m_limit; }
        void enqueue(uint64_t start, uint64_t limit, const e::slice& command);
        const std::vector<enqueued_proposal>& enqueued() const { return m_enqueued; }

    private:
        const ballot m_ballot;
        const std::vector<server_id> m_acceptors;
        std::vector<server_id> m_taken_up;
        std::vector<pvalue> m_pvals;
        uint64_t m_start;
        uint64_t m_limit;
        std::vector<enqueued_proposal> m_enqueued;

    private:
        scout(const scout&);
        scout& operator = (const scout&);
};

std::ostream&
operator << (std::ostream& lhs, const scout& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_scout_h_
