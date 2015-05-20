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

#ifndef replicant_daemon_leader_h_
#define replicant_daemon_leader_h_

// STL
#include <map>

// Replicant
#include "namespace.h"
#include "common/ids.h"
#include "daemon/ballot.h"
#include "daemon/pvalue.h"

BEGIN_REPLICANT_NAMESPACE
class daemon;
class scout;
class commander;

class leader
{
    public:
        leader(const scout& s);
        ~leader() throw ();

    public:
        const ballot& current_ballot() const { return m_ballot; }
        const std::vector<server_id>& acceptors() const { return m_acceptors; }
        size_t quorum_size() const { return m_quorum; }
        void send_all_proposals(daemon* d);
        bool accept(server_id si, const pvalue& p);
        void propose(daemon* d,
                     uint64_t slot_start,
                     uint64_t slot_limit,
                     const std::string& c);
        void set_window(daemon* d, uint64_t start, uint64_t limit);
        void fill_window(daemon* d);
        uint64_t window_start() const { return m_start; }
        uint64_t window_limit() const { return m_limit; }
        void garbage_collect(uint64_t below);

    private:
        void adjust_next();
        void insert_nop(daemon* d, uint64_t slot);
        void send_proposal(daemon* d, commander* c);

    private:
        typedef std::map<uint64_t, commander> commander_map_t;
        const ballot m_ballot;
        const std::vector<server_id> m_acceptors;
        const unsigned m_quorum;
        commander_map_t m_commanders;
        uint64_t m_start;
        uint64_t m_limit;
        uint64_t m_next;

    private:
        leader(const leader&);
        leader& operator = (const leader&);
};

std::ostream&
operator << (std::ostream& lhs, const leader& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_leader_h_
