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

#ifndef replicant_daemon_commander_h_
#define replicant_daemon_commander_h_

// Replicant
#include "namespace.h"
#include "common/constants.h"
#include "daemon/pvalue.h"

BEGIN_REPLICANT_NAMESPACE

class commander
{
    public:
        commander(const pvalue& pval);
        commander(const commander&);
        ~commander() throw ();

    public:
        const pvalue& pval() const { return m_pval; }
        void set_ballot(const ballot& b) { m_pval.b = b; }
        bool accepted_by(server_id si);
        void accept(server_id si);
        size_t accepted();
        uint64_t timestamp(unsigned idx);
        void timestamp(unsigned idx, uint64_t ts);

    public:
        commander& operator = (const commander&);

    private:
        pvalue m_pval;
        std::vector<server_id> m_accepted_by;
        uint64_t m_timestamps[REPLICANT_MAX_REPLICAS];
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_commander_h_
