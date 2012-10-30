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

#ifndef replicant_daemon_failure_manager_h_
#define replicant_daemon_failure_manager_h_

// C
#include <stdint.h>

// STL
#include <map>
#include <tr1/memory>
#include <vector>

// Replicant
#include "common/chain_node.h"
#include "daemon/failure_detector.h"

namespace replicant
{

class failure_manager
{
    public:
        failure_manager();
        ~failure_manager() throw ();

    public:
        void force_trust();
        void force_suspect();
        void heartbeat(uint64_t token, uint64_t now);

    public:
        void get_all_suspicions(uint64_t now, std::vector<std::pair<uint64_t, double> >* suspicions);
        void record_suspicions(uint64_t seqno, const std::vector<std::pair<uint64_t, double> >& suspicions);
        void reset(const std::vector<chain_node>& nodes);

    private:
        typedef std::map<uint64_t, std::tr1::shared_ptr<failure_detector> > failure_detector_map_t;
        failure_detector_map_t m_fds;
};

} // namespace replicant

#endif // replicant_daemon_failure_manager_h_
