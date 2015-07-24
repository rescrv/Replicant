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

#ifndef replicant_daemon_acceptor_h_
#define replicant_daemon_acceptor_h_

// STL
#include <memory>

// po6
#include <po6/path.h>

// Replicant
#include "namespace.h"
#include "common/bootstrap.h"
#include "common/server.h"
#include "daemon/ballot.h"
#include "daemon/pvalue.h"

BEGIN_REPLICANT_NAMESPACE

class acceptor
{
    public:
        acceptor();
        ~acceptor() throw ();

    public:
        // This *will* change the current directory to dir.
        bool open(const std::string& dir,
                  bool* saved, server* saved_us,
                  bootstrap* saved_bootstrap);
        bool save(server saved_us,
                  const bootstrap& saved_bootstrap);

    public:
        const ballot& current_ballot() { return m_ballot; }
        const std::vector<pvalue>& pvals();
        uint64_t lowest_acceptable_slot() const { return m_lowest_acceptable_slot; }
        bool failed() const { return m_permafail; }
        uint64_t write_cut() const { return m_opcount; }

    public:
        void adopt(const ballot& b);
        void accept(const pvalue& pval);
        void garbage_collect(uint64_t below);
        uint64_t sync_cut();
        bool record_snapshot(uint64_t slot, const e::slice& snapshot);
        bool load_latest_snapshot(e::slice* snapshot,
                                  std::auto_ptr<e::buffer>* snapshot_backing);

    private:
        struct log_segment;
        class garbage_collector;
        void compact_pvals(std::vector<pvalue>* pvals);
        bool atomic_read(const char* path, std::string* contents);
        bool atomic_write(const char* path, const std::string& contents);
        bool parse_identity(const std::string& ident,
                            server* saved_us, bootstrap* saved_bootstrap);
        log_segment* get_writable_log();
        static bool replay_log(int dir,
                               uint64_t lognum,
                               ballot* highest_ballot,
                               std::vector<pvalue>* pvals,
                               uint64_t* lowest_acceptable_slot);

    private:
        ballot m_ballot;
        std::vector<pvalue> m_pvals;
        uint64_t m_lowest_acceptable_slot;
        po6::io::fd m_dir;
        po6::io::fd m_lock;
        uint64_t m_opcount;
        bool m_permafail;
        std::auto_ptr<log_segment> m_current;
        std::auto_ptr<log_segment> m_previous;
        const std::auto_ptr<garbage_collector> m_gc;
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_acceptor_h_
