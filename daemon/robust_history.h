// Copyright (c) 2016, Robert Escriva
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

#ifndef replicant_daemon_robust_history_h_
#define replicant_daemon_robust_history_h_

// po6
#include <po6/threads/mutex.h>

// Google SparseHash
#include <google/dense_hash_set>

// Replicant
#include <replicant.h>
#include "namespace.h"
#include "daemon/pvalue.h"

BEGIN_REPLICANT_NAMESPACE

class robust_history
{
    public:
        robust_history();
        ~robust_history() throw ();

    public:
        bool has_output(uint64_t nonce,
                        uint64_t min_slot,
                        replicant_returncode* status,
                        std::string* output);
        void executed(const pvalue& p,
                      uint64_t command_nonce,
                      replicant_returncode status,
                      const std::string& result);
        void copy_up_to(robust_history* other, uint64_t slot);
        void inhibit_gc();
        void allow_gc();

    private:
        class entry;
        friend e::packer operator << (e::packer lhs, robust_history& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, robust_history& rhs);
        friend size_t pack_size(const robust_history& rhs);
        friend e::packer operator << (e::packer lhs, const robust_history::entry& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, robust_history::entry& rhs);
        friend size_t pack_size(const robust_history::entry& rhs);

    private:
        void cleanup();

    private:
        po6::threads::mutex m_mtx;
        std::list<entry> m_history;
        google::dense_hash_set<uint64_t> m_lookup;
        bool m_inhibit_gc;

    private:
        robust_history(const robust_history&);
        robust_history& operator = (const robust_history&);
};

e::packer
operator << (e::packer lhs, robust_history& rhs);
e::unpacker
operator >> (e::unpacker lhs, robust_history& rhs);
size_t
pack_size(const robust_history& rhs);

e::packer
operator << (e::packer lhs, const robust_history::entry& rhs);
e::unpacker
operator >> (e::unpacker lhs, robust_history::entry& rhs);
size_t
pack_size(const robust_history::entry& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_robust_history_h_
