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

#ifndef replicant_daemon_snapshot_h_
#define replicant_daemon_snapshot_h_

// C
#include <stdint.h>

// STL
#include <set>

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>
#include <e/slice.h>

// Replicant
#include "namespace.h"
#include "common/packing.h"

BEGIN_REPLICANT_NAMESPACE

class snapshot
{
    public:
        snapshot(uint64_t up_to);
        ~snapshot() throw ();

    public:
        uint64_t slot() const { return m_up_to; }
        void wait();
        void replica_internals(const e::slice& replica);
        void start_object(const std::string& name);
        void finish_object(const std::string& name, const std::string& snap);
        void abort_snapshot();
        bool done();
        const std::string& contents();

    private:
        bool done_condition();

    // refcount
    private:
        friend class e::intrusive_ptr<snapshot>;
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        size_t m_ref;

    private:
        const uint64_t m_up_to;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        bool m_failed;
        std::set<std::string> m_objects;
        std::string m_snapshot;
        e::packer m_packer;

    private:
        snapshot(const snapshot&);
        snapshot& operator = (const snapshot&);
};

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_snapshot_h_
