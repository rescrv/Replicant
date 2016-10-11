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

#ifndef replicant_daemon_object_h_
#define replicant_daemon_object_h_

// STL
#include <list>
#include <map>

// po6
#include <po6/io/fd.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/intrusive_ptr.h>
#include <e/slice.h>

// Replicant
#include <replicant.h>
#include "namespace.h"
#include "daemon/pvalue.h"

BEGIN_REPLICANT_NAMESPACE
class condition;
class replica;
class snapshot;

enum object_t
{
    OBJECT_LIBRARY = 1,
    OBJECT_GARBAGE = 255
};

class object
{
    public:
        object(replica* r, uint64_t slot, const std::string& name, object_t t, const std::string& init);
        ~object() throw ();

    public:
        const std::string& name() const { return m_obj_name; }
        uint64_t created_at() const { return m_obj_slot; }
        uint64_t last_executed() const;
        std::string last_state();
        void set_child(pid_t child, int fd);
        bool failed();
        bool done();
        // must call set_child before these functions
        void ctor();
        void rtor(e::unpacker up);
        void cond_wait(server_id si, uint64_t nonce,
                       const e::slice& cond,
                       uint64_t state);
        void call(const e::slice& func,
                  const e::slice& input,
                  const pvalue& p,
                  unsigned flags,
                  uint64_t command_nonce,
                  server_id si,
                  uint64_t request_nonce);
        void take_snapshot(e::intrusive_ptr<snapshot> snap);
        void fail_at(uint64_t slot);
        void keepalive();

    public:
        struct enqueued_cond_wait;
        struct enqueued_call;

    private:
        void run();
        void do_cond_wait(const enqueued_cond_wait& cw);
        void do_nop();
        void do_call(const enqueued_call& c);
        void do_snapshot(e::intrusive_ptr<snapshot> snap);
        void do_snapshot(std::string* s);
        void do_call_log(const enqueued_call& c);
        void do_call_cond_create();
        void do_call_cond_destroy();
        void do_call_cond_broadcast();
        void do_call_cond_broadcast_data();
        void do_call_cond_current_value();
        void do_call_tick_interval();
        void do_call_output(const enqueued_call& c);
        void do_failure();
        void fail();
        bool read(char* data, size_t sz);
        bool write(const char* data, size_t sz);

    // refcount
    private:
        friend class e::intrusive_ptr<object>;
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        size_t m_ref;

    private:
        // fixed state, read only everywhere
        replica* const m_replica;
        const uint64_t m_obj_slot;
        const std::string m_obj_name;
        const object_t m_type;
        const std::string m_init;

        // state to be protected by the mutex
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        pid_t m_obj_pid;
        po6::io::fd m_fd;
        bool m_has_ctor;
        bool m_has_rtor;
        std::string m_rtor;
        std::list<enqueued_cond_wait> m_cond_waits;
        std::list<enqueued_call> m_calls;
        std::list<e::intrusive_ptr<snapshot> > m_snapshots;
        uint64_t m_highest_slot;
        uint64_t m_fail_at;
        bool m_failed;
        bool m_done;
        bool m_keepalive;

        // snapshot state
        po6::threads::mutex m_snap_mtx;
        std::string m_snap;

        // state to only be used by the background thread (except at init)
        po6::threads::thread m_thread;
        typedef std::map<std::string, condition*> cond_map_t;
        cond_map_t m_conditions;
        std::string m_tick_func;
        uint64_t m_tick_interval;

        // to be written/read with atomics
        uint64_t m_last_executed;

    private:
        object(const object&);
        object& operator = (const object&);
};

e::packer
operator << (e::packer lhs, const object_t& rhs);
e::unpacker
operator >> (e::unpacker lhs, object_t& rhs);
size_t
pack_size(const object_t& rhs);

e::packer
operator << (e::packer lhs, const object::enqueued_call& rhs);
e::unpacker
operator >> (e::unpacker lhs, object::enqueued_call& rhs);
size_t
pack_size(const object::enqueued_call& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_object_h_
