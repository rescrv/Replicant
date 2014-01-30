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

#ifndef replicant_daemon_object_manager_h_
#define replicant_daemon_object_manager_h_

// C
#include <stdint.h>

// STL
#include <map>
#include <memory>
#include <set>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>
#include <e/slice.h>

// Replicant
#include "common/response_returncode.h"
#include "daemon/snapshot.h"

class replicant_state_machine_context;
namespace replicant
{
class daemon;

class object_manager
{
    public:
        class object;

    public:
        object_manager();
        ~object_manager() throw ();

    public:
        void enable_logging() { m_logging_enabled = true; }
        void set_callback(daemon* d, void (daemon::*command_cb)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data),
                                     void (daemon::*notify_cb)(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data),
                                     void (daemon::*snapshot_cb)(std::auto_ptr<snapshot>),
                                     void (daemon::*alarm_cb)(uint64_t obj_id, const char* func),
                                     void (daemon::*suspect_cb)(uint64_t obj_id, uint64_t cb_id, const e::slice& data));
        void enqueue(uint64_t slot, uint64_t object,
                     uint64_t client, uint64_t nonce,
                     const e::slice& data);
        void suspect(uint64_t client);
        void suspect_if_not_listed(const std::vector<uint64_t>& clients);
        void throttle(uint64_t object, size_t sz);
        void wait(uint64_t object, uint64_t client, uint64_t nonce, uint64_t cond, uint64_t state);
        void periodic(uint64_t now);

    public:
        int condition_create(object* o, uint64_t cond);
        int condition_destroy(object* o, uint64_t cond);
        int condition_broadcast(object* o, uint64_t cond, uint64_t* state);

    private:
        class command;
        typedef std::map<uint64_t, e::intrusive_ptr<object> > object_map_t;
        typedef std::set<e::intrusive_ptr<object> > object_set_t;
        friend class thread_wrapper;

    private:
        e::intrusive_ptr<object> common_object_initialize(uint64_t slot,
                                                          uint64_t client,
                                                          uint64_t nonce,
                                                          const e::slice& lib,
                                                          uint64_t* obj_id);
        e::intrusive_ptr<object> get_object(uint64_t obj_id);
        // del from m_objects and return the obj
        e::intrusive_ptr<object> del_object(uint64_t obj_id);
        void command_send_error_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc);
        void command_send_error_msg_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const char* resp);
        void command_send_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp);
        void notify_send_error_response(uint64_t client, uint64_t nonce, response_returncode rc);
        void notify_send_response(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp);
        void worker_thread(uint64_t obj_id, e::intrusive_ptr<object> obj);
        void log_messages(uint64_t obj_id, replicant_state_machine_context* ctx, const char* func);
        void dispatch_command(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);
        void dispatch_command_normal(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);
        void dispatch_command_wait(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);
        void dispatch_command_delete(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);
        void dispatch_command_snapshot(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);
        void dispatch_command_alarm(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);
        void dispatch_command_shutdown(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);

    private:
        object_manager(const object_manager&);
        object_manager& operator = (const object_manager&);

    private:
        daemon* m_daemon;
        void (daemon::*m_command_cb)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data);
        void (daemon::*m_notify_cb)(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data);
        void (daemon::*m_snapshot_cb)(std::auto_ptr<snapshot>);
        void (daemon::*m_alarm_cb)(uint64_t obj_id, const char* func);
        void (daemon::*m_suspect_cb)(uint64_t obj_id, uint64_t cb_id, const e::slice& data);
        object_map_t m_objects;
        bool m_logging_enabled;
};

} // namespace replicant

#endif // replicant_daemon_object_manager_h_
