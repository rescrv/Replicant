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
#include <set>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/intrusive_ptr.h>
#include <e/slice.h>

// Replicant
#include "common/response_returncode.h"

namespace replicant
{
class daemon;

class object_manager
{
    public:
        object_manager();
        ~object_manager() throw ();

    public:
        void set_callback(daemon* d, void (daemon::*command_cb)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data),
                                     void (daemon::*notify_cb)(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data));
        void enqueue(uint64_t slot, uint64_t object,
                     uint64_t client, uint64_t nonce,
                     const e::slice& data, std::string* backing);
        void wait(uint64_t object, uint64_t client, uint64_t nonce, uint64_t cond, uint64_t state);

    private:
        class command;
        class object;
        typedef std::map<uint64_t, e::intrusive_ptr<object> > object_map_t;
        typedef std::set<e::intrusive_ptr<object> > object_set_t;
        friend class conditions_wrapper;

    private:
        int condition_create(void* o, uint64_t cond);
        int condition_destroy(void* o, uint64_t cond);
        int condition_broadcast(void* o, uint64_t cond, uint64_t* state);

    private:
        void command_send_error_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc);
        void command_send_error_msg_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const char* resp);
        void command_send_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp);
        void notify_send_error_response(uint64_t client, uint64_t nonce, response_returncode rc);
        void notify_send_error_msg_response(uint64_t client, uint64_t nonce, response_returncode rc, const char* resp);
        void notify_send_response(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp);
        void worker_thread(uint64_t obj_id, e::intrusive_ptr<object> obj);
        void log_messages(uint64_t obj_id, e::intrusive_ptr<object> obj, uint64_t slot, const char* func);
        void dispatch_command(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown);

    private:
        object_manager(const object_manager&);
        object_manager& operator = (const object_manager&);

    private:
        daemon* m_daemon;
        void (daemon::*m_command_cb)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data);
        void (daemon::*m_notify_cb)(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data);
        object_map_t m_objects;
        // protects the m_cleanup_* members
        po6::threads::mutex m_cleanup_protect;
        object_set_t m_cleanup_queued;
        object_set_t m_cleanup_ready;
};

} // namespace replicant

#endif // replicant_daemon_object_manager_h_
