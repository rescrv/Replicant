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

// C
#include <cstdio>

// POSIX
#include <dlfcn.h>

// STL
#include <list>
#include <memory>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/io/fd.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/buffer.h>
#include <e/endian.h>

// BusyBee
#include <busybee_constants.h>

// Replicant
#include "common/network_msgtype.h"
#include "common/special_objects.h"
#include "daemon/daemon.h"
#include "daemon/object_manager.h"
#include "daemon/replicant_state_machine.h"
#include "daemon/replicant_state_machine_context.h"

using replicant::object_manager;

///////////////////////////////// Command Class ////////////////////////////////

class object_manager::command
{
    public:
        command();
        ~command() throw ();

    public:
        uint64_t slot;
        uint64_t client;
        uint64_t nonce;
        e::slice data;
        std::string backing;
};

object_manager :: command :: command()
    : slot(0)
    , client(0)
    , nonce(0)
    , data()
    , backing()
{
}

object_manager :: command :: ~command() throw ()
{
}

///////////////////////////////// Object Class /////////////////////////////////

class object_manager::object
{
    public:
        object();
        ~object() throw ();

    public:
        std::auto_ptr<po6::threads::thread> thread;
        po6::threads::mutex mtx;
        po6::threads::cond commands_avail;
        std::list<command> commands;
        uint64_t slot;
        void* lib;
        replicant_state_machine* sym;
        void* rsm;
        bool shutdown;

    private:
        object(const object&);
        object& operator = (const object&);

    private:
        friend class e::intrusive_ptr<object>;
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }
        size_t m_ref;
};

object_manager :: object :: object()
    : thread()
    , mtx()
    , commands_avail(&mtx)
    , commands()
    , slot(0)
    , lib(NULL)
    , sym(NULL)
    , rsm(NULL)
    , shutdown(false)
    , m_ref(0)
{
}

object_manager :: object :: ~object() throw ()
{
    if (lib)
    {
        dlclose(lib);
    }
}

///////////////////////////////// Public Class /////////////////////////////////

object_manager :: object_manager()
    : m_daemon()
    , m_daemon_cb()
    , m_objects()
    , m_cleanup_protect()
    , m_cleanup_queued()
    , m_cleanup_ready()
{
}

object_manager :: ~object_manager() throw ()
{
}

void
object_manager :: set_callback(replicant_daemon* d, void (replicant_daemon::*func)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data))
{
    m_daemon = d;
    m_daemon_cb = func;
}

void
object_manager :: enqueue(uint64_t slot, uint64_t obj_id,
                          uint64_t client, uint64_t nonce,
                          const e::slice& data, std::string* backing)
{
    if (obj_id == OBJECT_OBJ_NEW)
    {
        e::slice lib;

        if (data.size() < sizeof(uint64_t))
        {
            return send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
        }

        e::unpack64be(data.data(), &obj_id);
        lib = data;
        lib.advance(sizeof(uint64_t));
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it != m_objects.end())
        {
            return send_error_response(slot, client, nonce, RESPONSE_OBJ_EXIST);
        }

        e::intrusive_ptr<object> obj = new object();
        po6::threads::mutex::hold hold(&obj->mtx);
        char buf[38 /*strlen("./libreplicant<slot \lt 2**64>.so\x00")*/];
        sprintf(buf, "./libreplicant%lu.so", slot);
        po6::io::fd tmplib(open(buf, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU));

        if (tmplib.get() < 0)
        {
            PLOG(ERROR) << "could not open temporary library for slot " << slot;
            abort();
        }

        if (tmplib.xwrite(lib.data(), lib.size()) != static_cast<ssize_t>(lib.size()))
        {
            PLOG(ERROR) << "could not write temporary ibrary for slot " << slot;
            abort();
        }

        obj->slot = slot;
        obj->lib = dlopen(buf, RTLD_NOW|RTLD_LOCAL);

        if (unlink(buf) < 0)
        {
            PLOG(ERROR) << "could not unlink temporary ibrary for slot " << slot;
            abort();
        }

        // At this point, any unexpected failures are user error.  We should not
        // fail the server
        obj->thread.reset(new po6::threads::thread(std::tr1::bind(&object_manager::worker_thread, this, obj_id, obj)));
        obj->thread->start();
        m_objects.insert(std::make_pair(obj_id, obj));

        if (!obj->lib)
        {
            const char* err = dlerror();
            LOG(ERROR) << "could not load library for slot " << slot
                       << ": " << err << "; delete the object and try again";
            return send_error_msg_response(slot, client, nonce, RESPONSE_DLOPEN_FAIL, err);
        }

        obj->sym = static_cast<replicant_state_machine*>(dlsym(obj->lib, "rsm"));

        if (!obj->sym)
        {
            const char* err = dlerror();
            LOG(ERROR) << "could not find \"rsm\" symbol in library for slot "
                       << slot << ": " << err
                       << "; delete the object and try again";
            return send_error_msg_response(slot, client, nonce, RESPONSE_DLSYM_FAIL, err);
        }

        if (!obj->sym->ctor)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a constructor; delete the object and try again";
            return send_error_response(slot, client, nonce, RESPONSE_NO_CTOR);
        }

        if (!obj->sym->rtor)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a reconstructor; delete the object and try again";
            return send_error_response(slot, client, nonce, RESPONSE_NO_RTOR);
        }

        if (!obj->sym->dtor)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a deconstructor; delete the object and try again";
            return send_error_response(slot, client, nonce, RESPONSE_NO_DTOR);
        }

        if (!obj->sym->snap)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a snapshot function; delete the object and try again";
            return send_error_response(slot, client, nonce, RESPONSE_NO_SNAP);
        }

        replicant_state_machine_context ctx;
        ctx.object = obj_id;
        ctx.client = client;
        obj->rsm = obj->sym->ctor(&ctx);
        return send_response(slot, client, nonce, RESPONSE_SUCCESS, e::slice());
    }
    else if (obj_id == OBJECT_OBJ_DEL)
    {
        if (data.size() < sizeof(uint64_t))
        {
            return send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
        }

        e::unpack64be(data.data(), &obj_id);
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it == m_objects.end())
        {
            return send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        e::intrusive_ptr<object> obj = it->second;
        m_objects.erase(it);
        po6::threads::mutex::hold hold(&obj->mtx);
        obj->shutdown = true;
        obj->commands_avail.signal();
        return send_response(slot, client, nonce, RESPONSE_SUCCESS, e::slice());
    }
    else if (!IS_SPECIAL_OBJECT(obj_id))
    {
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it == m_objects.end())
        {
            return send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        e::intrusive_ptr<object> obj = it->second;
        // Allocate the command object
        std::list<command> tmp;
        tmp.push_back(command());
        tmp.back().slot = slot;
        tmp.back().client = client;
        tmp.back().nonce = nonce;
        tmp.back().data = data;
        tmp.back().backing.swap(*backing);
        // Push it onto the object's queue
        po6::threads::mutex::hold hold(&obj->mtx);
        obj->commands.splice(obj->commands.end(), tmp, tmp.begin());
        obj->commands_avail.signal();
    }
    else
    {
        LOG(ERROR) << "object_manager asked to work on special object " << obj_id;
        return send_error_response(slot, client, nonce, RESPONSE_SERVER_ERROR);
    }
}

void
object_manager :: send_error_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc)
{
    ((*m_daemon).*m_daemon_cb)(slot, client, nonce, rc, e::slice("", 0));
}

void
object_manager :: send_error_msg_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const char* resp)
{
    size_t resp_sz = strlen(resp) + 1;
    ((*m_daemon).*m_daemon_cb)(slot, client, nonce, rc, e::slice(resp, resp_sz));
}

void
object_manager :: send_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp)
{
    ((*m_daemon).*m_daemon_cb)(slot, client, nonce, rc, resp);
}

void
object_manager :: worker_thread(uint64_t obj_id, e::intrusive_ptr<object> obj)
{
    bool shutdown = false;

    while (!shutdown)
    {
        std::list<command> commands;

        {
            po6::threads::mutex::hold hold(&obj->mtx);

            while (obj->commands.empty() && !obj->shutdown)
            {
                obj->commands_avail.wait();
            }

            if (!obj->commands.empty())
            {
                commands.splice(commands.begin(), obj->commands);
            }

            shutdown = obj->shutdown;
        }

        while (!commands.empty())
        {
            command& cmd(commands.front());
            const char* func = reinterpret_cast<const char*>(cmd.data.data());
            size_t func_sz = strnlen(func, cmd.data.size());

            if (func_sz >= cmd.data.size())
            {
                send_error_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_MALFORMED);
                commands.pop_front();
                continue;
            }

            replicant_state_machine_step* rsms = obj->sym->steps;
            replicant_state_machine_step* trans = NULL;

            while (rsms->name)
            {
                if (strcmp(func, rsms->name) == 0)
                {
                    trans = rsms;
                    break;
                }

                ++rsms;
            }

            if (trans)
            {
                replicant_state_machine_context ctx;
                ctx.object = obj_id;
                ctx.client = cmd.client;
                const char* data = func + func_sz + 1;
                size_t data_sz = cmd.data.size() - func_sz - 1;
                trans->func(&ctx, obj->rsm, data, data_sz);

                if (!ctx.response)
                {
                    ctx.response = "";
                    ctx.response_sz = 0;
                }

                send_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_SUCCESS, e::slice(ctx.response, ctx.response_sz));
            }
            else
            {
                send_error_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_NO_FUNC);
            }

            commands.pop_front();
        }
    }
}
