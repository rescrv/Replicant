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
#include <signal.h>

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
#include "daemon/conditions_wrapper.h"
#include "daemon/object_manager.h"
#include "daemon/replicant_state_machine.h"
#include "daemon/replicant_state_machine_context.h"
#if defined __APPLE__ || defined __FreeBSD__
#ifdef __FreeBSD__
#include <sys/stat.h>
#endif
#include "memstream.h"
#endif

using replicant::object_manager;

///////////////////////////////// Command Class ////////////////////////////////

class object_manager::command
{
    public:
        command();
        ~command() throw ();

    public:
        enum { NORMAL, WAIT, DELETE } type;
        uint64_t slot;
        uint64_t client;
        uint64_t nonce;
        uint64_t cond;
        uint64_t state;
        e::slice data;
        std::string backing;
};

object_manager :: command :: command()
    : type(NORMAL)
    , slot(0)
    , client(0)
    , nonce(0)
    , cond(0)
    , state(0)
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
        class condition;

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
        char* output;
        size_t output_sz;
        std::map<uint64_t, condition> conditions;

    private:
        object(const object&);
        object& operator = (const object&);

    private:
        friend class e::intrusive_ptr<object>;
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
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
    , output(NULL)
    , output_sz(0)
    , conditions()
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

//////////////////////////////// Condition Class ///////////////////////////////

class object_manager::object::condition
{
    public:
        struct waiter;

    public:
        condition();
        ~condition() throw ();

    public:
        uint64_t count;
        std::set<waiter> waiters;
};

object_manager :: object :: condition :: condition()
    : count(0)
    , waiters()
{
}

object_manager :: object :: condition :: ~condition() throw ()
{
}

///////////////////////////////// Waiter Class /////////////////////////////////

class object_manager::object::condition::waiter
{
    public:
        waiter();
        waiter(uint64_t wait_for, uint64_t client, uint64_t nonce);
        ~waiter() throw ();

    public:
        bool operator < (const waiter& rhs) const;
        bool operator == (const waiter& rhs) const;

    public:
        uint64_t wait_for;
        uint64_t client;
        uint64_t nonce;
};

object_manager :: object :: condition :: waiter :: waiter()
    : wait_for()
    , client()
    , nonce()
{
}

object_manager :: object :: condition :: waiter :: waiter(uint64_t w, uint64_t c, uint64_t n)
    : wait_for(w)
    , client(c)
    , nonce(n)
{
}

object_manager :: object :: condition :: waiter :: ~waiter() throw ()
{
}

bool
object_manager :: object :: condition :: waiter :: operator < (const waiter& rhs) const
{
    if (wait_for < rhs.wait_for)
    {
        return true;
    }
    else if (wait_for == rhs.wait_for)
    {
        if (client < rhs.client)
        {
            return true;
        }
        else if (client == rhs.client)
        {
            return nonce < rhs.nonce;
        }
    }

    return false;
}

bool
object_manager :: object :: condition :: waiter :: operator == (const waiter& rhs) const
{
    return wait_for == rhs.wait_for &&
           client == rhs.client &&
           nonce == rhs.nonce;
}

///////////////////////////////// Public Class /////////////////////////////////

object_manager :: object_manager()
    : m_daemon()
    , m_command_cb()
    , m_notify_cb()
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
object_manager :: set_callback(daemon* d, void (daemon::*command_cb)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data),
                                          void (daemon::*notify_cb)(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data))
{
    m_daemon = d;
    m_command_cb = command_cb;
    m_notify_cb = notify_cb;
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
            return command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
        }

        e::unpack64be(data.data(), &obj_id);
        lib = data;
        lib.advance(sizeof(uint64_t));
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it != m_objects.end())
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_OBJ_EXIST);
        }

        e::intrusive_ptr<object> obj = new object();
        po6::threads::mutex::hold hold(&obj->mtx);
        char buf[43 /*strlen("./libreplicant-slot<slot \lt 2**64>.so\x00")*/];
        sprintf(buf, "./libreplicant-slot%lu.so", slot);
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
            return command_send_error_msg_response(slot, client, nonce, RESPONSE_DLOPEN_FAIL, err);
        }

        obj->sym = static_cast<replicant_state_machine*>(dlsym(obj->lib, "rsm"));

        if (!obj->sym)
        {
            const char* err = dlerror();
            LOG(ERROR) << "could not find \"rsm\" symbol in library for slot "
                       << slot << ": " << err
                       << "; delete the object and try again";
            return command_send_error_msg_response(slot, client, nonce, RESPONSE_DLSYM_FAIL, err);
        }

        if (!obj->sym->ctor)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a constructor; delete the object and try again";
            return command_send_error_response(slot, client, nonce, RESPONSE_NO_CTOR);
        }

        if (!obj->sym->rtor)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a reconstructor; delete the object and try again";
            return command_send_error_response(slot, client, nonce, RESPONSE_NO_RTOR);
        }

        if (!obj->sym->dtor)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a deconstructor; delete the object and try again";
            return command_send_error_response(slot, client, nonce, RESPONSE_NO_DTOR);
        }

        if (!obj->sym->snap)
        {
            LOG(WARNING) << "library for slot " << slot << " does not specify a snapshot function; delete the object and try again";
            return command_send_error_response(slot, client, nonce, RESPONSE_NO_SNAP);
        }

        replicant_state_machine_context ctx;
        ctx.object = obj_id;
        ctx.client = client;
        ctx.output = open_memstream(&obj->output, &obj->output_sz);
        conditions_wrapper cw(this, obj.get());
        ctx.conditions = cw;
        ctx.response = NULL;
        ctx.response_sz = 0;
        obj->rsm = obj->sym->ctor(&ctx);
        fclose(ctx.output);
        ctx.output = NULL;
        log_messages(obj_id, obj, slot, "the constructor");
        return command_send_response(slot, client, nonce, RESPONSE_SUCCESS, e::slice());
    }
    else if (obj_id == OBJECT_OBJ_DEL)
    {
        if (data.size() < sizeof(uint64_t))
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
        }

        e::unpack64be(data.data(), &obj_id);
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it == m_objects.end())
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        e::intrusive_ptr<object> obj = it->second;
        m_objects.erase(it);
        // Allocate the command object
        std::list<command> tmp;
        tmp.push_back(command());
        tmp.back().type = command::DELETE;
        tmp.back().slot = slot;
        tmp.back().client = client;
        tmp.back().nonce = nonce;
        tmp.back().data = data;
        tmp.back().backing.swap(*backing);
        po6::threads::mutex::hold hold(&obj->mtx);
        obj->commands.splice(obj->commands.end(), tmp, tmp.begin());
        obj->commands_avail.signal();
        return command_send_response(slot, client, nonce, RESPONSE_SUCCESS, e::slice());
    }
    else if (!IS_SPECIAL_OBJECT(obj_id))
    {
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it == m_objects.end())
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        e::intrusive_ptr<object> obj = it->second;
        // Allocate the command object
        std::list<command> tmp;
        tmp.push_back(command());
        tmp.back().type = command::NORMAL;
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
        return command_send_error_response(slot, client, nonce, RESPONSE_SERVER_ERROR);
    }
}

void
object_manager :: wait(uint64_t obj_id, uint64_t client, uint64_t nonce, uint64_t cond, uint64_t state)
{
    if (!IS_SPECIAL_OBJECT(obj_id))
    {
        object_map_t::iterator it = m_objects.find(obj_id);

        if (it == m_objects.end())
        {
            return notify_send_error_response(client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        e::intrusive_ptr<object> obj = it->second;
        // Allocate the command object
        std::list<command> tmp;
        tmp.push_back(command());
        tmp.back().type = command::WAIT;
        tmp.back().client = client;
        tmp.back().nonce = nonce;
        tmp.back().cond = cond;
        tmp.back().state = state;
        // Push it onto the object's queue
        po6::threads::mutex::hold hold(&obj->mtx);
        obj->commands.splice(obj->commands.end(), tmp, tmp.begin());
        obj->commands_avail.signal();
    }
    else
    {
        LOG(ERROR) << "object_manager asked to work on special object " << obj_id;
        return notify_send_error_response(client, nonce, RESPONSE_SERVER_ERROR);
    }
}

int
object_manager :: condition_create(object* obj, uint64_t cond)
{
    std::map<uint64_t, object::condition>::iterator it = obj->conditions.find(cond);

    if (it != obj->conditions.end())
    {
        return -1;
    }

    obj->conditions.insert(std::make_pair(cond, object::condition()));
    return 0;
}

int
object_manager :: condition_destroy(object* obj, uint64_t cond)
{
    std::map<uint64_t, object::condition>::iterator it = obj->conditions.find(cond);

    if (it == obj->conditions.end())
    {
        return -1;
    }

    while (!it->second.waiters.empty())
    {
        const object::condition::waiter& w(*it->second.waiters.begin());
        notify_send_response(w.client, w.nonce, RESPONSE_COND_DESTROYED, e::slice("", 0));
        it->second.waiters.erase(it->second.waiters.begin());
    }

    obj->conditions.erase(it);
    return 0;
}

int
object_manager :: condition_broadcast(object* obj, uint64_t cond, uint64_t* state)
{
    std::map<uint64_t, object::condition>::iterator it = obj->conditions.find(cond);

    if (it == obj->conditions.end())
    {
        return -1;
    }

    ++it->second.count;

    if (state)
    {
        *state = it->second.count;
    }

    while (!it->second.waiters.empty() &&
           it->second.waiters.begin()->wait_for < it->second.count)
    {
        const object::condition::waiter& w(*it->second.waiters.begin());
        notify_send_response(w.client, w.nonce, RESPONSE_SUCCESS, e::slice("", 0));
        it->second.waiters.erase(it->second.waiters.begin());
    }

    return 0;
}

void
object_manager :: command_send_error_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc)
{
    ((*m_daemon).*m_command_cb)(slot, client, nonce, rc, e::slice("", 0));
}

void
object_manager :: command_send_error_msg_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const char* resp)
{
    size_t resp_sz = strlen(resp) + 1;
    ((*m_daemon).*m_command_cb)(slot, client, nonce, rc, e::slice(resp, resp_sz));
}

void
object_manager :: command_send_response(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp)
{
    ((*m_daemon).*m_command_cb)(slot, client, nonce, rc, resp);
}

void
object_manager :: notify_send_error_response(uint64_t client, uint64_t nonce, response_returncode rc)
{
    ((*m_daemon).*m_notify_cb)(client, nonce, rc, e::slice("", 0));
}

void
object_manager :: notify_send_error_msg_response(uint64_t client, uint64_t nonce, response_returncode rc, const char* resp)
{
    size_t resp_sz = strlen(resp) + 1;
    ((*m_daemon).*m_notify_cb)(client, nonce, rc, e::slice(resp, resp_sz));
}

void
object_manager :: notify_send_response(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& resp)
{
    ((*m_daemon).*m_notify_cb)(client, nonce, rc, resp);
}

void
object_manager :: worker_thread(uint64_t obj_id, e::intrusive_ptr<object> obj)
{
    LOG(INFO) << "spawning worker thread for object " << obj_id;
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return;
    }

    int err = pthread_sigmask(SIG_BLOCK, &ss, NULL);

    if (err < 0)
    {
        errno = err;
        PLOG(ERROR) << "could not block signals";
        return;
    }

    bool shutdown = false;

    while (!shutdown)
    {
        std::list<command> commands;

        {
            po6::threads::mutex::hold hold(&obj->mtx);

            while (obj->commands.empty())
            {
                obj->commands_avail.wait();
            }

            if (!obj->commands.empty())
            {
                commands.splice(commands.begin(), obj->commands);
            }
        }

        while (!commands.empty())
        {
            dispatch_command(obj_id, obj, commands.front(), &shutdown);
            commands.pop_front();
        }
    }

    LOG(INFO) << "exiting worker thread for object " << obj_id;
}

void
object_manager :: log_messages(uint64_t obj_id, e::intrusive_ptr<object> obj, uint64_t slot, const char* func)
{
    char* ptr = obj->output;
    char* end = obj->output + obj->output_sz;

    while (ptr < end)
    {
        void* ptr_nl = memchr(ptr, '\n', end - ptr);
        void* ptr_0  = memchr(ptr, 0, end - ptr);
        char* eol = end;

        if (ptr_nl && ptr_0 && ptr_0 <= ptr_nl)
        {
            eol = static_cast<char*>(ptr_0);
        }
        else if (ptr_nl)
        {
            eol = static_cast<char*>(ptr_nl);
        }
        else if (ptr_0)
        {
            eol = static_cast<char*>(ptr_0);
        }

        LOG(INFO) << "object=" << obj_id << " slot=" << slot << " func=\"" << func << "\": " << std::string(ptr, eol);
        ptr = eol + 1;
    }

    free(obj->output);
    obj->output = NULL;
    obj->output_sz = 0;
}

void
object_manager :: dispatch_command(uint64_t obj_id, e::intrusive_ptr<object> obj, const command& cmd, bool* shutdown)
{
    if (cmd.type == command::NORMAL)
    {
        const char* func = reinterpret_cast<const char*>(cmd.data.data());
        size_t func_sz = strnlen(func, cmd.data.size());

        if (func_sz >= cmd.data.size())
        {
            command_send_error_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_MALFORMED);
            return;
        }

        replicant_state_machine_step* rsms = obj->sym ? obj->sym->steps : NULL;
        replicant_state_machine_step* trans = NULL;

        while (rsms && rsms->name)
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
            ctx.output = open_memstream(&obj->output, &obj->output_sz);
            conditions_wrapper cw(this, obj.get());
            ctx.conditions = cw;
            const char* data = func + func_sz + 1;
            size_t data_sz = cmd.data.size() - func_sz - 1;
            trans->func(&ctx, obj->rsm, data, data_sz);
            fclose(ctx.output);
            ctx.output = NULL;
            log_messages(obj_id, obj, cmd.slot, func);

            if (!ctx.response)
            {
                ctx.response = "";
                ctx.response_sz = 0;
            }

            command_send_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_SUCCESS, e::slice(ctx.response, ctx.response_sz));
        }
        else
        {
            command_send_error_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_NO_FUNC);
        }
    }
    else if (cmd.type == command::WAIT)
    {
        std::map<uint64_t, object::condition>::iterator it = obj->conditions.find(cmd.cond);

        if (it == obj->conditions.end())
        {
            notify_send_error_response(cmd.client, cmd.nonce, RESPONSE_COND_NOT_EXIST);
        }
        else
        {
            if (cmd.state < it->second.count)
            {
                notify_send_response(cmd.client, cmd.nonce, RESPONSE_SUCCESS, e::slice("", 0));
            }
            else
            {
                it->second.waiters.insert(object::condition::waiter(cmd.state, cmd.client, cmd.nonce));
            }
        }
    }
    else if (cmd.type == command::DELETE)
    {
        *shutdown = true;
        replicant_state_machine_context ctx;
        ctx.object = obj_id;
        ctx.client = cmd.client;
        ctx.output = open_memstream(&obj->output, &obj->output_sz);
        conditions_wrapper cw(this, obj.get());
        ctx.conditions = cw;
        ctx.response = NULL;
        ctx.response_sz = 0;

        if (obj->sym && obj->sym->dtor)
        {
            obj->sym->dtor(&ctx, obj->rsm);
        }

        fclose(ctx.output);
        ctx.output = NULL;
        log_messages(obj_id, obj, cmd.slot, "the destructor");

        while (!obj->conditions.empty())
        {
            condition_destroy(obj.get(), obj->conditions.begin()->first);
        }
    }
}
