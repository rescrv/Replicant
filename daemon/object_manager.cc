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

#define __STDC_LIMIT_MACROS
#include <ciso646> // detect std::lib

// C
#include <cstdio>
#include <stdint.h>

// POSIX
#include <dlfcn.h>
#include <signal.h>

// STL
#include <list>
#ifdef _LIBCPP_VERSION
#include <functional>
#include <memory>
#else
#include <tr1/functional>
#include <tr1/memory>
#endif

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
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>

// Replicant
#include "common/network_msgtype.h"
#include "common/special_clients.h"
#include "common/special_objects.h"
#include "daemon/daemon.h"
#include "daemon/object_manager.h"
#include "daemon/replicant_state_machine.h"
#include "daemon/replicant_state_machine_context.h"
#include "daemon/snapshot.h"
#if defined __APPLE__
#include "daemon/memstream.h"
#endif

#ifdef _LIBCPP_VERSION
#define REF std::ref
#define SHARED_PTR std::shared_ptr
#else
#define REF std::tr1::ref
#define SHARED_PTR std::tr1::shared_ptr
#endif

using replicant::object_manager;

namespace
{

std::string
obj_id_to_str(uint64_t obj_id)
{
    char buf[sizeof(uint64_t)];
    e::pack64be(obj_id, buf);
    size_t len = sizeof(uint64_t);

    while (len > 0 && buf[len - 1] == '\0')
    {
        --len;
    }

    return std::string(buf, buf + len);
}

} // namespace

///////////////////////////////// Command Class ////////////////////////////////

class object_manager::command
{
    public:
        enum type_t { NORMAL, WAIT, DELETE, SNAPSHOT, ALARM, SHUTDOWN };

    public:
        command();
        command(const command&);

    public:
        command& operator = (const command&);

    public:
        type_t type;
        uint64_t slot;
        uint64_t client;
        uint64_t nonce;
        uint64_t cond;
        uint64_t state;
        std::string data;
};

object_manager :: command :: command()
    : type(NORMAL)
    , slot(0)
    , client(0)
    , nonce(0)
    , cond(0)
    , state(0)
    , data()
{
}

object_manager :: command :: command(const command& other)
    : type(other.type)
    , slot(other.slot)
    , client(other.client)
    , nonce(other.nonce)
    , cond(other.cond)
    , state(other.state)
    , data(other.data)
{
}

///////////////////////////////// Object Class /////////////////////////////////

class object_manager::object
{
    public:
        class condition;
        class suspicion;

    public:
        object(uint64_t created_at_slot);
        ~object() throw ();

    // enqueue commands on the thread
    public:
        void start_thread(object_manager* om, uint64_t obj_id, e::intrusive_ptr<object> obj);
        // swaps backing to keep internals, so data remains valid
        void enqueue(command::type_t,
                     uint64_t slot,
                     uint64_t client,
                     uint64_t nonce,
                     const e::slice& data);
        void enqueue(command::type_t,
                     uint64_t slot,
                     uint64_t client,
                     uint64_t nonce,
                     uint64_t cond,
                     uint64_t state);
        void enqueue(command::type_t,
                     uint64_t state);
        void dequeue(std::list<command>* commands, bool* shutdown);
        void throttle(size_t sz);
        void set_alarm(const char* func, uint64_t seconds);
        bool trip_alarm(uint64_t now, const char** func);
        void set_suspect(uint64_t client,
                         uint64_t slot,
                         std::auto_ptr<e::buffer> callback);
        bool get_suspect_callback(uint64_t client, suspicion* s);
        void get_suspects_not_listed(const std::vector<uint64_t>& clients,
                                     std::vector<suspicion>* s);
        void clear_suspect(uint64_t slot);

    // the state machine; only manipulate within background thread
    public:
        void* lib;
        replicant_state_machine* sym;
        void* rsm;
        std::map<uint64_t, condition> conditions;
        const uint64_t created_at_slot;

    public:
        po6::threads::mutex* mtx() { return &m_mtx; }

    private:
        object(const object&);
        object& operator = (const object&);

    private:
        friend class e::intrusive_ptr<object>;
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        size_t m_ref;
        std::auto_ptr<po6::threads::thread> m_thread;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_commands_avail;
        po6::threads::cond m_command_consumed;
        std::list<command> m_commands;
        const char* m_alarm_func;
        uint64_t m_alarm_when;
        std::map<uint64_t, suspicion> m_suspicions;
};

struct object_manager::object::suspicion
{
    suspicion() : slot(0), callback() {}
    ~suspicion() throw () {}
    uint64_t slot;
    SHARED_PTR<e::buffer> callback;
};

object_manager :: object :: object(uint64_t slot)
    : lib(NULL)
    , sym(NULL)
    , rsm(NULL)
    , conditions()
    , created_at_slot(slot)
    , m_ref(0)
    , m_thread()
    , m_mtx()
    , m_commands_avail(&m_mtx)
    , m_command_consumed(&m_mtx)
    , m_commands()
    , m_alarm_func("")
    , m_alarm_when(0)
    , m_suspicions()
{
}

object_manager :: object :: ~object() throw ()
{
    if (lib)
    {
        dlclose(lib);
    }
}

namespace replicant
{

class thread_wrapper
{
    public:
        thread_wrapper(object_manager* om, uint64_t obj_id, e::intrusive_ptr<object_manager::object> obj)
            : m_om(om)
            , m_obj_id(obj_id)
            , m_obj(obj)
        {
        }

        thread_wrapper(const thread_wrapper& other)
            : m_om(other.m_om)
            , m_obj_id(other.m_obj_id)
            , m_obj(other.m_obj)
        {
        }

    public:
        void operator () ()
        {
            m_om->worker_thread(m_obj_id, m_obj);
        }

    private:
        thread_wrapper& operator = (const thread_wrapper&);

    private:
        object_manager* m_om;
        uint64_t m_obj_id;
        e::intrusive_ptr<object_manager::object> m_obj;
};

} // namespace replicant

void
object_manager :: object :: start_thread(object_manager* om, uint64_t obj_id, e::intrusive_ptr<object> obj)
{
    thread_wrapper tw(om, obj_id, obj);
    m_thread.reset(new po6::threads::thread(REF(tw)));
    m_thread->start();
}

void
object_manager :: object :: enqueue(command::type_t type,
                                    uint64_t slot,
                                    uint64_t client,
                                    uint64_t nonce,
                                    const e::slice& data)
{
    // Allocate the command object
    std::list<command> tmp;
    tmp.push_back(command());
    tmp.back().type = type;
    tmp.back().slot = slot;
    tmp.back().client = client;
    tmp.back().nonce = nonce;
    tmp.back().data = std::string(reinterpret_cast<const char*>(data.data()), data.size());
    // Push it onto the object's queue
    po6::threads::mutex::hold hold(&m_mtx);
    m_commands.splice(m_commands.end(), tmp, tmp.begin());
    m_commands_avail.signal();
}

void
object_manager :: object :: enqueue(command::type_t type,
                                    uint64_t slot,
                                    uint64_t client,
                                    uint64_t nonce,
                                    uint64_t cond,
                                    uint64_t state)
{
    // Allocate the command object
    std::list<command> tmp;
    tmp.push_back(command());
    tmp.back().type = type;
    tmp.back().slot = slot;
    tmp.back().client = client;
    tmp.back().nonce = nonce;
    tmp.back().cond = cond;
    tmp.back().state = state;
    // Push it onto the object's queue
    po6::threads::mutex::hold hold(&m_mtx);
    m_commands.splice(m_commands.end(), tmp, tmp.begin());
    m_commands_avail.signal();
}

void
object_manager :: object :: enqueue(command::type_t type,
                                    uint64_t state)
{
    // Allocate the command object
    std::list<command> tmp;
    tmp.push_back(command());
    tmp.back().type = type;
    tmp.back().state = state;
    // Push it onto the object's queue
    po6::threads::mutex::hold hold(&m_mtx);
    m_commands.splice(m_commands.end(), tmp, tmp.begin());
    m_commands_avail.signal();
}

void
object_manager :: object :: dequeue(std::list<command>* commands, bool*)
{
    assert(commands->empty());
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_commands.empty())
    {
        m_commands_avail.wait();
    }

    if (!m_commands.empty())
    {
        commands->splice(commands->begin(), m_commands);
    }

    m_command_consumed.signal();
}

void
object_manager :: object :: throttle(size_t sz)
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_commands.size() > sz)
    {
        m_command_consumed.wait();
    }
}

void
object_manager :: object :: set_alarm(const char* func, uint64_t when)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_alarm_func = func;
    uint64_t billion = 1000ULL * 1000ULL * 1000ULL;
    m_alarm_when = e::time() + billion * when;
}

bool
object_manager :: object :: trip_alarm(uint64_t now, const char** func)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (now >= m_alarm_when && m_alarm_when > 0)
    {
        *func = m_alarm_func;
        m_alarm_func = "";
        m_alarm_when = 0;
        return true;
    }
    else
    {
        return false;
    }
}

void
object_manager :: object :: set_suspect(uint64_t client, uint64_t slot, std::auto_ptr<e::buffer> _callback)
{
    po6::threads::mutex::hold hold(&m_mtx);
    suspicion s;
    s.slot = slot;
    s.callback.reset(_callback.release());
    m_suspicions[client] = s;
}

bool
object_manager :: object :: get_suspect_callback(uint64_t client, suspicion* s)
{
    po6::threads::mutex::hold hold(&m_mtx);
    std::map<uint64_t, suspicion>::iterator it = m_suspicions.find(client);

    if (it == m_suspicions.end())
    {
        return false;
    }

    *s = it->second;
    return true;
}

void
object_manager :: object :: get_suspects_not_listed(const std::vector<uint64_t>& clients,
                                                    std::vector<suspicion>* s)
{
    po6::threads::mutex::hold hold(&m_mtx);

    for (std::map<uint64_t, suspicion>::iterator it = m_suspicions.begin();
            it != m_suspicions.end(); ++it)
    {
        if (std::binary_search(clients.begin(), clients.end(), it->first))
        {
            continue;
        }

        s->push_back(it->second);
    }
}

void
object_manager :: object :: clear_suspect(uint64_t slot)
{
    po6::threads::mutex::hold hold(&m_mtx);

    for (std::map<uint64_t, suspicion>::iterator it = m_suspicions.begin();
            it != m_suspicions.end(); ++it)
    {
        if (it->second.slot == slot)
        {
            m_suspicions.erase(it);
            break;
        }
    }
}

//////////////////////////////// Condition Class ///////////////////////////////

class object_manager::object::condition
{
    public:
        struct waiter;

    public:
        condition();
        condition(uint64_t state);

    public:
        uint64_t count;
        std::set<waiter> waiters;
};

object_manager :: object :: condition :: condition()
    : count(0)
    , waiters()
{
}

object_manager :: object :: condition :: condition(uint64_t state)
    : count(state)
    , waiters()
{
}

///////////////////////////////// Waiter Class /////////////////////////////////

class object_manager::object::condition::waiter
{
    public:
        waiter();
        waiter(uint64_t wait_for, uint64_t client, uint64_t nonce);

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
    , m_snapshot_cb()
    , m_alarm_cb()
    , m_suspect_cb()
    , m_objects()
    , m_logging_enabled(false)
{
}

object_manager :: ~object_manager() throw ()
{
}

void
object_manager :: set_callback(daemon* d, void (daemon::*command_cb)(uint64_t slot, uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data),
                                          void (daemon::*notify_cb)(uint64_t client, uint64_t nonce, response_returncode rc, const e::slice& data),
                                          void (daemon::*snapshot_cb)(std::auto_ptr<snapshot>),
                                          void (daemon::*alarm_cb)(uint64_t obj_id, const char* func),
                                          void (daemon::*suspect_cb)(uint64_t obj_id, uint64_t cb_id, const e::slice& data))
{
    m_daemon = d;
    m_command_cb = command_cb;
    m_notify_cb = notify_cb;
    m_snapshot_cb = snapshot_cb;
    m_alarm_cb = alarm_cb;
    m_suspect_cb = suspect_cb;
}

void
object_manager :: enqueue(uint64_t slot, uint64_t obj_id,
                          uint64_t client, uint64_t nonce,
                          const e::slice& data)
{
    if (obj_id == OBJECT_OBJ_NEW)
    {
        e::slice lib;
        e::unpacker up(data.data(), data.size());
        up = up >> obj_id >> lib;

        if (up.error())
        {
            LOG(WARNING) << "could not unpack " << obj_id << " @ " << slot;
            command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
            return;
        }

        e::intrusive_ptr<object> obj;
        obj = common_object_initialize(slot, client, nonce, lib, &obj_id);

        if (!obj)
        {
            return;
        }

        replicant_state_machine_context ctx(slot, obj_id, client, this, obj.get());
        obj->rsm = obj->sym->ctor(&ctx);
        log_messages(obj_id, &ctx, "ctor");

        if (!obj->rsm)
        {
            LOG(WARNING) << "constructor for " << obj_id << " @ "  << slot << " failed; delete the object and try again";
            return command_send_error_response(slot, client, nonce, RESPONSE_CTOR_FAILED);
        }

        if (ctx.alarm_when > 0)
        {
            obj->set_alarm(ctx.alarm_func, ctx.alarm_when);
        }

        if (ctx.suspect_client > 0)
        {
            obj->set_suspect(ctx.suspect_client, slot, ctx.suspect_callback);
        }

        obj->start_thread(this, obj_id, obj);
        m_objects.insert(std::make_pair(obj_id, obj));
        return command_send_response(slot, client, nonce, RESPONSE_SUCCESS, e::slice());
    }
    else if (obj_id == OBJECT_OBJ_DEL)
    {
        if (data.size() < sizeof(uint64_t))
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
        }

        e::unpack64be(data.data(), &obj_id);
        e::intrusive_ptr<object> obj = del_object(obj_id);

        if (!obj)
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        obj->enqueue(command::DELETE, slot, client, nonce, data);
    }
    else if (obj_id == OBJECT_OBJ_SNAPSHOT)
    {
        if (data.size() < sizeof(uint64_t))
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
        }

        e::unpack64be(data.data(), &obj_id);
        e::intrusive_ptr<object> obj = get_object(obj_id);

        if (!obj)
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        obj->enqueue(command::SNAPSHOT, slot, client, nonce, data);
    }
    else if (obj_id == OBJECT_OBJ_RESTORE)
    {
        e::slice lib;
        e::slice back;
        e::unpacker up(data.data(), data.size());
        up = up >> obj_id >> lib >> back;

        if (up.error())
        {
            command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
            return;
        }

        e::intrusive_ptr<object> obj;
        obj = common_object_initialize(slot, client, nonce, lib, &obj_id);

        if (!obj)
        {
            return;
        }

        e::slice rtor;
        up = e::unpacker(back.data(), back.size());
        up = up >> rtor;

        while (!up.error() && !up.empty())
        {
            uint64_t cond = 0;
            uint64_t state = 0;
            up = up >> cond >> state;
            obj->conditions.insert(std::make_pair(cond, object::condition(state)));
        }

        if (up.error())
        {
            command_send_error_response(slot, client, nonce, RESPONSE_MALFORMED);
            return;
        }

        replicant_state_machine_context ctx(slot, obj_id, client, this, obj.get());
        obj->rsm = obj->sym->rtor(&ctx, reinterpret_cast<const char*>(rtor.data()), rtor.size());
        log_messages(obj_id, &ctx, "rtor");

        if (!obj->rsm)
        {
            LOG(WARNING) << "reconstructor for " << obj_id << " @ "  << slot << " failed; delete the object and try again";
            return command_send_error_response(slot, client, nonce, RESPONSE_CTOR_FAILED);
        }

        if (ctx.alarm_when > 0)
        {
            obj->set_alarm(ctx.alarm_func, ctx.alarm_when);
        }

        if (ctx.suspect_client > 0)
        {
            obj->set_suspect(ctx.suspect_client, slot, ctx.suspect_callback);
        }

        obj->start_thread(this, obj_id, obj);
        m_objects.insert(std::make_pair(obj_id, obj));
        return command_send_response(slot, client, nonce, RESPONSE_SUCCESS, e::slice());
    }
    else if (!IS_SPECIAL_OBJECT(obj_id))
    {
        e::intrusive_ptr<object> obj = get_object(obj_id);

        if (!obj)
        {
            return command_send_error_response(slot, client, nonce, RESPONSE_OBJ_NOT_EXIST);
        }

        obj->enqueue(command::NORMAL, slot, client, nonce, data);
    }
    else
    {
        LOG(ERROR) << "object_manager asked to work on special object " << obj_id;
        return command_send_error_response(slot, client, nonce, RESPONSE_SERVER_ERROR);
    }
}

void
object_manager :: suspect(uint64_t client)
{
    for (object_map_t::iterator it = m_objects.begin();
            it != m_objects.end(); ++it)
    {
        object::suspicion s;

        if (!it->second->get_suspect_callback(client, &s))
        {
            continue;
        }

        (m_daemon->*m_suspect_cb)(it->first, s.slot, s.callback->as_slice());
    }
}

void
object_manager :: suspect_if_not_listed(const std::vector<uint64_t>& clients)
{
    for (object_map_t::iterator it = m_objects.begin();
            it != m_objects.end(); ++it)
    {
        std::vector<object::suspicion> s;
        it->second->get_suspects_not_listed(clients, &s);

        for (size_t i = 0; i < s.size(); ++i)
        {
            (m_daemon->*m_suspect_cb)(it->first, s[i].slot, s[i].callback->as_slice());
        }
    }
}

void
object_manager :: throttle(uint64_t obj_id, size_t sz)
{
    object_map_t::iterator it = m_objects.find(obj_id);

    if (it == m_objects.end())
    {
        return;
    }

    e::intrusive_ptr<object> obj = it->second;
    obj->throttle(sz);
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
        obj->enqueue(command::WAIT, 0/*XXX*/, client, nonce, cond, state);
    }
    else
    {
        LOG(ERROR) << "object_manager asked to work on special object " << obj_id;
        return notify_send_error_response(client, nonce, RESPONSE_SERVER_ERROR);
    }
}

void
object_manager :: periodic(uint64_t now)
{
    for (object_map_t::iterator it = m_objects.begin();
            it != m_objects.end(); ++it)
    {
        it->second->enqueue(command::ALARM, now);
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

    std::set<object::condition::waiter>& waiters(it->second.waiters);

    while (!waiters.empty())
    {
        const object::condition::waiter& w(*waiters.begin());
        notify_send_response(w.client, w.nonce, RESPONSE_COND_DESTROYED, e::slice("", 0));
        waiters.erase(waiters.begin());
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

    std::set<object::condition::waiter>& waiters(it->second.waiters);

    while (!waiters.empty() &&
           waiters.begin()->wait_for <= it->second.count)
    {
        const object::condition::waiter& w(*waiters.begin());
        notify_send_response(w.client, w.nonce, RESPONSE_SUCCESS, e::slice("", 0));
        waiters.erase(waiters.begin());
    }

    return 0;
}

e::intrusive_ptr<object_manager::object>
object_manager :: common_object_initialize(uint64_t slot,
                                           uint64_t client,
                                           uint64_t nonce,
                                           const e::slice& lib,
                                           uint64_t* obj_id)
{
    // write out the library
    char buf[43 /*strlen("./libreplicant-slot<slot \lt 2**64>.so\x00")*/];
    sprintf(buf, "./libreplicant-slot%llu.so", slot);
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

    // create a new object, if none exists
    e::intrusive_ptr<object> obj = get_object(*obj_id);

    if (obj)
    {
        command_send_error_response(slot, client, nonce, RESPONSE_OBJ_EXIST);
        return NULL;
    }

    obj = new object(slot);
    po6::threads::mutex::hold hold(obj->mtx());
    obj->lib = dlopen(buf, RTLD_NOW|RTLD_LOCAL);

    if (!obj->lib)
    {
        const char* err = dlerror();
        LOG(ERROR) << "could not load library for slot " << slot
                   << ": " << err << "; delete the object and try again";
        command_send_error_msg_response(slot, client, nonce, RESPONSE_DLOPEN_FAIL, err);
        return NULL;
    }

    if (unlink(buf) < 0)
    {
        PLOG(ERROR) << "could not unlink temporary library " << buf;
        abort();
    }

    obj->sym = static_cast<replicant_state_machine*>(dlsym(obj->lib, "rsm"));

    if (!obj->sym)
    {
        const char* err = dlerror();
        LOG(ERROR) << "could not find \"rsm\" symbol in library for slot "
                   << slot << ": " << err
                   << "; delete the object and try again";
        command_send_error_msg_response(slot, client, nonce, RESPONSE_DLSYM_FAIL, err);
        return NULL;
    }

    if (!obj->sym->ctor)
    {
        LOG(WARNING) << "library for slot " << slot << " does not specify a constructor; delete the object and try again";
        command_send_error_response(slot, client, nonce, RESPONSE_NO_CTOR);
        return NULL;
    }

    if (!obj->sym->rtor)
    {
        LOG(WARNING) << "library for slot " << slot << " does not specify a reconstructor; delete the object and try again";
        command_send_error_response(slot, client, nonce, RESPONSE_NO_RTOR);
        return NULL;
    }

    if (!obj->sym->dtor)
    {
        LOG(WARNING) << "library for slot " << slot << " does not specify a destructor; delete the object and try again";
        command_send_error_response(slot, client, nonce, RESPONSE_NO_DTOR);
        return NULL;
    }

    if (!obj->sym->snap)
    {
        LOG(WARNING) << "library for slot " << slot << " does not specify a snapshot function; delete the object and try again";
        command_send_error_response(slot, client, nonce, RESPONSE_NO_SNAP);
        return NULL;
    }

    return obj;
}

e::intrusive_ptr<object_manager::object>
object_manager :: get_object(uint64_t obj_id)
{
    object_map_t::iterator it = m_objects.find(obj_id);

    if (it == m_objects.end())
    {
        return NULL;
    }

    return it->second;
}

e::intrusive_ptr<object_manager::object>
object_manager :: del_object(uint64_t obj_id)
{
    object_map_t::iterator it = m_objects.find(obj_id);

    if (it == m_objects.end())
    {
        return NULL;
    }

    e::intrusive_ptr<object> obj = it->second;
    m_objects.erase(it);
    return obj;
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
        obj->dequeue(&commands, &shutdown);

        while (!commands.empty() && !shutdown)
        {
            dispatch_command(obj_id, obj, commands.front(), &shutdown);
            commands.pop_front();
        }
    }

    LOG(INFO) << "exiting worker thread for object " << obj_id;
}

void
object_manager :: log_messages(uint64_t obj_id, replicant_state_machine_context* ctx, const char* func)
{
    ctx->close_log_output();

    if (!m_logging_enabled)
    {
        return;
    }

    std::string obj_str = obj_id_to_str(obj_id);
    const char* ptr = ctx->log_output;
    const char* end = ctx->log_output + ctx->log_output_sz;

    while (ptr < end)
    {
        const void* ptr_nl = memchr(ptr, '\n', end - ptr);
        const void* ptr_0  = memchr(ptr, '\0', end - ptr);
        const char* eol = end;

        if (ptr_nl && ptr_0 && ptr_0 <= ptr_nl)
        {
            eol = static_cast<const char*>(ptr_0);
        }
        else if (ptr_nl)
        {
            eol = static_cast<const char*>(ptr_nl);
        }
        else if (ptr_0)
        {
            eol = static_cast<const char*>(ptr_0);
        }

        LOG(INFO) << obj_str << ":" << func << " @ " << ctx->slot << ": " << std::string(ptr, eol);
        ptr = eol + 1;
    }
}

void
object_manager :: dispatch_command(uint64_t obj_id,
                                   e::intrusive_ptr<object> obj,
                                   const command& cmd,
                                   bool* shutdown)
{
    if (cmd.client == CLIENT_SUSPECT)
    {
        obj->clear_suspect(cmd.nonce);
    }

    switch (cmd.type)
    {
        case command::NORMAL:
            return dispatch_command_normal(obj_id, obj, cmd, shutdown);
        case command::WAIT:
            return dispatch_command_wait(obj_id, obj, cmd, shutdown);
        case command::DELETE:
            return dispatch_command_delete(obj_id, obj, cmd, shutdown);
        case command::SNAPSHOT:
            return dispatch_command_snapshot(obj_id, obj, cmd, shutdown);
        case command::ALARM:
            return dispatch_command_alarm(obj_id, obj, cmd, shutdown);
        case command::SHUTDOWN:
            return dispatch_command_shutdown(obj_id, obj, cmd, shutdown);
        default:
            LOG(ERROR) << "unknown command type " << static_cast<unsigned>(cmd.type);
    }
}

void
object_manager :: dispatch_command_normal(uint64_t obj_id,
                                          e::intrusive_ptr<object> obj,
                                          const command& cmd,
                                          bool*)
{
    const char* func = reinterpret_cast<const char*>(cmd.data.data());
    size_t func_sz = strnlen(func, cmd.data.size());

    if (func_sz >= cmd.data.size())
    {
        command_send_error_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_MALFORMED);
        return;
    }

    replicant_state_machine_step* syms = obj->sym->steps;
    replicant_state_machine_step* sym = NULL;

    while (syms->name)
    {
        if (strcmp(func, syms->name) == 0)
        {
            sym = syms;
            break;
        }

        ++syms;
    }

    if (sym)
    {
        replicant_state_machine_context ctx(cmd.slot, obj_id, cmd.client, this, obj.get());
        const char* data = func + func_sz + 1;
        size_t data_sz = cmd.data.size() - func_sz - 1;
        sym->func(&ctx, obj->rsm, data, data_sz);
        log_messages(obj_id, &ctx, func);

        if (ctx.alarm_when > 0)
        {
            obj->set_alarm(ctx.alarm_func, ctx.alarm_when);
        }

        if (ctx.suspect_client > 0)
        {
            obj->set_suspect(ctx.suspect_client, cmd.slot, ctx.suspect_callback);
        }

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
        std::string obj_str = obj_id_to_str(obj_id);
        LOG(INFO) << obj_str << ": no such call \"" << func << "\" @ " << cmd.slot;
    }
}

void
object_manager :: dispatch_command_wait(uint64_t,
                                        e::intrusive_ptr<object> obj,
                                        const command& cmd,
                                        bool*)
{
    std::map<uint64_t, object::condition>::iterator it = obj->conditions.find(cmd.cond);

    if (it == obj->conditions.end())
    {
        notify_send_error_response(cmd.client, cmd.nonce, RESPONSE_COND_NOT_EXIST);
    }
    else
    {
        if (cmd.state <= it->second.count)
        {
            notify_send_response(cmd.client, cmd.nonce, RESPONSE_SUCCESS, e::slice("", 0));
        }
        else
        {
            it->second.waiters.insert(object::condition::waiter(cmd.state, cmd.client, cmd.nonce));
        }
    }
}

void
object_manager :: dispatch_command_delete(uint64_t obj_id,
                                          e::intrusive_ptr<object> obj,
                                          const command& cmd,
                                          bool* shutdown)
{
    *shutdown = true;
    replicant_state_machine_context ctx(cmd.slot, obj_id, cmd.client, this, obj.get());
    obj->sym->dtor(&ctx, obj->rsm);
    log_messages(obj_id, &ctx, "dtor");

    while (!obj->conditions.empty())
    {
        condition_destroy(obj.get(), obj->conditions.begin()->first);
    }

    command_send_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_SUCCESS, e::slice("", 0));
}

void
object_manager :: dispatch_command_snapshot(uint64_t obj_id,
                                            e::intrusive_ptr<object> obj,
                                            const command& cmd,
                                            bool*)
{
    std::auto_ptr<snapshot> snap(new snapshot());
    snap->object_created_at_slot = obj->created_at_slot;
    replicant_state_machine_context ctx(cmd.slot, obj_id, cmd.client, this, obj.get());
    obj->sym->snap(&ctx, obj->rsm, &snap->data, &snap->data_sz);
    log_messages(obj_id, &ctx, "snap");

    if (!snap->data)
    {
        PLOG(ERROR) << "Out of memory?";
        abort();
    }

    if (ctx.alarm_when > 0)
    {
        obj->set_alarm(ctx.alarm_func, ctx.alarm_when);
    }

    if (ctx.suspect_client > 0)
    {
        obj->set_suspect(ctx.suspect_client, cmd.slot, ctx.suspect_callback);
    }

    // serialize the conditions
    snap->conditions.reserve(obj->conditions.size());

    for (std::map<uint64_t, object::condition>::iterator it = obj->conditions.begin();
            it != obj->conditions.end(); ++it)
    {
        snap->conditions.push_back(std::make_pair(it->first, it->second.count));
    }

    if (cmd.client == 0)
    {
        ((*m_daemon).*m_snapshot_cb)(snap);
    }
    else
    {
        size_t sz = sizeof(uint32_t) + snap->data_sz
                  + snap->conditions.size() * 2 * sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        e::buffer::packer pa = msg->pack_at(0);
        pa = pa << e::slice(snap->data, snap->data_sz);

        for (size_t i = 0; i < snap->conditions.size(); ++i)
        {
            pa = pa << snap->conditions[i].first
                    << snap->conditions[i].second;
        }

        command_send_response(cmd.slot, cmd.client, cmd.nonce, RESPONSE_SUCCESS, msg->as_slice());
    }
}

void
object_manager :: dispatch_command_alarm(uint64_t obj_id,
                                         e::intrusive_ptr<object> obj,
                                         const command& cmd,
                                         bool*)
{
    const char* alarm_func = "";

    if (obj->trip_alarm(cmd.state, &alarm_func))
    {
        ((*m_daemon).*m_alarm_cb)(obj_id, alarm_func);
    }
}

void
object_manager :: dispatch_command_shutdown(uint64_t,
                                            e::intrusive_ptr<object>,
                                            const command&,
                                            bool* shutdown)
{
    *shutdown = true;
    // intentionally leak the object for faster shutdown
    // we only enter this function if replicant is shutting down anyway
}
