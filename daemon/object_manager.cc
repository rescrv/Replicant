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

//PO6
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>
#include <po6/threads/cond.h>

// POSIX
#include <dlfcn.h>

// STL
#include <stdexcept>
#include <list>
#include <tr1/memory>

// Google Log
#include <glog/logging.h>

// BusyBee
#include <busybee_constants.h>

// Replicant
#include "common/network_msgtype.h"
#include "daemon/object_manager.h"
#include "daemon/replicant_state_machine.h"
#include "daemon/daemon.h"

class object_manager::object
{
    public:
        object(const po6::pathname& path);
        ~object() throw ();

    public:
        const po6::pathname& path() const;
        void* lib() const;
        replicant_state_machine* sym() const;
        void* rsm() const;
        void run_cmds(replicant_daemon* daemon);

    public:
        void set_lib(void* lib);
        void set_sym(void* sym);
        void set_rsm(void* rsm);

    private:
        friend class e::intrusive_ptr<object>;
        friend class object_manager;

    private:
        object(const object&);

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        object& operator = (const object&);

    private:
        typedef std::list<e::intrusive_ptr<command> > command_list;
        size_t m_ref;
        void* m_lib;
        replicant_state_machine* m_sym;
        void* m_rsm;
        po6::pathname m_path;
        po6::threads::cond m_commands_avail;
        po6::threads::mutex m_lock;
        command_list m_cmds;
        void get_next_cmds(std::list<e::intrusive_ptr<command> >& commands);
};

object_manager :: snapshot :: snapshot()
    : m_backings()
{
}

object_manager :: snapshot :: ~snapshot() throw ()
{
    for (size_t i = 0; i < m_backings.size(); ++i)
    {
        if (m_backings[i])
        {
            free(m_backings[i]);
        }
    }
}

object_manager :: object_manager()
    : m_objects()
    , m_threads()
{
}

object_manager :: ~object_manager() throw ()
{
}

bool
object_manager :: exists(uint64_t o) const
{
    return m_objects.find(o) != m_objects.end();
}

bool
object_manager :: valid_path(const e::slice& pathstr) const
{
    if (pathstr.size() + 3 >= PATH_MAX)
    {
        return false;
    }

    for (size_t i = 0; i < pathstr.size(); ++i)
    {
        if (!isalnum(pathstr.data()[i]) &&
            pathstr.data()[i] != '-' &&
            pathstr.data()[i] != '_' &&
            pathstr.data()[i] != '.' &&
            pathstr.data()[i] != '\x00')
        {
            return false;
        }
    }

    return true;
}

static int
callback_set_response(void* ctx, const char* data, size_t data_sz)
{
    try
    {
        command* cmd = static_cast<command*>(ctx);
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_RESPONSE)
                  + sizeof(uint64_t) + sizeof(uint8_t)
                  + data_sz;
        std::auto_ptr<e::buffer> response(e::buffer::create(sz));
        response->pack_at(sz - data_sz).copy(e::slice(data, data_sz));
        cmd->set_response(response);
        return 1;
    }
    catch (std::bad_alloc& a)
    {
        return 0;
    }
}

static uint64_t
callback_client_cmd(void* ctx)
{
    command* cmd = static_cast<command*>(ctx);
    return cmd->object();
}

static uint64_t
callback_client_obj(void*)
{
    return 0;
}

static void
callback_log_cmd(void* ctx, const char* msg)
{
    command* cmd = static_cast<command*>(ctx);
    LOG(INFO) << "log message for object " << cmd->object() << ": " << msg;
}

static void
callback_log_obj(void* ctx, const char* msg)
{
    uint64_t obj = reinterpret_cast<uint64_t>(ctx);
    LOG(INFO) << "log message for object " << obj << ": " << msg;
}

void
object_manager :: append_cmd(e::intrusive_ptr<command> cmd)
{
    object_map::iterator objiter = m_objects.find(cmd->object());
    if (objiter == m_objects.end())
    {
        LOG_EVERY_N(WARNING, 1024) << "received operation for non-existent object " << cmd->object();
        return;
    }

    e::intrusive_ptr<object> obj(objiter->second);
    po6::threads::mutex::hold hold(&obj->m_lock);
    std::list<e::intrusive_ptr<command> > tmp;
    tmp.push_back(cmd);
    obj->m_cmds.splice(obj->m_cmds.end(),tmp);
    obj->m_commands_avail.signal();
}

void
object_manager :: object :: get_next_cmds(std::list<e::intrusive_ptr<command> >& commands)
{
    po6::threads::mutex::hold hold(&m_lock);
    while(m_cmds.empty())
        m_commands_avail.wait();
    commands.splice(commands.end(),m_cmds);
}

void
object_manager :: object :: run_cmds(replicant_daemon* daemon)
{
    std::list<e::intrusive_ptr<command> > commands;
    LOG(INFO) << "Starting thread for object";
    while(true)
    {
        get_next_cmds(commands);
        while(!commands.empty())
        {
            e::intrusive_ptr<command> cmd = commands.front();
            commands.pop_front();

            // The response
            size_t sz = BUSYBEE_HEADER_SIZE
                + pack_size(REPLNET_COMMAND_RESPONSE)
                + sizeof(uint64_t) + sizeof(uint8_t);
            std::auto_ptr<e::buffer> response(e::buffer::create(sz));
            e::buffer::packer pa = response->pack_at(BUSYBEE_HEADER_SIZE);
            pa = pa << REPLNET_COMMAND_RESPONSE
                << cmd->nonce() << uint8_t(0);
            cmd->set_response(response);

            // Parse the request
            size_t off = BUSYBEE_HEADER_SIZE
                + pack_size(REPLNET_COMMAND_ISSUE)
                + 4 * sizeof(uint64_t);
            e::slice content = cmd->msg()->unpack_from(off).as_slice();
            const char* data = reinterpret_cast<const char*>(content.data());
            size_t data_sz = content.size();

            size_t cmd_sz = strnlen(data, data_sz);

            if (cmd_sz >= data_sz)
            {
                LOG(WARNING) << "cannot parse function name for " << cmd->object();
                return;
            }

            replicant_state_machine_step* rsms = m_sym->steps;

            while (rsms->name)
            {
                if (strcmp(data, rsms->name) == 0)
                {
                    replicant_state_machine_actions acts;
                    acts.ctx = cmd.get();
                    acts.client = callback_client_cmd;
                    acts.log = callback_log_cmd;
                    acts.set_response = callback_set_response;
                    rsms->func(&acts, m_rsm, data + cmd_sz + 1, data_sz - cmd_sz - 1);
                    break;
                }

                ++rsms;
            }

            pa = cmd->response()->pack_at(BUSYBEE_HEADER_SIZE);
            pa = pa << REPLNET_COMMAND_RESPONSE
                << cmd->nonce() << uint8_t(1);

            daemon->send_command_response(cmd);
        }
    }
}


void
object_manager :: apply(e::intrusive_ptr<command> cmd)
{
    // The response
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_RESPONSE)
              + sizeof(uint64_t) + sizeof(uint8_t);
    std::auto_ptr<e::buffer> response(e::buffer::create(sz));
    e::buffer::packer pa = response->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << REPLNET_COMMAND_RESPONSE
            << cmd->nonce() << uint8_t(0);
    cmd->set_response(response);

    // Parse the request
    size_t off = BUSYBEE_HEADER_SIZE
               + pack_size(REPLNET_COMMAND_ISSUE)
               + 4 * sizeof(uint64_t);
    e::slice content = cmd->msg()->unpack_from(off).as_slice();
    const char* data = reinterpret_cast<const char*>(content.data());
    size_t data_sz = content.size();
    object_map::iterator objiter = m_objects.find(cmd->object());

    if (objiter == m_objects.end())
    {
        LOG_EVERY_N(WARNING, 1024) << "received operation for non-existent object " << cmd->object();
        return;
    }

    size_t cmd_sz = strnlen(data, data_sz);

    if (cmd_sz >= data_sz)
    {
        LOG(WARNING) << "cannot parse function name for " << cmd->object();
        return;
    }

    e::intrusive_ptr<object> obj(objiter->second);
    replicant_state_machine_step* rsms = obj->sym()->steps;

    while (rsms->name)
    {
        if (strcmp(data, rsms->name) == 0)
        {
            replicant_state_machine_actions acts;
            acts.ctx = cmd.get();
            acts.client = callback_client_cmd;
            acts.log = callback_log_cmd;
            acts.set_response = callback_set_response;
            rsms->func(&acts, obj->rsm(), data + cmd_sz + 1, data_sz - cmd_sz - 1);
            break;
        }

        ++rsms;
    }

    pa = cmd->response()->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << REPLNET_COMMAND_RESPONSE
            << cmd->nonce() << uint8_t(1);
}

bool
object_manager :: create(uint64_t o, const e::slice& p, replicant_daemon* daemon)
{
    e::intrusive_ptr<object> obj(open_library(p));

    if (!obj)
    {
        return false;
    }

    std::pair<object_map::iterator, bool> inserted;
    inserted = m_objects.insert(std::make_pair(o, obj));
    assert(inserted.second);
    replicant_state_machine_actions acts;
    acts.ctx = reinterpret_cast<void*>(o);
    acts.client = callback_client_obj;
    acts.log = callback_log_obj;
    acts.set_response = NULL;
    void* rsm = obj->sym()->ctor(&acts);
    if (!rsm)
    {
        LOG(ERROR) << "could not create replicated object";
        return false;
    }

    obj->set_rsm(rsm);

    std::tr1::function<void (object*, replicant_daemon*)> 
        fobj(std::tr1::function<void (object*, replicant_daemon*)>(&object::run_cmds));
    thread_ptr t(new po6::threads::thread(std::tr1::bind(fobj, obj.get(),daemon)));
    t->start();
    m_threads.push_back(t);

    return true;
}

bool
object_manager :: restore(uint64_t id,
                          const e::slice& path,
                          const e::slice& snap)
{
    e::intrusive_ptr<object> obj(open_library(path));

    if (!obj)
    {
        return false;
    }

    std::pair<object_map::iterator, bool> inserted;
    inserted = m_objects.insert(std::make_pair(id, obj));
    assert(inserted.second);
    replicant_state_machine_actions acts;
    acts.ctx = reinterpret_cast<void*>(id);
    acts.client = callback_client_obj;
    acts.log = callback_log_obj;
    acts.set_response = NULL;
    void* rsm = obj->sym()->rtor(&acts,
                                 reinterpret_cast<const char*>(snap.data()),
                                 snap.size());

    if (!rsm)
    {
        LOG(ERROR) << "could not create replicated object";
        return false;
    }

    obj->set_rsm(rsm);
    return true;
}

void
object_manager :: take_snapshot(snapshot* snap,
                                std::vector<std::pair<uint64_t, std::pair<e::slice, e::slice> > >* objects)
{
    objects->clear();
    objects->reserve(m_objects.size());
    snap->m_backings.clear();
    snap->m_backings.reserve(m_objects.size());

    for (object_map::iterator it = m_objects.begin();
            it != m_objects.end(); ++it)
    {
        replicant_state_machine_actions acts;
        acts.ctx = reinterpret_cast<void*>(it->first);
        acts.client = callback_client_obj;
        acts.log = callback_log_obj;
        acts.set_response = NULL;
        char* data = NULL;
        size_t sz = 0;
        it->second->sym()->snap(&acts, it->second->rsm(), &data, &sz);

        if (data)
        {
            snap->m_backings.push_back(data);
        }

        e::slice path(it->second->path().get(), strlen(it->second->path().get()) + 1);
        objects->push_back(std::make_pair(it->first, std::make_pair(path, e::slice(data, sz))));
    }
}

e::intrusive_ptr<object_manager::object>
object_manager :: open_library(const e::slice& p)
{
    po6::pathname path(reinterpret_cast<const char*>(p.data()));
    e::intrusive_ptr<object> obj(new object(path));
    path = po6::join(po6::pathname(""), path);
    std::cout << path.get() << std::endl;
    obj->set_lib(dlopen(path.get(), RTLD_NOW|RTLD_LOCAL));

    if (!obj->lib())
    {
        LOG(ERROR) << "could not dlopen library " << dlerror();
        return NULL;
    }

    obj->set_sym(dlsym(obj->lib(), "rsm"));

    if (!obj->sym())
    {
        LOG(ERROR) << "could not retrieve symbol \"rsm\" from library \""
                   << path.get() << "\":  " << dlerror();
        return NULL;
    }

    if (!obj->sym()->ctor ||
        !obj->sym()->rtor ||
        !obj->sym()->dtor ||
        !obj->sym()->snap)
    {
        LOG(ERROR) << "symbol \"rsm\" from library \""
                   << path.get() << "\" contains one or more NULL functions";
        return NULL;
    }

    return obj;
}

object_manager :: object :: object(const po6::pathname& p)
    : m_ref(0)
    , m_lib(NULL)
    , m_sym(NULL)
    , m_rsm(NULL)
    , m_path(p)
    , m_cmds()
    , m_lock()
    , m_commands_avail(&m_lock)
{
}

object_manager :: object :: ~object() throw ()
{
}

const po6::pathname&
object_manager :: object :: path() const
{
    return m_path;
}

void*
object_manager :: object :: lib() const
{
    return m_lib;
}

replicant_state_machine*
object_manager :: object :: sym() const
{
    return m_sym;
}

void*
object_manager :: object :: rsm() const
{
    return m_rsm;
}

void
object_manager :: object :: set_lib(void* l)
{
    m_lib = l;
}

void
object_manager :: object :: set_sym(void* s)
{
    m_sym = static_cast<replicant_state_machine*>(s);
}

void
object_manager :: object :: set_rsm(void* r)
{
    m_rsm = r;
}
