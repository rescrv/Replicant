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

// C
#include <assert.h>

// POSIX
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>

// Google Log
#include <glog/logging.h>

// e
#include <e/atomic.h>
#include <e/endian.h>
#include <e/guard.h>

// BusyBee
#include <busybee_constants.h>

// Replicant
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "daemon/daemon.h"
#include "daemon/object.h"
#include "daemon/object_interface.h"
#include "daemon/replica.h"
#include "daemon/snapshot.h"

using replicant::object;

struct object::enqueued_cond_wait
{
    enqueued_cond_wait(uint64_t slot,
                       server_id si, uint64_t nonce,
                       const e::slice& cond,
                       uint64_t state);
    enqueued_cond_wait(const enqueued_cond_wait&);
    ~enqueued_cond_wait() throw ();

    uint64_t slot;
    server_id si;
    uint64_t nonce;
    std::string cond;
    uint64_t state;
};

object :: enqueued_cond_wait :: enqueued_cond_wait(uint64_t _slot,
                                                   server_id _si, uint64_t _nonce,
                                                   const e::slice& _cond,
                                                   uint64_t _state)
    : slot(_slot)
    , si(_si)
    , nonce(_nonce)
    , cond(_cond.cdata(), _cond.size())
    , state(_state)
{
}

object :: enqueued_cond_wait :: enqueued_cond_wait(const enqueued_cond_wait& other)
    : slot(other.slot)
    , si(other.si)
    , nonce(other.nonce)
    , cond(other.cond)
    , state(other.state)
{
}

object :: enqueued_cond_wait :: ~enqueued_cond_wait() throw ()
{
}

struct object::enqueued_call
{
    enqueued_call();
    enqueued_call(const e::slice& func,
                  const e::slice& input,
                  const pvalue& p,
                  unsigned flags,
                  uint64_t command_nonce,
                  server_id si,
                  uint64_t request_nonce);
    enqueued_call(const enqueued_call&);
    ~enqueued_call() throw ();

    std::string func;
    std::string input;
    pvalue p;
    unsigned flags;
    uint64_t command_nonce;
    server_id si;
    uint64_t request_nonce;
};

object :: enqueued_call :: enqueued_call()
    : func()
    , input()
    , p()
    , flags()
    , command_nonce()
    , si()
    , request_nonce()
{
}

object :: enqueued_call :: enqueued_call(const e::slice& _func,
                                         const e::slice& _input,
                                         const pvalue& _p,
                                         unsigned _flags,
                                         uint64_t _command_nonce,
                                         server_id _si,
                                         uint64_t _request_nonce)
    : func(_func.cdata(), _func.size())
    , input(_input.cdata(), _input.size())
    , p(_p)
    , flags(_flags)
    , command_nonce(_command_nonce)
    , si(_si)
    , request_nonce(_request_nonce)
{
}

object :: enqueued_call :: enqueued_call(const enqueued_call& other)
    : func(other.func)
    , input(other.input)
    , p(other.p)
    , flags(other.flags)
    , command_nonce(other.command_nonce)
    , si(other.si)
    , request_nonce(other.request_nonce)
{
}

object :: enqueued_call :: ~enqueued_call() throw ()
{
}

object :: object(replica* r, uint64_t slot, const std::string& n, object_t t, const std::string& init)
    : m_ref(0)
    , m_replica(r)
    , m_obj_slot(slot)
    , m_obj_name(n)
    , m_type(t)
    , m_init(init)
    , m_obj_pid()
    , m_mtx()
    , m_cond(&m_mtx)
    , m_has_ctor(false)
    , m_has_rtor(false)
    , m_rtor()
    , m_cond_waits()
    , m_calls()
    , m_snapshots()
    , m_highest_slot(0)
    , m_fail_at(UINT64_MAX)
    , m_failed(false)
    , m_done(false)
    , m_snap_mtx()
    , m_snap()
    , m_thread(po6::threads::make_thread_wrapper(&object::run, this))
    , m_fd()
    , m_conditions()
    , m_tick_func()
    , m_tick_interval()
    , m_last_executed(0)
{
    e::atomic::store_64_release(&m_last_executed, 0);
    m_thread.start();
}

object :: ~object() throw ()
{
    fail();
    m_thread.join();
    m_fd.close();
    po6::threads::mutex::hold hold(&m_mtx);

    for (std::map<std::string, condition*>::iterator it = m_conditions.begin();
            it != m_conditions.end(); ++it)
    {
        delete it->second;
    }
}

uint64_t
object :: last_executed() const
{
    return e::atomic::load_64_acquire(&m_last_executed);
}

std::string
object :: last_state()
{
    po6::threads::mutex::hold hold(&m_snap_mtx);
    return m_snap;
}

void
object :: set_child(pid_t child, int fd)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_obj_pid = child;
    m_fd = fd;
    m_cond.signal();
}

bool
object :: failed()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_failed;
}

bool
object :: done()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_done;
}

void
object :: ctor()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_has_ctor = true;
    m_cond.signal();
}

void
object :: rtor(e::unpacker up)
{
    po6::threads::mutex::hold hold(&m_mtx);
    e::slice tf;
    uint64_t cond_size;
    up = up >> m_fail_at >> tf >> m_tick_interval >> e::unpack_varint(cond_size);
    m_tick_func.assign(tf.str());

    for (uint64_t i = 0; i < cond_size; ++i)
    {
        e::slice s;
        std::auto_ptr<condition> c(new condition());
        up = up >> s >> *c;
        m_conditions[s.str()] = c.get();
        c.release();
    }

    e::slice state;
    up = up >> state;

    while (up.remain() && !up.error())
    {
        enqueued_call c;
        up = up >> c;

        if (!up.error())
        {
            m_calls.push_back(c);
        }
    }

    if (up.error())
    {
        m_fail_at = 0;
    }

    m_has_rtor = true;
    m_rtor.assign(state.cdata(), state.size());
    m_cond.signal();
}

void
object :: cond_wait(server_id si, uint64_t nonce,
                    const e::slice& cond,
                    uint64_t state)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (m_failed || m_highest_slot >= m_fail_at)
    {
        m_replica->m_daemon->callback_client(si, nonce, REPLICANT_MAYBE, "");
        return;
    }

    m_cond_waits.push_back(enqueued_cond_wait(m_highest_slot, si, nonce, cond, state));
    m_cond.signal();
}

void
object :: call(const e::slice& func,
               const e::slice& input,
               const pvalue& p,
               unsigned flags,
               uint64_t command_nonce,
               server_id si,
               uint64_t request_nonce)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (m_failed || p.s >= m_fail_at)
    {
        m_replica->executed(p, flags, command_nonce, si, request_nonce, REPLICANT_MAYBE, "");
        return;
    }

    assert(p.s > m_highest_slot);
    m_highest_slot = p.s;
    m_calls.push_back(enqueued_call(func, input, p, flags, command_nonce, si, request_nonce));
    m_cond.signal();
}

void
object :: take_snapshot(e::intrusive_ptr<snapshot> snap)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (m_failed || snap->slot() >= m_fail_at)
    {
        return;
    }

    snap->start_object(m_obj_name);
    m_snapshots.push_back(snap);
    m_cond.signal();
}

void
object :: fail_at(uint64_t slot)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_fail_at = slot;
    m_cond.signal();
}

void
object :: run()
{
    bool has_ctor = false;
    bool has_rtor = false;

    {
        po6::threads::mutex::hold hold(&m_mtx);

        while (!m_failed && m_fd.get() < 0)
        {
            m_cond.wait();
        }

        while (!m_failed && !m_has_ctor && !m_has_rtor)
        {
            m_cond.wait();
        }

        if (m_failed)
        {
            return;
        }

        has_ctor = m_has_ctor;
        has_rtor = m_has_rtor;
    }

    assert(has_ctor || has_rtor);

    if (has_ctor)
    {
        char c(ACTION_CTOR);

        if (!write(&c, 1))
        {
            return;
        }
    }
    else if (has_rtor)
    {
        char buf[5];
        buf[0] = ACTION_RTOR;
        e::pack32be(m_rtor.size(), buf + 1);

        if (!write(buf, 5))
        {
            return;
        }

        if (!write(m_rtor.data(), m_rtor.size()))
        {
            return;
        }
    }

    bool tor_done = false;

    while (!tor_done)
    {
        char buf[1];

        if (!read(buf, 1))
        {
            return;
        }

        command_response_t cr = static_cast<command_response_t>(buf[0]);
        enqueued_call c_log("<init>", "", pvalue(ballot(), m_obj_slot, ""), 0, 0, server_id(), 0);
        enqueued_call c_output("", "", pvalue(), 0, 0, server_id(), 0);

        switch (cr)
        {
            case COMMAND_RESPONSE_LOG:
                do_call_log(c_log);
                break;
            case COMMAND_RESPONSE_COND_CREATE:
                do_call_cond_create();
                break;
            case COMMAND_RESPONSE_COND_DESTROY:
                do_call_cond_destroy();
                break;
            case COMMAND_RESPONSE_COND_BROADCAST:
                do_call_cond_broadcast();
                break;
            case COMMAND_RESPONSE_COND_BROADCAST_DATA:
                do_call_cond_broadcast_data();
                break;
            case COMMAND_RESPONSE_COND_CURRENT_VALUE:
                do_call_cond_current_value();
                break;
            case COMMAND_RESPONSE_TICK_INTERVAL:
                do_call_tick_interval();
                break;
            case COMMAND_RESPONSE_OUTPUT:
                do_call_output(c_output);
                tor_done = true;
                break;
            default:
                fail();
                return;
        }
    }

    {
        std::string tmp;
        do_snapshot(&tmp);
    }

    std::list<enqueued_cond_wait> cond_waits;
    std::list<enqueued_call> calls;
    std::list<e::intrusive_ptr<snapshot> > snapshots;
    uint64_t failed_at = m_fail_at;

    while (true)
    {
        assert(calls.empty());

        {
            po6::threads::mutex::hold hold(&m_mtx);

            while (!m_failed && m_calls.empty() && m_cond_waits.empty() && m_snapshots.empty() && m_fail_at == UINT64_MAX)
            {
                m_cond.wait();
            }

            if (m_failed)
            {
                break;
            }

            cond_waits.splice(cond_waits.end(), m_cond_waits);
            calls.splice(calls.end(), m_calls);
            snapshots.splice(snapshots.end(), m_snapshots);
            failed_at = m_fail_at;
            assert(m_cond_waits.empty());
            assert(m_calls.empty());
            assert(m_snapshots.empty());
        }

        while (!calls.empty() || !snapshots.empty())
        {
            const uint64_t call_slot = calls.empty() ? UINT64_MAX : calls.front().p.s;
            const uint64_t snap_slot = snapshots.empty() ? UINT64_MAX : snapshots.front()->slot();
            assert(call_slot != UINT64_MAX || snap_slot != UINT64_MAX);

            if (snap_slot <= call_slot)
            {
                do_snapshot(snapshots.front());
                snapshots.pop_front();
            }
            else
            {
                do_call(calls.front());

                {
                    po6::threads::mutex::hold hold(&m_snap_mtx);
                    e::packer pa(&m_snap, m_snap.size());
                    pa = pa << calls.front();
                }

                calls.pop_front();
            }
        }

        while (!cond_waits.empty() && cond_waits.front().slot <= e::atomic::load_64_acquire(&m_last_executed))
        {
            do_cond_wait(cond_waits.front());
            cond_waits.pop_front();
        }

        if (failed_at < UINT64_MAX)
        {
            e::atomic::store_64_release(&m_last_executed, failed_at);
            fail();
        }
    }

    for (std::list<enqueued_cond_wait>::iterator it = cond_waits.begin();
            it != cond_waits.end(); ++it)
    {
        const enqueued_cond_wait& cw(*it);
        m_replica->m_daemon->callback_client(cw.si, cw.nonce, REPLICANT_MAYBE, "");
    }

    for (std::list<enqueued_call>::iterator it = calls.begin();
            it != calls.end(); ++it)
    {
        const enqueued_call& c(*it);
        m_replica->executed(c.p, c.flags, c.command_nonce, c.si, c.request_nonce, REPLICANT_MAYBE, "");
    }

    for (std::list<e::intrusive_ptr<snapshot> >::iterator it = m_snapshots.begin();
            it != m_snapshots.end(); ++it)
    {
        (*it)->abort_snapshot();
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(BUSYBEE_HEADER_SIZE + 8));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_OBJECT_FAILED;
    daemon* d = m_replica->m_daemon;
    d->send_from_non_main_thread(d->id(), msg);
    po6::threads::mutex::hold hold(&m_mtx);
    m_done = true;
}

void
object :: do_cond_wait(const enqueued_cond_wait& cw)
{
    if (failed())
    {
        m_replica->m_daemon->callback_client(cw.si, cw.nonce, REPLICANT_MAYBE, "");
        return;
    }

    std::map<std::string, condition*>::iterator it = m_conditions.find(cw.cond);

    if (it == m_conditions.end())
    {
        m_replica->m_daemon->callback_client(cw.si, cw.nonce, REPLICANT_OBJ_NOT_FOUND, "");
    }
    else
    {
        it->second->wait(m_replica->m_daemon, cw.si, cw.nonce, cw.state);
    }
}

void
object :: do_call(const enqueued_call& c)
{
    e::atomic::store_64_release(&m_last_executed, std::max(c.p.s, e::atomic::load_64_acquire(&m_last_executed)));

    if (failed())
    {
        m_replica->executed(c.p, c.flags, c.command_nonce, c.si, c.request_nonce, REPLICANT_MAYBE, "");
        return;
    }

    if (c.func == "__backup__")
    {
        m_replica->executed(c.p, c.flags, c.command_nonce, c.si, c.request_nonce, REPLICANT_SUCCESS, m_snap);
        return;
    }

    e::slice func(c.func);
    e::slice input(c.input);

    if (c.func == "__tick__")
    {
        uint64_t tick;
        e::unpacker up(input);
        up = up >> tick;

        if (up.error() || m_tick_func.empty() || tick % m_tick_interval != 0)
        {
            m_replica->executed(c.p, c.flags, c.command_nonce, c.si, c.request_nonce, REPLICANT_SUCCESS, "");
            return;
        }

        func = m_tick_func;
        input = "";
    }

    const size_t sz = sizeof(uint64_t)
                    + sizeof(uint32_t) + func.size()
                    + sizeof(uint32_t) + input.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz + 1));
    msg->pack_at(0)
        << uint8_t(ACTION_COMMAND)
        << uint64_t(sz)
        << uint32_t(func.size())
        << e::pack_memmove(func.data(), func.size())
        << uint32_t(input.size())
        << e::pack_memmove(input.data(), input.size());

    if (!write(msg->cdata(), msg->size()))
    {
        return;
    }

    while (true)
    {
        char buf[1];

        if (!read(buf, 1))
        {
            return;
        }

        command_response_t cr = static_cast<command_response_t>(buf[0]);

        switch (cr)
        {
            case COMMAND_RESPONSE_LOG:
                do_call_log(c);
                break;
            case COMMAND_RESPONSE_COND_CREATE:
                do_call_cond_create();
                break;
            case COMMAND_RESPONSE_COND_DESTROY:
                do_call_cond_destroy();
                break;
            case COMMAND_RESPONSE_COND_BROADCAST:
                do_call_cond_broadcast();
                break;
            case COMMAND_RESPONSE_COND_BROADCAST_DATA:
                do_call_cond_broadcast_data();
                break;
            case COMMAND_RESPONSE_COND_CURRENT_VALUE:
                do_call_cond_current_value();
                break;
            case COMMAND_RESPONSE_TICK_INTERVAL:
                do_call_tick_interval();
                break;
            case COMMAND_RESPONSE_OUTPUT:
                return do_call_output(c);
            default:
                fail();
                return;
        }
    }
}

void
object :: do_snapshot(e::intrusive_ptr<snapshot> snap)
{
    e::guard g_abort = e::makeobjguard(*snap, &snapshot::abort_snapshot);
    std::string s;
    do_snapshot(&s);
    g_abort.dismiss();
    snap->finish_object(m_obj_name, s);

    if (snap->done())
    {
        m_replica->snapshot_finished();
    }
}

void
object :: do_snapshot(std::string* s)
{
    if (failed())
    {
        return;
    }

    char buf[4];
    buf[0] = ACTION_SNAPSHOT;

    if (!write(buf, 1))
    {
        return;
    }

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> snapshot_buf(o);

    if (!read(&snapshot_buf[0], o))
    {
        return;
    }

    s->clear();
    e::packer pa(s);
    pa = pa << m_type << m_init
            << m_fail_at
            << e::slice(m_tick_func)
            << m_tick_interval
            << e::pack_varint(m_conditions.size());

    for (cond_map_t::iterator it = m_conditions.begin();
            it != m_conditions.end(); ++it)
    {
        pa = pa << it->first << *it->second;
    }

    pa = pa << e::slice(&snapshot_buf[0], snapshot_buf.size());
    po6::threads::mutex::hold hold(&m_snap_mtx);
    m_snap = *s;
}

void
object :: do_call_log(const enqueued_call& c)
{
    char buf[4];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> log_buf(o);

    if (!read(&log_buf[0], o))
    {
        return;
    }

    std::ostringstream ostr;
    ostr << "log output for object \"" << m_obj_name << "\":\n";

    const char* ptr = &log_buf[0];
    const char* end = &log_buf[0] + log_buf.size();

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

        ostr << m_obj_name << "." << c.func << " @ slot=" << c.p.s << ": " << std::string(ptr, eol) << "\n";
        ptr = eol + 1;
    }

    LOG(INFO) << ostr.str();
}

void
object :: do_call_cond_create()
{
    char buf[4];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> cond_buf(o);

    if (!read(&cond_buf[0], o))
    {
        return;
    }

    std::string cond(&cond_buf[0], o);
    cond_map_t::iterator it = m_conditions.find(cond);

    if (it == m_conditions.end())
    {
        std::auto_ptr<condition> c(new condition());
        m_conditions.insert(std::make_pair(cond, c.get()));
        c.release();
    }
}

void
object :: do_call_cond_destroy()
{
    char buf[4];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> cond_buf(o);

    if (!read(&cond_buf[0], o))
    {
        return;
    }

    std::string cond(&cond_buf[0], o);
    cond_map_t::iterator it = m_conditions.find(cond);

    if (it != m_conditions.end())
    {
        delete it->second;
        m_conditions.erase(it);
    }
}

void
object :: do_call_cond_broadcast()
{
    char buf[4];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> cond_buf(o);

    if (!read(&cond_buf[0], o))
    {
        return;
    }

    std::string cond(&cond_buf[0], o);
    cond_map_t::iterator it = m_conditions.find(cond);

    if (it != m_conditions.end())
    {
        it->second->broadcast(m_replica->m_daemon);
        buf[0] = 0;
        write(buf, 1);
    }
    else
    {
        buf[0] = 1;
        write(buf, 1);
    }
}

void
object :: do_call_cond_broadcast_data()
{
    char buf[4];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> cond_buf(o);

    if (!read(&cond_buf[0], o))
    {
        return;
    }

    std::string cond(&cond_buf[0], o);

    if (!read(buf, 4))
    {
        return;
    }

    e::unpack32be(buf, &o);
    std::vector<char> data_buf(o);

    if (!read(&data_buf[0], o))
    {
        return;
    }

    cond_map_t::iterator it = m_conditions.find(cond);

    if (it != m_conditions.end())
    {
        it->second->broadcast(m_replica->m_daemon, &data_buf[0], o);
        buf[0] = 0;
        write(buf, 1);
    }
    else
    {
        buf[0] = 1;
        write(buf, 1);
    }
}

void
object :: do_call_cond_current_value()
{
    char buf[12];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> cond_buf(o);

    if (!read(&cond_buf[0], o))
    {
        return;
    }

    std::string cond(&cond_buf[0], o);
    cond_map_t::iterator it = m_conditions.find(cond);

    if (it == m_conditions.end())
    {
        buf[0] = 1;
        write(buf, 1);
        return;
    }

    buf[0] = 0;
    write(buf, 1);
    uint64_t state;
    const char* data;
    size_t data_sz;
    it->second->peek_state(&state, &data, &data_sz);
    o = data_sz;
    e::pack64be(state, buf);
    e::pack32be(o, buf);
    write(buf, 12);
    write(data, o);
}

void
object :: do_call_tick_interval()
{
    char buf[8];

    if (!read(buf, 4))
    {
        return;
    }

    uint32_t o;
    e::unpack32be(buf, &o);
    std::vector<char> func_buf(o);

    if (!read(&func_buf[0], o))
    {
        return;
    }

    std::string func(&func_buf[0], o);

    if (!read(buf, 8))
    {
        return;
    }

    uint64_t tick;
    e::unpack64be(buf, &tick);
    m_tick_func = func;
    m_tick_interval = tick;
}

void
object :: do_call_output(const enqueued_call& c)
{
    char buf[6];

    if (!read(buf, 6))
    {
        return;
    }

    uint16_t s;
    uint32_t o;
    e::unpack16be(buf, &s);
    e::unpack32be(buf + 2, &o);
    std::vector<char> output_buf(o);

    if (!read(&output_buf[0], o))
    {
        return;
    }

    replicant_returncode status = static_cast<replicant_returncode>(s);
    std::string output(&output_buf[0], o);
    m_replica->executed(c.p, c.flags, c.command_nonce, c.si, c.request_nonce, status, output);
}

void
object :: fail()
{
    std::list<enqueued_cond_wait> cond_waits;
    std::list<enqueued_call> calls;

    {
        po6::threads::mutex::hold hold(&m_mtx);

        if (m_failed)
        {
            return;
        }

        cond_waits.splice(cond_waits.end(), m_cond_waits);
        calls.splice(calls.end(), m_calls);
        assert(m_cond_waits.empty());
        assert(m_calls.empty());
        m_failed = true;
        m_cond.signal();
    }

    int status;
    pid_t p = waitpid(m_obj_pid, &status, WNOHANG);

    if (p <= 0)
    {
        kill(m_obj_pid, SIGKILL);
        p = waitpid(m_obj_pid, &status, 0);
    }

    for (std::list<enqueued_cond_wait>::iterator it = cond_waits.begin();
            it != cond_waits.end(); ++it)
    {
        const enqueued_cond_wait& cw(*it);
        m_replica->m_daemon->callback_client(cw.si, cw.nonce, REPLICANT_MAYBE, "");
    }

    for (std::list<enqueued_call>::iterator it = calls.begin();
            it != calls.end(); ++it)
    {
        const enqueued_call& c(*it);
        m_replica->executed(c.p, c.flags, c.command_nonce, c.si, c.request_nonce, REPLICANT_MAYBE, "");
    }
}

bool
object :: read(char* data, size_t sz)
{
    if (m_fd.xread(data, sz) != ssize_t(sz))
    {
        fail();
        return false;
    }

    return true;
}

bool
object :: write(const char* data, size_t sz)
{
    if (m_fd.xwrite(data, sz) != ssize_t(sz))
    {
        fail();
        return false;
    }

    return true;
}

e::packer
replicant :: operator << (e::packer lhs, const object_t& rhs)
{
    return lhs << uint8_t(rhs);
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, object_t& rhs)
{
    uint8_t x;
    lhs = lhs >> x;
    rhs = object_t(x);
    return lhs;
}

size_t
replicant :: pack_size(const object_t&)
{
    return e::pack_size(uint8_t());
}

e::packer
replicant :: operator << (e::packer lhs, const object::enqueued_call& rhs)
{
    return lhs
        << e::slice(rhs.func)
        << e::slice(rhs.input)
        << rhs.p
        << uint32_t(rhs.flags)
        << rhs.command_nonce
        << rhs.si
        << rhs.request_nonce;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, object::enqueued_call& rhs)
{
    e::slice func;
    e::slice input;
    uint32_t flags;
    lhs = lhs >> func
              >> input
              >> rhs.p
              >> flags
              >> rhs.command_nonce
              >> rhs.si
              >> rhs.request_nonce;
    rhs.func.assign(func.cdata(), func.size());
    rhs.input.assign(input.cdata(), input.size());
    rhs.flags = flags;
    return lhs;
}
