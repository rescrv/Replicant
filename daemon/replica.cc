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

// POSIX
#include <signal.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>

// e
#include <e/guard.h>
#include <e/strescape.h>

// Replicant
#include "common/atomic_io.h"
#include "common/constants.h"
#include "common/packing.h"
#include "daemon/daemon.h"
#include "daemon/replica.h"
#include "daemon/slot_type.h"

#pragma GCC diagnostic ignored "-Wunsafe-loop-optimizations"

using replicant::replica;

extern bool s_debug_mode;

struct replica::history
{
    history()
        : slot(), nonce(), status(), output() {}
    history(uint64_t s, uint64_t n, replicant_returncode st, const std::string& o)
        : slot(s), nonce(n), status(st), output(o) {}
    history(const history& other)
        : slot(other.slot), nonce(other.nonce), status(other.status), output(other.output) {}
    ~history() throw () {}
    uint64_t slot;
    uint64_t nonce;
    replicant_returncode status;
    std::string output;
};

struct replica::repair_info
{
    repair_info() : when(0), highest(0), failures(), snapshot_slot(0), snapshot_content() {}
    ~repair_info() throw () {}
    uint64_t when;
    uint64_t highest;
    std::vector<server_id> failures;
    uint64_t snapshot_slot;
    std::string snapshot_content;
};

e::packer
replicant :: operator << (e::packer lhs, const replica::history& rhs)
{
    return lhs << rhs.slot << rhs.nonce << rhs.status << e::slice(rhs.output);
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, replica::history& rhs)
{
    e::slice o;
    lhs = lhs >> rhs.slot >> rhs.nonce >> rhs.status >> o;
    rhs.output.assign(o.cdata(), o.size());
    return lhs;
}

size_t
replicant :: pack_size(const replica::history& rhs)
{
    e::slice o(rhs.output);
    return 2 * sizeof(uint64_t) + pack_size(rhs.status) + pack_size(o);
}

replica :: replica(daemon* d, const configuration& c)
    : m_daemon(d)
    , m_slot(0)
    , m_pvalues()
    , m_configs()
    , m_slots(c.servers().size(), c.first_slot())
    , m_cond_config(c.version().get())
    , m_cond_tick(c.version().get())
    , m_counter(0)
    , m_command_nonces()
    , m_command_nonces_lookup()
    , m_objects()
    , m_dying_objects()
    , m_failed_objects()
    , m_snapshots_mtx()
    , m_snapshots()
    , m_latest_snapshot_mtx()
    , m_latest_snapshot_slot(0)
    , m_latest_snapshot_backing()
    , m_robust_mtx()
    , m_robust_history()
    , m_robust_history_lookup()
{
    m_command_nonces_lookup.set_empty_key(UINT64_MAX);
    m_command_nonces_lookup.set_deleted_key(UINT64_MAX - 1);
    m_robust_history_lookup.set_empty_key(UINT64_MAX);
    m_robust_history_lookup.set_deleted_key(UINT64_MAX - 1);
    m_configs.push_back(c);
}

replica :: ~replica() throw ()
{
}

bool
replica :: any_config_has(server_id si) const
{
    for (std::list<configuration>::const_iterator it = m_configs.begin();
            it != m_configs.end(); ++it)
    {
        if (it->has(si))
        {
            return true;
        }
    }

    return false;
}

bool
replica :: any_config_has(const po6::net::location& bind_to) const
{
    for (std::list<configuration>::const_iterator it = m_configs.begin();
            it != m_configs.end(); ++it)
    {
        if (it->has(bind_to))
        {
            return true;
        }
    }

    return false;
}

void
replica :: learn(const pvalue& p)
{
    if (p.s < m_slot)
    {
        return;
    }

    std::list<pvalue>::iterator it = m_pvalues.begin();

    while (it != m_pvalues.end() && it->s < p.s)
        ++it;

    if (it != m_pvalues.end() && it->s == p.s)
    {
        return;
    }

    m_pvalues.insert(it, p);
    LOG_IF(INFO, s_debug_mode) << "learned: " << p;

    while (!m_pvalues.empty() && m_pvalues.begin()->s == m_slot)
    {
        execute(m_pvalues.front());
        m_pvalues.erase(m_pvalues.begin());
        ++m_slot;

        while (m_configs.size() > 1 && (++m_configs.begin())->first_slot() <= m_slot)
        {
            m_configs.pop_front();
            const configuration& c(m_configs.front());
            m_cond_config.broadcast(m_daemon);
            assert(m_cond_config.peek_state() == c.version().get());
            m_slots = std::vector<uint64_t>(c.servers().size(), 0);
            initiate_snapshot();
        }

        if (m_slot % 250 == 0)
        {
            initiate_snapshot();
        }
    }
}

void
replica :: window(uint64_t* start, uint64_t* limit) const
{
    *start = m_slot;
    *limit = m_slot + REPLICANT_SLOTS_WINDOW;

    if (m_configs.size() > 1)
    {
        *limit = std::min(*limit, (++m_configs.begin())->first_slot());
    }
}

uint64_t
replica :: gc_up_to() const
{
    if (m_slots.empty())
    {
        return 0;
    }

    return *std::min_element(m_slots.begin(), m_slots.end());
}

void
replica :: cond_wait(server_id si, uint64_t nonce,
                     const e::slice& _obj,
                     const e::slice& _cond,
                     uint64_t state)
{
    std::string obj(_obj.cdata(), _obj.size());
    std::string cond(_cond.cdata(), _cond.size());

    if (obj == "replicant")
    {
        if (cond == "configuration")
        {
            m_cond_config.wait(m_daemon, si, nonce, state);
        }
        else if (cond == "tick")
        {
            m_cond_tick.wait(m_daemon, si, nonce, state);
        }
        else
        {
            LOG(WARNING) << "client requesting non-existent condition \"replicant." << e::strescape(obj) << "\"";
            m_daemon->callback_client(si, nonce, REPLICANT_COND_NOT_FOUND, "");
        }
    }
    else
    {
        object_map_t::iterator it = m_objects.find(obj);

        if (it != m_objects.end() && it->second)
        {
            it->second->cond_wait(si, nonce, _cond, state);
        }
        else if (it != m_objects.end())
        {
            LOG(WARNING) << "client requesting partially-initialized object \"" << e::strescape(obj) << "\"";
            m_daemon->callback_client(si, nonce, REPLICANT_MAYBE, "");
        }
        else
        {
            LOG(WARNING) << "client requesting non-existent object \"" << e::strescape(obj) << "\"";
            m_daemon->callback_client(si, nonce, REPLICANT_OBJ_NOT_FOUND, "");
        }
    }
}

bool
replica :: has_output(uint64_t nonce,
                      uint64_t min_slot,
                      replicant_returncode* status,
                      std::string* output)
{
    po6::threads::mutex::hold hold(&m_robust_mtx);

    if (!m_robust_history.empty() && min_slot < m_robust_history.front().slot)
    {
        *status = REPLICANT_MAYBE;
        *output = "";
        return true;
    }

    if (m_robust_history_lookup.find(nonce) == m_robust_history_lookup.end())
    {
        return false;
    }

    for (std::list<history>::iterator it = m_robust_history.begin();
            it != m_robust_history.end(); ++it)
    {
        if (it->nonce == nonce)
        {
            *status = it->status;
            *output = it->output;
            return true;
        }
    }

    abort();
}

void
replica :: clean_dead_objects()
{
    for (object_list_t::iterator it = m_dying_objects.begin();
            it != m_dying_objects.end(); )
    {
        if ((*it)->done())
        {
            it = m_dying_objects.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void
replica :: take_blocking_snapshot(uint64_t* snapshot_slot,
                                  e::slice* snapshot,
                                  std::auto_ptr<e::buffer>* snapshot_backing)
{
    initiate_snapshot();
    snapshot_barrier();
    get_last_snapshot(snapshot_slot, snapshot, snapshot_backing);
}

void
replica :: initiate_snapshot()
{
    e::intrusive_ptr<snapshot> snap;

    {
        po6::threads::mutex::hold hold(&m_snapshots_mtx);

        for (object_map_t::iterator it = m_objects.begin();
                it != m_objects.end(); ++it)
        {
            if (it->second->failed())
            {
                LOG(INFO) << "skipping snapshot because \"" << e::strescape(it->first) << "\" has failed";
                return;
            }
        }

        if (!m_snapshots.empty() && m_snapshots.back()->slot() >= m_slot)
        {
            return;
        }

        snap = new snapshot(m_slot);
        m_snapshots.push_back(snap);

        assert(!m_configs.empty());
        std::vector<uint64_t> command_nonces(m_command_nonces.begin(), m_command_nonces.end());
        po6::threads::mutex::hold hold2(&m_robust_mtx);
        std::vector<history> robust_history(m_robust_history.begin(), m_robust_history.end());
        const size_t sz = sizeof(uint64_t)
                        + sizeof(uint64_t)
                        + pack_size(m_configs.front())
                        + pack_size(m_cond_config)
                        + pack_size(m_cond_tick)
                        + sizeof(uint32_t) + sizeof(uint64_t) * m_slots.size()
                        + sizeof(uint32_t) + sizeof(uint64_t) * command_nonces.size()
                        + pack_size(robust_history);
        std::auto_ptr<e::buffer> tmp(e::buffer::create(sz));
        tmp->pack_at(0)
            << m_slot << m_counter << m_configs.front() << m_slots
            << m_cond_config << m_cond_tick << command_nonces << robust_history;
        snap->replica_internals(tmp->as_slice());

        for (object_map_t::iterator it = m_objects.begin();
                it != m_objects.end(); ++it)
        {
            it->second->take_snapshot(snap);
        }
    }

    if (snap->done())
    {
        snapshot_finished();
    }
}

void
replica :: snapshot_barrier()
{
    e::intrusive_ptr<snapshot> snap;

    if (!m_snapshots.empty())
    {
        po6::threads::mutex::hold hold(&m_snapshots_mtx);
        snap = m_snapshots.back();
    }

    if (snap)
    {
        snap->wait();
    }
}

uint64_t
replica :: last_snapshot_num()
{
    po6::threads::mutex::hold hold(&m_latest_snapshot_mtx);
    return m_latest_snapshot_slot;
}

void
replica :: get_last_snapshot(uint64_t* snapshot_slot,
                             e::slice* snapshot,
                             std::auto_ptr<e::buffer>* snapshot_backing)
{
    bool block = false;
    m_latest_snapshot_mtx.lock();
    block = !m_latest_snapshot_backing.get();
    m_latest_snapshot_mtx.unlock();

    if (block)
    {
        initiate_snapshot();
        snapshot_barrier();
    }

    po6::threads::mutex::hold hold(&m_latest_snapshot_mtx);

    if (m_latest_snapshot_slot == 0)
    {
        *snapshot_slot = 0;
        *snapshot = e::slice();
        return;
    }

    *snapshot_slot = m_latest_snapshot_slot;
    snapshot_backing->reset(m_latest_snapshot_backing->copy());
    *snapshot = (*snapshot_backing)->as_slice();
}

void
replica :: snapshot_finished()
{
    po6::threads::mutex::hold hold(&m_snapshots_mtx);
    uint64_t snap_slot = 0;

    for (std::list<e::intrusive_ptr<snapshot> >::reverse_iterator it = m_snapshots.rbegin();
            it != m_snapshots.rend(); ++it)
    {
        if ((*it)->done())
        {
            po6::threads::mutex::hold hold2(&m_latest_snapshot_mtx);
            m_latest_snapshot_slot = (*it)->slot();
            snap_slot = m_latest_snapshot_slot;
            const std::string& snap((*it)->contents());
            m_latest_snapshot_backing.reset(e::buffer::create(snap.size()));
            m_latest_snapshot_backing->resize(snap.size());
            memmove(m_latest_snapshot_backing->data(), snap.data(), snap.size());
            break;
        }
    }

    while (!m_snapshots.empty() && m_snapshots.front()->slot() <= snap_slot)
    {
        if (!m_snapshots.front()->done())
        {
            m_snapshots.front()->abort_snapshot();
        }

        m_snapshots.pop_front();
    }
}

replica*
replica :: from_snapshot(daemon* d, const e::slice& snap)
{
    uint64_t slot;
    uint64_t counter;
    configuration c;
    e::unpacker up(snap.data(), snap.size());
    up = up >> slot >> counter >> c;

    if (up.error())
    {
        LOG(ERROR) << "corrupt replica state";
        return NULL;
    }

    std::auto_ptr<replica> rep(new replica(d, c));
    rep->m_slot = slot;
    rep->m_counter = counter;
    std::vector<uint64_t> command_nonces;
    std::vector<history> robust_history;
    up = up >> rep->m_slots >> rep->m_cond_config >> rep->m_cond_tick
            >> command_nonces >> robust_history;

    if (up.error() || c.servers().size() != rep->m_slots.size())
    {
        LOG(ERROR) << "corrupt replica state";
        return NULL;
    }

    std::vector<std::pair<e::slice, e::slice> > objects;

    while (up.remain() && !up.error())
    {
        e::slice obj_name;
        e::slice snap_state;
        up = up >> obj_name >> snap_state;

        if (!up.error())
        {
            objects.push_back(std::make_pair(obj_name, snap_state));
        }
    }

    if (up.error())
    {
        LOG(ERROR) << "corrupt replica state";
        return NULL;
    }

    rep->m_command_nonces = std::deque<uint64_t>(command_nonces.begin(), command_nonces.end());
    rep->m_robust_history = std::list<history>(robust_history.begin(), robust_history.end());

    for (size_t i = 0; i < command_nonces.size(); ++i)
    {
        rep->m_command_nonces_lookup.insert(command_nonces[i]);
    }

    for (size_t i = 0; i < robust_history.size(); ++i)
    {
        rep->m_robust_history_lookup.insert(robust_history[i].nonce);
    }

    for (size_t i = 0; i < objects.size(); ++i)
    {
        e::slice name(objects[i].first);
        LOG(INFO) << "recreating object \"" << e::strescape(std::string(name.cdata(), name.size())) << "\"";

        if (!rep->relaunch(objects[i].first, rep->m_slot, objects[i].second))
        {
            LOG(ERROR) << "could not create object:  corrupt replica state";
            return NULL;
        }
    }

    return rep.release();
}

void
replica :: enqueue_failed_objects()
{
    for (object_map_t::iterator it = m_objects.begin();
            it != m_objects.end(); ++it)
    {
        if (it->second->failed())
        {
            std::string cmd;
            e::packer pa(&cmd);
            pa << m_daemon->id() << e::slice(it->first) << it->second->last_executed();
            m_daemon->enqueue_paxos_command(SLOT_OBJECT_FAILED, cmd);
        }
    }
}

void
replica :: execute(const pvalue& p)
{
    if (p.c.empty())
    {
        return;
    }

    slot_type type;
    uint8_t flags;
    uint64_t nonce;
    e::unpacker up(p.c.data(), p.c.size());
    up = up >> type >> flags >> nonce;

    if (up.error())
    {
        LOG(ERROR) << "bad command: " << e::slice(p.c).hex();
        return;
    }

    server_id si;
    uint64_t request_nonce;
    m_daemon->callback_enqueued(nonce, &si, &request_nonce);

    if (nonce != 0)
    {
        replicant_returncode status = REPLICANT_SUCCESS;
        std::string result;

        if (has_output(nonce, UINT64_MAX, &status, &result))
        {
            m_daemon->callback_client(si, nonce, status, result);
            return;
        }

        if (m_command_nonces_lookup.find(nonce) != m_command_nonces_lookup.end())
        {
            return;
        }

        m_command_nonces.push_back(nonce);
        m_command_nonces_lookup.insert(nonce);

        while (m_command_nonces.size() > REPLICANT_SERVER_DRIVEN_NONCE_HISTORY)
        {
            m_command_nonces_lookup.erase(m_command_nonces.front());
            m_command_nonces.pop_front();
        }
    }

    // SLOT_CALL may be executed asynchronously, which introduces complexity
    // with saving the output, etc.  Separate it into its own function, so that
    // logic doesn't pollute the other calls.
    if (type == SLOT_CALL)
    {
        execute_call(p, flags, nonce, si, request_nonce, up);
        return;
    }

    LOG_IF(INFO, (flags & 1)) << "internal error: robust flag set for non SLOT_CALL command";

    switch (type)
    {
        case SLOT_SERVER_BECOME_MEMBER:
            execute_server_become_member(p, up);
            break;
        case SLOT_SERVER_SET_GC_THRESH:
            execute_server_set_gc_thresh(up);
            break;
        case SLOT_INCREMENT_COUNTER:
            execute_increment_counter(up);
            break;
        case SLOT_OBJECT_FAILED:
            execute_object_failed(p, up);
            break;
        case SLOT_OBJECT_REPAIR:
            execute_object_repair(up);
            break;
        case SLOT_TICK:
            execute_tick(p, flags, nonce, si, request_nonce, up);
            break;
        case SLOT_POKE:
            execute_poke(up.remainder());
            break;
        case SLOT_CALL:
            abort();
        case SLOT_NOP:
            break;
        default:
            LOG(ERROR) << "bad command: " << e::slice(p.c).hex();
            break;
    }

    if (si != server_id())
    {
        m_daemon->callback_client(si, request_nonce, REPLICANT_SUCCESS, "");
    }
}

void
replica :: execute_server_become_member(const pvalue& p, e::unpacker up)
{
    server s;
    up = up >> s;

    if (up.error())
    {
        LOG(ERROR) << "invalid command for becomming a member";
        LOG(ERROR) << "check that the most recently launched "
                   << "server(s) are running the latest Replicant";
        return;
    }

    const configuration& c(m_configs.back());

    if (!c.has(s.id) && !c.has(s.bind_to))
    {
        LOG(INFO) << "adding " << s << " to " << c.cluster();
        m_configs.push_back(configuration(c, s, p.s + REPLICANT_SLOTS_WINDOW));
    }
}

void
replica :: execute_server_set_gc_thresh(e::unpacker up)
{
    server_id si;
    uint64_t threshold;
    up = up >> si >> threshold;

    if (up.error())
    {
        return;
    }

    const configuration& c(m_configs.front());
    assert(m_slots.size() == c.servers().size());

    for (size_t i = 0; i < c.servers().size(); ++i)
    {
        if (c.servers()[i].id == si)
        {
            m_slots[i] = std::max(m_slots[i], threshold);
        }
    }
}

void
replica :: execute_increment_counter(e::unpacker up)
{
    server_id si;
    uint64_t token;
    up = up >> si >> token;
    m_counter += REPLICANT_NONCE_INCREMENT;
    m_daemon->callback_nonce_sequence(si, token, m_counter);
}

void
replica :: execute_object_failed(const pvalue& p, e::unpacker up)
{
    server_id si;
    e::slice name;
    uint64_t when;
    up = up >> si >> name >> when;

    if (up.error())
    {
        LOG(ERROR) << "invalid command to record a crashed object";
        LOG(ERROR) << "check that the most recently launched "
                   << "server(s) are running the latest Replicant";
        LOG(ERROR) << "this error can be recovered from by removing and "
                   << "recreating the object";
        return;
    }

    object_map_t::iterator it = m_objects.find(name.str());

    if (it == m_objects.end())
    {
        return;
    }

    if (it->second->created_at() > when)
    {
        return;
    }

    const server* s = m_configs.front().get(si);

    if (!s)
    {
        return;
    }

    LOG(WARNING) << *s << " reports that \""
                 << e::strescape(name.str())
                 << "\" failed at slot " << when
                 << "; initiating repair process";

    it->second->fail_at(p.s);
    failure_map_t::iterator f = m_failed_objects.find(name.str());

    if (f == m_failed_objects.end())
    {
        std::pair<failure_map_t::iterator, bool> x =
            m_failed_objects.insert(std::make_pair(name.str(), repair_info()));
        assert(x.second);
        f = x.first;
        f->second.when = when;
        f->second.highest = when;
    }

    repair_info* ri = &f->second;
    ri->highest = std::max(ri->highest, when);

    if (std::find(ri->failures.begin(), ri->failures.end(), si) == ri->failures.end())
    {
        ri->failures.push_back(si);
        LOG(INFO) << "\"" << e::strescape(name.str()) << "\" failed on " << si << " @ " << when;
    }

    const std::vector<server>& servers(m_configs.front().servers());
    bool all_failed = true;

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (std::find(ri->failures.begin(), ri->failures.end(), servers[i].id) == ri->failures.end())
        {
            all_failed = false;
        }
    }

    if (all_failed)
    {
        LOG(INFO) << "all servers have agreed to fail \""
                  << e::strescape(name.str()) << "\"";
        post_fail_action(it->second.get(), ri);
    }
}

void
replica :: execute_kill_object(const pvalue& p,
                               unsigned flags,
                               uint64_t command_nonce,
                               server_id si,
                               uint64_t request_nonce,
                               const e::slice& input)
{
    const std::string name(input.str());
    object_map_t::iterator it = m_objects.find(name);

    if (it == m_objects.end())
    {
        LOG(ERROR) << "an administrative command tried to kill \""
                   << e::strescape(name) << "\", but it doesn't exist";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_OBJ_NOT_FOUND, "");
        return;
    }

    LOG(WARNING) << "an administrative command killed \""
                 << e::strescape(name) << "\"";

    it->second->fail_at(p.s);
    failure_map_t::iterator f = m_failed_objects.find(name);

    if (f == m_failed_objects.end())
    {
        std::pair<failure_map_t::iterator, bool> x =
            m_failed_objects.insert(std::make_pair(name, repair_info()));
        assert(x.second);
        f = x.first;
        f->second.when = p.s;
    }

    repair_info* ri = &f->second;
    const std::vector<server>& servers(m_configs.front().servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        ri->failures.push_back(servers[i].id);
    }

    post_fail_action(it->second.get(), ri);
    executed(p, flags, command_nonce, si, request_nonce, REPLICANT_SUCCESS, "");
}

void
replica :: execute_list_objects(const pvalue& p,
                                unsigned flags,
                                uint64_t command_nonce,
                                server_id si,
                                uint64_t request_nonce,
                                const e::slice&)
{
    std::ostringstream ostr;

    for (object_map_t::iterator it = m_objects.begin();
            it != m_objects.end(); ++it)
    {
        ostr << e::strescape(it->first) << std::endl;
    }

    executed(p, flags, command_nonce, si, request_nonce, REPLICANT_SUCCESS, ostr.str());
}

void
replica :: post_fail_action(object* obj, repair_info* ri)
{
    if (ri->highest == obj->last_executed())
    {
        std::string repair;
        e::packer pa(&repair);
        pa = pa << e::slice(obj->name()) << ri->when << m_daemon->id() << ri->highest << e::slice(obj->last_state());
        m_daemon->enqueue_paxos_command(SLOT_OBJECT_REPAIR, repair);
    }
}

void
replica :: execute_object_repair(e::unpacker up)
{
    e::slice name;
    uint64_t when;
    server_id si;
    uint64_t slot;
    e::slice state;
    up = up >> name >> when >> si >> slot >> state;

    if (up.error())
    {
        LOG(ERROR) << "invalid command to repair a crashed object";
        LOG(ERROR) << "check that the most recently launched "
                   << "server(s) are running the latest Replicant";
        LOG(ERROR) << "this error can be recovered from by removing and "
                   << "recreating the object";
        return;
    }

    object_map_t::iterator it = m_objects.find(name.str());

    if (it == m_objects.end())
    {
        return;
    }

    failure_map_t::iterator f = m_failed_objects.find(name.str());

    if (f == m_failed_objects.end() ||
        f->second.when != when)
    {
        return;
    }

    m_dying_objects.push_back(it->second);

    LOG(INFO) << "relaunching \"" << e::strescape(name.str())
              << "\" that failed at slot " << when
              << " using a snapshot taken on " << si << " at " << slot;

    if (!relaunch(name, slot, state))
    {
        it = m_objects.find(name.str());

        if (it != m_objects.end())
        {
            it->second->fail_at(slot);
        }
        else
        {
            LOG(ERROR) << "permanent error with \"" << e::strescape(name.str())
                       << "\" that may lead to its unavailability, or divergence";
            LOG(ERROR) << "delete the object and recreate it to avoid problems";
        }
    }
    else
    {
        m_failed_objects.erase(f);
    }
}

void
replica :: execute_poke(const e::slice& s)
{
    LOG(INFO) << "poke: " << e::strescape(std::string(s.cdata(), s.size()));
}

void
replica :: execute_tick(const pvalue& p,
                        unsigned flags,
                        uint64_t command_nonce,
                        server_id si,
                        uint64_t request_nonce,
                        e::unpacker up)
{
    e::slice t = up.remainder();
    uint64_t tick;
    up = up >> tick;

    if (up.error())
    {
        return;
    }

    if (m_cond_tick.peek_state() == tick)
    {
        m_cond_tick.broadcast(m_daemon);

        for (object_map_t::iterator it = m_objects.begin();
                it != m_objects.end(); ++it)
        {
            it->second->call("__tick__", t, p, flags, command_nonce, si, request_nonce);
        }
    }
}

void
replica :: execute_call(const pvalue& p,
                        unsigned flags,
                        uint64_t command_nonce,
                        server_id si,
                        uint64_t request_nonce,
                        e::unpacker up)
{
    e::slice obj;
    e::slice func;
    e::slice input;
    up = up >> obj >> func >> input;

    if (up.error())
    {
        LOG(ERROR) << "invalid rpc call";
        LOG(ERROR) << "check that the most recently launched "
                   << "server(s) are running the latest Replicant";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_INTERNAL, "bad command");
        return;
    }

    if (obj == e::slice("replicant"))
    {
        if (func == e::slice("new_object"))
        {
            execute_new_object(p, flags, command_nonce, si, request_nonce, input);
        }
        else if (func == e::slice("del_object"))
        {
            execute_del_object(p, flags, command_nonce, si, request_nonce, input);
        }
        else if (func == e::slice("backup_object"))
        {
            execute_backup_object(p, flags, command_nonce, si, request_nonce, input);
        }
        else if (func == e::slice("restore_object"))
        {
            execute_restore_object(p, flags, command_nonce, si, request_nonce, input);
        }
        else if (func == e::slice("kill_object"))
        {
            execute_kill_object(p, flags, command_nonce, si, request_nonce, input);
        }
        else if (func == e::slice("list_objects"))
        {
            execute_list_objects(p, flags, command_nonce, si, request_nonce, input);
        }
        else if (func == e::slice("kill_server"))
        {
            execute_kill_server(p, flags, command_nonce, si, request_nonce, input);
        }
        else
        {
            std::ostringstream ostr;
            ostr << "unknown function "
                 << std::string(obj.cdata(), obj.size()) << "."
                 << std::string(func.cdata(), func.size());
            LOG(ERROR) << ostr.str();
            executed(p, flags, command_nonce, si, request_nonce, REPLICANT_FUNC_NOT_FOUND, ostr.str());
        }
    }
    else
    {
        std::string o = obj.str();

        object_map_t::iterator it = m_objects.find(o);

        if (m_failed_objects.find(o) != m_failed_objects.end())
        {
            executed(p, flags, command_nonce, si, request_nonce, REPLICANT_MAYBE, "");
        }
        else if (it != m_objects.end() && it->second)
        {
            it->second->call(func, input, p, flags, command_nonce, si, request_nonce);
        }
        else if (it != m_objects.end())
        {
            executed(p, flags, command_nonce, si, request_nonce, REPLICANT_MAYBE, "");
        }
        else
        {
            executed(p, flags, command_nonce, si, request_nonce, REPLICANT_OBJ_NOT_FOUND, "object not found");
        }
    }
}

void
replica :: execute_new_object(const pvalue& p,
                              unsigned flags,
                              uint64_t command_nonce,
                              server_id si,
                              uint64_t request_nonce,
                              const e::slice& input)
{
    const size_t name_sz = strnlen(input.cdata(), input.size());
    const std::string name(input.cdata(), name_sz);

    if (m_objects.find(name) != m_objects.end())
    {
        LOG(ERROR) << "object \"" << e::strescape(name) << "\" already exists";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_OBJ_EXIST, "object already exists");
        return;
    }

    if (name_sz >= input.size())
    {
        LOG(ERROR) << "invalid new_object call";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_INTERNAL, "invalid library");
        return;
    }

    const std::string lib(input.cdata() + name_sz + 1, input.size() - name_sz - 1);
    LOG(INFO) << "creating object \"" << e::strescape(name) << "\"";
    object* obj = launch_library(name, p.s, lib);

    if (obj)
    {
        obj->ctor();
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_SUCCESS, "");
    }
    else
    {
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_MAYBE, "");
    }
}

void
replica :: execute_del_object(const pvalue& p,
                              unsigned flags,
                              uint64_t command_nonce,
                              server_id si,
                              uint64_t request_nonce,
                              const e::slice& input)
{
    const std::string name(input.str());
    object_map_t::iterator it = m_objects.find(name);

    if (it == m_objects.end())
    {
        LOG(ERROR) << "cannot erase \"" << e::strescape(name) << "\" because it doesn't exist";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_OBJ_NOT_FOUND, "object not found");
        return;
    }

    LOG(INFO) << "deleting object \"" << e::strescape(name) << "\"";
    m_dying_objects.push_back(it->second);
    m_failed_objects.erase(name);
    it->second->fail_at(0);
    m_objects.erase(it);
    executed(p, flags, command_nonce, si, request_nonce, REPLICANT_SUCCESS, "");
}

void
replica :: execute_backup_object(const pvalue& p,
                                 unsigned flags,
                                 uint64_t command_nonce,
                                 server_id si,
                                 uint64_t request_nonce,
                                 const e::slice& input)
{
    const std::string name(input.str());
    object_map_t::iterator it = m_objects.find(name);

    if (it == m_objects.end())
    {
        LOG(ERROR) << "cannot backup \"" << e::strescape(name) << "\" because it doesn't exist";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_OBJ_NOT_FOUND, "object not found");
        return;
    }

    it->second->call("__backup__", "", p, flags, command_nonce, si, request_nonce);
}

void
replica :: execute_restore_object(const pvalue& p,
                                  unsigned flags,
                                  uint64_t command_nonce,
                                  server_id si,
                                  uint64_t request_nonce,
                                  const e::slice& input)
{
    e::slice name;
    e::slice state;
    e::unpacker up(input);
    up = up >> name >> state;

    if (up.error())
    {
        LOG(ERROR) << "invalid command to restore an object";
        LOG(ERROR) << "check that the most recently launched "
                   << "server(s) are running the latest Replicant";
        LOG(ERROR) << "this error can be recovered from by removing and "
                   << "recreating the object";
        return;
    }

    object_map_t::iterator it = m_objects.find(name.str());
    LOG(INFO) << "restoring object \"" << e::strescape(name.str()) << "\"";

    if (it != m_objects.end())
    {
        LOG(ERROR) << "object \"" << e::strescape(name.str()) << "\" already exists";
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_OBJ_EXIST, "object already exists");
        return;
    }

    if (relaunch(name, p.s, state))
    {
        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_SUCCESS, "");
    }
    else
    {
        it = m_objects.find(name.str());

        if (it != m_objects.end())
        {
            it->second->fail_at(p.s);
        }
        else
        {
            LOG(ERROR) << "permanent error with \"" << e::strescape(name.str())
                       << "\" that may lead to its unavailability, or divergence";
            LOG(ERROR) << "delete the object and recreate it to avoid problems";
        }

        executed(p, flags, command_nonce, si, request_nonce, REPLICANT_MAYBE, "");
    }
}

void
replica :: execute_kill_server(const pvalue& p,
                               unsigned flags,
                               uint64_t command_nonce,
                               server_id si,
                               uint64_t request_nonce,
                               const e::slice& input)
{
    e::unpacker up(input.cdata(), input.size());
    server_id to_remove;
    up = up >> to_remove;

    if (up.error())
    {
        LOG(ERROR) << "invalid command to kill a server";
        return;
    }

    const configuration& c(m_configs.back());

    if (c.has(to_remove) && c.servers().size() == 1)
    {
        LOG(ERROR) << "refusing to remove "
                   << c.servers()[0]
                   << " from "
                   << c.cluster()
                   << " because it is the last server in the cluster";
    }
    else if (c.has(to_remove))
    {
        std::vector<server> servers;

        for (size_t i = 0; i < c.servers().size(); ++i)
        {
            if (c.servers()[i].id == to_remove)
            {
                LOG(INFO) << "removing " << c.servers()[i]
                          << " from "
                          << c.cluster();
            }
            else
            {
                servers.push_back(c.servers()[i]);
            }
        }

        assert(!servers.empty());
        m_configs.push_back(configuration(c.cluster(),
                                          version_id(c.version().get() + 1),
                                          p.s + REPLICANT_SLOTS_WINDOW,
                                          &servers[0],
                                          servers.size()));
    }
    else
    {
        LOG(INFO) << c.cluster() << " does not have member " << to_remove;
    }

    executed(p, flags, command_nonce, si, request_nonce, REPLICANT_SUCCESS, "");
}

void
replica :: executed(const pvalue& p,
                    unsigned flags,
                    uint64_t command_nonce,
                    server_id si,
                    uint64_t request_nonce,
                    replicant_returncode status,
                    const std::string& result)
{
    if (si != server_id())
    {
        m_daemon->callback_client(si, request_nonce, status, result);
    }

    if (!(flags & 1))
    {
        return;
    }

    po6::threads::mutex::hold hold(&m_robust_mtx);

    if (m_robust_history.empty())
    {
        m_robust_history.push_back(history(p.s, command_nonce, status, result));
    }
    // in practice, we'll never hit the next two cases because the RSMs will be
    // scheduled to never overrun the command_nonce history, but it's here as a
    // safety measure.
    else if (m_robust_history.front().slot > p.s)
    {
        m_robust_history.push_front(history(p.s, command_nonce, status, result));
    }
    else if (m_robust_history.front().slot == p.s)
    {
        return;
    }
    else
    {
        std::list<history>::iterator it = m_robust_history.end();
        --it;

        while (it != m_robust_history.begin() && it->slot >= p.s)
        {
            --it;
        }

        // it points to the history entry for the highest slot less than p.s
        ++it; // move forward one
        m_robust_history.insert(it, history(p.s, command_nonce, status, result));
    }

    m_robust_history_lookup.insert(command_nonce);

    while (m_robust_history.size() > REPLICANT_SERVER_DRIVEN_NONCE_HISTORY)
    {
        m_robust_history_lookup.erase(m_robust_history.front().nonce);
        m_robust_history.pop_front();
    }
}

static std::string
library_name(const std::string& name, uint64_t slot)
{
    std::ostringstream ostr;
    ostr << "./libreplicant-" << name << "-" << slot << ".so";
    return ostr.str();
}

bool
replica :: launch(object* obj, const char* executable, const char* const * args)
{
    // Open a Unix socket for communication
    int fds[2];

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0)
    {
        PLOG(ERROR) << "could not create object \"" << e::strescape(obj->name()) << "\"";
        return false;
    }

    e::guard g_fd0 = e::makeguard(close, fds[0]);
    e::guard g_fd1 = e::makeguard(close, fds[1]);
    char fdbuf[24];
    sprintf(fdbuf, "FD=%d", fds[1]);
    char* const envp[] = {fdbuf, 0};
    pid_t child = vfork();

    if (child == 0)
    {
        execve(executable, const_cast<char*const*>(args), envp);
        _exit(EXIT_FAILURE);
    }
    else if (child < 0)
    {
        PLOG(ERROR) << "could not create object \"" << e::strescape(obj->name()) << "\"";
        return false;
    }

    obj->set_child(child, fds[0]);
    g_fd0.dismiss();
    return true;
}

replicant::object*
replica :: launch_library(const std::string& name, uint64_t slot, const std::string& lib)
{
    e::intrusive_ptr<object> obj = new object(this, slot, name, OBJECT_LIBRARY, lib);
    m_objects[name] = obj;
    std::string libname = library_name(name, slot);

    if (!atomic_write(AT_FDCWD, libname.c_str(), lib))
    {
        PLOG(ERROR) << "could not spawn library for " << name;
        return NULL;
    }

    std::string exe(REPLICANT_EXEC_DIR);
    exe.append("/");
    exe.assign("/home/rescrv/src/replicant/replicant-rsm-dlopen");
    const char* const args[] = {exe.c_str(), libname.c_str(), 0};

    if (!launch(obj.get(), exe.c_str(), args))
    {
        return NULL;
    }

    return obj.get();
}

bool
replica :: relaunch(const e::slice& name, uint64_t slot, const e::slice& snap)
{
    object_t t;
    e::slice init;
    e::unpacker up(snap);
    up = up >> t >> init;

    if (up.error())
    {
        return false;
    }

    object* obj = NULL;

    switch (t)
    {
        case OBJECT_LIBRARY:
            obj = launch_library(name.str(), slot, init.str());
            break;
        case OBJECT_GARBAGE:
        default:
            return false;
    }

    if (!obj)
    {
        return false;
    }

    obj->rtor(up);
    return true;
}
