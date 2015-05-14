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

// e
#include <e/endian.h>
#include <e/pow2.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_st.h>

// Replicant
#include "common/generate_token.h"
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "client/client.h"
#include "client/pending.h"
#include "client/pending_call.h"
#include "client/pending_call_robust.h"
#include "client/pending_cond_wait.h"
#include "client/pending_generate_unique_number.h"
#include "client/pending_poke.h"
#include "client/pending_wait_new_config.h"
#include "client/server_selector.h"

using replicant::client;

#define ERROR(CODE) \
    *status = REPLICANT_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

#define PERROR(CODE) \
    p->set_status(REPLICANT_ ## CODE); \
    p->error(__FILE__, __LINE__)

client :: client(const char* coordinator, uint16_t port)
    : m_bootstrap(coordinator, port)
    , m_busybee_mapper(&m_config)
    , m_busybee()
    , m_random_token(0)
    , m_config_cond_state(0)
    , m_config()
    , m_bootstrap_count(1)
    , m_next_client_id(1)
    , m_next_nonce(1)
    , m_backoff()
    , m_pending()
    , m_pending_robust()
    , m_pending_retry()
    , m_pending_robust_retry()
    , m_complete()
    , m_last_error()
    , m_flagfd()
{
    if (!m_flagfd.valid())
    {
        throw std::bad_alloc();
    }

    reset_busybee();
}

client :: client(const char* cs)
    : m_bootstrap(cs)
    , m_busybee_mapper(&m_config)
    , m_busybee()
    , m_random_token(0)
    , m_config_cond_state(0)
    , m_config()
    , m_bootstrap_count(1)
    , m_next_client_id(1)
    , m_next_nonce(1)
    , m_backoff()
    , m_pending()
    , m_pending_robust()
    , m_pending_retry()
    , m_pending_robust_retry()
    , m_complete()
    , m_last_error()
    , m_flagfd()
{
    if (!m_flagfd.valid())
    {
        throw std::bad_alloc();
    }

    reset_busybee();
}

client :: ~client() throw ()
{
}

int64_t
client :: poke(replicant_returncode* status)
{
    if (!maintain_connection(status))
    {
        return -1;
    }

    const int64_t id = m_next_client_id++;
    e::intrusive_ptr<pending> p = new pending_poke(id, status);
    return send(p.get());
}

int64_t
client :: generate_unique_number(replicant_returncode* status,
                                 uint64_t* number)
{
    if (!maintain_connection(status))
    {
        return -1;
    }

    const int64_t id = m_next_client_id++;
    e::intrusive_ptr<pending> p = new pending_generate_unique_number(id, status, number);
    return send(p.get());
}

int64_t
client :: new_object(const char* object,
                     const char* path,
                     replicant_returncode* status)
{
    std::string lib;
    lib += std::string(object, strlen(object) + 1);

    char buf[4096];
    po6::io::fd fd(open(path, O_RDONLY));

    if (fd.get() < 0)
    {
        ERROR(SEE_ERRNO) << "could not open library: " << e::error::strerror(errno);
        return -1;
    }

    ssize_t amt = 0;

    while ((amt = fd.xread(buf, 4096)) > 0)
    {
        lib.append(buf, amt);
    }

    if (amt < 0)
    {
        ERROR(SEE_ERRNO) << "could not open library: " << e::error::strerror(errno);
        return -1;
    }

    return call("replicant", "new_object",
                lib.data(), lib.size(),
                REPLICANT_CALL_ROBUST,
                status, NULL, 0);
}

int64_t
client :: del_object(const char* object,
                     replicant_returncode* status)
{
    size_t object_sz = strlen(object);
    return call("replicant", "del_object",
                object, object_sz,
                REPLICANT_CALL_ROBUST,
                status, NULL, 0);
}

int64_t
client :: kill_object(const char* object,
                      replicant_returncode* status)
{
    size_t object_sz = strlen(object);
    return call("replicant", "kill_object",
                object, object_sz,
                REPLICANT_CALL_ROBUST,
                status, NULL, 0);
}

int64_t
client :: backup_object(const char* object,
                        enum replicant_returncode* status,
                        char** state, size_t* state_sz)
{
    size_t object_sz = strlen(object);
    return call("replicant", "backup_object",
                object, object_sz,
                REPLICANT_CALL_ROBUST,
                status, state, state_sz);
}

int64_t
client :: restore_object(const char* object,
                         const char* backup, size_t backup_sz,
                         enum replicant_returncode* status)
{
    size_t object_sz = strlen(object);
    std::string data;
    e::packer pa(&data);
    pa = pa << e::slice(object, object_sz)
            << e::slice(backup, backup_sz);
    return call("replicant", "restore_object",
                data.data(), data.size(),
                REPLICANT_CALL_ROBUST,
                status, NULL, 0);
}

int64_t
client :: list_objects(replicant_returncode* status, char** objects)
{
    return call("replicant", "list_objects",
                NULL, 0,
                REPLICANT_CALL_ROBUST,
                status, objects, NULL);
}

int64_t
client :: call(const char* object,
               const char* func,
               const char* input, size_t input_sz,
               unsigned flags,
               replicant_returncode* status,
               char** output, size_t* output_sz)
{
    if (!maintain_connection(status))
    {
        return -1;
    }

    const bool idempotent = flags & REPLICANT_CALL_IDEMPOTENT;
    const bool robust = flags & REPLICANT_CALL_ROBUST;
    const int64_t id = m_next_client_id++;

    if (robust)
    {
        e::intrusive_ptr<pending_call_robust> p = new pending_call_robust(id, object, func,
                                                                          input, input_sz,
                                                                          status,
                                                                          output, output_sz);
        return send_robust(p.get());
    }
    else
    {
        e::intrusive_ptr<pending> p = new pending_call(id, object, func,
                                                       input, input_sz,
                                                       idempotent, status,
                                                       output, output_sz);
        return send(p.get());
    }
}

int64_t
client :: cond_wait(const char* object,
                    const char* cond,
                    uint64_t state,
                    replicant_returncode* status,
                    char** data, size_t* data_sz)
{
    if (!maintain_connection(status))
    {
        return -1;
    }

    const int64_t id = m_next_client_id++;
    e::intrusive_ptr<pending> p = new pending_cond_wait(id, object, cond, state, status, data, data_sz);
    return send(p.get());
}

int64_t
client :: defended_call(const char* object,
                        const char* enter_func,
                        const char* enter_input, size_t enter_input_sz,
                        const char* exit_func,
                        const char* exit_input, size_t exit_input_sz,
                        replicant_returncode* status)
{
    if (!maintain_connection(status))
    {
        return -1;
    }

    return -1;
}

int
client :: conn_str(enum replicant_returncode* status, char** servers)
{
    *servers = NULL;

    if (!maintain_connection(status))
    {
        return -1;
    }

    std::string tmp = m_config.current_bootstrap().conn_str();
    *servers = static_cast<char*>(malloc(tmp.size() + 1));

    if (!*servers)
    {
        return -1;
    }

    memmove(*servers, tmp.c_str(), tmp.size() + 1);
    return 0;
}

int64_t
client :: kill_server(uint64_t token, replicant_returncode* status)
{
    char buf[8];
    e::pack64be(token, buf);
    return call("replicant", "kill_server", buf, 8, REPLICANT_CALL_ROBUST, status, NULL, 0);
}

int64_t
client :: loop(int timeout, replicant_returncode* status)
{
    while ((!m_pending.empty() ||
            !m_pending_robust.empty() ||
            !m_pending_retry.empty() ||
            !m_pending_robust_retry.empty()) &&
           m_complete.empty())
    {
        m_busybee->set_timeout(timeout);
        int64_t ret = inner_loop(status);

        if (ret < 0)
        {
            return ret;
        }

        assert(ret == 0);
    }

    if (!m_complete.empty())
    {
        e::intrusive_ptr<pending> p = m_complete.front();
        m_complete.pop_front();
        m_last_error = p->error();
        return p->client_visible_id();
    }

    if (!maintain_connection(status))
    {
        return -1;
    }

    possibly_clear_flagfd();
    ERROR(NONE_PENDING) << "no outstanding operations to process";
    return -1;
}

int64_t
client :: wait(int64_t id, int timeout, replicant_returncode* status)
{
    while (true)
    {
        for (pending_list_t::iterator it = m_complete.begin();
                it != m_complete.end(); ++it)
        {
            e::intrusive_ptr<pending> p = *it;

            if (p->client_visible_id() == id)
            {
                m_complete.erase(it);
                m_last_error = p->error();
                return p->client_visible_id();
            }
        }

        bool found = false;

        for (pending_map_t::iterator it = m_pending.begin();
                it != m_pending.end(); ++it)
        {
            if (it->second->client_visible_id() == id)
            {
                found = true;
                break;
            }
        }

        for (pending_robust_map_t::iterator it = m_pending_robust.begin();
                it != m_pending_robust.end(); ++it)
        {
            if (it->second->client_visible_id() == id)
            {
                found = true;
                break;
            }
        }

        for (pending_list_t::iterator it = m_pending_retry.begin();
                it != m_pending_retry.end(); ++it)
        {
            e::intrusive_ptr<pending> p = *it;

            if (p->client_visible_id() == id)
            {
                found = true;
                break;
            }
        }

        for (pending_robust_list_t::iterator it = m_pending_robust_retry.begin();
                it != m_pending_robust_retry.end(); ++it)
        {
            e::intrusive_ptr<pending_call_robust> p = *it;

            if (p->client_visible_id() == id)
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            break;
        }

        m_busybee->set_timeout(timeout);
        int64_t ret = inner_loop(status);

        if (ret < 0)
        {
            return ret;
        }

        assert(ret == 0);
    }

    if (!maintain_connection(status))
    {
        return -1;
    }

    possibly_clear_flagfd();
    ERROR(NONE_PENDING) << "no outstanding operation with id=" << id;
    return -1;
}

void
client :: kill(int64_t id)
{
    for (pending_map_t::iterator it = m_pending.begin();
            it != m_pending.end(); )
    {
        if (it->second->client_visible_id() == id)
        {
            m_pending.erase(it);
            it = m_pending.begin();
        }
        else
        {
            ++it;
        }
    }

    for (pending_robust_map_t::iterator it = m_pending_robust.begin();
            it != m_pending_robust.end(); )
    {
        if (it->second->client_visible_id() == id)
        {
            m_pending_robust.erase(it);
            it = m_pending_robust.begin();
        }
        else
        {
            ++it;
        }
    }

    for (pending_list_t::iterator it = m_pending_retry.begin();
            it != m_pending_retry.end(); )
    {
        if ((*it)->client_visible_id() == id)
        {
            it = m_pending_retry.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (pending_robust_list_t::iterator it = m_pending_robust_retry.begin();
            it != m_pending_robust_retry.end(); )
    {
        if ((*it)->client_visible_id() == id)
        {
            it = m_pending_robust_retry.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (pending_list_t::iterator it = m_complete.begin();
            it != m_complete.end(); )
    {
        if ((*it)->client_visible_id() == id)
        {
            it = m_complete.erase(it);
        }
        else
        {
            ++it;
        }
    }

    possibly_clear_flagfd();
}

int
client :: poll_fd()
{
    return m_busybee->poll_fd();
}

const char*
client :: error_message()
{
    return m_last_error.msg();
}

const char*
client :: error_location()
{
    return m_last_error.loc();
}

void
client :: set_error_message(const char* msg)
{
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

void
client :: reset_busybee()
{
    m_busybee.reset(new busybee_st(&m_busybee_mapper, 0));
    m_busybee->set_external_fd(m_flagfd.poll_fd());
}

int64_t
client :: inner_loop(replicant_returncode* status)
{
    if (m_backoff)
    {
        ERROR(COMM_FAILED) << "lost communication with the cluster; backoff before trying again";
        m_backoff = false;
        return -1;
    }

    if (!maintain_connection(status))
    {
        return -1;
    }

    while (!m_pending_retry.empty())
    {
        e::intrusive_ptr<pending> p = m_pending_retry.front();
        m_pending_retry.pop_front();
        send(p.get());
    }

    while (!m_pending_robust_retry.empty())
    {
        e::intrusive_ptr<pending_call_robust> p = m_pending_robust_retry.front();
        m_pending_robust_retry.pop_front();
        send_robust(p.get());
    }

    uint64_t id;
    std::auto_ptr<e::buffer> msg;
    const bool isset = m_flagfd.isset();
    m_flagfd.clear();
    busybee_returncode rc = m_busybee->recv(&id, &msg);

    if (isset)
    {
        m_flagfd.set();
    }

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_DISRUPTED:
            handle_disruption(server_id(id));
            return 0;
        case BUSYBEE_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            return -1;
        case BUSYBEE_TIMEOUT:
            ERROR(TIMEOUT) << "operation timed out";
            return -1;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_EXTERNAL:
        default:
            ERROR(INTERNAL)
                << "internal state is inconsistent; delete this instance and create another";
            return -1;
    }

    server_id si(id);
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    network_msgtype mt;
    up = up >> mt;

    if (up.error())
    {
        ERROR(SERVER_ERROR) << "communication error: " << si
                            << " sent invalid message="
                            << msg->as_slice().hex();
        return -1;
    }

    switch (mt)
    {
        case REPLNET_BOOTSTRAP:
        case REPLNET_SILENT_BOOTSTRAP:
            if (!handle_bootstrap(si, up, status))
            {
                return -1;
            }
            return 0;
        case REPLNET_CLIENT_RESPONSE:
            break;
        case REPLNET_NOP:
        case REPLNET_PING:
        case REPLNET_PONG:
        case REPLNET_STATE_TRANSFER:
        case REPLNET_SUGGEST_REJOIN:
        case REPLNET_WHO_ARE_YOU:
        case REPLNET_IDENTITY:
        case REPLNET_PAXOS_PHASE1A:
        case REPLNET_PAXOS_PHASE1B:
        case REPLNET_PAXOS_PHASE2A:
        case REPLNET_PAXOS_PHASE2B:
        case REPLNET_PAXOS_LEARN:
        case REPLNET_PAXOS_SUBMIT:
        case REPLNET_SERVER_BECOME_MEMBER:
        case REPLNET_UNIQUE_NUMBER:
        case REPLNET_OBJECT_FAILED:
        case REPLNET_POKE:
        case REPLNET_COND_WAIT:
        case REPLNET_CALL:
        case REPLNET_GET_ROBUST_PARAMS:
        case REPLNET_CALL_ROBUST:
        case REPLNET_GARBAGE:
            ERROR(SERVER_ERROR) << "received a " << mt << " from " << si
                                << " which is not handled by clients";
            return -1;
        default:
            ERROR(SERVER_ERROR) << "communication error: " << si
                                << " sent an invalid message";
            return -1;
    }

    uint64_t nonce;
    up = up >> nonce;

    if (up.error())
    {
        ERROR(SERVER_ERROR) << "communication error: " << si
                            << " sent invalid message="
                            << msg->as_slice().hex();
        return -1;
    }

    pending_map_t::iterator it = m_pending.find(std::make_pair(si, nonce));

    if (it == m_pending.end())
    {
        pending_robust_map_t::iterator rit = m_pending_robust.find(std::make_pair(si, nonce));

        if (rit == m_pending_robust.end())
        {
            return 0;
        }

        uint64_t command_nonce;
        uint64_t min_slot;
        up = up >> command_nonce >> min_slot;

        if (up.error())
        {
            rit->second->set_status(REPLICANT_SERVER_ERROR);
            rit->second->error(__FILE__, __LINE__)
                << "communication error: " << si
                << " sent invalid message during the call";
            m_complete.push_back(rit->second.get());
            m_pending_robust.erase(rit);
            return 0;
        }

        rit->second->set_params(command_nonce, min_slot);
        send(rit->second.get());
        m_pending_robust.erase(rit);
        return 0;
    }

    it->second->handle_response(msg, up);

    if (it->second->client_visible_id() >= 0)
    {
        m_complete.push_back(it->second);
    }

    m_pending.erase(it);
    return 0;
}

bool
client :: maintain_connection(replicant_returncode* status)
{
    if (m_random_token == 0)
    {
        if (!generate_token(&m_random_token))
        {
            m_random_token = 0;
        }
    }

    if (m_config_cond_state == 0)
    {
        configuration c;
        e::error e;
        replicant_returncode rc = m_bootstrap.do_it(&c, &e);

        if (rc != REPLICANT_SUCCESS)
        {
            *status = rc;
            m_last_error = e;
            return false;
        }

        m_config = c;
        m_config_cond_state = m_config.version().get();
        e::intrusive_ptr<pending> p = new pending_wait_new_config(this, c.version());
        send(p.get());
        return true;
    }
    else if (m_config_cond_state > m_config.version().get())
    {
        if (m_bootstrap_count <= 1 || e::is_pow2(m_bootstrap_count))
        {
            server_selector ss(m_config.server_ids(), m_random_token);
            server_id si;

            while ((si = ss.next()) != server_id())
            {
                const size_t sz = BUSYBEE_HEADER_SIZE + pack_size(REPLNET_SILENT_BOOTSTRAP);
                std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_SILENT_BOOTSTRAP;

                if (send(si, msg, status))
                {
                    ++m_bootstrap_count;
                    break;
                }
            }
        }

        return true;
    }
    else
    {
        return true;
    }
}

void
client :: possibly_clear_flagfd()
{
    if (m_pending.empty() &&
        m_pending_robust.empty() &&
        m_pending_retry.empty() &&
        m_pending_robust_retry.empty() &&
        m_complete.empty())
    {
        m_flagfd.clear();
    }
}

void
client ::handle_disruption(server_id si)
{
    for (pending_map_t::iterator it = m_pending.begin();
            it != m_pending.end(); )
    {
        if (it->first.first == si)
        {
            if (it->second->resend_on_failure())
            {
                m_pending_retry.push_back(it->second);
            }
            else
            {
                pending* p = it->second.get();
                PERROR(COMM_FAILED) << "communication failed while sending operation";
                m_complete.push_back(p);
            }

            m_pending.erase(it);
            it = m_pending.begin();
        }
        else
        {
            ++it;
        }
    }

    for (pending_robust_map_t::iterator it = m_pending_robust.begin();
            it != m_pending_robust.end(); )
    {
        if (it->first.first == si)
        {
            m_pending_robust_retry.push_back(it->second);
            m_pending_robust.erase(it);
            it = m_pending_robust.begin();
        }
        else
        {
            ++it;
        }
    }

    possibly_clear_flagfd();
}

bool
client :: handle_bootstrap(server_id si, e::unpacker up, replicant_returncode* status)
{
    m_bootstrap_count = 1;
    configuration new_config;
    up = up >> new_config;

    if (up.error() || !new_config.validate())
    {
        ERROR(SERVER_ERROR) << "error bootstrapping off " << si
                            << ": invalid configuration";
        return false;
    }

    bool changed = false;

    if (m_config.cluster() != new_config.cluster())
    {
        while (!m_pending.empty())
        {
            e::intrusive_ptr<pending> p = m_pending.begin()->second;
            m_pending.erase(m_pending.begin());
            p->set_status(REPLICANT_CLUSTER_JUMP);
            p->error(__FILE__, __LINE__)
                << "client jumped from " << m_config.cluster()
                << " to " << new_config.cluster();
            m_complete.push_back(p);
        }

        while (!m_pending_robust.empty())
        {
            e::intrusive_ptr<pending> p = m_pending_robust.begin()->second.get();
            m_pending_robust.erase(m_pending_robust.begin());
            p->set_status(REPLICANT_CLUSTER_JUMP);
            p->error(__FILE__, __LINE__)
                << "client jumped from " << m_config.cluster()
                << " to " << new_config.cluster();
            m_complete.push_back(p);
        }

        reset_busybee();
        changed = true;
    }
    else if (m_config.version() < new_config.version())
    {
        std::vector<server_id> old_servers = m_config.server_ids();
        std::vector<server_id> new_servers = new_config.server_ids();
        std::sort(new_servers.begin(), new_servers.end());

        for (size_t i = 0; i < old_servers.size(); ++i)
        {
            if (!std::binary_search(new_servers.begin(), new_servers.end(), old_servers[i]))
            {
                m_busybee->drop(old_servers[i].get());
                handle_disruption(old_servers[i]);
            }
        }

        changed = true;
    }

    if (changed)
    {
        m_config = new_config;
        e::intrusive_ptr<pending> p = new pending_wait_new_config(this, m_config.version());
        send(p.get());
    }

    return true;
}

int64_t
client :: send(pending* p)
{
    server_selector ss(m_config.server_ids(), m_random_token);
    server_id si;

    while ((si = ss.next()) != server_id())
    {
        const uint64_t nonce = m_next_nonce++;
        std::auto_ptr<e::buffer> msg = p->request(nonce);
        bool sent = send(si, msg, p->status_ptr());

        if (!sent && !p->resend_on_failure())
        {
            PERROR(COMM_FAILED) << "communication failed while sending operation";
            m_last_error = p->error();
            return -1;
        }
        else if (sent)
        {
            m_flagfd.set();
            m_pending.insert(std::make_pair(std::make_pair(si, nonce), p));
            return p->client_visible_id();
        }
    }

    if (p->resend_on_failure())
    {
        m_flagfd.set();
        m_backoff = true;
        m_pending_retry.push_back(p);
        return p->client_visible_id();
    }
    else
    {
        PERROR(COMM_FAILED) << "communication failed while sending operation";
        m_last_error = p->error();
        return -1;
    }
}

int64_t
client :: send_robust(pending_call_robust* p)
{
    assert(p->resend_on_failure());
    server_selector ss(m_config.server_ids(), m_random_token);
    server_id si;
    m_flagfd.set();

    while ((si = ss.next()) != server_id())
    {
        const uint64_t nonce = m_next_nonce++;
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(REPLNET_GET_ROBUST_PARAMS)
                        + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_GET_ROBUST_PARAMS << nonce;
        bool sent = send(si, msg, p->status_ptr());

        if (sent)
        {
            m_pending_robust.insert(std::make_pair(std::make_pair(si, nonce), p));
            return p->client_visible_id();
        }
    }

    m_backoff = true;
    m_pending_robust_retry.push_back(p);
    return p->client_visible_id();
}

bool
client :: send(server_id si, std::auto_ptr<e::buffer> msg, replicant_returncode* status)
{
    busybee_returncode rc = m_busybee->send(si.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(si);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            ERROR(INTERNAL)
                << "internal state is inconsistent; delete this instance and create another";
            return false;
    }
}
