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

// e
#include <e/endian.h>
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_st.h>

// Replicant
#include "common/bootstrap.h"
#include "common/configuration.h"
#include "common/macros.h"
#include "common/mapper.h"
#include "common/network_msgtype.h"
#include "common/response_returncode.h"
#include "common/special_objects.h"
#include "client/command.h"
#include "client/replicant.h"

using namespace replicant;

#define COMMAND_HEADER_SIZE (BUSYBEE_HEADER_SIZE + pack_size(REPLNET_COMMAND_SUBMIT) + 4 * sizeof(uint64_t))

#define OBJ_STR2NUM(STR, NUM) \
    do \
    { \
        if (strlen(STR) > sizeof(uint64_t)) \
        { \
            *status = REPLICANT_NAME_TOO_LONG; \
            return -1; \
        } \
        char object_buf[sizeof(uint64_t)]; \
        memset(object_buf, 0, sizeof(object_buf)); \
        memmove(object_buf, STR, strlen(STR)); \
        e::unpack64be(object_buf, &NUM); \
    } while (0)

#define ERROR(CODE) \
    *status = REPLICANT_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

#define _BUSYBEE_ERROR(BBRC) \
    case BUSYBEE_ ## BBRC: \
        ERROR(INTERNAL_ERROR) << "internal error: BusyBee unexpectedly returned " XSTR(BBRC) << ": please file a bug"

#define BUSYBEE_ERROR_CASE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return -1;

#define BUSYBEE_ERROR_CASE_DISCONNECT(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    reset_to_disconnected(); \
    return -1;

#define BUSYBEE_ERROR_CASE_CONTINUE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    continue;

#define UNEXPECTED_MESSAGE_CASE(FROM, MT) \
    case REPLNET_ ## MT: \
        ERROR(MISBEHAVING_SERVER) << "communication error: server " \
                                  << FROM << " sent a message of type " << XSTR(MT); \
        return -1

void
replicant_destroy_output(const char* output, size_t)
{
    uint16_t sz = 0;
    e::unpack16le(output - 2, &sz);
    const e::buffer* buf = reinterpret_cast<const e::buffer*>(output - sz);
    delete buf;
}

replicant_client :: replicant_client(const char* connection_string)
    : m_gc(new e::garbage_collector())
    , m_busybee_mapper(new replicant::mapper())
    , m_busybee(new busybee_st(m_gc.get(), m_busybee_mapper.get(), 0))
    , m_config(new replicant::configuration())
    , m_bootstrap()
    , m_token(0x4141414141414141ULL)
    , m_nonce(1)
    , m_cluster(0)
    , m_state(REPLCL_DISCONNECTED)
    , m_commands()
    , m_complete()
    , m_resend()
    , m_last_error()
    , m_cluster_jump(false)
    , m_gc_ts()
{
    m_gc->register_thread(&m_gc_ts);

    if (!bootstrap_parse_hosts(connection_string, &m_bootstrap))
    {
        replicant_returncode _status;
        replicant_returncode* status = &_status;
        ERROR(NEED_BOOTSTRAP) << "could not parse bootstrap connection string";
    }
}

replicant_client :: replicant_client(const char* host, in_port_t port)
    : m_gc(new e::garbage_collector())
    , m_busybee_mapper(new replicant::mapper())
    , m_busybee(new busybee_st(m_gc.get(), m_busybee_mapper.get(), 0))
    , m_config(new replicant::configuration())
    , m_bootstrap()
    , m_token(0x4141414141414141ULL)
    , m_nonce(1)
    , m_cluster(0)
    , m_state(REPLCL_DISCONNECTED)
    , m_commands()
    , m_complete()
    , m_resend()
    , m_last_error()
    , m_cluster_jump(false)
    , m_gc_ts()
{
    m_gc->register_thread(&m_gc_ts);
    m_bootstrap.push_back(po6::net::hostname(host, port));
}

replicant_client :: replicant_client(po6::net::hostname* bootstrap, size_t bootstrap_sz)
    : m_gc(new e::garbage_collector())
    , m_busybee_mapper(new replicant::mapper())
    , m_busybee(new busybee_st(m_gc.get(), m_busybee_mapper.get(), 0))
    , m_config(new replicant::configuration())
    , m_bootstrap(bootstrap, bootstrap + bootstrap_sz)
    , m_token(0x4141414141414141ULL)
    , m_nonce(1)
    , m_cluster(0)
    , m_state(REPLCL_DISCONNECTED)
    , m_commands()
    , m_complete()
    , m_resend()
    , m_last_error()
    , m_cluster_jump(false)
    , m_gc_ts()
{
    m_gc->register_thread(&m_gc_ts);
}

replicant_client :: ~replicant_client() throw ()
{
    m_gc->deregister_thread(&m_gc_ts);
}

int64_t
replicant_client :: new_object(const char* obj,
                               const char* path,
                               replicant_returncode* status,
                               const char** errmsg, size_t* errmsg_sz)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    // Read the library
    std::vector<char> lib;
    char buf[4096];
    po6::io::fd fd(open(path, O_RDONLY));

    if (fd.get() < 0)
    {
        ERROR(BAD_LIBRARY) << "could not open library: " << e::error::strerror(errno);
        return -1;
    }

    ssize_t amt = 0;

    while ((amt = fd.xread(buf, 4096)) > 0)
    {
        size_t tmp = lib.size();
        lib.resize(tmp + amt);
        memmove(&lib[tmp], buf, amt);
    }

    if (amt < 0)
    {
        ERROR(BAD_LIBRARY) << "could not open library: " << e::error::strerror(errno);
        return -1;
    }

    // Pack the message to send
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t object;
    OBJ_STR2NUM(obj, object);
    size_t sz = COMMAND_HEADER_SIZE + sizeof(uint64_t) + lib.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << REPLNET_COMMAND_SUBMIT << uint64_t(OBJECT_OBJ_NEW)
            << m_token << nonce << object
            << e::slice(&lib[0], lib.size());
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, errmsg, errmsg_sz);
    return send_to_preferred_chain_position(cmd, status);
}

int64_t
replicant_client :: del_object(const char* obj,
                               replicant_returncode* status,
                               const char** errmsg, size_t* errmsg_sz)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    // Pack the message to send
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t object;
    OBJ_STR2NUM(obj, object);
    size_t sz = COMMAND_HEADER_SIZE + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_COMMAND_SUBMIT << uint64_t(OBJECT_OBJ_DEL) << m_token << nonce << object;
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, errmsg, errmsg_sz);
    return send_to_preferred_chain_position(cmd, status);
}

int64_t
replicant_client :: backup_object(const char* obj,
                                  replicant_returncode* status,
                                  const char** output, size_t* output_sz)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    // Pack the message to send
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t object;
    OBJ_STR2NUM(obj, object);
    size_t sz = COMMAND_HEADER_SIZE + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_COMMAND_SUBMIT << uint64_t(OBJECT_OBJ_SNAPSHOT) << m_token << nonce << object;
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, output, output_sz);
    return send_to_preferred_chain_position(cmd, status);
}

int64_t
replicant_client :: restore_object(const char* obj,
                                   const char* path,
                                   const char* backup,
                                   replicant_returncode* status,
                                   const char** errmsg, size_t* errmsg_sz)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    char buf[4096];
    ssize_t amt = 0;

    // Read the library
    std::vector<char> lib;
    po6::io::fd fdlib(open(path, O_RDONLY));

    if (fdlib.get() < 0)
    {
        ERROR(BAD_LIBRARY) << "could not open library: " << e::error::strerror(errno);
        return -1;
    }

    while ((amt = fdlib.xread(buf, 4096)) > 0)
    {
        size_t tmp = lib.size();
        lib.resize(tmp + amt);
        memmove(&lib[tmp], buf, amt);
    }

    if (amt < 0)
    {
        ERROR(BAD_LIBRARY) << "could not open library: " << e::error::strerror(errno);
        return -1;
    }

    // Read the backup
    std::vector<char> back;
    po6::io::fd fdback(open(backup, O_RDONLY));

    if (fdback.get() < 0)
    {
        ERROR(BAD_LIBRARY) << "could not open backup: " << e::error::strerror(errno);
        return -1;
    }

    amt = 0;

    while ((amt = fdback.xread(buf, 4096)) > 0)
    {
        size_t tmp = back.size();
        back.resize(tmp + amt);
        memmove(&back[tmp], buf, amt);
    }

    if (amt < 0)
    {
        ERROR(BAD_LIBRARY) << "could not open backup: " << e::error::strerror(errno);
        return -1;
    }

    // Pack the message to send
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t object;
    OBJ_STR2NUM(obj, object);
    size_t sz = COMMAND_HEADER_SIZE
              + sizeof(uint64_t)
              + 2 * sizeof(uint32_t)
              + lib.size()
              + back.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << REPLNET_COMMAND_SUBMIT << uint64_t(OBJECT_OBJ_RESTORE)
            << m_token << nonce << object
            << e::slice(&lib[0], lib.size())
            << e::slice(&back[0], back.size());
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, errmsg, errmsg_sz);
    return send_to_preferred_chain_position(cmd, status);
}

int64_t
replicant_client :: send(const char* obj,
                         const char* func,
                         const char* data, size_t data_sz,
                         replicant_returncode* status,
                         const char** output, size_t* output_sz)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    // Pack the message to send
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t object;
    OBJ_STR2NUM(obj, object);
    size_t func_sz = strlen(func) + 1;
    size_t sz = COMMAND_HEADER_SIZE + func_sz + data_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << REPLNET_COMMAND_SUBMIT << object << m_token << nonce;
    pa = pa.copy(e::slice(func, func_sz));
    pa = pa.copy(e::slice(data, data_sz));
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, output, output_sz);
    return send_to_preferred_chain_position(cmd, status);
}

int64_t
replicant_client :: wait(const char* obj,
                         const char* cond,
                         uint64_t state,
                         replicant_returncode* status)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t object;
    OBJ_STR2NUM(obj, object);
    uint64_t condition;
    OBJ_STR2NUM(cond, condition);
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_CONDITION_WAIT)
              + sizeof(uint64_t) /*nonce*/
              + sizeof(uint64_t) /*object*/
              + sizeof(uint64_t) /*cond*/
              + sizeof(uint64_t) /*state*/;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CONDITION_WAIT << nonce << object << condition << state;
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, NULL, 0);
    return send_to_preferred_chain_position(cmd, status);
}

replicant_returncode
replicant_client :: disconnect()
{
    replicant_returncode rc;
    int64_t ret = maintain_connection(&rc);

    if (ret < 0)
    {
        return REPLICANT_NEED_BOOTSTRAP;
    }

    uint64_t nonce = m_nonce;
    ++m_nonce;
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_CLIENT_DISCONNECT)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CLIENT_DISCONNECT << nonce;
    e::intrusive_ptr<command> cmd = new command(&rc, nonce, msg, NULL, NULL);
    msg.reset(cmd->request()->copy());
    send_to_chain_head(msg, &rc);

    while (m_commands.find(nonce) != m_commands.end() &&
           m_resend.find(nonce) == m_commands.end())
    {
        m_busybee->set_timeout(-1);
        inner_loop(&rc);
    }

    reset_to_disconnected();
    return REPLICANT_SUCCESS;
}

int64_t
replicant_client :: loop(int timeout, replicant_returncode* status)
{
    while ((!m_commands.empty() || !m_resend.empty())
           && m_complete.empty())
    {
        // Always set timeout
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
        e::intrusive_ptr<command> c = m_complete.begin()->second;
        m_complete.erase(m_complete.begin());
        m_last_error = c->error();
        return c->clientid();
    }

    if (m_commands.empty())
    {
        ERROR(NONE_PENDING) << "no outstanding operations to process";
        return -1;
    }

    ERROR(INTERNAL_ERROR) << "unhandled exit case from loop";
    return -1;
}

int64_t
replicant_client :: loop(int64_t id, int timeout, replicant_returncode* status)
{
    while (m_commands.find(id) != m_commands.end() ||
           m_resend.find(id) != m_resend.end())
    {
        // Always set timeout
        m_busybee->set_timeout(timeout);
        int64_t ret = inner_loop(status);

        if (ret < 0)
        {
            return ret;
        }

        assert(ret == 0);
    }

    command_map::iterator it = m_complete.find(id);

    if (it == m_complete.end())
    {
        ERROR(NONE_PENDING) << "no outstanding operation with id=" << id;
        return -1;
    }

    e::intrusive_ptr<command> c = it->second;
    m_complete.erase(it);
    m_last_error = c->error();
    return c->clientid();
}

void
replicant_client :: kill(int64_t id)
{
    m_commands.erase(id);
    m_complete.erase(id);
    m_resend.erase(id);
}

int64_t
replicant_client :: list_servers(replicant_returncode* status,
                                 std::string* servers)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    std::ostringstream ostr;

    for (const chain_node* n = m_config->members_begin();
            n != m_config->members_end(); ++n)
    {
        ostr << n->address << "\n";
    }

    *servers = ostr.str();
    return 0;
}

int64_t
replicant_client :: connect_str(replicant_returncode* status,
                                std::string* servers)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    std::vector<po6::net::hostname> hns;

    for (const chain_node* n = m_config->members_begin();
            n != m_config->members_end(); ++n)
    {
        std::ostringstream ostr;
        ostr << n->address.address;
        hns.push_back(po6::net::hostname(ostr.str().c_str(), n->address.port));
    }

    *servers = replicant::bootstrap_hosts_to_string(&hns[0], hns.size());
    return 0;
}

#ifdef _MSC_VER
fd_set*
#else
int
#endif
replicant_client :: poll_fd()
{
    return m_busybee->poll_fd();
}

int64_t
replicant_client :: inner_loop(replicant_returncode* status)
{
    m_gc->quiescent_state(&m_gc_ts);
    int64_t ret = maintain_connection(status);

    if (ret != 0)
    {
        return ret;
    }

    if (m_cluster_jump)
    {
        return -1;
    }

    // Resend all those that need it
    while (!m_resend.empty())
    {
        ret = send_to_preferred_chain_position(m_resend.begin()->second, status);

        // As this is a retransmission, we only care about errors (< 0)
        // not the success half (>=0).
        if (ret < 0)
        {
            return ret;
        }

        m_resend.erase(m_resend.begin());
    }

    // Receive a message
    uint64_t id;
    std::auto_ptr<e::buffer> msg;
    busybee_returncode rc = m_busybee->recv(&id, &msg);
    const chain_node* node = m_config->node_from_token(id);

    // And process it
    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_DISRUPTED:
            if (node)
            {
                handle_disruption(*node, status);
            }

            return 0;
        case BUSYBEE_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            return -1;
        case BUSYBEE_TIMEOUT:
            ERROR(TIMEOUT) << "operation timed out";
            return -1;
        BUSYBEE_ERROR_CASE(SHUTDOWN);
        BUSYBEE_ERROR_CASE(POLLFAILED);
        BUSYBEE_ERROR_CASE(ADDFDFAIL);
        BUSYBEE_ERROR_CASE(EXTERNAL);
        default:
            ERROR(INTERNAL_ERROR) << "internal error: BusyBee unexpectedly returned "
                                  << (unsigned) rc << ": please file a bug";
            return -1;
    }

    if (!node)
    {
        m_busybee->drop(id);
        return 0;
    }

    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    replicant_network_msgtype mt;
    up = up >> mt;

    if (up.error())
    {
        ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                  << *node << " sent message="
                                  << msg->as_slice().hex()
                                  << " that is invalid";
        return -1;
    }

    switch (mt)
    {
        case REPLNET_COMMAND_RESPONSE:
        case REPLNET_CONDITION_NOTIFY:
            if ((ret = handle_command_response(*node, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        case REPLNET_INFORM:
            if ((ret = handle_inform(*node, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        case REPLNET_CLIENT_UNKNOWN:
            return report_cluster_jump(status);
        case REPLNET_CLIENT_DECEASED:
            reset_to_disconnected();
            break;
        case REPLNET_PING:
            if ((ret = handle_ping(*node, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        UNEXPECTED_MESSAGE_CASE(*node, NOP);
        UNEXPECTED_MESSAGE_CASE(*node, BOOTSTRAP);
        UNEXPECTED_MESSAGE_CASE(*node, SERVER_REGISTER);
        UNEXPECTED_MESSAGE_CASE(*node, SERVER_REGISTER_FAILED);
        UNEXPECTED_MESSAGE_CASE(*node, SERVER_CHANGE_ADDRESS);
        UNEXPECTED_MESSAGE_CASE(*node, SERVER_IDENTIFY);
        UNEXPECTED_MESSAGE_CASE(*node, SERVER_IDENTITY);
        UNEXPECTED_MESSAGE_CASE(*node, CONFIG_PROPOSE);
        UNEXPECTED_MESSAGE_CASE(*node, CONFIG_ACCEPT);
        UNEXPECTED_MESSAGE_CASE(*node, CONFIG_REJECT);
        UNEXPECTED_MESSAGE_CASE(*node, CLIENT_REGISTER);
        UNEXPECTED_MESSAGE_CASE(*node, CLIENT_DISCONNECT);
        UNEXPECTED_MESSAGE_CASE(*node, CLIENT_TIMEOUT);
        UNEXPECTED_MESSAGE_CASE(*node, COMMAND_SUBMIT);
        UNEXPECTED_MESSAGE_CASE(*node, COMMAND_ISSUE);
        UNEXPECTED_MESSAGE_CASE(*node, COMMAND_ACK);
        UNEXPECTED_MESSAGE_CASE(*node, HEAL_REQ);
        UNEXPECTED_MESSAGE_CASE(*node, HEAL_RETRY);
        UNEXPECTED_MESSAGE_CASE(*node, HEAL_RESP);
        UNEXPECTED_MESSAGE_CASE(*node, HEAL_DONE);
        UNEXPECTED_MESSAGE_CASE(*node, STABLE);
        UNEXPECTED_MESSAGE_CASE(*node, CONDITION_WAIT);
        UNEXPECTED_MESSAGE_CASE(*node, PONG);
        default:
            ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                      << *node << " sent a message of uknown type";
            return -1;
    }

    return 0;
}

int64_t
replicant_client :: maintain_connection(replicant_returncode* status)
{
    while (true)
    {
        int64_t ret;

        if (m_cluster_jump)
        {
            reset_to_disconnected();
        }

        switch (m_state)
        {
            case REPLCL_DISCONNECTED:
                if ((ret = perform_bootstrap(status)) < 0)
                {
                    return ret;
                }
                if (m_cluster_jump)
                {
                    return report_cluster_jump(status);
                }
                break;
            case REPLCL_BOOTSTRAPPED:
                if ((ret = send_token_registration(status)) < 0)
                {
                    return ret;
                }
                break;
            case REPLCL_REGISTER_SENT:
                if ((ret = wait_for_token_registration(status)) < 0)
                {
                    return ret;
                }
                break;
            case REPLCL_REGISTERED:
                return 0;
            default:
                ERROR(INTERNAL_ERROR) << "internal error: client in corrupt state " << (unsigned) m_state;
                return -1;
        }
    }
}

int64_t
replicant_client :: perform_bootstrap(replicant_returncode* status)
{
    m_token = generate_token();
    m_busybee->set_id(m_token);
    configuration initial;
    size_t idx = 0;
    replicant::bootstrap_returncode rc = replicant::BOOTSTRAP_GARBAGE;

    for (idx = 0; idx < m_bootstrap.size(); ++idx)
    {
        rc = replicant::bootstrap(m_bootstrap[idx], &initial);

        switch (rc)
        {
            case replicant::BOOTSTRAP_SUCCESS:
                m_state = REPLCL_BOOTSTRAPPED;
                *m_config = initial;

                if (m_cluster > 0 && m_cluster != initial.cluster())
                {
                    return report_cluster_jump(status);
                }

                m_cluster = initial.cluster();
                m_cluster_jump = false;
                return 0;
            case replicant::BOOTSTRAP_SEE_ERRNO:
                ERROR(NEED_BOOTSTRAP) << "cannot connect to cluster: " << e::error::strerror(errno);
                break;
            case replicant::BOOTSTRAP_COMM_FAIL:
                ERROR(INTERNAL_ERROR) << "cannot connect to cluster: internal error: " << e::error::strerror(errno);
                break;
            case replicant::BOOTSTRAP_TIMEOUT:
                ERROR(NEED_BOOTSTRAP) << "cannot connect to cluster: operation timed out";
                break;
            case replicant::BOOTSTRAP_CORRUPT_INFORM:
                ERROR(NEED_BOOTSTRAP) << "cannot connect to cluster: server " << m_bootstrap[idx] << " sent a corrupt INFORM message";
                break;
            case replicant::BOOTSTRAP_NOT_CLUSTER_MEMBER:
                ERROR(MISBEHAVING_SERVER) << "cannot connect to cluster: server " << m_bootstrap[idx] << " is not a member of the cluster";
                break;
            case replicant::BOOTSTRAP_GARBAGE:
            default:
                ERROR(INTERNAL_ERROR) << "cannot connect to cluster: bootstrap failed with " << (unsigned) rc;
                break;
        }
    }

    reset_to_disconnected();
    return -1;
}

int64_t
replicant_client :: send_token_registration(replicant_returncode* status)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_CLIENT_REGISTER)
              + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CLIENT_REGISTER << m_token;
    int64_t ret = send_to_chain_head(msg, status);

    if (ret >= 0)
    {
        m_state = REPLCL_REGISTER_SENT;
    }

    return ret;
}

int64_t
replicant_client :: wait_for_token_registration(replicant_returncode* status)
{
    while (true)
    {
        uint64_t token;
        std::auto_ptr<e::buffer> msg;
        m_busybee->set_timeout(1000);
        busybee_returncode rc = m_busybee->recv(&token, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                reset_to_disconnected();
                return -1;
            case BUSYBEE_TIMEOUT:
                ERROR(TIMEOUT) << "operation timed out";
                reset_to_disconnected();
                return -1;
            case BUSYBEE_DISRUPTED:
                ERROR(NEED_BOOTSTRAP) << "could not register with the cluster: "
                                      << e::error::strerror(errno);
                reset_to_disconnected();
                return -1;
            BUSYBEE_ERROR_CASE_DISCONNECT(SHUTDOWN);
            BUSYBEE_ERROR_CASE_DISCONNECT(POLLFAILED);
            BUSYBEE_ERROR_CASE_DISCONNECT(ADDFDFAIL);
            BUSYBEE_ERROR_CASE_DISCONNECT(EXTERNAL);
            default:
                ERROR(INTERNAL_ERROR) << "internal error: BusyBee unexpectedly returned "
                                      << (unsigned) rc << ": please file a bug";
                reset_to_disconnected();
                return -1;
        }

        const chain_node* node = m_config->node_from_token(token);

        if (!node)
        {
            ERROR(NEED_BOOTSTRAP) << "server claims to be " << token << " but that is not a cluster member";
            reset_to_disconnected();
            return -1;
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        replicant_network_msgtype mt;
        up = up >> mt;

        if (up.error())
        {
            ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                      << *node << " sent invalid bootstrap message="
                                      << msg->as_slice().hex()
                                      << " that is invalid";
            reset_to_disconnected();
            return -1;
        }

        if (mt != REPLNET_COMMAND_RESPONSE)
        {
            ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                      << *node << " sent invalid bootstrap message="
                                      << msg->as_slice().hex()
                                      << " in response to a registration request";
            reset_to_disconnected();
            return -1;
        }

        uint64_t nonce;
        replicant::response_returncode rrc;
        up = up >> nonce >> rrc;

        if (up.error())
        {
            ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                      << *node << " sent invalid bootstrap message="
                                      << msg->as_slice().hex()
                                      << " that is invalid";
            reset_to_disconnected();
            return -1;
        }

        if (rrc == replicant::RESPONSE_SUCCESS)
        {
            m_state = REPLCL_REGISTERED;
            send_nops_to_preferred_quorum(status); // error irrelevant
            return 0;
        }
        else
        {
            ERROR(BACKOFF) << "server could not register us with token " << m_token
                           << "; backoff before retrying";
            reset_to_disconnected();
            return -1;
        }
    }
}

int64_t
replicant_client :: handle_inform(const chain_node& node,
                                  std::auto_ptr<e::buffer> msg,
                                  e::unpacker up,
                                  replicant_returncode* status)
{
    configuration new_config;
    up = up >> new_config;

    if (up.error())
    {
        ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                  << node << " sent invalid INFORM message="
                                  << msg->as_slice().hex()
                                  << " that is invalid";
        return -1;
    }

    if (!new_config.validate())
    {
        ERROR(MISBEHAVING_SERVER) << "sever " << node
                                  << " sent INFORM with invalid configuration "
                                  << new_config;
        return -1;
    }

    if (m_cluster > 0 && m_cluster != new_config.cluster())
    {
        return report_cluster_jump(status);
    }

    m_cluster = new_config.cluster();
    m_cluster_jump = false;

    if (m_config->version() < new_config.version())
    {
        // disconnect old nodes
        for (const chain_node* n = m_config->members_begin();
                n < m_config->members_end(); ++n)
        {
            if (!new_config.in_config_chain(n->token))
            {
                m_busybee->drop(n->token);
            }
        }

        // switch to the new config
        *m_config = new_config;

        // enqueue all commands to be retried
        for (command_map::iterator it = m_commands.begin(); it != m_commands.end(); )
        {
            m_resend.insert(*it);
            m_commands.erase(it);
            it = m_commands.begin();
        }

        // establish a suitable number of connections
        send_nops_to_preferred_quorum(status); // error irrelevant
    }

    return 0;
}

int64_t
replicant_client :: handle_ping(const replicant::chain_node& node,
                                std::auto_ptr<e::buffer> msg,
                                e::unpacker up,
                                replicant_returncode* status)
{
    uint64_t version = 0;
    uint64_t seqno = 0;
    up = up >> version >> seqno;

    if (up.error())
    {
        ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                  << node << " sent invalid PING message="
                                  << msg->as_slice().hex();
        return -1;
    }

    msg->clear();
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PONG << seqno;
    return send_to_specific_node(&node, msg, status);
}

int64_t
replicant_client :: send_to_specific_node(const chain_node* node,
                                          std::auto_ptr<e::buffer> msg,
                                          replicant_returncode* status)
{
    m_busybee_mapper->set(*node);
    busybee_returncode rc = m_busybee->send(node->token, msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return 0;
        case BUSYBEE_DISRUPTED:
            ERROR(BACKOFF) << "connection to " << *node << " broke ("
                           << strerror(errno) << "):  backoff before retrying";
            handle_disruption(*node, status);
            return -1;
        BUSYBEE_ERROR_CASE(SHUTDOWN);
        BUSYBEE_ERROR_CASE(POLLFAILED);
        BUSYBEE_ERROR_CASE(ADDFDFAIL);
        BUSYBEE_ERROR_CASE(TIMEOUT);
        BUSYBEE_ERROR_CASE(EXTERNAL);
        BUSYBEE_ERROR_CASE(INTERRUPTED);
        default:
            ERROR(INTERNAL_ERROR) << "internal error: BusyBee unexpectedly returned "
                                  << (unsigned) rc << ": please file a bug";
            return -1;
    }
}

int64_t
replicant_client :: send_to_chain_head(std::auto_ptr<e::buffer> msg,
                                       replicant_returncode* status)
{
    const chain_node* head = m_config->head();

    if (!head)
    {
        ERROR(NEED_BOOTSTRAP) << "bootstrapped to an empty cluster: file a bug";
        reset_to_disconnected();
        return -1;
    }

    return send_to_specific_node(head, msg, status);
}

int64_t
replicant_client :: send_to_preferred_chain_position(e::intrusive_ptr<command> cmd,
                                                     replicant_returncode* status)
{
    const uint64_t* const start = m_config->chain_begin();
    const uint64_t* const limit = m_config->chain_end();
    size_t chain_length = limit - start;
    size_t offset = ((m_token >> 32) * chain_length) >> 32;

    for (size_t i = 0; i < chain_length; ++i)
    {
        const uint64_t* n = start + ((i + offset) % chain_length);
        const chain_node* node = m_config->node_from_token(*n);
        std::auto_ptr<e::buffer> msg(cmd->request()->copy());

        if (send_to_specific_node(node, msg, status) < 0)
        {
            continue;
        }

        cmd->set_sent_to(*node);
        m_commands[cmd->nonce()] = cmd;
        return cmd->clientid();
    }

    // We have an error captured above.
    return -1;
}

int64_t
replicant_client :: send_nops_to_preferred_quorum(replicant_returncode* status)
{
    const uint64_t* const start = m_config->chain_begin();
    const uint64_t* const limit = m_config->chain_end();
    size_t chain_length = limit - start;
    size_t offset = ((m_token >> 32) * chain_length) >> 32;
    size_t quorum = chain_length / 2 + 1;
    int64_t id = 0;

    for (size_t i = 0; i < chain_length; ++i)
    {
        const uint64_t* n = start + ((i + offset) % chain_length);

        if (quorum == 0)
        {
            m_busybee->drop(*n);
            continue;
        }

        const chain_node* node = m_config->node_from_token(*n);
        size_t sz = BUSYBEE_HEADER_SIZE + pack_size(REPLNET_NOP);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_NOP;

        if (send_to_specific_node(node, msg, status) < 0)
        {
            --id;
            continue;
        }

        // one less message that needs to be sent to produce a quorum
        --quorum;
    }

    return id;
}

void
replicant_client :: handle_disruption(const chain_node& from,
                                      replicant_returncode*)
{
    for (command_map::iterator it = m_commands.begin(); it != m_commands.end(); )
    {
        e::intrusive_ptr<command> c = it->second;

        // If this op wasn't sent to the failed host, then skip it
        if (c->sent_to().token != from.token)
        {
            ++it;
            continue;
        }

        m_resend.insert(*it);
        m_commands.erase(it);
        it = m_commands.begin();
    }
}

int64_t
replicant_client :: handle_command_response(const chain_node& node,
                                            std::auto_ptr<e::buffer> msg,
                                            e::unpacker up,
                                            replicant_returncode* status)
{
    // Parse the command response
    uint64_t nonce;
    replicant::response_returncode rc;
    up = up >> nonce >> rc;

    if (up.error())
    {
        ERROR(MISBEHAVING_SERVER) << "communication error: server "
                                  << node << " sent invalid command response="
                                  << msg->as_slice().hex();
        return -1;
    }

    // Find the command
    command_map::iterator it = m_commands.find(nonce);
    command_map* map = &m_commands;

    if (it == map->end())
    {
        it = m_resend.find(nonce);
        map = &m_resend;
    }

    if (it == map->end())
    {
        return 0;
    }

    // Pass the response to the command
    e::intrusive_ptr<command> c = it->second;
    *status = REPLICANT_SUCCESS;
    m_last_error = e::error();

    switch (rc)
    {
        case replicant::RESPONSE_SUCCESS:
            c->succeed(msg, up.as_slice(), REPLICANT_SUCCESS);
            break;
        case replicant::RESPONSE_COND_NOT_EXIST:
            c->fail(REPLICANT_COND_NOT_FOUND);
            ERROR(COND_NOT_FOUND) << "condition not found";
            break;
        case replicant::RESPONSE_COND_DESTROYED:
            c->fail(REPLICANT_COND_DESTROYED);
            ERROR(COND_DESTROYED) << "condition destroyed";
            break;
        case replicant::RESPONSE_REGISTRATION_FAIL:
            c->fail(REPLICANT_MISBEHAVING_SERVER);
            ERROR(MISBEHAVING_SERVER) << "server treated request as a registration";
            break;
        case replicant::RESPONSE_OBJ_EXIST:
            c->fail(REPLICANT_OBJ_EXIST);
            ERROR(OBJ_EXIST) << "object already exists";
            break;
        case replicant::RESPONSE_OBJ_NOT_EXIST:
            c->fail(REPLICANT_OBJ_NOT_FOUND);
            ERROR(OBJ_NOT_FOUND) << "object not found";
            break;
        case replicant::RESPONSE_SERVER_ERROR:
            c->fail(REPLICANT_SERVER_ERROR);
            ERROR(SERVER_ERROR) << "server reports error; consult server logs for details";
            break;
        case replicant::RESPONSE_DLOPEN_FAIL:
            c->fail(REPLICANT_BAD_LIBRARY);
            ERROR(BAD_LIBRARY) << "library cannot be loaded on the server";
            break;
        case replicant::RESPONSE_DLSYM_FAIL:
            c->fail(REPLICANT_BAD_LIBRARY);
            ERROR(BAD_LIBRARY) << "state machine not found in library";
            break;
        case replicant::RESPONSE_NO_CTOR:
            c->fail(REPLICANT_BAD_LIBRARY);
            ERROR(BAD_LIBRARY) << "state machine doesn't contain a constructor";
            break;
        case replicant::RESPONSE_NO_RTOR:
            c->fail(REPLICANT_BAD_LIBRARY);
            ERROR(BAD_LIBRARY) << "state machine doesn't contain a reconstructor";
            break;
        case replicant::RESPONSE_NO_DTOR:
            c->fail(REPLICANT_BAD_LIBRARY);
            ERROR(BAD_LIBRARY) << "state machine doesn't contain a denstructor";
            break;
        case replicant::RESPONSE_NO_SNAP:
            c->fail(REPLICANT_BAD_LIBRARY);
            ERROR(BAD_LIBRARY) << "state machine doesn't contain a snapshot function";
            break;
        case replicant::RESPONSE_NO_FUNC:
            c->fail(REPLICANT_FUNC_NOT_FOUND);
            ERROR(FUNC_NOT_FOUND) << "state machine doesn't contain the requested function";
            break;
        case replicant::RESPONSE_CTOR_FAILED:
            c->fail(REPLICANT_CTOR_FAILED);
            ERROR(CTOR_FAILED) << "state machine's constructor failed";
            break;
        case replicant::RESPONSE_MALFORMED:
            c->fail(REPLICANT_INTERNAL_ERROR);
            ERROR(INTERNAL_ERROR) << "server reports that request was malformed";
            break;
        default:
            c->fail(REPLICANT_MISBEHAVING_SERVER);
            ERROR(MISBEHAVING_SERVER) << "unknown response code";
            break;
    }

    c->set_error(m_last_error);
    *status = REPLICANT_SUCCESS;
    m_last_error = e::error();
    map->erase(it);
    m_complete.insert(std::make_pair(c->nonce(), c));
    return 0;
}

int64_t
replicant_client :: report_cluster_jump(replicant_returncode* status)
{
    m_cluster_jump = true;
    ERROR(CLUSTER_JUMP) << "jumped to another cluster";
    return -1;
}

uint64_t
replicant_client :: generate_token()
{
    try
    {
        po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

        if (sysrand.get() < 0)
        {
            return e::time();
        }

        uint64_t token;

        if (sysrand.read(&token, sizeof(token)) != sizeof(token))
        {
            return e::time();
        }

        return token;
    }
    catch (po6::error& e)
    {
        return e::time();
    }
}

void
replicant_client :: reset_to_disconnected()
{
    m_busybee_mapper.reset(new replicant::mapper());
    m_busybee.reset(new busybee_st(m_gc.get(), m_busybee_mapper.get(), 0));
    m_config.reset(new configuration());
    m_token = 0x4141414141414141ULL;
    // leave m_nonce
    m_state = REPLCL_DISCONNECTED;
    killall(REPLICANT_NEED_BOOTSTRAP, m_last_error);
    // Don't touch the error items
    // Don't touch m_cluster
}

void
replicant_client :: killall(replicant_returncode status, e::error err)
{
    while (!m_commands.empty())
    {
        e::intrusive_ptr<command> cmd = m_commands.begin()->second;
        cmd->fail(status);
        cmd->set_error(err);
        m_complete.insert(*m_commands.begin());
        m_commands.erase(m_commands.begin());
    }

    while (!m_resend.empty())
    {
        e::intrusive_ptr<command> cmd = m_commands.begin()->second;
        cmd->fail(status);
        cmd->set_error(err);
        m_complete.insert(*m_resend.begin());
        m_resend.erase(m_resend.begin());
    }
}

std::ostream&
operator << (std::ostream& lhs, replicant_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(REPLICANT_SUCCESS);
        STRINGIFY(REPLICANT_NAME_TOO_LONG);
        STRINGIFY(REPLICANT_FUNC_NOT_FOUND);
        STRINGIFY(REPLICANT_OBJ_EXIST);
        STRINGIFY(REPLICANT_OBJ_NOT_FOUND);
        STRINGIFY(REPLICANT_COND_NOT_FOUND);
        STRINGIFY(REPLICANT_COND_DESTROYED);
        STRINGIFY(REPLICANT_SERVER_ERROR);
        STRINGIFY(REPLICANT_CTOR_FAILED);
        STRINGIFY(REPLICANT_CLUSTER_JUMP);
        STRINGIFY(REPLICANT_BAD_LIBRARY);
        STRINGIFY(REPLICANT_TIMEOUT);
        STRINGIFY(REPLICANT_BACKOFF);
        STRINGIFY(REPLICANT_NEED_BOOTSTRAP);
        STRINGIFY(REPLICANT_MISBEHAVING_SERVER);
        STRINGIFY(REPLICANT_INTERNAL_ERROR);
        STRINGIFY(REPLICANT_NONE_PENDING);
        STRINGIFY(REPLICANT_INTERRUPTED);
        STRINGIFY(REPLICANT_GARBAGE);
        default:
            lhs << "unknown replicant_returncode (" << static_cast<unsigned int>(rhs) << ")";
    }

    return lhs;
}
