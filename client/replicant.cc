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

// e
#include <e/endian.h>
#include <e/timer.h>

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

#define REPLSETERROR(CODE, DESC) \
    do \
    { \
        m_last_error_desc = DESC; \
        m_last_error_file = __FILE__; \
        m_last_error_line = __LINE__; \
        *status = CODE; \
    } while (0)

#define REPLSETSUCCESS REPLSETERROR(REPLICANT_SUCCESS, "operation succeeded")

#define COMMAND_HEADER_SIZE (BUSYBEE_HEADER_SIZE + pack_size(REPLNET_COMMAND_SUBMIT) + 4 * sizeof(uint64_t))

#define BUSYBEE_ERROR(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        REPLSETERROR(REPLICANT_ ## REPRC, "BusyBee returned " xstr(BBRC)); \
        return -1

#define BUSYBEE_ERROR_DISCONNECT(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        REPLSETERROR(REPLICANT_ ## REPRC, "BusyBee returned " xstr(BBRC)); \
        reset_to_disconnected(); \
        return -1

#define BUSYBEE_ERROR_CONTINUE(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        REPLSETERROR(REPLICANT_ ## REPRC, "BusyBee returned " xstr(BBRC)); \
        continue

#define REPL_UNEXPECTED(MT) \
    case REPLNET_ ## MT: \
        REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unexpected " xstr(MT) " message"); \
        m_last_error_host = from; \
        return -1

#define REPL_UNEXPECTED_DISCONNECT(MT) \
    case REPLNET_ ## MT: \
        REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unexpected " xstr(MT) " message"); \
        m_last_error_host = from; \
        reset_to_disconnected(); \
        return -1

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

void
replicant_destroy_output(const char* output, size_t)
{
    uint16_t sz = 0;
    e::unpack16le(output - 2, &sz);
    const e::buffer* buf = reinterpret_cast<const e::buffer*>(output - sz);
    delete buf;
}

replicant_client :: replicant_client(const char* host, in_port_t port)
    : m_busybee_mapper(new replicant::mapper())
    , m_busybee(new busybee_st(m_busybee_mapper.get(), 0))
    , m_config(new replicant::configuration())
    , m_bootstrap(host, port)
    , m_token(0x4141414141414141ULL)
    , m_nonce(1)
    , m_state(REPLCL_DISCONNECTED)
    , m_commands()
    , m_complete()
    , m_resend()
    , m_last_error_desc("")
    , m_last_error_file(__FILE__)
    , m_last_error_line(__LINE__)
    , m_last_error_host()
{
}

replicant_client :: ~replicant_client() throw ()
{
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
        REPLSETERROR(REPLICANT_BAD_LIBRARY, "could not open library; see errno for details");
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
        REPLSETERROR(REPLICANT_BAD_LIBRARY, "could not open library; see errno for details");
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
            << m_token << nonce << object;
    pa = pa.copy(e::slice(&lib[0], lib.size()));
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, errmsg, errmsg_sz);
    return send_to_preferred_chain_member(cmd, status);
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
    return send_to_preferred_chain_member(cmd, status);
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
    return send_to_preferred_chain_member(cmd, status);
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
    return send_to_preferred_chain_member(cmd, status);
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
    send_to_preferred_chain_member(cmd, &rc);

    while (m_commands.find(nonce) != m_commands.end())
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
        m_last_error_desc = c->last_error_desc();
        m_last_error_file = c->last_error_file();
        m_last_error_line = c->last_error_line();
        m_last_error_host = c->sent_to().address;
        return c->clientid();
    }

    if (m_commands.empty())
    {
        REPLSETERROR(REPLICANT_NONE_PENDING, "no outstanding operations to process");
        return -1;
    }

    REPLSETERROR(REPLICANT_INTERNAL_ERROR, "unhandled exit case from loop");
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
        REPLSETERROR(REPLICANT_NONE_PENDING, "no outstanding operation with the specified id");
        return -1;
    }

    e::intrusive_ptr<command> c = it->second;
    m_complete.erase(it);
    m_last_error_desc = c->last_error_desc();
    m_last_error_file = c->last_error_file();
    m_last_error_line = c->last_error_line();
    m_last_error_host = c->sent_to().address;
    return c->clientid();
}

void
replicant_client :: kill(int64_t id)
{
    m_commands.erase(id);
    m_complete.erase(id);
    m_resend.erase(id);
}

int
replicant_client :: poll_fd()
{
    return m_busybee->poll_fd();
}

int64_t
replicant_client :: inner_loop(replicant_returncode* status)
{
    int64_t ret = maintain_connection(status);

    if (ret != 0)
    {
        return ret;
    }

    // Resend all those that need it
    while (!m_resend.empty())
    {
        ret = send_to_preferred_chain_member(m_resend.begin()->second, status);

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
    chain_node node = m_config->get(id);
    po6::net::location from = node.address;

    // And process it
    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_DISRUPTED:
            if ((ret = handle_disruption(node, status)) < 0)
            {
                return ret;
            }
            return 0;
        case BUSYBEE_INTERRUPTED:
            REPLSETERROR(REPLICANT_INTERRUPTED, "signal received");
            return -1;
        case BUSYBEE_TIMEOUT:
            REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
            return -1;
        BUSYBEE_ERROR(INTERNAL_ERROR, SHUTDOWN);
        BUSYBEE_ERROR(INTERNAL_ERROR, POLLFAILED);
        BUSYBEE_ERROR(INTERNAL_ERROR, ADDFDFAIL);
        BUSYBEE_ERROR(INTERNAL_ERROR, EXTERNAL);
        default:
            REPLSETERROR(REPLICANT_INTERNAL_ERROR, "BusyBee returned unknown error");
            return -1;
    }

    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    replicant_network_msgtype mt;
    up = up >> mt;

    if (up.error())
    {
        REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    switch (mt)
    {
        case REPLNET_COMMAND_RESPONSE:
        case REPLNET_CONDITION_NOTIFY:
            if ((ret = handle_command_response(from, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        case REPLNET_INFORM:
            if ((ret = handle_inform(from, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        REPL_UNEXPECTED(NOP);
        REPL_UNEXPECTED(BOOTSTRAP);
        REPL_UNEXPECTED(JOIN);
        REPL_UNEXPECTED(CONFIG_PROPOSE);
        REPL_UNEXPECTED(CONFIG_ACCEPT);
        REPL_UNEXPECTED(CONFIG_REJECT);
        REPL_UNEXPECTED(CLIENT_REGISTER);
        REPL_UNEXPECTED(CLIENT_DISCONNECT);
        REPL_UNEXPECTED(COMMAND_SUBMIT);
        REPL_UNEXPECTED(COMMAND_ISSUE);
        REPL_UNEXPECTED(COMMAND_ACK);
        REPL_UNEXPECTED(HEAL_REQ);
        REPL_UNEXPECTED(HEAL_RESP);
        REPL_UNEXPECTED(HEAL_DONE);
        REPL_UNEXPECTED(CONDITION_WAIT);
        REPL_UNEXPECTED(PING);
        REPL_UNEXPECTED(PONG);
        default:
            REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "invalid message type");
            m_last_error_host = from;
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

        switch (m_state)
        {
            case REPLCL_DISCONNECTED:
                if ((ret = perform_bootstrap(status)) < 0)
                {
                    return ret;
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
                REPLSETERROR(REPLICANT_INTERNAL_ERROR, "client corrupted");
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
    replicant::bootstrap_returncode rc = replicant::bootstrap(m_bootstrap, &initial);

    switch (rc)
    {
        case replicant::BOOTSTRAP_SUCCESS:
            m_state = REPLCL_BOOTSTRAPPED;
            *m_config = initial;
            return 0;
        case replicant::BOOTSTRAP_SEE_ERRNO:
        case replicant::BOOTSTRAP_COMM_FAIL:
        case replicant::BOOTSTRAP_TIMEOUT:
            REPLSETERROR(REPLICANT_NEED_BOOTSTRAP, "cannot connect to the cluster");
            reset_to_disconnected();
            return -1;
        case replicant::BOOTSTRAP_CORRUPT_INFORM:
            REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "server sent corrupt INFORM message");
            reset_to_disconnected();
            return -1;
        case replicant::BOOTSTRAP_NOT_CLUSTER_MEMBER:
            REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "server sent INFORM message, but it is not a cluster member");
            reset_to_disconnected();
            return -1;
        default:
            REPLSETERROR(REPLICANT_INTERNAL_ERROR, "bootstrap failed for unknown reasons");
            reset_to_disconnected();
            return -1;
    }
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
        busybee_returncode rc = m_busybee->recv(&token, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
                return -1;
            case BUSYBEE_INTERRUPTED:
                REPLSETERROR(REPLICANT_INTERRUPTED, "signal received");
                return -1;
            BUSYBEE_ERROR_DISCONNECT(NEED_BOOTSTRAP, DISRUPTED);
            BUSYBEE_ERROR_DISCONNECT(INTERNAL_ERROR, SHUTDOWN);
            BUSYBEE_ERROR_DISCONNECT(INTERNAL_ERROR, POLLFAILED);
            BUSYBEE_ERROR_DISCONNECT(INTERNAL_ERROR, ADDFDFAIL);
            BUSYBEE_ERROR_DISCONNECT(INTERNAL_ERROR, EXTERNAL);
            default:
                REPLSETERROR(REPLICANT_INTERNAL_ERROR, "BusyBee returned unknown error");
                reset_to_disconnected();
                return -1;
        }

        po6::net::location from = m_config->head().address;

        if (token != m_config->head().token)
        {
            REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "a node that is not the head replied to \"REGISTER\"");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        replicant_network_msgtype mt;
        up = up >> mt;

        if (up.error())
        {
            REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unpack failed");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        switch (mt)
        {
            case REPLNET_COMMAND_RESPONSE:
                break;
            REPL_UNEXPECTED_DISCONNECT(NOP);
            REPL_UNEXPECTED_DISCONNECT(BOOTSTRAP);
            REPL_UNEXPECTED_DISCONNECT(JOIN);
            REPL_UNEXPECTED_DISCONNECT(INFORM);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_PROPOSE);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_ACCEPT);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_REJECT);
            REPL_UNEXPECTED_DISCONNECT(CLIENT_REGISTER);
            REPL_UNEXPECTED_DISCONNECT(CLIENT_DISCONNECT);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_SUBMIT);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_ISSUE);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_ACK);
            REPL_UNEXPECTED_DISCONNECT(HEAL_REQ);
            REPL_UNEXPECTED_DISCONNECT(HEAL_RESP);
            REPL_UNEXPECTED_DISCONNECT(HEAL_DONE);
            REPL_UNEXPECTED_DISCONNECT(CONDITION_WAIT);
            REPL_UNEXPECTED_DISCONNECT(CONDITION_NOTIFY);
            REPL_UNEXPECTED_DISCONNECT(PING);
            REPL_UNEXPECTED_DISCONNECT(PONG);
            default:
                REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "invalid message type");
                m_last_error_host = from;
                reset_to_disconnected();
                return -1;
        }

        uint64_t nonce;
        replicant::response_returncode rrc;
        up = up >> nonce >> rrc;

        if (up.error())
        {
            REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unpack of BOOTSTRAP failed");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        if (rrc == replicant::RESPONSE_SUCCESS)
        {
            m_state = REPLCL_REGISTERED;
            return 0;
        }
        else
        {
            REPLSETERROR(REPLICANT_NEED_BOOTSTRAP, "could not register with the server");
            reset_to_disconnected();
            return -1;
        }
    }
}

int64_t
replicant_client :: handle_inform(const po6::net::location& from,
                                  std::auto_ptr<e::buffer>,
                                  e::unpacker up,
                                  replicant_returncode* status)
{
    configuration new_config;
    up = up >> new_config;

    if (up.error())
    {
        REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unpack of INFORM failed");
        m_last_error_host = from;
        return -1;
    }

    if (!new_config.validate())
    {
        REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "INFORM message contains invalid configuration");
        m_last_error_host = from;
        return -1;
    }

    if (m_config->version() < new_config.version())
    {
        *m_config = new_config;

        for (command_map::iterator it = m_commands.begin(); it != m_commands.end(); )
        {
            chain_node n = m_config->get(it->first);
            e::intrusive_ptr<command> c = it->second;

            // If this op wasn't sent to a removed host, then skip it
            if (m_config->in_cluster(n))
            {
                ++it;
                continue;
            }

            m_resend.insert(*it);
            m_commands.erase(it);
            it = m_commands.begin();
        }
    }

    return 0;
}

int64_t
replicant_client :: send_to_chain_head(std::auto_ptr<e::buffer> msg,
                                       replicant_returncode* status)
{
    int64_t ret = -1;
    m_busybee_mapper->set(m_config->head());
    busybee_returncode rc = m_busybee->send(m_config->head().token, msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return 0;
        case BUSYBEE_DISRUPTED:
            if ((ret = handle_disruption(m_config->head(), status)) < 0)
            {
                return ret;
            }
            return -1;
        case BUSYBEE_TIMEOUT:
            REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
            return -1;
        case BUSYBEE_INTERRUPTED:
            REPLSETERROR(REPLICANT_INTERRUPTED, "signal received");
            return -1;
        BUSYBEE_ERROR(INTERNAL_ERROR, SHUTDOWN);
        BUSYBEE_ERROR(INTERNAL_ERROR, POLLFAILED);
        BUSYBEE_ERROR(INTERNAL_ERROR, ADDFDFAIL);
        BUSYBEE_ERROR(INTERNAL_ERROR, EXTERNAL);
        default:
            REPLSETERROR(REPLICANT_INTERNAL_ERROR, "BusyBee returned unknown error");
            return -1;
    }
}

int64_t
replicant_client :: send_to_preferred_chain_member(e::intrusive_ptr<command> cmd,
                                                   replicant_returncode* status)
{
    int64_t ret = -1;
    bool sent = false;
    const chain_node* sent_to = NULL;

    for (const chain_node* n = m_config->members_begin();
            !sent && n != m_config->members_end(); ++n)
    {
        sent_to = n;
        std::auto_ptr<e::buffer> msg(cmd->request()->copy());
        m_busybee_mapper->set(*n);
        busybee_returncode rc = m_busybee->send(n->token, msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                sent = true;
                break;
            case BUSYBEE_DISRUPTED:
                if ((ret = handle_disruption(*n, status)) < 0)
                {
                    return ret;
                }
                REPLSETERROR(REPLICANT_BACKOFF, "backoff before retrying");
                continue;
            case BUSYBEE_TIMEOUT:
                REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
                return -1;
            case BUSYBEE_INTERRUPTED:
                REPLSETERROR(REPLICANT_INTERRUPTED, "signal received");
                return -1;
            BUSYBEE_ERROR_CONTINUE(INTERNAL_ERROR, SHUTDOWN);
            BUSYBEE_ERROR_CONTINUE(INTERNAL_ERROR, POLLFAILED);
            BUSYBEE_ERROR_CONTINUE(INTERNAL_ERROR, ADDFDFAIL);
            BUSYBEE_ERROR_CONTINUE(INTERNAL_ERROR, EXTERNAL);
            default:
                REPLSETERROR(REPLICANT_INTERNAL_ERROR, "BusyBee returned unknown error");
                continue;
        }
    }

    if (sent)
    {
        cmd->set_sent_to(*sent_to);
        m_commands[cmd->nonce()] = cmd;
        return cmd->clientid();
    }
    else
    {
        // We have an error captured by REPLSETERROR above.
        return -1;
    }
}

int64_t
replicant_client :: handle_disruption(const chain_node& from,
                                      replicant_returncode*)
{
    for (command_map::iterator it = m_commands.begin(); it != m_commands.end(); )
    {
        e::intrusive_ptr<command> c = it->second;

        // If this op wasn't sent to the failed host, then skip it
        if (c->sent_to() != from)
        {
            ++it;
            continue;
        }

        m_resend.insert(*it);
        m_commands.erase(it);
        it = m_commands.begin();
    }

    return 0;
}

int64_t
replicant_client :: handle_command_response(const po6::net::location& from,
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
        REPLSETERROR(REPLICANT_MISBEHAVING_SERVER, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    // Find the command
    command_map::iterator it = m_commands.find(nonce);
    // XXX perhaps we should consider m_resend as well

    if (it == m_commands.end())
    {
        return 0;
    }

    // Pass the response to the command
    e::intrusive_ptr<command> c = it->second;
    REPLSETSUCCESS;

    switch (rc)
    {
        case replicant::RESPONSE_SUCCESS:
            c->succeed(msg, up.as_slice(), REPLICANT_SUCCESS);
            break;
        case replicant::RESPONSE_COND_NOT_EXIST:
            c->fail(REPLICANT_COND_NOT_FOUND);
            m_last_error_desc = "condition not found";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_REGISTRATION_FAIL:
            c->fail(REPLICANT_MISBEHAVING_SERVER);
            m_last_error_desc = "server treated request as a registration";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_OBJ_EXIST:
            c->fail(REPLICANT_OBJ_EXIST);
            m_last_error_desc = "object already exists";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_OBJ_NOT_EXIST:
            c->fail(REPLICANT_OBJ_NOT_FOUND);
            m_last_error_desc = "object not found";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_SERVER_ERROR:
            c->fail(REPLICANT_SERVER_ERROR);
            m_last_error_desc = "server reports error; consult server logs for details";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_DLOPEN_FAIL:
            c->fail(REPLICANT_BAD_LIBRARY);
            m_last_error_desc = "library cannot be loaded on the server";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_DLSYM_FAIL:
            c->fail(REPLICANT_BAD_LIBRARY);
            m_last_error_desc = "state machine not found in library";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_NO_CTOR:
            c->fail(REPLICANT_BAD_LIBRARY);
            m_last_error_desc = "state machine not doesn't contain a constructor";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_NO_RTOR:
            c->fail(REPLICANT_BAD_LIBRARY);
            m_last_error_desc = "state machine not doesn't contain a reconstructor";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_NO_DTOR:
            c->fail(REPLICANT_BAD_LIBRARY);
            m_last_error_desc = "state machine not doesn't contain a denstructor";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_NO_SNAP:
            c->fail(REPLICANT_BAD_LIBRARY);
            m_last_error_desc = "state machine not doesn't contain a snapshot function";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_NO_FUNC:
            c->fail(REPLICANT_FUNC_NOT_FOUND);
            m_last_error_desc = "state machine not doesn't contain the requested function";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case replicant::RESPONSE_MALFORMED:
            c->fail(REPLICANT_INTERNAL_ERROR);
            m_last_error_desc = "server reports that request was malformed";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        default:
            c->fail(REPLICANT_MISBEHAVING_SERVER);
            m_last_error_desc = "unknown response code";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
    }

    c->set_last_error_desc(m_last_error_desc);
    c->set_last_error_file(m_last_error_file);
    c->set_last_error_line(m_last_error_line);
    m_commands.erase(it);
    m_complete.insert(std::make_pair(c->nonce(), c));
    return 0;
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
    m_busybee.reset(new busybee_st(m_busybee_mapper.get(), 0));
    m_config.reset(new configuration());
    m_token = 0x4141414141414141ULL;
    // leave m_nonce
    m_state = REPLCL_DISCONNECTED;

    while (!m_commands.empty())
    {
        e::intrusive_ptr<command> cmd = m_commands.begin()->second;
        cmd->fail(REPLICANT_NEED_BOOTSTRAP);
        cmd->set_last_error_desc(m_last_error_desc);
        cmd->set_last_error_file(m_last_error_file);
        cmd->set_last_error_line(m_last_error_line);
        m_complete.insert(*m_commands.begin());
        m_commands.erase(m_commands.begin());
    }

    while (!m_resend.empty())
    {
        e::intrusive_ptr<command> cmd = m_commands.begin()->second;
        cmd->fail(REPLICANT_NEED_BOOTSTRAP);
        cmd->set_last_error_desc(m_last_error_desc);
        cmd->set_last_error_file(m_last_error_file);
        cmd->set_last_error_line(m_last_error_line);
        m_complete.insert(*m_resend.begin());
        m_resend.erase(m_resend.begin());
    }

    // Don't touch the error items
}

std::ostream&
operator << (std::ostream& lhs, replicant_returncode rhs)
{
    switch (rhs)
    {
        stringify(REPLICANT_SUCCESS);
        stringify(REPLICANT_NAME_TOO_LONG);
        stringify(REPLICANT_FUNC_NOT_FOUND);
        stringify(REPLICANT_OBJ_EXIST);
        stringify(REPLICANT_OBJ_NOT_FOUND);
        stringify(REPLICANT_COND_NOT_FOUND);
        stringify(REPLICANT_SERVER_ERROR);
        stringify(REPLICANT_BAD_LIBRARY);
        stringify(REPLICANT_TIMEOUT);
        stringify(REPLICANT_BACKOFF);
        stringify(REPLICANT_NEED_BOOTSTRAP);
        stringify(REPLICANT_MISBEHAVING_SERVER);
        stringify(REPLICANT_INTERNAL_ERROR);
        stringify(REPLICANT_NONE_PENDING);
        stringify(REPLICANT_INTERRUPTED);
        stringify(REPLICANT_GARBAGE);
        default:
            lhs << "unknown returncode (" << static_cast<unsigned int>(rhs) << ")";
    }

    return lhs;
}
