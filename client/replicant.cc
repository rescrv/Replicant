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
#include "common/configuration.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/special_objects.h"
#include "client/command.h"
#include "client/replicant.h"

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
        REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unexpected " xstr(MT) " message"); \
        m_last_error_host = from; \
        return -1

#define REPL_UNEXPECTED_DISCONNECT(MT) \
    case REPLNET_ ## MT: \
        REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unexpected " xstr(MT) " message"); \
        m_last_error_host = from; \
        reset_to_disconnected(); \
        return -1

void
replicant_destroy_output(const char* output, size_t)
{
    uint16_t sz = 0;
    e::unpack16le(output - 2, &sz);
    const e::buffer* buf = reinterpret_cast<const e::buffer*>(output - sz);
    delete buf;
}

replicant :: replicant(const char* host, in_port_t port)
    : m_busybee(new busybee_st())
    , m_config(new configuration())
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
    for (size_t i = 0; i < 32; ++i)
    {
        m_identify_sent[i] = 0;
        m_identify_recv[i] = 0;
    }
}

replicant :: ~replicant() throw ()
{
}

int64_t
replicant :: create_object(const char* obj, size_t obj_sz,
                           const char* path,
                           replicant_returncode* status)
{
    int64_t ret = maintain_connection(status);

    if (ret < 0)
    {
        return ret;
    }

    // Pack the message to send
    size_t path_sz = strlen(path) + 1;
    size_t sz = COMMAND_HEADER_SIZE + sizeof(uint64_t) + sizeof(uint32_t) + path_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t garbage = compute_garbage();
    char object_buf[sizeof(uint64_t)];
    memset(object_buf, 0, sizeof(object_buf));
    memmove(object_buf, obj, std::min(obj_sz, sizeof(object_buf)));
    uint64_t object;
    e::unpack64be(object_buf, &object);
    pa = pa << REPLNET_COMMAND_SUBMIT
            << m_token << nonce << garbage << uint64_t(OBJECT_CREATE)
            << object << e::slice(path, path_sz);
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, NULL, NULL);
    return send_to_any_chain_member(cmd, status);
}

int64_t
replicant :: send(const char* obj, size_t obj_sz, const char* func,
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
    size_t func_sz = strlen(func) + 1;
    size_t sz = COMMAND_HEADER_SIZE + func_sz + data_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t garbage = compute_garbage();
    char object_buf[sizeof(uint64_t)];
    memset(object_buf, 0, sizeof(object_buf));
    memmove(object_buf, obj, std::min(obj_sz, sizeof(object_buf)));
    uint64_t object;
    e::unpack64be(object_buf, &object);
    pa = pa << REPLNET_COMMAND_SUBMIT << m_token << nonce << garbage << object;
    pa = pa.copy(e::slice(func, func_sz));
    pa = pa.copy(e::slice(data, data_sz));
    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, output, output_sz);
    return send_to_any_chain_member(cmd, status);
}

replicant_returncode
replicant :: disconnect()
{
    replicant_returncode rc;
    int64_t ret = maintain_connection(&rc);

    if (ret < 0)
    {
        return REPLICANT_NEEDBOOTSTRAP;
    }

    uint64_t nonce = m_nonce;
    ++m_nonce;
    uint64_t garbage = m_nonce;
    uint64_t object = OBJECT_DEPART;
    std::auto_ptr<e::buffer> msg(e::buffer::create(COMMAND_HEADER_SIZE));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_COMMAND_SUBMIT << m_token << nonce << garbage << object;
    e::intrusive_ptr<command> cmd = new command(&rc, nonce, msg, NULL, NULL);
    m_state = REPLCL_DISCONNECT_SENT;
    send_to_any_chain_member(cmd, &rc);

    while (m_commands.find(nonce) != m_commands.end() ||
           m_resend.find(nonce) != m_resend.end())
    {
        m_busybee->set_timeout(-1);
        inner_loop(&rc);
    }

    reset_to_disconnected();
    m_commands.clear();
    m_commands.clear();
    m_commands.clear();
    return REPLICANT_SUCCESS;
}

int64_t
replicant :: loop(int timeout, replicant_returncode* status)
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
        m_last_error_host = c->sent_to();
        return c->clientid();
    }

    if (m_commands.empty())
    {
        REPLSETERROR(REPLICANT_NONEPENDING, "no outstanding operations to process");
        return -1;
    }

    REPLSETERROR(REPLICANT_INTERNALERROR, "unhandled exit case from loop");
    return -1;
}

int64_t
replicant :: loop(int64_t id, int timeout, replicant_returncode* status)
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
        REPLSETERROR(REPLICANT_NONEPENDING, "no outstanding operation with the specified id");
        return -1;
    }

    e::intrusive_ptr<command> c = it->second;
    m_complete.erase(it);
    m_last_error_desc = c->last_error_desc();
    m_last_error_file = c->last_error_file();
    m_last_error_line = c->last_error_line();
    m_last_error_host = c->sent_to();
    return c->clientid();
}

int64_t
replicant :: inner_loop(replicant_returncode* status)
{
    int64_t ret = maintain_connection(status);

    if (ret != 0)
    {
        return ret;
    }

    // Resend all those that need it
    while (!m_resend.empty())
    {
        ret = send_to_any_chain_member(m_resend.begin()->second, status);

        // As this is a retransmission, we only care about errors (< 0)
        // not the success half (>=0).
        if (ret < 0)
        {
            return ret;
        }

        m_resend.erase(m_resend.begin());
    }

    // Receive a message
    po6::net::location from;
    std::auto_ptr<e::buffer> msg;
    busybee_returncode rc = m_busybee->recv(&from, &msg);

    // And process it
    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_DISCONNECT:
        case BUSYBEE_CONNECTFAIL:
            if ((ret = handle_disconnect(from, status)) < 0)
            {
                return ret;
            }
            return 0;
        case BUSYBEE_TIMEOUT:
            REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
            return -1;
        BUSYBEE_ERROR(BUFFERFULL, BUFFERFULL);
        BUSYBEE_ERROR(INTERNALERROR, SHUTDOWN);
        BUSYBEE_ERROR(INTERNALERROR, QUEUED);
        BUSYBEE_ERROR(INTERNALERROR, POLLFAILED);
        BUSYBEE_ERROR(INTERNALERROR, ADDFDFAIL);
        BUSYBEE_ERROR(INTERNALERROR, EXTERNAL);
        default:
            REPLSETERROR(REPLICANT_INTERNALERROR, "BusyBee returned unknown error");
            return -1;
    }

    e::buffer::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    replicant_network_msgtype mt;
    up = up >> mt;

    if (up.error())
    {
        REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    switch (mt)
    {
        case REPLNET_COMMAND_RESPONSE:
            if ((ret = handle_command_response(from, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        case REPLNET_COMMAND_RESEND:
            if ((ret = handle_command_resend(from, msg, up, status)) < 0)
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
        case REPLNET_IDENTIFIED:
            if ((ret = handle_identified(from, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        REPL_UNEXPECTED(NOP);
        REPL_UNEXPECTED(JOIN);
        REPL_UNEXPECTED(BECOME_SPARE);
        REPL_UNEXPECTED(BECOME_STANDBY);
        REPL_UNEXPECTED(BECOME_MEMBER);
        REPL_UNEXPECTED(CONFIG_PROPOSE);
        REPL_UNEXPECTED(CONFIG_ACCEPT);
        REPL_UNEXPECTED(CONFIG_REJECT);
        REPL_UNEXPECTED(IDENTIFY);
        REPL_UNEXPECTED(CLIENT_LIST);
        REPL_UNEXPECTED(COMMAND_SUBMIT);
        REPL_UNEXPECTED(COMMAND_ISSUE);
        REPL_UNEXPECTED(COMMAND_ACK);
        REPL_UNEXPECTED(REQ_STATE);
        REPL_UNEXPECTED(RESP_STATE);
        REPL_UNEXPECTED(SNAPSHOT);
        REPL_UNEXPECTED(HEALED);
        default:
            REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "invalid message type");
            m_last_error_host = from;
            return -1;
    }

    return 0;
}

int64_t
replicant :: maintain_connection(replicant_returncode* status)
{
    while (true)
    {
        int64_t ret;

        switch (m_state)
        {
            case REPLCL_DISCONNECTED:
                if ((ret = send_join_message(status)) < 0)
                {
                    return ret;
                }
                break;
            case REPLCL_JOIN_SENT:
                if ((ret = wait_for_bootstrap(status)) < 0)
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
            case REPLCL_PARTIALLY_CONNECTED:
                if ((ret = send_identify_messages(status)) < 0)
                {
                    return ret;
                }
                return 0;
            case REPLCL_CONNECTED:
            case REPLCL_DISCONNECT_SENT:
                return 0;
            default:
                REPLSETERROR(REPLICANT_INTERNALERROR, "client corrupted");
                return -1;
        }
    }
}

int64_t
replicant :: send_join_message(replicant_returncode* status)
{
    m_token = generate_token();
    po6::net::location host = m_bootstrap.lookup(SOCK_STREAM, IPPROTO_TCP);
    size_t sz = BUSYBEE_HEADER_SIZE + pack_size(REPLNET_JOIN);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_JOIN;
    int64_t ret = send_preconnect_message(host, msg, status);

    if (ret >= 0)
    {
        m_state = REPLCL_JOIN_SENT;
    }

    return ret;
}

int64_t
replicant :: wait_for_bootstrap(replicant_returncode* status)
{
    while (true)
    {
        po6::net::location from;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee->recv(&from, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
                return -1;
            BUSYBEE_ERROR_DISCONNECT(NEEDBOOTSTRAP, DISCONNECT);
            BUSYBEE_ERROR_DISCONNECT(NEEDBOOTSTRAP, CONNECTFAIL);
            BUSYBEE_ERROR_DISCONNECT(BUFFERFULL, BUFFERFULL);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, SHUTDOWN);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, QUEUED);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, POLLFAILED);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, ADDFDFAIL);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, EXTERNAL);
            default:
                REPLSETERROR(REPLICANT_INTERNALERROR, "BusyBee returned unknown error");
                reset_to_disconnected();
                return -1;
        }

        e::buffer::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        replicant_network_msgtype mt;
        up = up >> mt;

        if (up.error())
        {
            REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack failed");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        switch (mt)
        {
            case REPLNET_INFORM:
                break;
            case REPLNET_COMMAND_RESPONSE:
                // throw it away because we aren't yet connected, so it's a
                // remnant from a previous time.
                continue;
            REPL_UNEXPECTED_DISCONNECT(NOP);
            REPL_UNEXPECTED_DISCONNECT(JOIN);
            REPL_UNEXPECTED_DISCONNECT(BECOME_SPARE);
            REPL_UNEXPECTED_DISCONNECT(BECOME_STANDBY);
            REPL_UNEXPECTED_DISCONNECT(BECOME_MEMBER);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_PROPOSE);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_ACCEPT);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_REJECT);
            REPL_UNEXPECTED_DISCONNECT(IDENTIFY);
            REPL_UNEXPECTED_DISCONNECT(IDENTIFIED);
            REPL_UNEXPECTED_DISCONNECT(CLIENT_LIST);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_SUBMIT);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_ISSUE);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_ACK);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_RESEND);
            REPL_UNEXPECTED_DISCONNECT(REQ_STATE);
            REPL_UNEXPECTED_DISCONNECT(RESP_STATE);
            REPL_UNEXPECTED_DISCONNECT(SNAPSHOT);
            REPL_UNEXPECTED_DISCONNECT(HEALED);
            default:
                REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "invalid message type");
                m_last_error_host = from;
                reset_to_disconnected();
                return -1;
        }

        configuration newconfig;
        up = up >> newconfig;

        if (up.error())
        {
            REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack of INFORM failed");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        if (m_config->version() < newconfig.version())
        {
            *m_config = newconfig;
        }

        m_state = REPLCL_BOOTSTRAPPED;
        return 0;
    }
}

int64_t
replicant :: send_token_registration(replicant_returncode* status)
{
    po6::net::location host(m_config->head().receiver());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_SUBMIT)
              + sizeof(uint64_t) * 4;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint64_t nonce = m_nonce;
    ++m_nonce;
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_SUBMIT
                                      << m_token << nonce
                                      << uint64_t(0) << uint64_t(OBJECT_CLIENTS);
    int64_t ret = send_preconnect_message(host, msg, status);

    if (ret >= 0)
    {
        m_state = REPLCL_REGISTER_SENT;
    }

    return ret;
}

int64_t
replicant :: wait_for_token_registration(replicant_returncode* status)
{
    while (true)
    {
        po6::net::location from;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee->recv(&from, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
                return -1;
            BUSYBEE_ERROR_DISCONNECT(NEEDBOOTSTRAP, DISCONNECT);
            BUSYBEE_ERROR_DISCONNECT(NEEDBOOTSTRAP, CONNECTFAIL);
            BUSYBEE_ERROR_DISCONNECT(BUFFERFULL, BUFFERFULL);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, SHUTDOWN);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, QUEUED);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, POLLFAILED);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, ADDFDFAIL);
            BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, EXTERNAL);
            default:
                REPLSETERROR(REPLICANT_INTERNALERROR, "BusyBee returned unknown error");
                reset_to_disconnected();
                return -1;
        }

        e::buffer::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        replicant_network_msgtype mt;
        up = up >> mt;

        if (up.error())
        {
            REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack failed");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        switch (mt)
        {
            case REPLNET_COMMAND_RESPONSE:
                break;
            REPL_UNEXPECTED_DISCONNECT(NOP);
            REPL_UNEXPECTED_DISCONNECT(JOIN);
            REPL_UNEXPECTED_DISCONNECT(INFORM);
            REPL_UNEXPECTED_DISCONNECT(BECOME_SPARE);
            REPL_UNEXPECTED_DISCONNECT(BECOME_STANDBY);
            REPL_UNEXPECTED_DISCONNECT(BECOME_MEMBER);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_PROPOSE);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_ACCEPT);
            REPL_UNEXPECTED_DISCONNECT(CONFIG_REJECT);
            REPL_UNEXPECTED_DISCONNECT(IDENTIFY);
            REPL_UNEXPECTED_DISCONNECT(IDENTIFIED);
            REPL_UNEXPECTED_DISCONNECT(CLIENT_LIST);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_SUBMIT);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_ISSUE);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_ACK);
            REPL_UNEXPECTED_DISCONNECT(COMMAND_RESEND);
            REPL_UNEXPECTED_DISCONNECT(REQ_STATE);
            REPL_UNEXPECTED_DISCONNECT(RESP_STATE);
            REPL_UNEXPECTED_DISCONNECT(SNAPSHOT);
            REPL_UNEXPECTED_DISCONNECT(HEALED);
            default:
                REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "invalid message type");
                m_last_error_host = from;
                reset_to_disconnected();
                return -1;
        }

        uint8_t flags;
        up = up >> flags;

        if (up.error())
        {
            REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack of BOOTSTRAP failed");
            m_last_error_host = from;
            reset_to_disconnected();
            return -1;
        }

        if (flags & 1)
        {
            m_state = REPLCL_PARTIALLY_CONNECTED;
            return 0;
        }
        else
        {
            REPLSETERROR(REPLICANT_NEEDBOOTSTRAP, "could not register with the server");
            reset_to_disconnected();
            return -1;
        }
    }
}

int64_t
replicant :: send_identify_messages(replicant_returncode* status)
{
    const chain_node* members = m_config->members_begin();
    size_t members_sz = m_config->members_end() - m_config->members_begin();
    assert(members_sz < 256);

    for (size_t i = 0; i < members_sz; ++i)
    {
        uint8_t byte = i / 8;
        uint8_t bit = 1 << (i & 7U);

        if (!(m_identify_sent[byte] & bit))
        {
            po6::net::location host(members[i].receiver());
            size_t sz = BUSYBEE_HEADER_SIZE + pack_size(REPLNET_IDENTIFY) + sizeof(uint64_t);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_IDENTIFY << m_token;

            switch (m_busybee->send(host, msg))
            {
                case BUSYBEE_SUCCESS:
                case BUSYBEE_QUEUED:
                    break;
                case BUSYBEE_TIMEOUT:
                    REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
                    return -1;
                BUSYBEE_ERROR_CONTINUE(NEEDBOOTSTRAP, DISCONNECT);
                BUSYBEE_ERROR_CONTINUE(NEEDBOOTSTRAP, CONNECTFAIL);
                BUSYBEE_ERROR_CONTINUE(BUFFERFULL, BUFFERFULL);
                BUSYBEE_ERROR(INTERNALERROR, SHUTDOWN);
                BUSYBEE_ERROR(INTERNALERROR, POLLFAILED);
                BUSYBEE_ERROR(INTERNALERROR, ADDFDFAIL);
                BUSYBEE_ERROR(INTERNALERROR, EXTERNAL);
                default:
                    REPLSETERROR(REPLICANT_INTERNALERROR, "BusyBee returned unknown error");
                    m_last_error_host = host;
                    return -1;
            }

            m_identify_sent[byte] |= bit;
        }
    }

    return 0;
}

int64_t
replicant :: handle_inform(const po6::net::location& from,
                           std::auto_ptr<e::buffer>,
                           e::buffer::unpacker up,
                           replicant_returncode* status)
{
    configuration newconfig;
    up = up >> newconfig;

    if (up.error())
    {
        REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack of INFORM failed");
        m_last_error_host = from;
        return -1;
    }

    if (m_config->version() < newconfig.version())
    {
        uint8_t identify_sent[32];
        uint8_t identify_recv[32];

        // Clear them
        for (size_t i = 0; i < 32; ++i)
        {
            identify_sent[i] = 0;
            identify_recv[i] = 0;
        }

        const chain_node* members = newconfig.members_begin();
        size_t members_sz = newconfig.members_end() - newconfig.members_begin();
        assert(members_sz < 256);

        // Keep track of sent/recv bits for known chain_nodes
        for (size_t i = 0; i < members_sz; ++i)
        {
            uint8_t byte = i / 8;
            uint8_t bit = 1 << (i & 7U);

            const chain_node* oldmembers = m_config->members_begin();
            size_t oldmembers_sz = m_config->members_end() - m_config->members_begin();
            assert(oldmembers_sz < 256);

            for (size_t j = 0; j < oldmembers_sz; ++j)
            {
                if (oldmembers[j] == members[i])
                {
                    uint8_t oldbyte = j / 8;
                    uint8_t oldbit = 1 << (j & 7U);

                    if ((m_identify_sent[oldbyte] & oldbit))
                    {
                        identify_sent[byte] |= bit;
                    }
                    if ((m_identify_recv[oldbyte] & oldbit))
                    {
                        identify_recv[byte] |= bit;
                    }
                }
            }
        }

        // Move it all
        *m_config = newconfig;

        for (size_t i = 0; i < 32; ++i)
        {
            m_identify_sent[i] = identify_sent[i];
            m_identify_recv[i] = identify_recv[i];
        }

        // Just bump it
        if (m_state == REPLCL_CONNECTED)
        {
            m_state = REPLCL_PARTIALLY_CONNECTED;
        }
    }

    return 0;
}

int64_t
replicant :: handle_identified(const po6::net::location& from,
                               std::auto_ptr<e::buffer>,
                               e::buffer::unpacker,
                               replicant_returncode*)
{
    const chain_node* members = m_config->members_begin();
    size_t members_sz = m_config->members_end() - m_config->members_begin();
    assert(members_sz < 256);
    size_t connected = 0;

    for (size_t i = 0; i < members_sz; ++i)
    {
        uint8_t byte = i / 8;
        uint8_t bit = 1 << (i & 7U);

        if (members[i].receiver() == from && (m_identify_sent[byte] & bit))
        {
            m_identify_recv[byte] |= bit;
        }

        if ((m_identify_recv[byte] & bit))
        {
            ++connected;
        }
    }

    if (connected == members_sz)
    {
        m_state = REPLCL_CONNECTED;
    }

    return 0;
}

int64_t
replicant :: handle_disconnect(const po6::net::location& from,
                               replicant_returncode* status)
{
    // Mark the server as not identified
    const chain_node* members = m_config->members_begin();
    size_t members_sz = m_config->members_end() - m_config->members_begin();
    assert(members_sz < 256);
    size_t connected = 0;
    bool removed = false;

    for (size_t i = 0; i < members_sz; ++i)
    {
        uint8_t byte = i / 8;
        uint8_t bit = 1 << (i & 7U);

        if (members[i].receiver() == from)
        {
            m_identify_sent[byte] &= ~bit;
            m_identify_recv[byte] &= ~bit;
            removed = true;
        }

        if ((m_identify_recv[byte] & bit))
        {
            ++connected;
        }
    }

    if (removed && m_state == REPLCL_CONNECTED)
    {
        m_state = REPLCL_PARTIALLY_CONNECTED;
    }

    if (connected == 0)
    {
        REPLSETERROR(REPLICANT_NEEDBOOTSTRAP, "lost connection with all servers");
        reset_to_disconnected();
        return 0;
    }

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
replicant :: send_preconnect_message(const po6::net::location& host,
                                     std::auto_ptr<e::buffer> msg,
                                     replicant_returncode* status)
{
    switch (m_busybee->send(host, msg))
    {
        case BUSYBEE_SUCCESS:
        case BUSYBEE_QUEUED:
            return 0;
        case BUSYBEE_TIMEOUT:
            REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
            return -1;
        BUSYBEE_ERROR_DISCONNECT(NEEDBOOTSTRAP, DISCONNECT);
        BUSYBEE_ERROR_DISCONNECT(NEEDBOOTSTRAP, CONNECTFAIL);
        BUSYBEE_ERROR_DISCONNECT(BUFFERFULL, BUFFERFULL);
        BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, SHUTDOWN);
        BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, POLLFAILED);
        BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, ADDFDFAIL);
        BUSYBEE_ERROR_DISCONNECT(INTERNALERROR, EXTERNAL);
        default:
            REPLSETERROR(REPLICANT_INTERNALERROR, "BusyBee returned unknown error");
            m_last_error_host = host;
            reset_to_disconnected();
            return -1;
    }
}

int64_t
replicant :: send_to_any_chain_member(e::intrusive_ptr<command> cmd,
                                      replicant_returncode* status)
{
    int64_t ret = -1;
    bool sent = false;
    po6::net::location host;

    for (const chain_node* n = m_config->members_begin();
            !sent && n != m_config->members_end(); ++n)
    {
        host = n->receiver();
        std::auto_ptr<e::buffer> msg(cmd->request()->copy());
        busybee_returncode rc = m_busybee->send(host, msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
            case BUSYBEE_QUEUED:
                sent = true;
                break;
            case BUSYBEE_DISCONNECT:
            case BUSYBEE_CONNECTFAIL:
                if ((ret = handle_disconnect(host, status)) < 0)
                {
                    return ret;
                }
                continue;
            case BUSYBEE_TIMEOUT:
                REPLSETERROR(REPLICANT_TIMEOUT, "operation timed out");
                return -1;
            BUSYBEE_ERROR_CONTINUE(BUFFERFULL, BUFFERFULL);
            BUSYBEE_ERROR_CONTINUE(INTERNALERROR, SHUTDOWN);
            BUSYBEE_ERROR_CONTINUE(INTERNALERROR, POLLFAILED);
            BUSYBEE_ERROR_CONTINUE(INTERNALERROR, ADDFDFAIL);
            BUSYBEE_ERROR_CONTINUE(INTERNALERROR, EXTERNAL);
            default:
                REPLSETERROR(REPLICANT_INTERNALERROR, "BusyBee returned unknown error");
                continue;
        }
    }

    if (sent)
    {
        cmd->set_sent_to(host);
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
replicant :: handle_command_response(const po6::net::location& from,
                                     std::auto_ptr<e::buffer> msg,
                                     e::buffer::unpacker up,
                                     replicant_returncode* status)
{
    // Parse the command response
    uint64_t nonce;
    uint8_t flag;
    up = up >> nonce >> flag;

    if (up.error())
    {
        REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    // Find the command
    command_map::iterator it = m_commands.find(nonce);

    if (it == m_commands.end())
    {
        REPLSETERROR(REPLICANT_INTERNALERROR, "this should not happen");
        return 0;
    }

    // Pass the response to the command
    e::intrusive_ptr<command> c = it->second;
    REPLSETSUCCESS;

    if (flag)
    {
        c->succeed(msg, up.as_slice(), REPLICANT_SUCCESS);
    }
    else
    {
        c->fail(REPLICANT_BADCALL);
        m_last_error_desc = "there is no such object/function pair";
        m_last_error_file = __FILE__;
        m_last_error_line = __LINE__;
    }

    c->set_last_error_desc(m_last_error_desc);
    c->set_last_error_file(m_last_error_file);
    c->set_last_error_line(m_last_error_line);
    m_commands.erase(it);
    m_complete.insert(std::make_pair(c->nonce(), c));
    return 0;
}

int64_t
replicant :: handle_command_resend(const po6::net::location& from,
                                   std::auto_ptr<e::buffer>,
                                   e::buffer::unpacker up,
                                   replicant_returncode* status)
{
    // Parse the command response
    uint64_t client;
    uint64_t nonce;
    up = up >> client >> nonce;

    if (up.error())
    {
        REPLSETERROR(REPLICANT_MISBEHAVINGSERVER, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    // Find the command
    command_map::iterator it = m_commands.find(nonce);

    if (it == m_commands.end())
    {
        REPLSETERROR(REPLICANT_INTERNALERROR, "this should not happen");
        return 0;
    }

    if (client != m_token)
    {
        REPLSETERROR(REPLICANT_INTERNALERROR, "this should not happen");
        return 0;
    }

    // Compute the changed parts of the request
    nonce = m_nonce;
    ++m_nonce;
    uint64_t garbage = compute_garbage();

    // Resend the request
    e::intrusive_ptr<command> c = it->second;
    c->request()->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_SUBMIT << m_token << nonce << garbage;
    c->set_nonce(nonce);
    m_commands.erase(it);
    send_to_any_chain_member(c, status);
    return 0;
}

uint64_t
replicant :: generate_token()
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

uint64_t
replicant :: compute_garbage()
{
    uint64_t garbage = m_nonce;

    if (!m_commands.empty())
    {
        garbage = std::min(garbage, m_commands.begin()->first);
    }

    if (!m_resend.empty())
    {
        garbage = std::min(garbage, m_resend.begin()->first);
    }

    return garbage;
}

void
replicant :: reset_to_disconnected()
{
    m_busybee.reset(new busybee_st());
    m_config.reset(new configuration());
    m_token = 0;
    // leave m_nonce
    m_state = REPLCL_DISCONNECTED;

    while (!m_commands.empty())
    {
        e::intrusive_ptr<command> cmd = m_commands.begin()->second;
        cmd->fail(REPLICANT_NEEDBOOTSTRAP);
        cmd->set_last_error_desc(m_last_error_desc);
        cmd->set_last_error_file(m_last_error_file);
        cmd->set_last_error_line(m_last_error_line);
        m_complete.insert(*m_commands.begin());
        m_commands.erase(m_commands.begin());
    }

    while (!m_resend.empty())
    {
        e::intrusive_ptr<command> cmd = m_commands.begin()->second;
        cmd->fail(REPLICANT_NEEDBOOTSTRAP);
        cmd->set_last_error_desc(m_last_error_desc);
        cmd->set_last_error_file(m_last_error_file);
        cmd->set_last_error_line(m_last_error_line);
        m_complete.insert(*m_resend.begin());
        m_resend.erase(m_resend.begin());
    }

    // clear the bits for identify
    for (size_t i = 0; i < 32; ++i)
    {
        m_identify_sent[i] = 0;
        m_identify_recv[i] = 0;
    }

    // Don't touch the error items
}

std::ostream&
operator << (std::ostream& lhs, replicant_returncode rhs)
{
    switch (rhs)
    {
        stringify(REPLICANT_SUCCESS);
        stringify(REPLICANT_BADCALL);
        stringify(REPLICANT_TIMEOUT);
        stringify(REPLICANT_NEEDBOOTSTRAP);
        stringify(REPLICANT_BUFFERFULL);
        stringify(REPLICANT_MISBEHAVINGSERVER);
        stringify(REPLICANT_INTERNALERROR);
        stringify(REPLICANT_NONEPENDING);
        stringify(REPLICANT_GARBAGE);
        default:
            lhs << "unknown returncode (" << static_cast<unsigned int>(rhs) << ")";
    }

    return lhs;
}
