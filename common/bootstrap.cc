// Copyright (c) 2012-2015, Robert Escriva
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
#include <poll.h>

// STL
#include <memory>

// po6
#include <po6/time.h>

// Replicant
#include "common/bootstrap.h"
#include "common/configuration.h"
#include "common/network_msgtype.h"

using replicant::bootstrap;

bool
bootstrap :: parse_hosts(const char* conn_str,
                         std::vector<po6::net::hostname>* hosts)
{
    const size_t conn_str_sz = strlen(conn_str);
    std::vector<char> cs(conn_str, conn_str + conn_str_sz + 1);
    char* ptr = &cs[0];
    char* const end = ptr + conn_str_sz;

    while (ptr < end)
    {
        char* eoh = strchr(ptr, ',');
        eoh = eoh ? eoh : end;
        *eoh = '\0';
        char* colon = strrchr(ptr, ':');

        if (colon == NULL)
        {
            hosts->push_back(po6::net::hostname(ptr, 1982));
            ptr = eoh + 1;
            continue;
        }

        char* tmp = NULL;
        errno = 0;
        unsigned long port = strtoul(colon + 1, &tmp, 10);

        if (errno != 0)
        {
            return false;
        }

        std::string host;

        if (*ptr == '[' && colon > ptr && *(colon - 1) == ']')
        {
            host.assign(ptr + 1, colon - 1);
        }
        else
        {
            host.assign(ptr, colon);
        }

        hosts->push_back(po6::net::hostname(host.c_str(), port));
        ptr = eoh + 1;
    }

    return true;
}

std::string
bootstrap :: conn_str(const po6::net::hostname* hns, size_t hns_sz)
{
    std::ostringstream ostr;

    for (size_t i = 0; i < hns_sz; ++i)
    {
        if (i > 0)
        {
            ostr << ",";
        }

        ostr << hns[i];
    }

    return ostr.str();
}

bootstrap :: bootstrap()
    : m_hosts()
    , m_conns()
    , m_valid(true)
{
}

bootstrap :: bootstrap(const char* host, uint16_t port)
    : m_hosts()
    , m_conns()
    , m_valid(true)
{
    m_hosts.push_back(po6::net::hostname(host, port));
}

bootstrap :: bootstrap(const char* cs)
    : m_hosts()
    , m_conns()
    , m_valid(true)
{
    m_valid = parse_hosts(cs, &m_hosts);
}

bootstrap :: bootstrap(const char* host, uint16_t port, const char* cs)
    : m_hosts()
    , m_conns()
    , m_valid(true)
{
    m_valid = parse_hosts(cs, &m_hosts);
    m_hosts.push_back(po6::net::hostname(host, port));
}

bootstrap :: bootstrap(const std::vector<po6::net::hostname>& h)
    : m_hosts(h)
    , m_conns()
    , m_valid(true)
{
}

bootstrap :: bootstrap(const bootstrap& other)
    : m_hosts(other.m_hosts)
    , m_conns()
    , m_valid(other.m_valid)
{
}

bootstrap :: ~bootstrap() throw ()
{
}

bool
bootstrap :: valid() const
{
    if (!m_valid)
    {
        return false;
    }

    for (size_t i = 0; i < m_hosts.size(); ++i)
    {
        if (m_hosts[i].port <= 0 || m_hosts[i].port >= (1 << 16))
        {
            return false;
        }
    }

    return true;
}

replicant_returncode
bootstrap :: do_it(int timeout, configuration* config, e::error* err)
{
    if (!m_valid)
    {
        err->set_loc(__FILE__, __LINE__);
        err->set_msg() << "invalid bootstrap connection string";
        return REPLICANT_COMM_FAILED;
    }

    if (m_hosts.empty())
    {
        err->set_loc(__FILE__, __LINE__);
        err->set_msg() << "no hosts to bootstrap from";
        return REPLICANT_COMM_FAILED;
    }

    m_conns.resize(m_hosts.size());
    replicant_returncode rc = REPLICANT_TIMEOUT;
    err->set_loc(__FILE__, __LINE__);
    err->set_msg() << "timed out connecting to the cluster";
    std::vector<pollfd> pfds;
    pfds.resize(m_conns.size());

    for (size_t i = 0; i < m_hosts.size(); ++i)
    {
        if (!m_conns[i].get())
        {
            m_conns[i].reset(busybee_single::create(m_hosts[i]));
        }
    }

    while (true)
    {
        for (size_t i = 0; i < m_conns.size(); ++i)
        {
            rc = send_bootstrap(i, err);
            pfds[i].fd = m_conns[i]->poll_fd();
            pfds[i].events = POLLIN|POLLOUT|POLLHUP|POLLERR;
            pfds[i].revents = 0;
        }

        rc = REPLICANT_TIMEOUT;
        err->set_loc(__FILE__, __LINE__);
        err->set_msg() << "timed out connecting to the cluster";
        int ret = poll(&pfds[0], pfds.size(), timeout);

        if (ret < 0)
        {
            int e = errno;
            err->set_loc(__FILE__, __LINE__);
            err->set_msg() << "poll failed: " << po6::strerror(e) << " [" << po6::strerrno(e) << "]";
            errno = e;
            return REPLICANT_SEE_ERRNO;
        }
        else if (ret == 0)
        {
            return rc;
        }

        for (size_t i = 0; i < m_conns.size(); ++i)
        {
            if (pfds[i].fd < 0 || pfds[i].revents == 0)
            {
                continue;
            }

            std::auto_ptr<e::buffer> msg;

            switch (m_conns[i]->recv(timeout, &msg))
            {
                case BUSYBEE_SUCCESS:
                    break;
                case BUSYBEE_TIMEOUT:
                    break;
                case BUSYBEE_SHUTDOWN:
                case BUSYBEE_DISRUPTED:
                case BUSYBEE_EXTERNAL:
                case BUSYBEE_INTERRUPTED:
                case BUSYBEE_SEE_ERRNO:
                    err->set_loc(__FILE__, __LINE__);
                    err->set_msg() << "communication error with " << m_hosts[i];
                    rc = REPLICANT_COMM_FAILED;
                    continue;
                default:
                    abort();
            }

            if (!msg.get())
            {
                continue;
            }

            network_msgtype mt = REPLNET_NOP;
            e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
            up >> mt >> *config;

            if (up.error() || mt != REPLNET_BOOTSTRAP || !config->validate())
            {
                err->set_loc(__FILE__, __LINE__);
                err->set_msg() << "received a malformed bootstrap message from " << m_hosts[i];
                rc = REPLICANT_COMM_FAILED;
                continue;
            }

            return REPLICANT_SUCCESS;
        }
    }
}

std::string
bootstrap :: conn_str() const
{
    return conn_str(&m_hosts[0], m_hosts.size());
}

bootstrap&
bootstrap :: operator = (const bootstrap& rhs)
{
    if (this != &rhs)
    {
        m_hosts = rhs.m_hosts;
        m_valid = rhs.m_valid;
    }

    return *this;
}

replicant_returncode
bootstrap :: send_bootstrap(size_t i, e::error* err)
{
    replicant_returncode rc = REPLICANT_SUCCESS;
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_BOOTSTRAP);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BOOTSTRAP;

    switch (m_conns[i]->send(msg))
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_TIMEOUT:
            err->set_loc(__FILE__, __LINE__);
            err->set_msg() << "timed out connecting to " << m_hosts[i];
            rc = REPLICANT_TIMEOUT;
            break;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_DISRUPTED:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        case BUSYBEE_SEE_ERRNO:
            err->set_loc(__FILE__, __LINE__);
            err->set_msg() << "communication error with " << m_hosts[i];
            rc = REPLICANT_COMM_FAILED;
            break;
        default:
            abort();
    }

    return rc;
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const bootstrap& rhs)
{
    return lhs << rhs.conn_str();
}
