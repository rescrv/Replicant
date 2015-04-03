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

// STL
#include <memory>

// BusyBee
#include <busybee_constants.h>
#include <busybee_single.h>

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

replicant_returncode
bootstrap :: bootstrap_one(const po6::net::hostname& hn,
                           configuration* config, e::error* err)
{
    try
    {
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(REPLNET_BOOTSTRAP);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BOOTSTRAP;
        busybee_single bbs(hn);

        switch (bbs.send(msg))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                err->set_loc(__FILE__, __LINE__);
                err->set_msg() << "timed out connecting to " << hn;
                return REPLICANT_TIMEOUT;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
            case BUSYBEE_INTERRUPTED:
                err->set_loc(__FILE__, __LINE__);
                err->set_msg() << "communication error with " << hn;
                return REPLICANT_COMM_FAILED;
            default:
                abort();
        }

        bbs.set_timeout(250);

        switch (bbs.recv(&msg))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                err->set_loc(__FILE__, __LINE__);
                err->set_msg() << "timed out connecting to " << hn;
                return REPLICANT_TIMEOUT;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
            case BUSYBEE_INTERRUPTED:
                err->set_loc(__FILE__, __LINE__);
                err->set_msg() << "communication error with " << hn;
                return REPLICANT_COMM_FAILED;
            default:
                abort();
        }

        network_msgtype mt = REPLNET_NOP;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up >> mt >> *config;

        if (up.error() || mt != REPLNET_BOOTSTRAP || !config->validate())
        {
            err->set_loc(__FILE__, __LINE__);
            err->set_msg() << "received a malformed bootstrap message from " << hn;
            return REPLICANT_COMM_FAILED;
        }

        return REPLICANT_SUCCESS;
    }
    catch (po6::error& e)
    {
        err->set_loc(__FILE__, __LINE__);
        err->set_msg() << "could not connect to " << hn << ": " << e::error::strerror(errno);
        return REPLICANT_COMM_FAILED;
    }
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
    , m_valid(true)
{
}

bootstrap :: bootstrap(const char* host, uint16_t port)
    : m_hosts()
    , m_valid(true)
{
    m_hosts.push_back(po6::net::hostname(host, port));
}

bootstrap :: bootstrap(const char* cs)
    : m_hosts()
    , m_valid(true)
{
    m_valid = parse_hosts(cs, &m_hosts);
}

bootstrap :: bootstrap(const char* host, uint16_t port, const char* cs)
    : m_hosts()
    , m_valid(true)
{
    m_valid = parse_hosts(cs, &m_hosts);
    m_hosts.push_back(po6::net::hostname(host, port));
}

bootstrap :: bootstrap(const std::vector<po6::net::hostname>& hosts)
    : m_hosts(hosts)
    , m_valid(true)
{
}

bootstrap :: ~bootstrap() throw ()
{
}

replicant_returncode
bootstrap :: do_it(configuration* config, e::error* err) const
{
    if (!m_valid)
    {
        err->set_loc(__FILE__, __LINE__);
        err->set_msg() << "invalid bootstrap connection string";
        return REPLICANT_COMM_FAILED;
    }

    replicant_returncode rc = REPLICANT_COMM_FAILED;
    err->set_loc(__FILE__, __LINE__);
    err->set_msg() << "no hosts to bootstrap from";

    for (size_t i = 0; i < m_hosts.size(); ++i)
    {
        rc = bootstrap_one(m_hosts[i], config, err);

        if (rc == REPLICANT_SUCCESS)
        {
            return REPLICANT_SUCCESS;
        }
    }

    // return the error corresponding to the last failure
    return rc;
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

std::ostream&
replicant :: operator << (std::ostream& lhs, const bootstrap& rhs)
{
    return lhs << rhs.conn_str();
}
