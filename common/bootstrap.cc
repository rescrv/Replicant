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
#include <e/time.h>

// STL
#include <memory>
#include <sstream>

// BusyBee
#include <busybee_constants.h>
#include <busybee_single.h>

// Replicant
#include "common/bootstrap.h"
#include "common/macros.h"
#include "common/network_msgtype.h"

using replicant::bootstrap_returncode;

namespace replicant
{

bootstrap_returncode
bootstrap_common(const po6::net::hostname& hn,
                 std::auto_ptr<e::buffer> req,
                 std::auto_ptr<e::buffer>* resp,
                 uint64_t* token,
                 po6::net::location* remote)
{
    try
    {
        busybee_single bbs(hn);

        switch (bbs.send(req))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                return BOOTSTRAP_TIMEOUT;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
            case BUSYBEE_INTERRUPTED:
                return BOOTSTRAP_COMM_FAIL;
            default:
                abort();
        }

        bbs.set_timeout(250);

        // Receive the inform message
        switch (bbs.recv(resp))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                return BOOTSTRAP_TIMEOUT;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
            case BUSYBEE_INTERRUPTED:
                return BOOTSTRAP_COMM_FAIL;
            default:
                abort();
        }

        *token = bbs.token();
        *remote = bbs.remote();
        return BOOTSTRAP_SUCCESS;
    }
    catch (po6::error& e)
    {
        errno = e;
        return BOOTSTRAP_SEE_ERRNO;
    }
}

} // namespace

#ifdef _MSC_VER
    typedef std::shared_ptr<e::buffer> ptr_t;
#else
    typedef std::auto_ptr<e::buffer> ptr_t;
#endif

bootstrap_returncode
replicant :: bootstrap(const po6::net::hostname& hn, configuration* config)
{
    ptr_t msg(e::buffer::create(BUSYBEE_HEADER_SIZE + pack_size(REPLNET_BOOTSTRAP)));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BOOTSTRAP;
    uint64_t token;
    po6::net::location remote;
    bootstrap_returncode rc = bootstrap_common(hn, msg, &msg, &token, &remote);

    if (rc != BOOTSTRAP_SUCCESS)
    {
        return rc;
    }

    replicant_network_msgtype mt = REPLNET_NOP;
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    up >> mt >> *config;

    if (up.error() || mt != REPLNET_INFORM || !config->validate())
    {
        return BOOTSTRAP_CORRUPT_INFORM;
    }

    if (!config->is_member(chain_node(token, remote)))
    {
        return BOOTSTRAP_NOT_CLUSTER_MEMBER;
    }

    return BOOTSTRAP_SUCCESS;
}

bootstrap_returncode
replicant :: bootstrap_identity(const po6::net::hostname& hn, chain_node* cn)
{
    ptr_t msg(e::buffer::create(BUSYBEE_HEADER_SIZE + pack_size(REPLNET_BOOTSTRAP)));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_SERVER_IDENTIFY;
    uint64_t token;
    po6::net::location remote;
    bootstrap_returncode rc = bootstrap_common(hn, msg, &msg, &token, &remote);

    if (rc != BOOTSTRAP_SUCCESS)
    {
        return rc;
    }

    replicant_network_msgtype mt = REPLNET_NOP;
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    up >> mt >> *cn;

    if (up.error() || mt != REPLNET_SERVER_IDENTITY)
    {
        return BOOTSTRAP_CORRUPT_INFORM;
    }

    return BOOTSTRAP_SUCCESS;
}

bootstrap_returncode
replicant :: bootstrap(const po6::net::hostname* hns, size_t hns_sz,
                       configuration* config)
{
    bootstrap_returncode rc;

    for (size_t i = 0; i < hns_sz; ++i)
    {
        rc = bootstrap(hns[i], config);

        if (rc == BOOTSTRAP_SUCCESS)
        {
            return BOOTSTRAP_SUCCESS;
        }
    }

    return rc;
}

bool
replicant :: bootstrap_parse_hosts(const char* connection_string,
                                   std::vector<po6::net::hostname>* _hosts)
{
    const size_t connection_string_sz = strlen(connection_string);
    std::vector<po6::net::hostname> hosts;
    std::vector<char> connstr(connection_string, connection_string + connection_string_sz + 1);
    char* ptr = &connstr[0];
    char* const end = ptr + connection_string_sz;

    while (ptr < end)
    {
        char* eoh = strchr(ptr, ',');
        eoh = eoh ? eoh : end;
        *eoh = '\0';
        char* colon = strrchr(ptr, ':');

        if (colon == NULL)
        {
            hosts.push_back(po6::net::hostname(ptr, 1982));
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

        hosts.push_back(po6::net::hostname(host.c_str(), port));
        ptr = eoh + 1;
    }

    for (size_t i = 0; i < hosts.size(); ++i)
    {
        _hosts->push_back(hosts[i]);
    }

    return true;
}

std::string
replicant :: bootstrap_hosts_to_string(const po6::net::hostname* hns, size_t hns_sz)
{
    std::ostringstream ostr;

    for (size_t i = 0; i < hns_sz; ++i)
    {
        ostr << hns[i];

        if (i + 1 < hns_sz)
        {
            ostr << ",";
        }
    }

    return ostr.str();
}

std::ostream&
replicant :: operator << (std::ostream& lhs, const bootstrap_returncode& rhs)
{
    switch (rhs)
    {
        STRINGIFY(BOOTSTRAP_SUCCESS);
        STRINGIFY(BOOTSTRAP_TIMEOUT);
        STRINGIFY(BOOTSTRAP_COMM_FAIL);
        STRINGIFY(BOOTSTRAP_SEE_ERRNO);
        STRINGIFY(BOOTSTRAP_CORRUPT_INFORM);
        STRINGIFY(BOOTSTRAP_NOT_CLUSTER_MEMBER);
        STRINGIFY(BOOTSTRAP_GARBAGE);
        default:
            lhs << "unknown bootstrap error";
    }

    return lhs;
}
