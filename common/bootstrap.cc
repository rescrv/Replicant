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

// STL
#include <memory>

// BusyBee
#include <busybee_constants.h>
#include <busybee_single.h>

// Replicant
#include "common/bootstrap.h"
#include "common/network_msgtype.h"

using replicant::bootstrap_returncode;

bootstrap_returncode
replicant :: bootstrap(const po6::net::hostname& hn, configuration* config)
{
    try
    {
        std::auto_ptr<e::buffer> msg;
        busybee_single bbs(hn);
        // Send the bootstrap message
        msg.reset(e::buffer::create(BUSYBEE_HEADER_SIZE + pack_size(REPLNET_BOOTSTRAP)));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BOOTSTRAP;

        switch (bbs.send(msg))
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

        // Receive the inform message
        switch (bbs.recv(&msg))
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

        replicant_network_msgtype mt = REPLNET_NOP;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up >> mt >> *config;

        if (up.error() || mt != REPLNET_INFORM || !config->validate())
        {
            return BOOTSTRAP_CORRUPT_INFORM;
        }

        bool found = false;

        for (const chain_node* n = config->members_begin();
                !found && n != config->members_end(); ++n)
        {
            if (n->address == bbs.remote() && n->token == bbs.token())
            {
                found = true;
            }
        }

        if (!found)
        {
            return BOOTSTRAP_NOT_CLUSTER_MEMBER;
        }

        return BOOTSTRAP_SUCCESS;
    }
    catch (po6::error& e)
    {
        return BOOTSTRAP_SEE_ERRNO;
    }
}
