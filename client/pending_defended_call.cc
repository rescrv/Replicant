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

// BusyBee
#include <busybee_constants.h>

// Replicant
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "client/client.h"
#include "client/pending_defended_call.h"

using replicant::pending_defended_call;

pending_defended_call :: pending_defended_call(int64_t id,
                                               const char* object,
                                               const char* enter_func,
                                               const char* enter_input, size_t enter_input_sz,
                                               const char* exit_func,
                                               const char* exit_input, size_t exit_input_sz,
                                               replicant_returncode* st)
    : pending_robust(id, st)
    , m_object(object)
    , m_enter_func(enter_func)
    , m_enter_input(enter_input, enter_input_sz)
    , m_exit_func(exit_func)
    , m_exit_input(exit_input, exit_input_sz)
{
}

pending_defended_call :: ~pending_defended_call() throw ()
{
}

std::auto_ptr<e::buffer>
pending_defended_call :: request(uint64_t nonce)
{
    std::string input;
    e::packer pa(&input);
    pa = pa << e::slice(m_object)
            << e::slice(m_enter_func)
            << e::slice(m_enter_input)
            << e::slice(m_exit_func)
            << e::slice(m_exit_input);

    assert(command_nonce() > 0);
    e::slice obj("replicant");
    e::slice func("defended");
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_CALL_ROBUST)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + pack_size(obj)
                    + pack_size(func)
                    + pack_size(e::slice(input));
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CALL_ROBUST << nonce << command_nonce() << min_slot() << obj << func << e::slice(input);
    return msg;
}

bool
pending_defended_call :: resend_on_failure()
{
    return true;
}

void
pending_defended_call :: handle_response(client* cl, std::auto_ptr<e::buffer>, e::unpacker up)
{
    replicant_returncode st;
    e::slice output;
    up = up >> st >> output;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "received bad call response";
    }
    else if (st == REPLICANT_SUCCESS)
    {
        this->success();
        cl->add_defense(command_nonce());
    }
    else
    {
        this->set_status(st);
        this->error(__FILE__, __LINE__) << output.str();
    }
}
