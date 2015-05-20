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
#include "client/pending_call_robust.h"

using replicant::pending_call_robust;

pending_call_robust :: pending_call_robust(int64_t id,
                                           const char* object,
                                           const char* func,
                                           const char* input, size_t input_sz,
                                           replicant_returncode* st,
                                           char** output, size_t* output_sz)
    : pending(id, st)
    , m_object(object)
    , m_func(func)
    , m_input(input, input_sz)
    , m_output(output)
    , m_output_sz(output_sz)
    , m_command_nonce(0)
    , m_min_slot(0)
{
    if (m_output)
    {
        *m_output = NULL;
    }

    if (m_output_sz)
    {
        *m_output_sz = 0;
    }
}

pending_call_robust :: ~pending_call_robust() throw ()
{
}

void
pending_call_robust :: set_params(uint64_t command_nonce, uint64_t min_slot)
{
    m_command_nonce = command_nonce;
    m_min_slot = min_slot;
}

std::auto_ptr<e::buffer>
pending_call_robust :: request(uint64_t nonce)
{
    assert(m_command_nonce > 0);
    e::slice obj(m_object);
    e::slice func(m_func);
    e::slice input(m_input);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_CALL_ROBUST)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + pack_size(obj)
                    + pack_size(func)
                    + pack_size(input);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CALL_ROBUST << nonce << m_command_nonce << m_min_slot << obj << func << input;
    return msg;
}

bool
pending_call_robust :: resend_on_failure()
{
    return true;
}

void
pending_call_robust :: handle_response(client*, std::auto_ptr<e::buffer>, e::unpacker up)
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

        if (output.size() && m_output)
        {
            *m_output = static_cast<char*>(malloc(output.size()));

            if (!*m_output)
            {
                this->set_status(REPLICANT_SEE_ERRNO);
                return;
            }

            if (m_output_sz)
            {
                *m_output_sz = output.size();
            }

            memmove(*m_output, output.data(), output.size());
        }
    }
    else
    {
        this->set_status(st);
        this->error(__FILE__, __LINE__) << output.str();
    }
}
