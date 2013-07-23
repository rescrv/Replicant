// Copyright (c) 2013, Robert Escriva
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

#ifndef replicant_request_response_h_
#define replicant_request_response_h_

// Google Log
#include <glog/logging.h>

// BusyBee
#include <busybee_single.h>

namespace replicant
{

template<typename hostname>
bool
request_response(hostname hn,
                 int timeout,
                 std::auto_ptr<e::buffer> request,
                 const char* errmsg,
                 std::auto_ptr<e::buffer>* response)
{
    busybee_single bbs(hn);
    bbs.set_timeout(timeout);

    switch (bbs.send(request))
    {
#define BUSYBEE_CASES \
        case BUSYBEE_SUCCESS: \
            break; \
        case BUSYBEE_TIMEOUT: \
            LOG(ERROR) << "timeout while " << errmsg << hn; \
            return false; \
        case BUSYBEE_SHUTDOWN: \
        case BUSYBEE_POLLFAILED: \
        case BUSYBEE_DISRUPTED: \
        case BUSYBEE_ADDFDFAIL: \
        case BUSYBEE_EXTERNAL: \
        case BUSYBEE_INTERRUPTED: \
            PLOG(ERROR) << "communication failure while " << errmsg << hn; \
            return false; \
        default: \
            abort();
        BUSYBEE_CASES
    }

    switch (bbs.recv(response))
    {
        BUSYBEE_CASES
#undef BUSYBEE_CASES
    }

    return true;
}

} // namespace replicant

#endif // replicant_request_response_h_
