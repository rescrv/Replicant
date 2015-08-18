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
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Replicant
#include "common/bootstrap.h"
#include "common/configuration.h"
#include "client/server_status.h"

int
replicant :: server_status(const char* host, uint16_t port, int timeout,
                           replicant_returncode* status,
                           char** human_readable)
{
    *status = REPLICANT_INTERNAL;
    *human_readable = NULL;
    bootstrap bs(host, port);

    if (!bs.valid())
    {
        *status = REPLICANT_COMM_FAILED;
        *human_readable = strdup("invalid host/port combination");

        if (!*human_readable)
        {
            *status = REPLICANT_SEE_ERRNO;
        }

        return -1;
    }

    configuration c;
    e::error e;
    *status = bs.do_it(timeout, &c, &e);

    if (*status != REPLICANT_SUCCESS)
    {
        *human_readable = strdup(e.msg());

        if (!*human_readable)
        {
            *status = REPLICANT_SEE_ERRNO;
        }

        return -1;
    }

    std::ostringstream ostr;
    ostr << "cluster: " << c.cluster().get() << " version " << c.version().get() << "\n"
         << "bootstrap: " << c.current_bootstrap().conn_str() << "\n";

    *human_readable = strdup(ostr.str().c_str());

    if (!*human_readable)
    {
        *status = REPLICANT_SEE_ERRNO;
    }

    return 0;
}
