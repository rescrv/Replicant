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

#define __STDC_LIMIT_MACROS

// POSIX
#include <errno.h>
#include <fcntl.h>

// po6
#include <po6/errno.h>

// e
#include <e/error.h>

// Replicant
#include <replicant.h>
#include "common/atomic_io.h"
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    const char* output_cstr = NULL;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS] <object> <backup>");
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 2)
    {
        std::cerr << "command requires the object name and backup state\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    std::string output;

    if (output_cstr)
    {
        output = output_cstr;
    }
    else
    {
        output = ap.args()[0];
        output += ".backup";
    }

    std::string state;

    if (!replicant::atomic_read(AT_FDCWD, ap.args()[1], &state))
    {
        std::cerr << "could not read state: " << po6::strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        replicant_client* r = replicant_client_create(conn.host(), conn.port());
        replicant_returncode re = REPLICANT_GARBAGE;
        int64_t rid = replicant_client_restore_object(r, ap.args()[0], state.data(), state.size(), &re);

        if (!cli_finish(r, rid, &re))
        {
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
