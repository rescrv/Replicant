// Copyright (c) 2014, Robert Escriva
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

// e
#include <e/strescape.h>

// Replicant
#include "client/replicant.h"
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    bool _connect_str = false;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.add("Connect to a cluster:", conn.parser());
    ap.arg().name('c', "connect-string")
            .description("print a valid connection-string for the cluster instead of a server list")
            .set_true(&_connect_str);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command takes no arguments\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    try
    {
        replicant_client r(conn.host(), conn.port());
        int rc = 0;
        replicant_returncode status = REPLICANT_GARBAGE;
        std::string servers;

        if (_connect_str)
        {
            rc = r.connect_str(&status, &servers);
            servers += "\n";
        }
        else
        {
            rc = r.list_servers(&status, &servers);
        }

        if (rc < 0)
        {
            std::cerr << "could not list servers: " << r.last_error().msg()
                      << " (" << status << ")" << std::endl;
            return EXIT_FAILURE;
        }

        std::cout << servers << std::flush;
        return EXIT_SUCCESS;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
