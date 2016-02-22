// Copyright (c) 2016, Robert Escriva
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
#include <e/guard.h>

// Replicant
#include <replicant.h>
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    long servers = 5;
    long timeout = 10;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.arg().name('s', "servers")
            .description("wait for N servers to join the cluster (default: 5)")
            .metavar("N").as_long(&servers);
    ap.arg().name('t', "timeout")
            .description("wait at most S seconds (default: 10)")
            .metavar("S").as_long(&timeout);
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command takes no positional arguments\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    replicant_client* r = replicant_client_create(conn.host(), conn.port());

    if (!r)
    {
        std::cerr << "could not create replicant client" << std::endl;
        return EXIT_FAILURE;
    }

    e::guard g_r = e::makeguard(replicant_client_destroy, r);
    replicant_returncode re = REPLICANT_GARBAGE;
    int ret = replicant_client_availability_check(r, servers, timeout, &re);

    if (ret < 0)
    {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
