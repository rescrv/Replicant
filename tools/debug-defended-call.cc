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

// e
#include <e/strescape.h>

// Replicant
#include <replicant.h>
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    const char* obj = "log";
    const char* enter_func = "log";
    const char* enter_input = "enter";
    const char* exit_func = "log";
    const char* exit_input = "exit";
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.arg().name('o', "object")
            .description("object that maintains the condition")
            .metavar("OBJ").as_string(&obj);
    ap.arg().long_name("entry-func")
            .description("function call on entry")
            .metavar("FUNC").as_string(&enter_func);
    ap.arg().long_name("entry-input")
            .description("input to entry function")
            .metavar("DATA").as_string(&enter_input);
    ap.arg().long_name("exit-func")
            .description("function call on exit")
            .metavar("FUNC").as_string(&exit_func);
    ap.arg().long_name("exit-input")
            .description("input to exit function")
            .metavar("DATA").as_string(&exit_input);
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
    replicant_returncode re = REPLICANT_GARBAGE;
    int64_t rid = replicant_client_defended_call(r, obj,
                                                 enter_func, enter_input, strlen(enter_input) + 1,
                                                 exit_func, exit_input, strlen(exit_input) + 1, &re);

    if (!cli_finish(r, rid, &re))
    {
        return EXIT_FAILURE;
    }

    while (true)
    {
        replicant_returncode le = REPLICANT_GARBAGE;
        int64_t lid = replicant_client_loop(r, -1, &le);

        if (lid < 0 && le == REPLICANT_TIMEOUT)
        {
            continue;
        }
        else if (lid < 0 && le == REPLICANT_INTERRUPTED)
        {
            continue;
        }
        else
        {
            cli_log_error(r, le);
        }
    }

    return EXIT_SUCCESS;
}
