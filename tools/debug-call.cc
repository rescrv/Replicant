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
    const char* obj = "replicant";
    const char* func = "nop";
    bool idempotent = false;
    bool robust = false;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.arg().name('o', "object")
            .description("object that maintains the condition")
            .metavar("OBJ").as_string(&obj);
    ap.arg().name('f', "func")
            .description("function call")
            .metavar("FUNC").as_string(&func);
    ap.arg().name('i', "idempotent")
            .description("use the idempotent method")
            .set_true(&idempotent);
    ap.arg().name('r', "robust")
            .description("use the robust method")
            .set_true(&robust);
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

    unsigned flags = 0;
    flags |= idempotent ? REPLICANT_CALL_IDEMPOTENT : 0;
    flags |= robust ? REPLICANT_CALL_ROBUST : 0;

    try
    {
        replicant_client* r = replicant_client_create(conn.host(), conn.port());
        std::string s;

        while (std::getline(std::cin, s))
        {
            replicant_returncode re = REPLICANT_GARBAGE;
            char* output = NULL;
            size_t output_sz = 0;
            int64_t rid = replicant_client_call(r, obj, func, s.data(), s.size(), flags, &re, &output, &output_sz);

            if (!cli_finish(r, rid, &re))
            {
                return EXIT_FAILURE;
            }

            std::cout << e::strescape(std::string(output, output_sz)) << std::endl;

            if (output)
            {
                free(output);
            }
        }

        return EXIT_SUCCESS;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
