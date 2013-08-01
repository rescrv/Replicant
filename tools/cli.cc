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
#include <e/strescape.h>

// Replicant
#include "client/replicant.h"
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    static const char* object = "echo";
    static const char* function = "echo";
    connect_opts conn;
    e::argparser obj;
    obj.arg().name('o', "object")
             .description("manipulate a specific object (default: \"echo\")")
             .metavar("object").as_string(&object);
    obj.arg().name('f', "function")
             .description("call a specific function (default: \"echo\")")
             .metavar("function").as_string(&function);
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.add("Connect to a cluster:", conn.parser());
    ap.add("Manipulate an object:", obj);

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

    try
    {
        replicant_client r(conn.host(), conn.port());
        std::string s;

        while (std::getline(std::cin, s))
        {
            replicant_returncode re = REPLICANT_GARBAGE;
            replicant_returncode le = REPLICANT_GARBAGE;
            int64_t rid = 0;
            int64_t lid = 0;
            const char* output;
            size_t output_sz;

            rid = r.send(object, function, s.c_str(), s.size() + 1,
                         &re, &output, &output_sz);

            if (rid < 0)
            {
                std::cerr << "could not send request: " << r.last_error_desc()
                          << " (" << re << ")" << std::endl;
                return EXIT_FAILURE;
            }

            lid = r.loop(-1, &le);

            if (lid < 0)
            {
                std::cerr << "could not loop: " << r.last_error_desc()
                          << " (" << le << ")" << std::endl;
                return EXIT_FAILURE;
            }

            if (rid != lid)
            {
                std::cerr << "could not process request: internal error" << std::endl;
                return EXIT_FAILURE;
            }

            if (re != REPLICANT_SUCCESS)
            {
                std::cerr << "could not process request: " << r.last_error_desc()
                          << " (" << re << ")" << std::endl;
                return EXIT_FAILURE;
            }

            std::string out(output, output_sz);
            std::cout << e::strescape(out) << std::endl;
            replicant_destroy_output(output, output_sz);
        }

        replicant_returncode e = r.disconnect();

        if (e != REPLICANT_SUCCESS)
        {
            std::cerr << "error disconnecting from cluster: " << r.last_error_desc()
                          << " (" << e << ")" << std::endl;
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }
    catch (po6::error& e)
    {
        std::cerr << "system error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
