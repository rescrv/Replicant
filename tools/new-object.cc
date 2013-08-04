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

// Replicant
#include "client/replicant.h"
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS] <object-id> <library-path>");
    ap.add("Connect to a cluster:", conn.parser());

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

    if (ap.args_sz() != 2)
    {
        std::cerr << "please specify the library and object\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    try
    {
        replicant_client r(conn.host(), conn.port());
        replicant_returncode re = REPLICANT_GARBAGE;
        replicant_returncode le = REPLICANT_GARBAGE;
        int64_t rid = 0;
        int64_t lid = 0;
        const char* errmsg;
        size_t errmsg_sz;

        rid = r.new_object(ap.args()[0], ap.args()[1], &re, &errmsg, &errmsg_sz);

        if (rid < 0)
        {
            std::cerr << "could not create object: " << r.last_error().msg()
                      << " (" << re << ")" << std::endl;
            return EXIT_FAILURE;
        }

        lid = r.loop(-1, &le);

        if (lid < 0)
        {
            std::cerr << "could not create object: " << r.last_error().msg()
                      << " (" << le << ")" << std::endl;
            return EXIT_FAILURE;
        }

        if (rid != lid)
        {
            std::cerr << "could not create object: internal error" << std::endl;
            return EXIT_FAILURE;
        }

        if (re != REPLICANT_SUCCESS)
        {
            std::cerr << "could not create object: " << r.last_error().msg()
                      << " (" << re << ")" << std::endl;
            return EXIT_FAILURE;
        }

        replicant_returncode e = r.disconnect();

        if (e != REPLICANT_SUCCESS)
        {
            std::cerr << "error disconnecting from cluster: " << r.last_error().msg()
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
