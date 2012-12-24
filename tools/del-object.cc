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

// C
#include <cstdlib>

// po6
#include <po6/error.h>

// e
#include <e/guard.h>

// Replicant
#include "client/replicant.h"
#include "tools/common.h"

static struct poptOption popts[] = {
    POPT_AUTOHELP
    CONNECT_TABLE
    POPT_TABLEEND
};

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    poptSetOtherOptionHelp(poptcon, "[OPTIONS] <object-id> <library>");
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'h':
                if (!check_host())
                {
                    return EXIT_FAILURE;
                }
                break;
            case 'p':
                if (!check_port())
                {
                    return EXIT_FAILURE;
                }
                break;
            case POPT_ERROR_NOARG:
            case POPT_ERROR_BADOPT:
            case POPT_ERROR_BADNUMBER:
            case POPT_ERROR_OVERFLOW:
                std::cerr << poptStrerror(rc) << " " << poptBadOption(poptcon, 0) << std::endl;
                return EXIT_FAILURE;
            case POPT_ERROR_OPTSTOODEEP:
            case POPT_ERROR_BADQUOTE:
            case POPT_ERROR_ERRNO:
            default:
                std::cerr << "logic error in argument parsing" << std::endl;
                return EXIT_FAILURE;
        }
    }

    const char** args = poptGetArgs(poptcon);
    size_t num_args = 0;

    while (args && args[num_args])
    {
        ++num_args;
    }

    if (num_args != 1)
    {
        std::cerr << "please specify the object to delete\n" << std::endl;
        poptPrintUsage(poptcon, stderr, 0);
        return EXIT_FAILURE;
    }

    try
    {
        replicant_client r(_connect_host, _connect_port);
        replicant_returncode re = REPLICANT_GARBAGE;
        replicant_returncode le = REPLICANT_GARBAGE;
        int64_t rid = 0;
        int64_t lid = 0;
        const char* errmsg;
        size_t errmsg_sz;

        rid = r.del_object(args[0], &re, &errmsg, &errmsg_sz);

        if (rid < 0)
        {
            std::cerr << "could not destroy object: " << r.last_error_desc()
                      << " (" << re << ")" << std::endl;
            return EXIT_FAILURE;
        }

        lid = r.loop(-1, &le);

        if (lid < 0)
        {
            std::cerr << "could not destroy object: " << r.last_error_desc()
                      << " (" << le << ")" << std::endl;
            return EXIT_FAILURE;
        }

        if (rid != lid)
        {
            std::cerr << "could not destroy object: internal error" << std::endl;
            return EXIT_FAILURE;
        }

        if (re != REPLICANT_SUCCESS)
        {
            std::cerr << "could not destroy object: " << r.last_error_desc()
                      << " (" << re << ")" << std::endl;
            return EXIT_FAILURE;
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
