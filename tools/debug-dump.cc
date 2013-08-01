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

// e
#include <e/popt.h>

// Replicant
#include "daemon/fact_store.h"

int
main(int argc, const char* argv[])
{
    const char* _data = ".";
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('D', "data")
            .description("store persistent state in this directory (default: .)")
            .as_string(&_data).metavar("dir");

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command takes no arguments" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    try
    {
        po6::pathname data(_data);
        replicant::fact_store fs;
        return fs.debug_dump(data) ? EXIT_SUCCESS : EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
