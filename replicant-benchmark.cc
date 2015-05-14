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

// Google SparseHash
#include <google/dense_hash_map>

// e
#include <e/time.h>

// Ygor
#include <ygor.h>

// Replicant
#include <replicant.h>
#include "tools/common.h"

#define MILLIS (1000ULL * 1000ULL)

int
main(int argc, const char* argv[])
{
    const char* output = "benchmark.dat.bz2";
    long interval = 100;
    long outstanding = 10;
    long count = 1000;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.arg().long_name("output")
            .description("where to save the recorded benchmark stats (default: benchmark.dat.bz2)")
            .as_string(&output);
    ap.arg().long_name("interval")
            .description("issue a new operation every <interval> ms (default: 100)")
            .as_long(&interval);
    ap.arg().long_name("outstanding")
            .description("abort if more than <outstanding> operations are incomplete (default: 10)")
            .as_long(&outstanding);
    ap.arg().long_name("count")
            .description("number of operations to issue (default: 1000)")
            .as_long(&count);
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command requires no positional arguments\n" << std::endl;
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
        ygor_data_logger* dl = ygor_data_logger_create(output, 1000000, 1000);

        if (!dl)
        {
            std::cerr << "could not open output: " << strerror(errno) << std::endl;
            return EXIT_FAILURE;
        }

        google::dense_hash_map<int64_t, uint64_t> times;
        times.set_empty_key(INT64_MAX);
        times.set_deleted_key(INT64_MAX - 1);
        replicant_returncode rr;
        replicant_client* r = replicant_client_create(conn.host(), conn.port());
        uint64_t done = 0;
        uint64_t next = e::time();

        while (done < uint64_t(count))
        {
            const uint64_t now = e::time();

            if (now >= next)
            {
                int64_t id = replicant_client_call(r, "echo", "echo", "", 0, 0, &rr, NULL, 0);

                if (id < 0)
                {
                    std::cerr << "call failed: "
                              << replicant_client_error_message(r) << " @ "
                              << replicant_client_error_location(r) << std::endl;
                    return EXIT_FAILURE;
                }

                times.insert(std::make_pair(id, now));
                next = now + interval * MILLIS;
            }

            if (times.size() > uint64_t(outstanding))
            {
                std::cerr << "too many operations outstanding: " << times.size() << " > " << outstanding << std::endl;
                return EXIT_FAILURE;
            }

            uint64_t millis = (next - now) / MILLIS;
            replicant_returncode lr;
            int64_t id = replicant_client_loop(r, millis, &lr);
            const uint64_t end = e::time();

            if (id < 0 && (lr == REPLICANT_TIMEOUT || lr == REPLICANT_NONE_PENDING))
            {
                continue;
            }
            else if (id < 0)
            {
                std::cerr << "loop failed: "
                          << replicant_client_error_message(r) << " @ "
                          << replicant_client_error_location(r) << std::endl;
                return EXIT_FAILURE;
            }

            if (rr != REPLICANT_SUCCESS)
            {
                std::cerr << "call failed: "
                          << replicant_client_error_message(r) << " @ "
                          << replicant_client_error_location(r) << std::endl;
                return EXIT_FAILURE;
            }

            google::dense_hash_map<int64_t, uint64_t>::iterator it = times.find(id);

            if (it == times.end())
            {
                std::cerr << "bad map handling code" << std::endl;
                return EXIT_FAILURE;
            }

            const uint64_t start = it->second;
            times.erase(it);

            ygor_data_record dr;
            dr.series = 1;
            dr.when = start;
            dr.data = end - start;

            if (ygor_data_logger_record(dl, &dr) < 0)
            {
                std::cerr << "could not record data point: " << strerror(errno) << std::endl;
                return false;
            }

            ++done;
        }

        if (ygor_data_logger_flush_and_destroy(dl) < 0)
        {
            std::cerr << "could not close output: " << strerror(errno) << std::endl;
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
