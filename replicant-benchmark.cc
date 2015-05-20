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

// po6
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/atomic.h>
#include <e/time.h>

// Ygor
#include <ygor.h>

// Replicant
#include <replicant.h>
#include "tools/common.h"

#define MICROS 1000ULL
#define MILLIS (1000ULL * MICROS)
#define SECONDS (1000ULL * MILLIS)

struct benchmark
{
    benchmark();
    ~benchmark() throw ();

    void producer();
    void consumer();

    const char* output;
    const char* object;
    const char* function;
    long target;
    long length;

    ygor_data_logger* dl;
    uint32_t done;

    po6::threads::mutex mtx;
    replicant_client* client;
    google::dense_hash_map<int64_t, uint64_t> times;
    replicant_returncode rr;

    private:
        benchmark(const benchmark&);
        benchmark& operator = (const benchmark&);
};

benchmark :: benchmark()
    : output("benchmark.dat.bz2")
    , object("echo")
    , function("echo")
    , target(100)
    , length(60)
    , dl(NULL)
    , done(0)
    , mtx()
    , client(NULL)
    , times()
    , rr()
{
    times.set_empty_key(INT64_MAX);
    times.set_deleted_key(INT64_MAX - 1);
}

benchmark :: ~benchmark() throw ()
{
}

void
benchmark :: producer()
{
    const uint64_t start = e::time();
    const uint64_t end = start + length * SECONDS;
    uint64_t now = start;
    uint64_t issued = 0;

    while (now < end)
    {
        double elapsed = double(now - start) / SECONDS;
        uint64_t expected = elapsed * target;

        for (uint64_t i = issued; i < expected; ++i)
        {
            po6::threads::mutex::hold hold(&mtx);
            now = e::time();
            int64_t id = replicant_client_call(client, object, function, "", 0, 0, &rr, NULL, 0);

            if (id < 0)
            {
                std::cerr << "call failed: "
                          << replicant_client_error_message(client) << " @ "
                          << replicant_client_error_location(client) << std::endl;
                abort();
            }

            times.insert(std::make_pair(id, now));
        }

        issued = expected;

        timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 1 * MILLIS;
        nanosleep(&ts, NULL);

        now = e::time();
    }

    e::atomic::store_32_release(&done, 1);
}

void
benchmark :: consumer()
{
    while (true)
    {
        replicant_client_block(client, 250);
        po6::threads::mutex::hold hold(&mtx);
        replicant_returncode lr;
        int64_t id = replicant_client_loop(client, 0, &lr);

        if (id < 0 && lr == REPLICANT_NONE_PENDING && e::atomic::load_32_acquire(&done) != 0)
        {
            break;
        }
        else if (id < 0 && (lr == REPLICANT_TIMEOUT || lr == REPLICANT_NONE_PENDING))
        {
            continue;
        }
        else if (id < 0)
        {
            std::cerr << "loop failed: "
                      << replicant_client_error_message(client) << " @ "
                      << replicant_client_error_location(client) << std::endl;
            abort();
        }

        if (rr != REPLICANT_SUCCESS)
        {
            std::cerr << "call failed: "
                      << replicant_client_error_message(client) << " @ "
                      << replicant_client_error_location(client) << std::endl;
            abort();
        }

        const uint64_t end = e::time();
        google::dense_hash_map<int64_t, uint64_t>::iterator it = times.find(id);

        if (it == times.end())
        {
            std::cerr << "bad map handling code" << std::endl;
            abort();
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
            abort();
        }
    }
}

int
main(int argc, const char* argv[])
{
    benchmark b;
    connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('o', "output")
            .description("where to save the recorded benchmark stats (default: benchmark.dat.bz2)")
            .as_string(&b.output);
    ap.arg().long_name("object")
            .description("object to call (default: echo)")
            .as_string(&b.object);
    ap.arg().long_name("function")
            .description("function to call (default: echo)")
            .as_string(&b.function);
    ap.arg().name('t', "throughput")
            .description("target throughput (default: 100 ops/s)")
            .as_long(&b.target);
    ap.arg().name('r', "runtime")
            .description("total test runtime length in seconds (default: 60)")
            .as_long(&b.length);
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

    ygor_data_logger* dl = ygor_data_logger_create(b.output, 1000000, 1000);

    if (!dl)
    {
        std::cerr << "could not open output: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    b.client = replicant_client_create(conn.host(), conn.port());
    b.dl = dl;
    po6::threads::thread prod(po6::threads::make_thread_wrapper(&benchmark::producer, &b));
    po6::threads::thread cons(po6::threads::make_thread_wrapper(&benchmark::consumer, &b));
    prod.start();
    cons.start();
    prod.join();
    cons.join();

    if (ygor_data_logger_flush_and_destroy(dl) < 0)
    {
        std::cerr << "could not close output: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
