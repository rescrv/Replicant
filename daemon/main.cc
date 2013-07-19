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

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/hostname.h>

// e
#include <e/popt.h>

// BusyBee
#include <busybee_utils.h>

// Replicant
#include "daemon/daemon.h"

#include <cstdio>
int
main(int argc, const char* argv[])
{
    bool daemonize = true;
    const char* data = ".";
    bool listen;
    const char* listen_host = "auto";
    long listen_port = 1982;
    bool connect;
    const char* connect_host = "127.0.0.1";
    long connect_port = 1982;
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('d', "daemon")
            .description("run replicant in the background")
            .set_true(&daemonize);
    ap.arg().name('f', "foreground")
            .description("run replicant in the foreground")
            .set_false(&daemonize);
    ap.arg().name('D', "data")
            .description("store persistent state in this directory (default: .)")
            .metavar("dir").as_string(&data);
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: auto)")
            .metavar("IP").as_string(&listen_host).set_true(&listen);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: 1982)")
            .metavar("port").as_long(&listen_port).set_true(&listen);
    ap.arg().name('c', "connect")
            .description("join an existing replicant cluster through IP address or hostname")
            .metavar("addr").as_string(&connect_host).set_true(&connect);
    ap.arg().name('P', "connect-port")
            .description("connect to an alternative port (default: 1982)")
            .metavar("port").as_long(&connect_port).set_true(&connect);

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

    if (listen_port >= (1 << 16))
    {
        std::cerr << "listen-port is out of range" << std::endl;
        return EXIT_FAILURE;
    }

    if (connect_port >= (1 << 16))
    {
        std::cerr << "connect-port is out of range" << std::endl;
        return EXIT_FAILURE;
    }

    po6::net::ipaddr listen_ip;
    po6::net::location bind_to;

    if (strcmp(listen_host, "auto") == 0)
    {
        if (!busybee_discover(&listen_ip))
        {
            std::cerr << "cannot automatically discover local address; specify one manually" << std::endl;
            return EXIT_FAILURE;
        }

        bind_to = po6::net::location(listen_ip, listen_port);
    }
    else
    {
        try
        {
            listen_ip = po6::net::ipaddr(listen_host);
            bind_to = po6::net::location(listen_ip, listen_port);
        }
        catch (po6::error& e)
        {
            // fallthrough
        }
        catch (std::invalid_argument& e)
        {
            // fallthrough
        }

        if (bind_to == po6::net::location())
        {
            bind_to = po6::net::hostname(listen_host, 0).lookup(AF_UNSPEC, IPPROTO_TCP);
            bind_to.port = listen_port;
        }
    }

    if (bind_to == po6::net::location())
    {
        std::cerr << "cannot interpret listen address as hostname or IP address" << std::endl;
        return EXIT_FAILURE;
    }

    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    try
    {
        replicant::daemon d;
        return d.run(daemonize,
                     po6::pathname(data),
                     listen, bind_to,
                     connect, po6::net::hostname(connect_host, connect_port));
    }
    catch (po6::error& e)
    {
        std::cerr << "system error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
