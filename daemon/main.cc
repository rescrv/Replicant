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

#define __STDC_LIMIT_MACROS

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

int
main(int argc, const char* argv[])
{
    const char* wrap_name = getenv("REPLICANT_WRAP");

    if (wrap_name)
    {
        argv[0] = wrap_name;
    }

    bool daemonize = true;
    const char* data = ".";
    const char* log = NULL;
    bool listen = false;
    const char* listen_host = "auto";
    long listen_port = 1982;
    bool connect = false;
    const char* connect_host = "127.0.0.1";
    long connect_port = 1982;
    const char* pidfile = "";
    bool has_pidfile = false;
    bool init = false;
    const char* init_obj = NULL;
    const char* init_lib = NULL;
    const char* init_str = NULL;
    const char* init_rst = NULL;
    bool log_immediate = false;

    e::argparser ap;
    ap.autohelp();
    ap.arg().name('d', "daemon")
            .description("run in the background")
            .set_true(&daemonize);
    ap.arg().name('f', "foreground")
            .description("run in the foreground")
            .set_false(&daemonize);
    ap.arg().name('D', "data")
            .description("store persistent state in this directory (default: .)")
            .metavar("dir").as_string(&data);
    ap.arg().name('L', "log")
            .description("store logs in this directory (default: --data)")
            .metavar("dir").as_string(&log);
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: auto)")
            .metavar("IP").as_string(&listen_host).set_true(&listen);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: 1982)")
            .metavar("port").as_long(&listen_port).set_true(&listen);
    ap.arg().name('c', "connect")
            .description("join an existing cluster through IP address or hostname")
            .metavar("addr").as_string(&connect_host).set_true(&connect);
    ap.arg().name('P', "connect-port")
            .description("connect to an alternative port (default: 1982)")
            .metavar("port").as_long(&connect_port).set_true(&connect);
    ap.arg().long_name("pidfile")
            .description("write the PID to a file (default: don't)")
            .metavar("file").as_string(&pidfile).set_true(&has_pidfile);
    ap.arg().long_name("object")
            .description("initialize a new cluster with this object")
            .metavar("object").as_string(&init_obj).set_true(&init).hidden();
    ap.arg().long_name("library")
            .description("initialize a new cluster with this library for the object")
            .metavar("library").as_string(&init_lib).set_true(&init).hidden();
    ap.arg().long_name("init-string")
            .description("initialize a new cluster by calling \"init\" on the object")
            .metavar("library").as_string(&init_str).hidden();
    ap.arg().long_name("restore")
            .description("initialize a new cluster by restoring object/library with this backup")
            .metavar("restore").as_string(&init_rst).hidden();
    ap.arg().long_name("log-immediate")
            .description("immediately flush all log output")
            .set_true(&log_immediate).hidden();

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

    if (bind_to.address == po6::net::ipaddr("0.0.0.0"))
    {
        std::cerr << "cannot bind to " << bind_to << " because it is not routable" << std::endl;
        return EXIT_FAILURE;
    }

    if (init && (init_obj == NULL || init_lib == NULL))
    {
        std::cerr << "object and library must be either omitted or presented as a pair" << std::endl;
        return EXIT_FAILURE;
    }

    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    if (log_immediate)
    {
        FLAGS_logbufsecs = 0;
    }

    try
    {
        replicant::daemon d;
        return d.run(daemonize,
                     po6::pathname(data),
                     po6::pathname(log ? log : data),
                     po6::pathname(pidfile), has_pidfile,
                     listen, bind_to,
                     connect, po6::net::hostname(connect_host, connect_port),
                     init_obj, init_lib, init_str, init_rst);
    }
    catch (std::exception& e)
    {
        std::cerr << "error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
