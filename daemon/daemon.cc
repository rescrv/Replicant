// Copyright (c) 2012-2015, Robert Escriva
// Copyright (c) 2017, Robert Escriva, Cornell University
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// C
#include <limits.h>
#include <math.h>
#include <string.h>

// POSIX
#include <dlfcn.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// po6
#include <po6/time.h>

// e
#include <e/compat.h>
#include <e/daemon.h>
#include <e/daemonize.h>
#include <e/endian.h>
#include <e/guard.h>
#include <e/error.h>
#include <e/strescape.h>

// BusyBee
#include <busybee.h>

// Replicant
#include "common/atomic_io.h"
#include "common/bootstrap.h"
#include "common/constants.h"
#include "common/ids.h"
#include "common/generate_token.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/packing.h"
#include "common/quorum_calc.h"
#include "daemon/daemon.h"
#include "daemon/leader.h"
#include "daemon/scout.h"

using replicant::daemon;

#define CHECK_UNPACK(MSGTYPE, UNPACKER) \
    do \
    { \
        if (UNPACKER.error()) \
        { \
            network_msgtype CONCAT(_anon, __LINE__)(REPLNET_ ## MSGTYPE); \
            LOG(WARNING) << "received corrupt \"" \
                         << CONCAT(_anon, __LINE__) << "\" message"; \
            return; \
        } \
    } while (0)

int s_interrupts = 0;
bool s_debug_dump = false;
bool s_debug_mode = false;

static void
exit_on_signal(int /*signum*/)
{
    RAW_LOG(ERROR, "interrupted: exiting");
    __sync_fetch_and_add(&s_interrupts, 1);
}

static void
handle_debug_dump(int /*signum*/)
{
    s_debug_dump = true;
}

static void
handle_debug_mode(int /*signum*/)
{
    s_debug_mode = !s_debug_mode;
}

daemon :: daemon()
    : m_gc()
    , m_gc_ts()
    , m_us()
    , m_config_mtx()
    , m_config()
    , m_busybee_controller(&m_config_mtx, &m_config)
    , m_busybee(NULL)
    , m_ft(&m_config)
    , m_periodic()
    , m_bootstrap_thread()
    , m_bootstrap_stop(0)
    , m_unique_token(0)
    , m_unique_base(0)
    , m_unique_offset(0)
    , m_unordered_mtx()
    , m_unordered_cmds()
    , m_unassigned_cmds()
    , m_msgs_waiting_for_persistence()
    , m_msgs_waiting_for_nonces()
    , m_acceptor()
    , m_scout()
    , m_scout_wait_cycles(0)
    , m_leader()
    , m_replica()
    , m_last_replica_snapshot(0)
    , m_last_gc_slot(0)
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);
    m_unordered_cmds.set_empty_key(INT64_MAX);
    m_unordered_cmds.set_deleted_key(INT64_MAX - 1);
    register_periodic(250, &daemon::periodic_maintain);
    register_periodic(500, &daemon::periodic_ping_servers);
    register_periodic(1000, &daemon::periodic_generate_nonce_sequence);
    register_periodic(1000, &daemon::periodic_flush_enqueued_commands);
    register_periodic(1000, &daemon::periodic_maintain_objects);
    register_periodic(1000, &daemon::periodic_tick);
    register_periodic(10 * 1000, &daemon::periodic_warn_scout_stuck);
    register_periodic(10 * 1000, &daemon::periodic_check_address);
    m_gc.register_thread(&m_gc_ts);
}

daemon :: ~daemon() throw ()
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    for (unordered_map_t::iterator it = m_unordered_cmds.begin();
            it != m_unordered_cmds.end(); ++it)
    {
        delete it->second;
    }

    for (unordered_list_t::iterator it = m_unassigned_cmds.begin();
            it != m_unassigned_cmds.end(); ++it)
    {
        delete *it;
    }

    while (!m_msgs_waiting_for_persistence.empty())
    {
        delete m_msgs_waiting_for_persistence.front().msg;
        m_msgs_waiting_for_persistence.pop_front();
    }

    while (!m_msgs_waiting_for_nonces.empty())
    {
        delete m_msgs_waiting_for_nonces.front().msg;
        m_msgs_waiting_for_nonces.pop_front();
    }

    if (m_busybee)
    {
        delete m_busybee;
    }

    m_gc.deregister_thread(&m_gc_ts);
}

static void
atomically_allow_pending_blocked_signals()
{
    sigset_t ss;
    sigemptyset(&ss);

    if (sigpending(&ss) == 0 &&
        (sigismember(&ss, SIGHUP) == 1 ||
         sigismember(&ss, SIGINT) == 1 ||
         sigismember(&ss, SIGTERM) == 1 ||
         sigismember(&ss, SIGQUIT) == 1))
    {
        sigemptyset(&ss);
        sigsuspend(&ss);
    }
}

int
daemon :: run(bool background,
              std::string data,
              std::string log,
              std::string pidfile,
              bool has_pidfile,
              bool set_bind_to,
              po6::net::location bind_to,
              bool set_existing,
              const bootstrap& existing,
              const char* init_obj,
              const char* init_lib,
              const char* init_str,
              const char* init_rst)
{
    if (!e::block_all_signals())
    {
        std::cerr << "could not block signals; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!e::daemonize(background, log, "replicant-daemon-", pidfile, has_pidfile))
    {
        return EXIT_FAILURE;
    }

    if (!e::install_signal_handler(SIGHUP, exit_on_signal) ||
        !e::install_signal_handler(SIGINT, exit_on_signal) ||
        !e::install_signal_handler(SIGTERM, exit_on_signal) ||
        !e::install_signal_handler(SIGQUIT, exit_on_signal) ||
        !e::install_signal_handler(SIGUSR1, handle_debug_dump) ||
        !e::install_signal_handler(SIGUSR2, handle_debug_mode))
    {
        PLOG(ERROR) << "could not install signal handlers";
        return EXIT_FAILURE;
    }

    bool saved = false;
    server saved_us;
    bootstrap saved_bootstrap;

    if (!m_acceptor.open(data, &saved, &saved_us, &saved_bootstrap))
    {
        return EXIT_FAILURE;
    }

    m_us.bind_to = bind_to;
    bool init = false;

    // case 1: start a new cluster
    if (!saved && !set_existing)
    {
        uint64_t cluster;
        uint64_t this_server;

        if (m_acceptor.current_ballot() != ballot())
        {
            this_server = m_acceptor.current_ballot().leader.get();
        }
        else
        {
            if (!generate_token(&this_server))
            {
                PLOG(ERROR) << "could not generate random identifier for this server";
                return EXIT_FAILURE;
            }
        }

        if (!generate_token(&cluster))
        {
            PLOG(ERROR) << "could not generate random identifier for the cluster";
            return EXIT_FAILURE;
        }

        m_us.id = server_id(this_server);
        m_config_mtx.lock();
        m_config = configuration(cluster_id(cluster), version_id(1), 0, &m_us, 1);
        m_config_mtx.unlock();
        saved_bootstrap = m_config.current_bootstrap();
        LOG(INFO) << "starting " << m_config.cluster() << " from this server (" << m_us << ")";
        init = init_obj && init_lib;

        m_acceptor.adopt(ballot(m_acceptor.current_ballot().number + 1, m_us.id));
        pvalue p(m_acceptor.current_ballot(), 0, construct_become_member_command(m_us));
        m_acceptor.accept(p);
        m_replica.reset(new replica(this, m_config));
        m_replica->learn(p);

        uint64_t snapshot_slot;
        e::slice snapshot;
        std::auto_ptr<e::buffer> snapshot_backing;
        m_replica->take_blocking_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

        if (!m_acceptor.record_snapshot(snapshot_slot, snapshot))
        {
            LOG(ERROR) << "error saving starting replica state to disk: " << po6::strerror(errno);
            return EXIT_FAILURE;
        }

        e::atomic::store_ptr_release(&m_busybee, busybee_server::create(&m_busybee_controller, m_us.id.get(), m_us.bind_to, &m_gc));
    }
    // case 2: new node, joining an existing cluster
    else if (!saved && set_existing)
    {
        uint64_t this_server = 0;

        if (!generate_token(&this_server))
        {
            PLOG(ERROR) << "could not generate random identifier for this server";
            return EXIT_FAILURE;
        }

        m_us.id = server_id(this_server);
        saved_bootstrap = existing;
        e::atomic::store_ptr_release(&m_busybee, busybee_server::create(&m_busybee_controller, m_us.id.get(), m_us.bind_to, &m_gc));
        setup_replica_from_bootstrap(existing, &m_replica);

        if (!m_replica.get())
        {
            return EXIT_FAILURE;
        }

        if (m_replica->config().has(m_us.bind_to))
        {
            LOG(ERROR) << "configuration already has a server on our address";
            LOG(ERROR) << "use the command line tools to remove said server and restart this one";
            return EXIT_FAILURE;
        }
    }
    // case 3: existing node, coming back online
    else
    {
        m_us = saved_us;

        if (set_bind_to)
        {
            m_us.bind_to = bind_to;
        }

        if (set_existing)
        {
            saved_bootstrap = existing;
        }

        LOG(INFO) << "re-joining cluster as " << m_us << " using bootstrap " << saved_bootstrap;
        e::atomic::store_ptr_release(&m_busybee, busybee_server::create(&m_busybee_controller, m_us.id.get(), m_us.bind_to, &m_gc));

        e::slice snapshot;
        std::auto_ptr<e::buffer> snapshot_backing;

        if (!m_acceptor.load_latest_snapshot(&snapshot, &snapshot_backing))
        {
            LOG(ERROR) << "error loading replica state from disk: " << po6::strerror(errno);
            return EXIT_FAILURE;
        }

        m_replica.reset(replica::from_snapshot(this, snapshot));

        if (!m_replica.get())
        {
            LOG(ERROR) << "could not restore replica from previous execution";
            return EXIT_FAILURE;
        }
    }

    if (!m_acceptor.save(m_us, saved_bootstrap))
    {
        return EXIT_FAILURE;
    }

    if (!init && init_rst)
    {
        LOG(INFO) << "asked to restore from \"" << e::strescape(init_rst) << "\" "
                  << "but we are not initializing a new cluster";
        LOG(INFO) << "the restore operations only have an effect when "
                  << "starting a fresh cluster";
        LOG(INFO) << "this means you'll want to start with a new data-dir "
                  << "and omit any options for connecting to an existing cluster";
        return EXIT_FAILURE;
    }

    if (!m_replica->config().has(m_us.id))
    {
        bootstrap current = m_replica->config().current_bootstrap();
        LOG(WARNING) << "this " << m_us << " is not in configuration " << m_replica->config().version()
                     << "; adding it to the configuration now";
        m_replica.reset();
        become_cluster_member(current);

        for (size_t i = 0; __sync_fetch_and_add(&s_interrupts, 0) == 0 && i < 10; ++i)
        {
            setup_replica_from_bootstrap(current, &m_replica);

            if (m_replica.get() && m_replica->config().has(m_us.id))
            {
                break;
            }

            atomically_allow_pending_blocked_signals();

            if (i + 1 < 10)
            {
                LOG(INFO) << "this server still not visible in the configuration; retrying in 1s";
            }

            timespec ts;
            ts.tv_sec = 1;
            ts.tv_nsec = 0;
            nanosleep(&ts, NULL);
        }
    }

    if (__sync_fetch_and_add(&s_interrupts, 0) > 0)
    {
        return EXIT_FAILURE;
    }

    assert(m_replica.get());

    if (!m_replica->config().has(m_us.id))
    {
        LOG(ERROR) << "despite repeated efforts to rectify the situation, " << m_us
                   << " is not in configuration " << m_replica->config().version()
                   << "; exiting";
        return EXIT_FAILURE;
    }

    m_config_mtx.lock();
    m_config = m_replica->config();
    m_config_mtx.unlock();
    m_ft.set_server_id(m_us.id);

    if (!post_config_change_hook())
    {
        return EXIT_SUCCESS;
    }

    if (init)
    {
        assert(init_obj);
        assert(init_lib);
        std::string lib;

        if (!atomic_read(AT_FDCWD, init_lib, &lib))
        {
            PLOG(ERROR) << "could not read library";
            return EXIT_FAILURE;
        }

        if (init_rst)
        {
            std::string rst;

            if (!atomic_read(AT_FDCWD, init_rst, &rst))
            {
                PLOG(ERROR) << "could not read restore file";
                return EXIT_FAILURE;
            }

            std::string input;
            e::packer pai(&input);
            pai = pai << e::slice(init_obj, strlen(init_obj))
                      << e::slice(rst);

            std::string cmd;
            e::packer pac(&cmd);
            pac = pac << SLOT_CALL
                      << uint8_t(0)
                      << uint64_t(0)
                      << e::slice("replicant")
                      << e::slice("restore_object")
                      << e::slice(input);
            m_acceptor.accept(pvalue(m_acceptor.current_ballot(), 1, cmd));
        }
        // else this is an initialization
        else
        {
            std::string input1(init_obj, strlen(init_obj) + 1);
            input1 += lib;
            std::string cmd1;
            e::packer pa1(&cmd1);
            pa1 = pa1 << SLOT_CALL 
                      << uint8_t(0)
                      << uint64_t(0)
                      << e::slice("replicant")
                      << e::slice("new_object")
                      << e::slice(input1);
            m_acceptor.accept(pvalue(m_acceptor.current_ballot(), 1, cmd1));

            if (init_str)
            {
                std::string input2(init_str, strlen(init_str) + 1);
                std::string cmd2;
                e::packer pa2(&cmd2);
                pa2 = pa2 << SLOT_CALL
                          << uint8_t(0)
                          << uint64_t(0)
                          << e::slice(init_obj, strlen(init_obj))
                          << e::slice("init", 4)
                          << e::slice(input2);
                m_acceptor.accept(pvalue(m_acceptor.current_ballot(), 2, cmd2));
            }
        }
    }

    e::atomic::store_32_nobarrier(&m_bootstrap_stop, 0);
    m_bootstrap_thread.reset(new po6::threads::thread(po6::threads::make_obj_func(&daemon::rebootstrap, this, saved_bootstrap)));
    m_bootstrap_thread->start();

    while (__sync_fetch_and_add(&s_interrupts, 0) == 0)
    {
        m_gc.quiescent_state(&m_gc_ts);

        if (m_acceptor.failed())
        {
            LOG(ERROR) << "acceptor has failed; exiting";
            __sync_fetch_and_add(&s_interrupts, 1);
            continue;
        }

        flush_acceptor_messages();
        run_periodic();

        bool debug_mode = s_debug_mode;
        uint64_t token;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee->recv(&m_gc_ts, 1, &token, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                continue;
            case BUSYBEE_INTERRUPTED:
                if (s_debug_mode != debug_mode)
                {
                    if (s_debug_mode)
                    {
                        debug_dump();
                        LOG(INFO) << "enabling debug mode; will log all state transitions";
                    }
                    else
                    {
                        LOG(INFO) << "disabling debug mode; will go back to normal operation";
                    }
                }
                else if (s_debug_dump)
                {
                    debug_dump();
                    s_debug_dump = false;
                }

                continue;
            case BUSYBEE_DISRUPTED:
                continue;
            case BUSYBEE_SEE_ERRNO:
                LOG(ERROR) << "receive error: " << po6::strerror(errno);
                continue;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "BusyBee returned " << rc << " during a \"recv\" call";
                continue;
        }

        assert(msg.get());
        server_id si(token);
        network_msgtype mt = REPLNET_NOP;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> mt;

        switch (mt)
        {
            case REPLNET_NOP:
                break;
            case REPLNET_BOOTSTRAP:
                process_bootstrap(si, msg, up);
                break;
            case REPLNET_STATE_TRANSFER:
                process_state_transfer(si, msg, up);
                break;
            case REPLNET_WHO_ARE_YOU:
                process_who_are_you(si, msg, up);
                break;
            case REPLNET_PAXOS_PHASE1A:
                process_paxos_phase1a(si, msg, up);
                break;
            case REPLNET_PAXOS_PHASE1B:
                process_paxos_phase1b(si, msg, up);
                break;
            case REPLNET_PAXOS_PHASE2A:
                process_paxos_phase2a(si, msg, up);
                break;
            case REPLNET_PAXOS_PHASE2B:
                process_paxos_phase2b(si, msg, up);
                break;
            case REPLNET_PAXOS_LEARN:
                process_paxos_learn(si, msg, up);
                break;
            case REPLNET_PAXOS_SUBMIT:
                process_paxos_submit(si, msg, up);
                break;
            case REPLNET_SERVER_BECOME_MEMBER:
                process_server_become_member(si, msg, up);
                break;
            case REPLNET_UNIQUE_NUMBER:
                process_unique_number(si, msg, up);
                break;
            case REPLNET_OBJECT_FAILED:
                process_object_failed(si, msg, up);
                break;
            case REPLNET_POKE:
                process_poke(si, msg, up);
                break;
            case REPLNET_COND_WAIT:
                process_cond_wait(si, msg, up);
                break;
            case REPLNET_CALL:
                process_call(si, msg, up);
                break;
            case REPLNET_GET_ROBUST_PARAMS:
                process_get_robust_params(si, msg, up);
                break;
            case REPLNET_CALL_ROBUST:
                process_call_robust(si, msg, up);
                break;
            case REPLNET_PING:
                process_ping(si, msg, up);
                break;
            case REPLNET_PONG:
                process_pong(si, msg, up);
                break;
            case REPLNET_IDENTITY:
            case REPLNET_CLIENT_RESPONSE:
            case REPLNET_GARBAGE:
                LOG(WARNING) << "dropping \"" << mt << "\" received by server";
                break;
            default:
                LOG(WARNING) << "unknown message type; here's some hex:  " << msg->hex();
                break;
        }
    }

    e::atomic::store_32_nobarrier(&m_bootstrap_stop, 1);
    m_bootstrap_thread->join();

    LOG(INFO) << "replicant is gracefully shutting down";
    LOG(INFO) << "replicant will now terminate";
    return EXIT_SUCCESS;
}

void
daemon :: become_cluster_member(bootstrap current)
{
    LOG(INFO) << "trying to join the existing cluster using " << current;
    e::error err;
    bool has_err = false;
    bool success = false;
    bool has_params = false;
    uint64_t cluster_nonce;
    uint64_t min_slot;
    std::string us_packed;
    e::packer(&us_packed) << m_us;
    std::string call;
    e::packer(&call) << e::slice("replicant") << e::slice("add_server") << e::slice(us_packed);

    for (unsigned iteration = 0; __sync_fetch_and_add(&s_interrupts, 0) == 0 && iteration < 100; ++iteration)
    {
        configuration c;
        replicant_returncode rc = current.do_it(10000, &c, &err);
        atomically_allow_pending_blocked_signals();

        if (rc == REPLICANT_TIMEOUT)
        {
            continue;
        }
        else if (rc != REPLICANT_SUCCESS)
        {
            has_err = true;
            continue;
        }

        if (c.has(m_us.id))
        {
            success = true;
            break;
        }

        if (iteration > 0)
        {
            if (iteration % 10 == 0)
            {
                LOG(INFO) << "still trying...";
            }

            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 100 * 1000ULL * 1000ULL;
            nanosleep(&ts, NULL);
            atomically_allow_pending_blocked_signals();
        }

        for (size_t i = 0; !has_params && i < c.servers().size(); ++i)
        {
            std::auto_ptr<busybee_single> bbs(busybee_single::create(c.servers()[i].bind_to));
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(REPLNET_GET_ROBUST_PARAMS)
                            + sizeof(uint64_t);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_GET_ROBUST_PARAMS << uint64_t(0);
            bbs->send(msg);
            bbs->recv(1000, &msg);

            if (!msg.get())
            {
                continue;
            }

            network_msgtype mt;
            uint64_t nonce;
            e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
            up = up >> mt >> nonce >> cluster_nonce >> min_slot;

            if (up.error() || mt != REPLNET_CLIENT_RESPONSE)
            {
                continue;
            }

            has_params = true;
        }

        for (size_t i = 0; has_params && i < c.servers().size(); ++i)
        {
            std::auto_ptr<busybee_single> bbs(busybee_single::create(c.servers()[i].bind_to));
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(REPLNET_CALL_ROBUST)
                            + 3 * sizeof(uint64_t)
                            + call.size();
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << REPLNET_CALL_ROBUST << uint64_t(iteration) << cluster_nonce
                << min_slot << e::pack_memmove(call.data(), call.size());

            bbs->send(msg);
            bbs->recv(1000, &msg);

            if (!msg.get())
            {
                continue;
            }

            network_msgtype mt;
            uint64_t nonce;
            e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
            up = up >> mt >> nonce >> rc;

            if (rc == REPLICANT_SUCCESS)
            {
                success = true;
                break;
            }
            else
            {
                has_err = true;
                err.set_loc(__FILE__, __LINE__);
                err.set_msg() << "joining cluster failed; check server logs on " << c.servers()[i]
                              << " for details";
            }
        }

        for (size_t i = 0; !success && i < c.servers().size(); ++i)
        {
            std::auto_ptr<busybee_single> bbs(busybee_single::create(c.servers()[i].bind_to));
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(REPLNET_SERVER_BECOME_MEMBER)
                            + pack_size(m_us);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << REPLNET_SERVER_BECOME_MEMBER << m_us;

            bbs->send(msg);
            bbs->recv(1000, &msg);

            if (!msg.get())
            {
                continue;
            }

            network_msgtype mt;
            configuration tmpc;
            e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
            up = up >> mt >> tmpc;

            if (!up.error() &&
                c.cluster() == tmpc.cluster() &&
                c.version() < tmpc.version())
            {
                break;
            }
        }
    }

    if (success)
    {
    }
    else if (has_err)
    {
        LOG(ERROR) << "join process encountered an error: " << err.msg();
    }
    else
    {
        LOG(ERROR) << "join process timed out, or was interrupted by the user";
    }
}

void
daemon :: setup_replica_from_bootstrap(bootstrap current,
                                       std::auto_ptr<replica>* rep)
{
    LOG(INFO) << "copying replica state from existing cluster using " << current;
    configuration c;
    e::error err;
    bool has_err = false;
    rep->reset();

    for (unsigned iteration = 0; __sync_fetch_and_add(&s_interrupts, 0) == 0 && iteration < 100; ++iteration)
    {
        replicant_returncode rc = current.do_it(10000, &c, &err);
        atomically_allow_pending_blocked_signals();

        if (rc == REPLICANT_TIMEOUT)
        {
            continue;
        }
        else if (rc != REPLICANT_SUCCESS)
        {
            has_err = true;
            continue;
        }

        for (size_t i = 0; !rep->get() && i < c.servers().size(); ++i)
        {
            std::auto_ptr<busybee_single> bbs(busybee_single::create(c.servers()[i].bind_to));
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(REPLNET_STATE_TRANSFER);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_STATE_TRANSFER;
            bbs->send(msg);
            bbs->recv(60000, &msg);

            if (!msg.get())
            {
                continue;
            }

            network_msgtype mt;
            uint64_t slot;
            e::slice snapshot;
            e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
            up = up >> mt >> slot >> snapshot;

            if (!up.error())
            {
                rep->reset(replica::from_snapshot(this, snapshot));

                if (rep->get())
                {
                    uint64_t snapshot_slot;
                    std::auto_ptr<e::buffer> snapshot_backing;
                    (*rep)->take_blocking_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

                    if (!m_acceptor.record_snapshot(snapshot_slot, snapshot))
                    {
                        LOG(ERROR) << "error saving starting replica state to disk: " << po6::strerror(errno);
                        rep->reset();
                    }

                    return;
                }
            }
        }
    }

    if (has_err)
    {
        LOG(ERROR) << "replica state transfer encountered an error: " << err.msg();
    }
    else
    {
        LOG(ERROR) << "replica state transfer timed out, or was interrupted by the user";
    }
}

void
daemon :: send_bootstrap(server_id si)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_BOOTSTRAP)
              + pack_size(m_us)
              + pack_size(m_config);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BOOTSTRAP << m_us << m_config;
    send(si, msg);
}

void
daemon :: process_bootstrap(server_id si,
                            std::auto_ptr<e::buffer>,
                            e::unpacker)
{
    po6::net::location addr;

    if (m_busybee->get_addr(si.get(), &addr) == BUSYBEE_SUCCESS)
    {
        LOG(INFO) << "introducing " << addr << " to the cluster";
    }
    else
    {
        LOG(INFO) << "introducing " << si << " to the cluster";
    }

    send_bootstrap(si);
}

void
daemon :: process_state_transfer(server_id si,
                                 std::auto_ptr<e::buffer>,
                                 e::unpacker)
{
    uint64_t snapshot_slot;
    e::slice snapshot;
    std::auto_ptr<e::buffer> snapshot_backing;
    m_replica->get_last_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

    if (snapshot_slot == 0)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_NOP);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_NOP;
        send(si, msg);
        return;
    }

    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_STATE_TRANSFER)
              + sizeof(uint64_t)
              + pack_size(snapshot);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_STATE_TRANSFER << snapshot_slot << snapshot;
    send(si, msg);
}

void
daemon :: process_who_are_you(server_id si,
                              std::auto_ptr<e::buffer>,
                              e::unpacker)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_IDENTITY)
              + pack_size(m_us);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_IDENTITY << m_us;
    send(si, msg);
}

void
daemon :: send_paxos_phase1a(server_id to, const ballot& b)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PAXOS_PHASE1A)
              + pack_size(b);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PAXOS_PHASE1A << b;
    send(to, msg);
}

void
daemon :: process_paxos_phase1a(server_id si,
                                std::auto_ptr<e::buffer>,
                                e::unpacker up)
{
    ballot b;
    up = up >> b;
    CHECK_UNPACK(PAXOS_PHASE1A, up);

    if (si == b.leader && b > m_acceptor.current_ballot())
    {
        m_acceptor.adopt(b);

        if (b.leader != m_us.id)
        {
            m_scout.reset();
            m_leader.reset();
        }

        m_ft.proof_of_life(si);
        LOG(INFO) << "phase 1a:  taking up " << b;
        flush_enqueued_commands_with_stale_leader();
    }

    LOG_IF(ERROR, si != b.leader) << si << " is misusing " << b;
    send_paxos_phase1b(b.leader);
}

void
daemon :: send_paxos_phase1b(server_id to)
{
    const std::vector<pvalue>& pvals(m_acceptor.pvals());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PAXOS_PHASE1B)
              + pack_size(m_acceptor.current_ballot())
              + pack_size(pvals);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_PAXOS_PHASE1B
        << m_acceptor.current_ballot()
        << pvals;
    send_when_acceptor_persistent(to, msg);
}

void
daemon :: process_paxos_phase1b(server_id si,
                                std::auto_ptr<e::buffer>,
                                e::unpacker up)
{
    ballot b;
    std::vector<pvalue> accepted;
    up = up >> b >> accepted;
    CHECK_UNPACK(PAXOS_PHASE1B, up);

    if (m_us.id != b.leader)
    {
        return;
    }

    if (m_scout.get() && m_scout->current_ballot() == b)
    {
        if (m_scout->take_up(si, &accepted[0], accepted.size()))
        {
            LOG(INFO) << "phase 1b:  " << si << " has taken up " << b;
        }

        std::vector<server_id> missing = m_scout->missing();
        bool all_missing_are_suspected = true;

        for (size_t i = 0; i < missing.size(); ++i)
        {
            if (!m_ft.suspect_failed(missing[i], m_replica->current_settings().SUSPECT_TIMEOUT))
            {
                all_missing_are_suspected = false;
            }
        }

        if (all_missing_are_suspected && m_scout->adopted())
        {
            LOG(INFO) << "phase 1 complete: transitioning to phase 2 on " << b;
            m_leader.reset(new leader(*m_scout));

            if (m_replica->fill_window())
            {
                m_leader->fill_window(this);
            }

            m_leader->send_all_proposals(this);
            m_scout.reset();
        }
    }
}

void
daemon :: send_paxos_phase2a(server_id to, const pvalue& p)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PAXOS_PHASE2A)
              + pack_size(p);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PAXOS_PHASE2A << p;
    send(to, msg);
}

void
daemon :: process_paxos_phase2a(server_id si,
                                std::auto_ptr<e::buffer>,
                                e::unpacker up)
{
    pvalue p;
    up = up >> p;
    CHECK_UNPACK(PAXOS_PHASE2A, up);

    if (p.s < m_acceptor.lowest_acceptable_slot())
    {
        return;
    }

    if (si == p.b.leader && p.b == m_acceptor.current_ballot())
    {
        m_acceptor.accept(p);
        LOG_IF(INFO, s_debug_mode && p.s >= m_config.first_slot()) << "p2a: " << p;
    }

    send_paxos_phase2b(p.b.leader, p);
    LOG_IF(ERROR, si != p.b.leader) << si << " is misusing " << p.b;
}

void
daemon :: send_paxos_phase2b(server_id to, const pvalue& p)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PAXOS_PHASE2B)
              + pack_size(m_acceptor.current_ballot())
              + pack_size(p);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PAXOS_PHASE2B << m_acceptor.current_ballot() << p;
    send_when_acceptor_persistent(to, msg);
}

void
daemon :: process_paxos_phase2b(server_id si,
                                std::auto_ptr<e::buffer>,
                                e::unpacker up)
{
    ballot b;
    pvalue p;
    up = up >> b >> p;
    CHECK_UNPACK(PAXOS_PHASE2B, up);

    if (m_leader.get() && m_leader->current_ballot() == b && b == p.b)
    {
        if (m_leader->accept(si, p))
        {
            for (size_t i = 0; i < m_config.servers().size(); ++i)
            {
                send_paxos_learn(m_config.servers()[i].id, p);
            }
        }

        LOG_IF(INFO, s_debug_mode) << "p2b: " << p;
    }
}

void
daemon :: send_paxos_learn(server_id to, const pvalue& pval)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PAXOS_LEARN)
              + pack_size(pval);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PAXOS_LEARN << pval;
    send(to, msg);
}

void
daemon :: process_paxos_learn(server_id si,
                              std::auto_ptr<e::buffer>,
                              e::unpacker msg_up)
{
    pvalue p;
    msg_up = msg_up >> p;
    CHECK_UNPACK(PAXOS_LEARN, msg_up);

    if (si == p.b.leader)
    {
        m_replica->learn(p);
        m_ft.proof_of_life(p.b.leader);

        if (m_replica->config().version() > m_config.version())
        {
            m_config_mtx.lock();
            m_config = m_replica->config();
            m_config_mtx.unlock();
            m_scout.reset();
            m_leader.reset();

            if (!post_config_change_hook())
            {
                return;
            }
        }

        uint64_t start;
        uint64_t limit;
        m_replica->window(&start, &limit);

        if (m_scout.get())
        {
            m_scout->set_window(start, limit);
        }

        if (m_leader.get())
        {
            m_leader->set_window(this, start, limit);

            if (m_replica->fill_window())
            {
                m_leader->fill_window(this);
            }
        }

        if (m_last_replica_snapshot < m_replica->last_snapshot_num())
        {
            uint64_t snapshot_slot;
            e::slice snapshot;
            std::auto_ptr<e::buffer> snapshot_backing;
            m_replica->get_last_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

            if (m_acceptor.record_snapshot(snapshot_slot, snapshot))
            {
                char buf[16];
                e::pack64be(m_us.id.get(), buf);
                e::pack64be(snapshot_slot, buf + 8);
                std::string cmd(buf, buf + 16);
                enqueue_paxos_command(SLOT_SERVER_SET_GC_THRESH, cmd);
                LOG(INFO) << "snapshotting state at " << snapshot_slot;
                m_last_replica_snapshot = snapshot_slot;
            }
            else
            {
                LOG(ERROR) << "could not save snapshot: " << po6::strerror(errno);
            }
        }

        if (m_last_gc_slot < m_replica->gc_up_to())
        {
            m_last_gc_slot = m_replica->gc_up_to();
            m_acceptor.garbage_collect(m_last_gc_slot);

            if (m_leader.get())
            {
                m_leader->garbage_collect(m_last_gc_slot);
            }
        }

        e::atomic::store_32_nobarrier(&m_bootstrap_stop, 1);
    }
    else
    {
        LOG(ERROR) << si << " is misusing " << p.b;
    }
}

void
daemon :: send_paxos_submit(uint64_t slot_start,
                            uint64_t slot_limit,
                            const e::slice& command)
{
    if (m_acceptor.current_ballot() == ballot())
    {
        LOG_IF(INFO, s_debug_mode) << "dropping command submission because the leader is unknown";
        return;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_PAXOS_SUBMIT)
                    + 2 * sizeof(uint64_t)
                    + pack_size(command);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_PAXOS_SUBMIT << slot_start << slot_limit << command;
    LOG_IF(INFO, s_debug_mode) << "submitting to "
                               << m_acceptor.current_ballot().leader
                               << " command: [" << slot_start << ", "
                               << slot_limit << ") "
                               << e::strescape(std::string(command.cdata(), command.size()));
    send(m_acceptor.current_ballot().leader, msg);
}

void
daemon :: process_paxos_submit(server_id,
                               std::auto_ptr<e::buffer> msg,
                               e::unpacker up)
{
    uint64_t slot_start;
    uint64_t slot_limit;
    e::slice command;
    up = up >> slot_start >> slot_limit >> command;
    CHECK_UNPACK(PAXOS_SUBMIT, up);

    if (m_leader.get())
    {
        std::string c(command.cdata(), command.size());
        m_leader->propose(this, slot_start, slot_limit, c);
    }
    else if (m_scout.get())
    {
        m_scout->enqueue(slot_start, slot_limit, command);
    }
    else if (m_acceptor.current_ballot().leader != m_us.id)
    {
        LOG_IF(INFO, s_debug_mode) << "forwarding command to leader of " << m_acceptor.current_ballot();
        send(m_acceptor.current_ballot().leader, msg);
    }
}

void
daemon :: enqueue_paxos_command(slot_type t,
                                const std::string& command)
{
    enqueue_paxos_command(server_id(), 0, t, command);
}

void
daemon :: enqueue_paxos_command(server_id on_behalf_of,
                                uint64_t request_nonce,
                                slot_type t,
                                const std::string& command)
{
    unordered_command* uc = new unordered_command(on_behalf_of, request_nonce, t, command);
    uint64_t command_nonce;
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    if ((m_unordered_cmds.size() >= REPLICANT_COMMANDS_TO_LEADER && t == SLOT_CALL) ||
        !generate_nonce(&command_nonce))
    {
        m_unassigned_cmds.push_back(uc);
        return;
    }

    uc->set_command_nonce(command_nonce);
    m_unordered_cmds[command_nonce] = uc;
    send_unordered_command(uc);
}

void
daemon :: enqueue_robust_paxos_command(server_id on_behalf_of,
                                       uint64_t request_nonce,
                                       uint64_t command_nonce,
                                       uint64_t min_slot,
                                       slot_type t,
                                       const std::string& command)
{
    unordered_command* uc = new unordered_command(on_behalf_of, request_nonce, t, command);
    uc->set_command_nonce(command_nonce);

    {
        po6::threads::mutex::hold hold(&m_unordered_mtx);
        m_unordered_cmds[command_nonce] = uc;
    }

    uc->set_lowest_possible_slot(min_slot);
    uc->set_robust();
    send_unordered_command(uc);
}

void
daemon :: flush_enqueued_commands_with_stale_leader()
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    for (unordered_map_t::iterator it = m_unordered_cmds.begin();
            it != m_unordered_cmds.end(); ++it)
    {
        unordered_command* uc = it->second;

        if (uc->last_used_ballot() < m_acceptor.current_ballot())
        {
            send_unordered_command(uc);
        }
    }
}

void
daemon :: periodic_flush_enqueued_commands(uint64_t)
{
    convert_unassigned_to_unordered();
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    if (!m_unordered_cmds.empty())
    {
        send_unordered_command(m_unordered_cmds.begin()->second);
    }
}

void
daemon :: convert_unassigned_to_unordered()
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    while (!m_unassigned_cmds.empty() && m_unordered_cmds.size() < REPLICANT_COMMANDS_TO_LEADER)
    {
        uint64_t command_nonce;

        if (!generate_nonce(&command_nonce))
        {
            break;
        }

        unordered_command* uc = m_unassigned_cmds.front();
        m_unassigned_cmds.pop_front();
        uc->set_command_nonce(command_nonce);
        m_unordered_cmds[command_nonce] = uc;
        send_unordered_command(uc);
    }
}

void
daemon :: send_unordered_command(unordered_command* uc)
{
    assert(uc->command_nonce() != 0);

    if (!uc->robust())
    {
        uint64_t start;
        uint64_t limit;
        m_replica->window(&start, &limit);
        assert(uc->lowest_possible_slot() <= start);
        uc->set_lowest_possible_slot(start);
    }

    const uint64_t start = uc->lowest_possible_slot();
    const uint64_t limit = start + REPLICANT_SERVER_DRIVEN_NONCE_HISTORY;

    std::string cmd;
    char c = static_cast<char>(uc->type());
    cmd.append(&c, 1);
    c = uc->robust() ? 1 : 0;
    cmd.append(&c, 1);
    char nbuf[8];
    e::pack64be(uc->command_nonce(), nbuf);
    cmd.append(nbuf, 8);
    cmd.append(uc->command());

    if (m_leader.get())
    {
        uc->set_last_used_ballot(m_leader->current_ballot());
        m_leader->propose(this, start, limit, cmd);
    }
    else if (m_acceptor.current_ballot().leader != m_us.id)
    {
        uc->set_last_used_ballot(m_acceptor.current_ballot());
        send_paxos_submit(start, limit, e::slice(cmd));
    }
}

void
daemon :: periodic_maintain(uint64_t)
{
    if (m_scout.get())
    {
        periodic_maintain_scout();
    }
    else if (m_leader.get())
    {
        periodic_maintain_leader();
    }
    else
    {
        periodic_start_scout();
    }
}

void
daemon :: periodic_maintain_scout()
{
    assert(m_scout.get());
    std::vector<server_id> sids = m_scout->missing();

    for (size_t i = 0; i < sids.size(); ++i)
    {
        send_paxos_phase1a(sids[i], m_scout->current_ballot());
    }
}

void
daemon :: periodic_maintain_leader()
{
    assert(m_leader.get());
    m_leader->send_all_proposals(this);
}

void
daemon :: periodic_start_scout()
{
    if (m_scout_wait_cycles == 0)
    {
        m_scout_wait_cycles =  1ULL << m_config.index(m_us.id);
    }
    else if (m_scout_wait_cycles == 1)
    {
        m_scout_wait_cycles = 0;
    }
    else
    {
        --m_scout_wait_cycles;
        return;
    }

    ballot next_ballot(m_acceptor.current_ballot().number + 1, m_us.id);

    if (m_replica->discontinuous())
    {
        LOG(INFO) << "starting scout for " << next_ballot
                  << " because our ledger is discontinuous";
    }
    else if (m_acceptor.current_ballot().leader == server_id())
    {
        LOG(INFO) << "starting scout for " << next_ballot
                  << " because there is no ballot floating around";
    }
    else if (m_acceptor.current_ballot().leader == m_us.id)
    {
        LOG(INFO) << "starting scout for " << next_ballot
                  << " because the currently adopted ballot"
                  << " comes from this server in a previous"
                  << " execution";
    }
    else if (m_ft.suspect_failed(m_acceptor.current_ballot().leader, m_replica->current_settings().SUSPECT_TIMEOUT))
    {
        LOG(INFO) << "starting scout for " << next_ballot
                  << " because we suspect "
                  << m_acceptor.current_ballot().leader
                  << " is incapbable of leading";
    }
    else
    {
        return;
    }

    std::vector<server_id> servers = m_config.server_ids();
    m_scout.reset(new scout(next_ballot, &servers[0], servers.size()));
    uint64_t start;
    uint64_t limit;
    m_replica->window(&start, &limit);
    m_scout->set_window(start, limit);
    periodic_maintain_scout();
}

void
daemon :: periodic_warn_scout_stuck(uint64_t)
{
    if (!m_scout.get())
    {
        return;
    }

    std::vector<server_id> missing = m_scout->missing();
    bool all_missing_are_suspected = true;

    for (size_t i = 0; i < missing.size(); ++i)
    {
        if (!m_ft.suspect_failed(missing[i], m_replica->current_settings().SUSPECT_TIMEOUT))
        {
            all_missing_are_suspected = false;
        }
    }

    if (!m_scout->adopted() && all_missing_are_suspected)
    {
        LOG(INFO) << *m_scout << " is not making progress because too many servers are offline";
        const size_t sz = m_scout->acceptors().size();
        const size_t quorum = quorum_calc(sz);
        assert(missing.size() <= sz);
        const size_t not_missing = sz - missing.size();
        LOG(INFO) << "bring " << (quorum - not_missing)
                  << " or more of the following servers online to restore liveness:";

        for (size_t i = 0; i < missing.size(); ++i)
        {
            const server* s = m_config.get(missing[i]);
            assert(s);
            LOG(INFO) << *s;
        }
    }
}

bool
daemon :: post_config_change_hook()
{
    if (!m_config.has(m_us.id))
    {
        LOG(WARNING) << "exiting because we were removed from the configuration";
        m_scout.reset();
        m_leader.reset();
        __sync_fetch_and_add(&s_interrupts, 1);
        return false;
    }

    m_ft.assume_all_alive();
    m_busybee_controller.clear_aux();
    return true;
}

std::string
daemon :: construct_become_member_command(const server& s)
{
    const size_t sz = pack_size(SLOT_SERVER_BECOME_MEMBER)
                    + sizeof(uint8_t)
                    + sizeof(uint64_t)
                    + pack_size(s);
    std::auto_ptr<e::buffer> cmd(e::buffer::create(sz));
    cmd->pack_at(0)
        << SLOT_SERVER_BECOME_MEMBER << uint8_t(0) << uint64_t(0) << s;
    return std::string(cmd->as_slice().cdata(), cmd->size());
}

void
daemon :: process_server_become_member(server_id si,
                                       std::auto_ptr<e::buffer>,
                                       e::unpacker up)
{
    server s;
    up = up >> s;
    CHECK_UNPACK(SERVER_BECOME_MEMBER, up);
    LOG(INFO) << "received request from " << s << " to become a member";
    LOG(WARNING) << s << " is using an old method to join the cluster that is deprecated as of Replicant 0.9; "
                         "please upgrade all machines to 0.9 before upgrading to a future release";

    if (m_replica.get() && m_replica->any_config_has(s.id))
    {
        LOG(INFO) << "request ignored because the ID is already in use";
    }
    else if (m_replica.get() && m_replica->any_config_has(s.bind_to))
    {
        LOG(INFO) << "request ignored because the address is already in use";
    }
    else
    {
        LOG(INFO) << "submitting the request to the cluster for consensus";
        send_paxos_submit(0, UINT64_MAX, construct_become_member_command(s));
    }

    send_bootstrap(si);
}

void
daemon :: periodic_check_address(uint64_t)
{
    const server* s = m_config.get(m_us.id);

    if (!s || s->bind_to == m_us.bind_to)
    {
        return;
    }

    LOG(WARNING) << "configuration says " << m_us.id << " is bound to "
                 << s->bind_to << ", but it is bound to " << m_us.bind_to
                 << "; initiating a config change to correct the configuration "
                 << "to match reality";
    std::string cmd;
    e::packer pa(&cmd);
    pa = pa << m_us;
    enqueue_paxos_command(SLOT_SERVER_CHANGE_ADDRESS, cmd);
}

void
daemon :: process_unique_number(server_id si,
                                std::auto_ptr<e::buffer> msg,
                                e::unpacker up)
{
    uint64_t client_nonce;
    up = up >> client_nonce;
    CHECK_UNPACK(UNIQUE_NUMBER, up);
    uint64_t cluster_nonce;

    if (!generate_nonce(&cluster_nonce))
    {
        process_when_nonces_available(si, msg);
        return;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CLIENT_RESPONSE << client_nonce << cluster_nonce;
    send(si, msg);
}

void
daemon :: periodic_generate_nonce_sequence(uint64_t)
{
    if (m_unique_token > 0 && m_unique_base > 0 && m_unique_offset < REPLICANT_NONCE_INCREMENT)
    {
        return;
    }

    uint64_t new_token;

    if (!generate_token(&new_token))
    {
        LOG(ERROR) << "could not read from /dev/urandom";
        return;
    }

    const size_t sz = pack_size(SLOT_INCREMENT_COUNTER)
                    + pack_size(m_us.id)
                    + sizeof(uint8_t)
                    + 2 * sizeof(uint64_t);
    std::auto_ptr<e::buffer> cmd(e::buffer::create(sz));
    cmd->pack_at(0)
        << SLOT_INCREMENT_COUNTER << uint8_t(0) << uint64_t(0) << m_us.id << new_token;
    send_paxos_submit(0, UINT64_MAX, std::string(cmd->as_slice().cdata(), cmd->size()));
    m_unique_token = new_token;
}

void
daemon :: callback_nonce_sequence(server_id si, uint64_t token, uint64_t counter)
{
    if (si == m_us.id && token == m_unique_token)
    {
        m_unique_base = counter;
        m_unique_offset = 0;

        while (!m_msgs_waiting_for_nonces.empty())
        {
            si = m_msgs_waiting_for_nonces.front().si;
            std::auto_ptr<e::buffer> msg(m_msgs_waiting_for_nonces.front().msg);
            m_msgs_waiting_for_nonces.pop_front();
            m_busybee->deliver(si.get(), msg);
        }

        convert_unassigned_to_unordered();
    }
}

bool
daemon :: generate_nonce(uint64_t* nonce)
{
    if (m_unique_base > 0 && m_unique_offset < REPLICANT_NONCE_INCREMENT)
    {
        *nonce = m_unique_base + m_unique_offset;
        ++m_unique_offset;

        if (m_unique_offset + REPLICANT_NONCE_GENERATE_WHEN_FEWER_THAN == REPLICANT_NONCE_INCREMENT)
        {
            m_unique_token = 0;
            periodic_generate_nonce_sequence(0);
        }

        return true;
    }

    return false;
}

void
daemon :: process_when_nonces_available(server_id si, std::auto_ptr<e::buffer> msg)
{
    m_msgs_waiting_for_nonces.push_back(deferred_msg(0, si, msg.release()));
}

void
daemon :: process_object_failed(server_id si,
                                std::auto_ptr<e::buffer>,
                                e::unpacker)
{
    if (si == m_us.id)
    {
        m_replica->enqueue_failed_objects();
    }
}

void
daemon :: periodic_maintain_objects(uint64_t)
{
    m_replica->clean_dead_objects();
    m_replica->keepalive_objects();
}

void
daemon :: callback_condition(server_id si,
                             uint64_t nonce,
                             uint64_t state,
                             const std::string& _data)
{
    e::slice data(_data);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(REPLICANT_SUCCESS)
                    + sizeof(uint64_t)
                    + pack_size(data);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CLIENT_RESPONSE << nonce << REPLICANT_SUCCESS << state << data;
    send_from_non_main_thread(si, msg);
}

void
daemon :: callback_enqueued(uint64_t command_nonce,
                            server_id* si,
                            uint64_t* request_nonce)
{
    *si = server_id();
    *request_nonce = 0;
    convert_unassigned_to_unordered();
    po6::threads::mutex::hold hold(&m_unordered_mtx);
    unordered_map_t::iterator it = m_unordered_cmds.find(command_nonce);

    if (it == m_unordered_cmds.end())
    {
        return;
    }

    unordered_command* uc = it->second;
    assert(uc->command_nonce() == command_nonce);
    m_unordered_cmds.erase(it);
    *si = uc->on_behalf_of();
    *request_nonce = uc->request_nonce();
    delete uc;
}

void
daemon :: callback_client(server_id si, uint64_t nonce,
                          replicant_returncode status,
                          const std::string& result)
{
    e::slice output(result);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(status)
                    + pack_size(output);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CLIENT_RESPONSE << nonce << status << output;
    send_from_non_main_thread(si, msg);
}

void
daemon :: process_poke(server_id si,
                       std::auto_ptr<e::buffer>,
                       e::unpacker up)
{
    uint64_t client_nonce;
    up = up >> client_nonce;
    CHECK_UNPACK(POKE, up);

    std::ostringstream ostr;
    po6::net::location addr;

    if (m_busybee->get_addr(si.get(), &addr) == BUSYBEE_SUCCESS)
    {
        ostr << m_us << " poked by " << addr << "/nonce(" << client_nonce << ")";
    }
    else
    {
        ostr << m_us << " poked by " << si << "/nonce(" << client_nonce << ")";
    }

    enqueue_paxos_command(si, client_nonce, SLOT_POKE, ostr.str());
}

void
daemon :: process_cond_wait(server_id si,
                            std::auto_ptr<e::buffer>,
                            e::unpacker up)
{
    uint64_t client_nonce;
    e::slice obj;
    e::slice cond;
    uint64_t state;
    up = up >> client_nonce >> obj >> cond >> state;
    CHECK_UNPACK(COND_WAIT, up);
    m_replica->cond_wait(si, client_nonce, obj, cond, state);
}

void
daemon :: process_call(server_id si,
                       std::auto_ptr<e::buffer>,
                       e::unpacker up)
{
    uint64_t client_nonce;
    up = up >> client_nonce;
    CHECK_UNPACK(CALL, up);
    e::slice command(up.remainder());
    enqueue_paxos_command(si, client_nonce, SLOT_CALL, std::string(command.cdata(), command.size()));
}

void
daemon :: process_get_robust_params(server_id si,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up)
{
    uint64_t client_nonce;
    up = up >> client_nonce;
    CHECK_UNPACK(GET_ROBUST_PARAMS, up);
    uint64_t cluster_nonce;

    if (!generate_nonce(&cluster_nonce))
    {
        process_when_nonces_available(si, msg);
        return;
    }

    uint64_t start;
    uint64_t limit;
    m_replica->window(&start, &limit);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t)
                    + sizeof(uint64_t);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_CLIENT_RESPONSE << client_nonce << cluster_nonce << start;
    send(si, msg);
}

void
daemon :: process_call_robust(server_id si,
                              std::auto_ptr<e::buffer>,
                              e::unpacker up)
{
    uint64_t client_nonce;
    uint64_t command_nonce;
    uint64_t min_slot;
    up = up >> client_nonce >> command_nonce >> min_slot;
    CHECK_UNPACK(CALL_ROBUST, up);
    replicant_returncode status;
    std::string output;

    if (m_replica->has_output(command_nonce, min_slot, &status, &output))
    {
        callback_client(si, client_nonce, status, output);
        return;
    }

    e::slice command(up.remainder());
    enqueue_robust_paxos_command(si, client_nonce, command_nonce, min_slot, SLOT_CALL, std::string(command.cdata(), command.size()));
}

void
daemon :: periodic_tick(uint64_t)
{
    if (!m_leader.get())
    {
        return;
    }

    const uint64_t tick = m_replica->last_tick();
    std::string cmd;
    e::packer pa(&cmd);
    pa = pa << m_replica->last_tick();
    enqueue_paxos_command(SLOT_TICK, cmd);
    const uint64_t DEFEND_TIMEOUT = m_replica->current_settings().DEFEND_TIMEOUT;

    if (tick >= DEFEND_TIMEOUT)
    {
        m_replica->set_defense_threshold(tick - DEFEND_TIMEOUT);
    }
}

void
daemon :: send_ping(server_id to)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PING)
              + pack_size(m_acceptor.current_ballot());
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PING << m_acceptor.current_ballot();
    send(to, msg);
}

void
daemon :: process_ping(server_id si,
                       std::auto_ptr<e::buffer>,
                       e::unpacker up)
{
    ballot b;
    up = up >> b;
    CHECK_UNPACK(PING, up);
    send_pong(si);
}

void
daemon :: send_pong(server_id to)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PONG);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PONG;
    send(to, msg);
}

void
daemon :: process_pong(server_id si,
                       std::auto_ptr<e::buffer>,
                       e::unpacker)
{
    if (si != m_acceptor.current_ballot().leader)
    {
        m_ft.proof_of_life(si);
    }
}

void
daemon :: periodic_ping_servers(uint64_t)
{
    const std::vector<server>& servers(m_config.servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id != m_us.id)
        {
            send_ping(servers[i].id);
        }
    }
}

void
daemon :: rebootstrap(bootstrap bs)
{
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    const std::vector<po6::net::hostname>& hosts(bs.hosts());
    timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 50 * 1000ULL * 1000ULL;
    uint64_t count = 0;

    while (__sync_fetch_and_add(&s_interrupts, 0) == 0 &&
           e::atomic::load_32_nobarrier(&m_bootstrap_stop) == 0)
    {
        nanosleep(&ts, NULL);
        ++count;

        if (count % 20 != 0)
        {
            continue;
        }

        configuration config;

        {
            po6::threads::mutex::hold hold(&m_config_mtx);
            config = m_config;
        }

        for (size_t i = 0; i < hosts.size(); ++i)
        {
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(REPLNET_WHO_ARE_YOU);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_WHO_ARE_YOU;
            std::auto_ptr<busybee_single> bbs(busybee_single::create(hosts[i]));

            if (bbs->send(msg) != BUSYBEE_SUCCESS)
            {
                continue;
            }

            if (bbs->recv(1000, &msg) != BUSYBEE_SUCCESS)
            {
                continue;
            }

            network_msgtype mt = REPLNET_NOP;
            server s;
            e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
            up >> mt >> s;

            if (up.error() || mt != REPLNET_IDENTITY)
            {
                continue;
            }

            const server* sptr = config.get(s.id);

            if (!sptr || sptr->bind_to != s.bind_to)
            {
                m_busybee_controller.add_aux(s);
            }
        }
    }
}

bool
daemon :: send(server_id si, std::auto_ptr<e::buffer> msg)
{
    if (si == m_us.id)
    {
        return m_busybee->deliver(si.get(), msg);
    }

    busybee_returncode rc = m_busybee->send(si.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SEE_ERRNO:
            LOG(ERROR) << "could not send message: " << po6::strerror(errno);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
            LOG(ERROR) << "could not send message: " << rc;
        default:
            return false;
    }
}

bool
daemon :: send_from_non_main_thread(server_id si, std::auto_ptr<e::buffer> msg)
{
    busybee_server* bb = NULL;

    while (!(bb = e::atomic::load_ptr_acquire(&m_busybee)) &&
           __sync_fetch_and_add(&s_interrupts, 0) == 0)
    {
        struct timespec tv;
        tv.tv_sec = 0;
        tv.tv_nsec = 1000ULL * 1000ULL;
        nanosleep(&tv, NULL);
    }

    if (__sync_fetch_and_add(&s_interrupts, 0) > 0)
    {
        return true;
    }

    if (si == m_us.id)
    {
        return bb->deliver(si.get(), msg);
    }

    busybee_returncode rc = bb->send(si.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SEE_ERRNO:
            LOG(ERROR) << "could not send message: " << po6::strerror(errno);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
            LOG(ERROR) << "could not send message: " << rc;
        default:
            return false;
    }
}

bool
daemon :: send_when_acceptor_persistent(server_id si, std::auto_ptr<e::buffer> msg)
{
    m_msgs_waiting_for_persistence.push_back(deferred_msg(m_acceptor.write_cut(), si, msg.release()));
    return true;
}

void
daemon :: flush_acceptor_messages()
{
    uint64_t when = m_acceptor.sync_cut();

    while (!m_msgs_waiting_for_persistence.empty() && m_msgs_waiting_for_persistence.front().when <= when)
    {
        deferred_msg* dm = &m_msgs_waiting_for_persistence.front();
        std::auto_ptr<e::buffer> msg(dm->msg);
        send(dm->si, msg);
        m_msgs_waiting_for_persistence.pop_front();
    }
}

void
daemon :: debug_dump()
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);
    LOG(INFO) << "============================ Debug Dump Begins Here ============================";
    LOG(INFO) << "we are " << m_us;
    LOG(INFO) << "our configuration is " << m_config;
    LOG(INFO) << "we have " << m_unordered_cmds.size() << " unordered commands";
    LOG(INFO) << "we have " << m_unassigned_cmds.size() << " unassigned commands";
    LOG(INFO) << "--------------------------------------------------------------------------------";
    LOG(INFO) << "Acceptor: currently adopted " << m_acceptor.current_ballot() << " and accepted pvalues:";
    const std::vector<pvalue>& pvals(m_acceptor.pvals());

    for (size_t i = 0; i < pvals.size(); ++i)
    {
        LOG(INFO) << pvals[i];
    }

    LOG(INFO) << "--------------------------------------------------------------------------------";

    if (m_scout.get())
    {
        LOG(INFO) << "Scout: " << m_scout->current_ballot();
        LOG(INFO) << "window: [" << m_scout->window_start() << ", " << m_scout->window_limit() << ")";

        for (size_t i = 0; i < m_scout->acceptors().size(); ++i)
        {
            LOG(INFO) << "acceptor[" << i << "] = " << m_scout->acceptors()[i];
        }

        for (size_t i = 0; i < m_scout->taken_up().size(); ++i)
        {
            LOG(INFO) << "taken-by[" << i << "] = " << m_scout->taken_up()[i];
        }

        LOG(INFO) << "adopted = " << (m_scout->adopted() ? "yes" : "no");
        LOG(INFO) << "pvals:";

        for (size_t i = 0; i < m_scout->pvals().size(); ++i)
        {
            LOG(INFO) << m_scout->pvals()[i];
        }
    }
    else
    {
        LOG(INFO) << "Scout: none";
    }

    LOG(INFO) << "--------------------------------------------------------------------------------";

    if (m_leader.get())
    {
        LOG(INFO) << "Leader: " << m_leader->current_ballot();
        LOG(INFO) << "window: [" << m_leader->window_start() << ", " << m_leader->window_limit() << ")";
        LOG(INFO) << "a response from " << m_leader->quorum_size() << " of the following is a quorum of acceptors:";

        for (size_t i = 0; i < m_leader->acceptors().size(); ++i)
        {
            LOG(INFO) << "acceptor[" << i << "] = " << m_leader->acceptors()[i];
        }
    }
    else
    {
        LOG(INFO) << "Leader: none";
    }

    LOG(INFO) << "--------------------------------------------------------------------------------";

    if (m_replica.get())
    {
        LOG(INFO) << "Replica: " << m_replica->config();
        uint64_t start;
        uint64_t limit;
        m_replica->window(&start, &limit);
        LOG(INFO) << "window: [" << start << ", " << limit << ")";
        LOG(INFO) << "gc: " << m_replica->gc_up_to();
        LOG(INFO) << "discontinuous: " << (m_replica->discontinuous() ? "yes" : "no");
        std::vector<configuration> configs(m_replica->configs().begin(),
                                           m_replica->configs().end());

        for (size_t i = 0; i < configs.size(); ++i)
        {
            LOG(INFO) << "config[" << configs[i].first_slot() << "] = " << configs[i];
        }
    }
    else
    {
        LOG(INFO) << "Replica: none";
    }

    LOG(INFO) << "============================= Debug Dump Ends Here =============================";
}

struct daemon::periodic
{
    periodic()
        : interval_nanos(UINT64_MAX), next_run(0), fp() {}
    periodic(uint64_t in, periodic_fptr f)
        : interval_nanos(in), next_run(0), fp(f) {}
    ~periodic() throw () {}

    uint64_t interval_nanos;
    uint64_t next_run;
    periodic_fptr fp;
};

void
daemon :: register_periodic(unsigned interval_ms, periodic_fptr fp)
{
    uint64_t interval_nanos = interval_ms;
    interval_nanos *= 1000000ULL;
    m_periodic.push_back(periodic(interval_nanos, fp));
}

// round x up to the lowest multiple of y greater than x
static uint64_t
next_interval(uint64_t x, uint64_t y)
{
    uint64_t z = ((x + y) / y) * y;
    assert(x < z);
    return z;
}

void
daemon :: run_periodic()
{
    uint64_t now = po6::monotonic_time();

    for (size_t i = 0; i < m_periodic.size(); ++i)
    {
        if (m_periodic[i].next_run <= now)
        {
            (this->*m_periodic[i].fp)(now);
            m_periodic[i].next_run = next_interval(now, m_periodic[i].interval_nanos); 
        }
    }
}
