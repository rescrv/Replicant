// Copyright (c) 2012-2015, Robert Escriva
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
#include <cmath>

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
#include <po6/pathname.h>

// e
#include <e/compat.h>
#include <e/endian.h>
#include <e/envconfig.h>
#include <e/guard.h>
#include <e/error.h>
#include <e/strescape.h>
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_mta.h>
#include <busybee_single.h>

// Replicant
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
static uint64_t
monotonic_time()
{
    timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    {
        throw po6::error(errno);
    }

    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

daemon :: daemon()
    : m_s()
    , m_gc()
    , m_gc_ts()
    , m_busybee_mapper(&m_config_mtx, &m_config)
    , m_busybee()
    , m_busybee_init(0)
    , m_us()
    , m_config_mtx()
    , m_config()
    , m_bootstrap()
    , m_periodic()
    , m_unique_token(0)
    , m_unique_base(0)
    , m_unique_offset(0)
    , m_last_seen()
    , m_suspect_counts()
    , m_unordered_mtx()
    , m_unordered_cmds()
    , m_first_unordered(0)
    , m_deferred_msgs()
    , m_msgs_waiting_for_nonces()
    , m_acceptor()
    , m_highest_ballot()
    , m_first_scout(true)
    , m_scout()
    , m_scouts_since_last_leader(0)
    , m_scout_wait_cycles(0)
    , m_leader()
    , m_replica()
    , m_last_replica_snapshot(0)
    , m_last_gc_slot(0)
{
    register_periodic(25, &daemon::periodic_scout);
    register_periodic(25, &daemon::periodic_abdicate);
    register_periodic(10 * 1000, &daemon::periodic_warn_scout_stuck);
    register_periodic(1000, &daemon::periodic_submit_dead_nodes);
    register_periodic(1000, &daemon::periodic_generate_nonce_sequence);
    register_periodic(10, &daemon::periodic_ping_acceptors);
    register_periodic(100, &daemon::periodic_flush_enqueued_commands);
    register_periodic(1000, &daemon::periodic_clean_dead_objects);
    register_periodic(1000, &daemon::periodic_tick);
    m_gc.register_thread(&m_gc_ts);
}

daemon :: ~daemon() throw ()
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    while (!m_unordered_cmds.empty())
    {
        delete m_unordered_cmds.front();
        m_unordered_cmds.pop_front();
    }

    while (!m_deferred_msgs.empty())
    {
        delete m_deferred_msgs.front().msg;
        m_deferred_msgs.pop_front();
    }

    while (!m_msgs_waiting_for_nonces.empty())
    {
        delete m_msgs_waiting_for_nonces.front().msg;
        m_msgs_waiting_for_nonces.pop_front();
    }

    m_gc.deregister_thread(&m_gc_ts);
}

static bool
install_signal_handler(int signum, void (*f)(int))
{
    struct sigaction handle;
    handle.sa_handler = f;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

int
daemon :: run(bool daemonize,
              po6::pathname data,
              po6::pathname log,
              po6::pathname pidfile,
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
    if (!install_signal_handler(SIGHUP, exit_on_signal))
    {
        std::cerr << "could not install SIGHUP handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGINT, exit_on_signal))
    {
        std::cerr << "could not install SIGINT handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGTERM, exit_on_signal))
    {
        std::cerr << "could not install SIGTERM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGUSR1, handle_debug_dump))
    {
        std::cerr << "could not install SIGUSR2 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGUSR2, handle_debug_mode))
    {
        std::cerr << "could not install SIGUSR2 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return EXIT_FAILURE;
    }

    sigdelset(&ss, SIGPROF);

    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return EXIT_FAILURE;
    }

    google::LogToStderr();

    if (daemonize)
    {
        struct stat x;

        if (lstat(log.get(), &x) < 0 || !S_ISDIR(x.st_mode))
        {
            LOG(ERROR) << "cannot fork off to the background because "
                       << log.get() << " does not exist or is not writable";
            return EXIT_FAILURE;
        }

        if (!has_pidfile)
        {
            LOG(INFO) << "forking off to the background";
            LOG(INFO) << "you can find the log at " << log.get() << "/replicant-daemon-YYYYMMDD-HHMMSS.sssss";
            LOG(INFO) << "provide \"--foreground\" on the command-line if you want to run in the foreground";
        }

        google::SetLogSymlink(google::INFO, "");
        google::SetLogSymlink(google::WARNING, "");
        google::SetLogSymlink(google::ERROR, "");
        google::SetLogSymlink(google::FATAL, "");
        log = po6::join(log, "replicant-daemon-");
        google::SetLogDestination(google::INFO, log.get());

        if (::daemon(1, 0) < 0)
        {
            PLOG(ERROR) << "could not daemonize";
            return EXIT_FAILURE;
        }

        if (has_pidfile)
        {
            char buf[21];
            ssize_t buf_sz = sprintf(buf, "%d\n", getpid());
            assert(buf_sz < static_cast<ssize_t>(sizeof(buf)));
            po6::io::fd pid(open(pidfile.get(), O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));

            if (pid.get() < 0 || pid.xwrite(buf, buf_sz) != buf_sz)
            {
                PLOG(ERROR) << "could not create pidfile " << pidfile.get();
                return EXIT_FAILURE;
            }
        }
    }
    else
    {
        LOG(INFO) << "running in the foreground";
        LOG(INFO) << "no log will be generated; instead, the log messages will print to the terminal";
        LOG(INFO) << "provide \"--daemon\" on the command-line if you want to run in the background";
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

    // case 1:  start a new cluster
    if (!saved && !set_existing)
    {
        uint64_t cluster;
        uint64_t this_server;

        if (!generate_token(&cluster) ||
            !generate_token(&this_server))
        {
            PLOG(ERROR) << "could not read random tokens from /dev/urandom";
            return EXIT_FAILURE;
        }

        m_us.id = server_id(this_server);
        m_config_mtx.lock();
        m_config = configuration(cluster_id(cluster), version_id(1), 0, &m_us, 1);
        m_config_mtx.unlock();
        LOG(INFO) << "started " << m_config.cluster();
        init = init_obj && init_lib;

        m_acceptor.adopt(ballot(1, m_us.id));
        m_acceptor.accept(pvalue(m_acceptor.current_ballot(), 0, construct_become_member_command(m_us)));
        m_replica.reset(new replica(this, m_config));
        uint64_t snapshot_slot;
        e::slice snapshot;
        std::auto_ptr<e::buffer> snapshot_backing;
        m_replica->take_blocking_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

        if (!m_acceptor.record_snapshot(snapshot_slot, snapshot))
        {
            LOG(ERROR) << "error saving starting replica state to disk: " << e::error::strerror(errno);
            return EXIT_FAILURE;
        }
    }
    // case 2: joining a new cluster
    else if (!saved && set_existing)
    {
        uint64_t this_server;

        if (!generate_token(&this_server))
        {
            PLOG(ERROR) << "could not read random tokens from /dev/urandom";
            return EXIT_FAILURE;
        }

        m_us.id = server_id(this_server);
        join_the_cluster(existing);

        if (m_config.has(m_us.bind_to) && !m_config.has(m_us.id))
        {
            LOG(ERROR) << "configuration already has a server on our address";
            LOG(ERROR) << "use the command line tools to remove said server and restart this one";
            return EXIT_FAILURE;
        }

        if (!m_replica.get())
        {
            return EXIT_FAILURE;
        }

        saved_bootstrap = existing;
    }
    else
    {
        LOG(INFO) << "re-joining cluster as " << saved_us.id;
        m_us.id = saved_us.id;

        if (!set_bind_to)
        {
            m_us.bind_to = saved_us.bind_to;
        }

        if (set_existing)
        {
            saved_bootstrap = existing;
        }

        e::slice snapshot;
        std::auto_ptr<e::buffer> snapshot_backing;

        if (!m_acceptor.load_latest_snapshot(&snapshot, &snapshot_backing))
        {
            LOG(ERROR) << "error loading replica state from disk: " << e::error::strerror(errno);
            return EXIT_FAILURE;
        }

        m_replica.reset(replica::from_snapshot(this, snapshot));

        if (m_replica.get() && !m_replica->config().has(m_us.id))
        {
            m_replica.reset();
            join_the_cluster(saved_bootstrap);
        }

        if (!m_replica.get())
        {
            if (__sync_fetch_and_add(&s_interrupts, 0) == 0)
            {
                LOG(ERROR) << "gave up trying to join the cluster after 10s";
            }

            return EXIT_FAILURE;
        }
    }

    if (!m_acceptor.save(m_us, saved_bootstrap))
    {
        return EXIT_FAILURE;
    }

    assert(m_replica.get());
    //assert(m_replica->last_snapshot_num() > 0);
    m_config_mtx.lock();
    m_config = m_replica->config();
    m_config_mtx.unlock();

    if (!init && init_rst)
    {
        LOG(INFO) << "asked to restore from \"" << e::strescape(init_rst) << "\" "
                  << "but we are not initializing a new cluster";
        LOG(INFO) << "the restore operations only have an effect when "
                  << "starting a fresh cluster";
        LOG(INFO) << "this likely means you'll want to start with a new data-dir "
                  << "and omit any options for connecting to an existing cluster";
        return EXIT_FAILURE;
    }

    m_busybee.reset(new busybee_mta(&m_gc, &m_busybee_mapper, m_us.bind_to, m_us.id.get(), 0/*we don't use pause/unpause*/));
    e::atomic::store_32_release(&m_busybee_init, 1);

    if (!post_config_change_hook())
    {
        return EXIT_SUCCESS;
    }

    if (init)
    {
        assert(init_obj);
        assert(init_lib);
        std::vector<char> lib(sizeof(uint64_t) + sizeof(uint32_t));

        // Encode the object name
        assert(strlen(init_obj) <= sizeof(uint64_t));
        memset(&lib[0], 0, sizeof(lib.size()));
        memmove(&lib[0], init_obj, strlen(init_obj));
        uint64_t obj = 0;
        e::unpack64be(&lib[0], &obj);

        // Read the library
        char buf[4096];
        po6::io::fd fd(open(init_lib, O_RDONLY));

        if (fd.get() < 0)
        {
            PLOG(ERROR) << "could not open library";
            return EXIT_FAILURE;
        }

        ssize_t amt = 0;

        while ((amt = fd.xread(buf, 4096)) > 0)
        {
            size_t tmp = lib.size();
            lib.resize(tmp + amt);
            memmove(&lib[tmp], buf, amt);
        }

        if (amt < 0)
        {
            PLOG(ERROR) << "could not read library";
            return EXIT_FAILURE;
        }

        // XXX
#if 0
        // if this is a restore
        if (init_rst)
        {
            size_t offset = lib.size();
            lib.resize(offset + sizeof(uint32_t));
            fd = open(init_rst, O_RDONLY);

            if (fd.get() < 0)
            {
                PLOG(ERROR) << "could not open restore file";
                return EXIT_FAILURE;
            }

            while ((amt = fd.xread(buf, 4096)) > 0)
            {
                size_t tmp = lib.size();
                lib.resize(tmp + amt);
                memmove(&lib[tmp], buf, amt);
            }

            if (amt < 0)
            {
                PLOG(ERROR) << "could not read restore file";
                return EXIT_FAILURE;
            }

            uint32_t lib_sz = offset - sizeof(uint64_t) - sizeof(uint32_t);
            uint32_t rst_sz = lib.size() - offset - sizeof(uint32_t);
            e::pack32be(lib_sz, &lib[sizeof(uint64_t)]);
            e::pack32be(rst_sz, &lib[offset]);
            e::slice cmd_slice(&lib[0], lib.size());
            issue_command(1, OBJECT_OBJ_RESTORE, 0, 1, cmd_slice);
            LOG(INFO) << "restoring " << init_obj << " from \"" << e::strescape(init_rst) << "\"";
        }
        // else this is an initialization
        else
        {
            e::pack32be(lib.size() - sizeof(uint64_t) - sizeof(uint32_t),
                        &lib[sizeof(uint64_t)]);
            e::slice cmd_slice(&lib[0], lib.size());
            issue_command(1, OBJECT_OBJ_NEW, 0, 1, cmd_slice);
            LOG(INFO) << "initializing " << init_obj << " with \"" << e::strescape(init_lib) << "\"";

            if (init_str)
            {
                std::vector<char> init_buf(5 + strlen(init_str) + 1);
                memmove(&init_buf[0], "init\x00", 5);
                memmove(&init_buf[5], init_str, strlen(init_str) + 1);
                e::slice init_slice(&init_buf[0], init_buf.size());
                issue_command(2, obj, 0, 2, init_slice);
            }
        }
#endif
    }

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
        m_busybee->set_timeout(1);
        busybee_returncode rc = m_busybee->recv(&m_gc_ts, &token, &msg);

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
                handle_disruption(server_id(token));
                continue;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "BusyBee returned " << rc << " during a \"recv\" call";
                return false;
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
            case REPLNET_SILENT_BOOTSTRAP:
                process_silent_bootstrap(si, msg, up);
                break;
            case REPLNET_STATE_TRANSFER:
                process_state_transfer(si, msg, up);
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
            case REPLNET_CLIENT_RESPONSE:
                LOG(WARNING) << "dropping \"CLIENT_RESPONSE\" received by server";
                break;
            case REPLNET_GARBAGE:
                LOG(WARNING) << "dropping \"GARBAGE\" received by server";
                break;
            default:
                LOG(WARNING) << "unknown message type; here's some hex:  " << msg->hex();
                break;
        }
    }

    LOG(INFO) << "replicant is gracefully shutting down";

    uint64_t snapshot_slot;
    e::slice snapshot;
    std::auto_ptr<e::buffer> snapshot_backing;
    m_replica->take_blocking_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

    if (snapshot_slot == 0)
    {
        LOG(ERROR) << "could not take snapshot when shutting down";
        return EXIT_FAILURE;
    }
    else if (!m_acceptor.record_snapshot(snapshot_slot, snapshot))
    {
        LOG(ERROR) << "error saving starting replica state to disk: " << e::error::strerror(errno);
        m_replica.reset();
    }

    LOG(INFO) << "replicant will now terminate";
    return EXIT_SUCCESS;
}

void
daemon :: join_the_cluster(const bootstrap& existing)
{
    if (m_replica.get())
    {
        return;
    }

    bool success = false;
    configuration c;

    for (unsigned iteration = 0; __sync_fetch_and_add(&s_interrupts, 0) == 0 && iteration < 100; ++iteration)
    {
        LOG_IF(INFO, iteration == 0) << "bootstrapping off existing cluster using " << existing;
        e::error err;

        if (existing.do_it(&c, &err) != REPLICANT_SUCCESS)
        {
            LOG(ERROR) << err.msg();
            return;
        }

        if (c.has(m_us.id))
        {
            success = true;
            break;
        }

        sigset_t tmp;
        sigset_t old;
        sigemptyset(&tmp);
        pthread_sigmask(SIG_SETMASK, &tmp, &old);
        e::guard g_sigmask = e::makeguard(pthread_sigmask, SIG_SETMASK, &old, (sigset_t*)NULL);

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
        }

        for (size_t i = 0; i < c.servers().size(); ++i)
        {
            busybee_single bs(c.servers()[i].bind_to);
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(REPLNET_SERVER_BECOME_MEMBER)
                            + pack_size(m_us);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << REPLNET_SERVER_BECOME_MEMBER << m_us;

            try
            {
                bs.send(msg);
                bs.set_timeout(10);
                bs.recv(&msg);
            }
            catch (po6::error& e)
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
                c = tmpc;
            }
        }
    }

    if (!success)
    {
        return;
    }

    LOG(INFO) << "joining " << c.cluster() << " as " << m_us.id;

    for (size_t i = 0; !m_replica.get() && i < c.servers().size(); ++i)
    {
        busybee_single bs(c.servers()[i].bind_to);
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(REPLNET_STATE_TRANSFER);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_STATE_TRANSFER;

        try
        {
            bs.send(msg);
            bs.recv(&msg);
        }
        catch (po6::error& e)
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
            m_replica.reset(replica::from_snapshot(this, snapshot));
        }
    }

    if (m_replica.get())
    {
        uint64_t snapshot_slot;
        e::slice snapshot;
        std::auto_ptr<e::buffer> snapshot_backing;
        m_replica->take_blocking_snapshot(&snapshot_slot, &snapshot, &snapshot_backing);

        if (snapshot_slot == 0)
        {
            LOG(ERROR) << "could not take snapshot of new state";
            m_replica.reset();
        }
        else if (!m_acceptor.record_snapshot(snapshot_slot, snapshot))
        {
            LOG(ERROR) << "error saving starting replica state to disk: " << e::error::strerror(errno);
            m_replica.reset();
        }

        m_config_mtx.lock();

        if (m_replica.get())
        {
            m_config = m_replica->config();
        }

        m_config_mtx.unlock();
    }
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
daemon :: process_silent_bootstrap(server_id si,
                                   std::auto_ptr<e::buffer>,
                                   e::unpacker)
{
    send_bootstrap(si);
}

void
daemon :: send_bootstrap(server_id si)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_BOOTSTRAP)
              + pack_size(m_config);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BOOTSTRAP << m_config;
    send(si, msg);
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
        LOG(INFO) << "phase 1a:  taking up " << b;
        flush_enqueued_commands_with_stale_leader();
    }

    LOG_IF(ERROR, si != b.leader) << si << " is misusing " << b;
    send_paxos_phase1b(b.leader);
    observe_ballot(b);
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
            if (!suspect_failed(missing[i]))
            {
                all_missing_are_suspected = false;
            }
        }

        if (all_missing_are_suspected && m_scout->adopted())
        {
            LOG(INFO) << "phase 1 complete: transitioning to phase 2 on " << b;
            m_leader.reset(new leader(*m_scout));
            m_leader->send_all_proposals(this);
            m_scout.reset();
            m_scouts_since_last_leader = 0;
            m_first_scout = false;
        }
    }

    observe_ballot(b);
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
        LOG(INFO) << "TRACE XXX " << si << " " << p.s << " " << m_acceptor.lowest_acceptable_slot();
        return;
    }

    if (si == p.b.leader && p.b == m_acceptor.current_ballot() && p.s >= m_config.first_slot())
    {
        m_acceptor.accept(p);
        LOG_IF(INFO, s_debug_mode) << "p2a: " << p;
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

    observe_ballot(b);
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
            uint64_t fill = m_replica->fill_up_to();

            if (start < fill)
            {
                m_leader->nop_fill(this, fill);
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
                LOG(ERROR) << "could not save snapshot: " << e::error::strerror(errno);
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
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(REPLNET_PAXOS_SUBMIT)
                    + 2 * sizeof(uint64_t)
                    + pack_size(command);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_PAXOS_SUBMIT << slot_start << slot_limit << command;
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
    std::auto_ptr<unordered_command> auc;
    auc.reset(new unordered_command(on_behalf_of, request_nonce, t, command));
    unordered_command* uc = auc.get();
    std::list<unordered_command*> ucl;
    ucl.push_back(uc);
    m_unordered_mtx.lock();
    m_unordered_cmds.splice(m_unordered_cmds.end(), ucl);
    m_unordered_mtx.unlock();
    auc.release();
    uint64_t command_nonce;

    if (!generate_nonce(&command_nonce))
    {
        return;
    }

    uc->set_command_nonce(command_nonce);
    uint64_t start;
    uint64_t limit;
    m_replica->window(&start, &limit);
    uc->set_lowest_possible_slot(start);
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
    std::auto_ptr<unordered_command> auc;
    auc.reset(new unordered_command(on_behalf_of, request_nonce, t, command));
    unordered_command* uc = auc.get();
    std::list<unordered_command*> ucl;
    ucl.push_back(uc);
    m_unordered_mtx.lock();
    m_unordered_cmds.splice(m_unordered_cmds.end(), ucl);
    m_unordered_mtx.unlock();
    auc.release();
    uc->set_command_nonce(command_nonce);
    uc->set_lowest_possible_slot(min_slot);
    uc->set_robust();
    send_unordered_command(uc);
}

void
daemon :: flush_enqueued_commands_with_stale_leader()
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    for (std::list<unordered_command*>::iterator it = m_unordered_cmds.begin();
            it != m_unordered_cmds.end(); ++it)
    {
        unordered_command* uc = *it;

        if (uc->last_used_ballot() < m_acceptor.current_ballot())
        {
            send_unordered_command(uc);
        }
    }
}

void
daemon :: periodic_flush_enqueued_commands(uint64_t)
{
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    if (!m_unordered_cmds.empty() && m_first_unordered != m_unordered_cmds.front()->command_nonce())
    {
        m_first_unordered = m_unordered_cmds.front()->command_nonce();
        return;
    }

    uint64_t counter = 0;

    for (std::list<unordered_command*>::iterator it = m_unordered_cmds.begin();
            counter < REPLICANT_SLOTS_WINDOW && it != m_unordered_cmds.end(); ++it)
    {
        unordered_command* uc = *it;
        send_unordered_command(uc);
        ++counter;
    }
}

void
daemon :: send_unordered_command(unordered_command* uc)
{
    if (uc->command_nonce() == 0)
    {
        uint64_t command_nonce;

        if (!generate_nonce(&command_nonce))
        {
            return;
        }

        uc->set_command_nonce(command_nonce);
    }

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
daemon :: observe_ballot(const ballot& b)
{
    m_highest_ballot = std::max(b, m_highest_ballot);

    if (m_scout.get() && m_scout->current_ballot() < m_highest_ballot)
    {
        LOG(INFO) << "stopping " << *m_scout << " because " << m_highest_ballot << " is floating around";
        m_scout.reset();
    }

    if (m_leader.get() && m_leader->current_ballot() < m_highest_ballot)
    {
        LOG(INFO) << "stopping " << *m_leader << " because " << m_highest_ballot << " is floating around";
        m_leader.reset();
    }
}

void
daemon :: periodic_scout(uint64_t)
{
    if (m_leader.get())
    {
        return;
    }

    if (!m_scout.get() && m_scout_wait_cycles == 0)
    {
        if (!m_first_scout &&
            !m_replica->discontinuous() &&
            m_acceptor.current_ballot().leader != server_id() &&
            m_acceptor.current_ballot().leader != m_us.id &&
            !suspect_failed(m_acceptor.current_ballot().leader))
        {
            return;
        }

        std::vector<server_id> servers = m_config.server_ids();
        ballot next_ballot(m_highest_ballot.number + 1, m_us.id);
        LOG(INFO) << "starting scout for " << next_ballot;
        m_scout.reset(new scout(next_ballot, &servers[0], servers.size()));
        m_scouts_since_last_leader += 1ULL << m_config.index(m_us.id);
        m_scout_wait_cycles = m_scouts_since_last_leader;
        uint64_t start;
        uint64_t limit;
        m_replica->window(&start, &limit);
        m_scout->set_window(start, limit);
    }

    if (m_scout_wait_cycles > 0)
    {
        --m_scout_wait_cycles;
        return;
    }

    assert(m_scout.get());

    if (m_acceptor.current_ballot() > m_scout->current_ballot())
    {
        m_scout.reset();
        return;
    }

    std::vector<server_id> sids = m_scout->missing();

    for (size_t i = 0; i < sids.size(); ++i)
    {
        send_paxos_phase1a(sids[i], m_scout->current_ballot());
    }
}

void
daemon :: periodic_abdicate(uint64_t)
{
    if (!m_leader.get())
    {
        return;
    }

    const size_t quorum = m_leader->quorum_size();
    const std::vector<server_id>& acceptors(m_leader->acceptors());
    size_t not_suspected = 0;

    for (size_t i = 0; i < acceptors.size(); ++i)
    {
        if (!suspect_failed(acceptors[i]))
        {
            ++not_suspected;
        }
    }

    if (not_suspected < quorum)
    {
        LOG(WARNING) << "abdicating leadership of " << m_leader->current_ballot()
                     << " because only " << not_suspected
                     << " servers seem to be alive, and we need " << quorum
                     << " to make progress";
        m_leader.reset();
    }
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
        if (!suspect_failed(missing[i]))
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
    m_bootstrap = m_config.current_bootstrap();

    while (!m_config.has(m_us.id))
    {
        LOG(WARNING) << "exiting because we were removed from the configuration";
        m_scout.reset();
        m_leader.reset();
        __sync_fetch_and_add(&s_interrupts, 1);
        return false;
    }

    // don't move code above to below and vice versa
    // this is a comment barrier

    m_highest_ballot = std::max(m_highest_ballot, m_acceptor.current_ballot());
    m_last_seen.resize(m_config.servers().size());
    m_suspect_counts.resize(m_config.servers().size());
    const uint64_t now = monotonic_time();

    for (size_t i = 0; i < m_last_seen.size(); ++i)
    {
        m_last_seen[i] = now;
        m_suspect_counts[i] = 0;
    }

    periodic_generate_nonce_sequence(now);
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
daemon :: periodic_clean_dead_objects(uint64_t)
{
    m_replica->clean_dead_objects();
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
    po6::threads::mutex::hold hold(&m_unordered_mtx);

    for (std::list<unordered_command*>::iterator it = m_unordered_cmds.begin();
            it != m_unordered_cmds.end(); ++it)
    {
        unordered_command* uc = *it;

        if (uc->command_nonce() == command_nonce)
        {
            m_unordered_cmds.erase(it);
            *si = uc->on_behalf_of();
            *request_nonce = uc->request_nonce();
            delete uc;
            return;
        }
    }
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
                              std::auto_ptr<e::buffer> msg,
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
    std::string cmd;
    e::packer pa(&cmd);
    pa = pa << m_replica->last_tick();
    enqueue_paxos_command(SLOT_TICK, cmd);
}

void
daemon :: send_ping(server_id to)
{
    if (to == m_us.id)
    {
        return;
    }

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
    m_highest_ballot = std::max(b, m_highest_ballot);
}

void
daemon :: send_pong(server_id to)
{
    if (to == m_us.id)
    {
        return;
    }

    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_PONG)
              + pack_size(m_acceptor.current_ballot());
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PONG << m_acceptor.current_ballot();
    send(to, msg);
}

void
daemon :: process_pong(server_id si,
                       std::auto_ptr<e::buffer>,
                       e::unpacker up)
{
    ballot b;
    up = up >> b;
    CHECK_UNPACK(PING, up);
    m_highest_ballot = std::max(b, m_highest_ballot);
    const std::vector<server>& servers(m_config.servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id == si)
        {
            m_last_seen[i] = monotonic_time();
        }
    }
}

void
daemon :: periodic_ping_acceptors(uint64_t)
{
    const std::vector<server>& servers(m_config.servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        send_ping(servers[i].id);
    }
}

bool
daemon :: suspect_failed(server_id si)
{
    assert(!m_last_seen.empty());
    const uint64_t now = monotonic_time();
    const uint64_t last = *std::max_element(m_last_seen.begin(), m_last_seen.end());
    const uint64_t self_suspicion = now - last;

    if (si == m_us.id)
    {
        return false;
    }

    const std::vector<server>& servers(m_config.servers());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id == si)
        {
            const uint64_t diff = now - m_last_seen[i];
            const uint64_t susp = diff - self_suspicion;
            return susp > m_s.SUSPICION_TIMEOUT;
        }
    }

    return !m_config.has(si);
}

void
daemon :: periodic_submit_dead_nodes(uint64_t)
{
    const std::vector<server>& servers(m_config.servers());
    assert(m_last_seen.size() == servers.size());
    assert(m_suspect_counts.size() == servers.size());
    assert(!m_last_seen.empty());
    const uint64_t now = monotonic_time();
    const uint64_t last = *std::max_element(m_last_seen.begin(), m_last_seen.end());
    const uint64_t self_suspicion = now - last;

    for (size_t i = 0; i < servers.size(); ++i)
    {
        const uint64_t diff = now - m_last_seen[i];
        const uint64_t susp = diff - self_suspicion;

        if (servers[i].id != m_us.id && susp > m_s.SUSPICION_TIMEOUT)
        {
            ++m_suspect_counts[i];
        }
        else
        {
            m_suspect_counts[i] = 0;
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
            handle_disruption(si);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
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
    if (e::atomic::load_32_acquire(&m_busybee_init) == 0)
    {
        return false;
    }

    busybee_returncode rc = m_busybee->send(si.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
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
    m_deferred_msgs.push_back(deferred_msg(m_acceptor.write_cut(), si, msg.release()));
    return true;
}

void
daemon :: flush_acceptor_messages()
{
    uint64_t when = m_acceptor.sync_cut();

    while (!m_deferred_msgs.empty() && m_deferred_msgs.front().when <= when)
    {
        deferred_msg* dm = &m_deferred_msgs.front();
        std::auto_ptr<e::buffer> msg(dm->msg);
        send(dm->si, msg);
        m_deferred_msgs.pop_front();
    }
}

void
daemon :: handle_disruption(server_id si)
{
    // suspect a disrupted server immediately
    const std::vector<server>& servers(m_config.servers());
    assert(m_last_seen.size() == servers.size());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (servers[i].id == si)
        {
            m_last_seen[i] = 0;
        }
    }

    // remove invalid messages

    for (std::list<deferred_msg>::iterator it = m_deferred_msgs.begin();
            it != m_deferred_msgs.end(); )
    {
        if (it->si == si)
        {
            it = m_deferred_msgs.erase(it);
        }
        else
        {
            ++it;
        }
    }

    for (std::list<deferred_msg>::iterator it = m_msgs_waiting_for_nonces.begin();
            it != m_msgs_waiting_for_nonces.end(); )
    {
        if (it->si == si)
        {
            it = m_msgs_waiting_for_nonces.erase(it);
        }
        else
        {
            ++it;
        }
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

// round x up to a multiple of y
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
    uint64_t now = monotonic_time();

    for (size_t i = 0; i < m_periodic.size(); ++i)
    {
        if (m_periodic[i].next_run <= now)
        {
            (this->*m_periodic[i].fp)(now);
            m_periodic[i].next_run = next_interval(now, m_periodic[i].interval_nanos); 
        }
    }
}
