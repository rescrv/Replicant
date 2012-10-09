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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// POSIX
#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// po6
#include <po6/pathname.h>

// e
#include <e/endian.h>
#include <e/envconfig.h>
#include <e/timer.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_sta.h>

// Replicant
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/special_objects.h"
#include "daemon/command.h"
#include "daemon/daemon.h"
#include "daemon/heal_next.h"

#define CHECK_UNPACK(MSGTYPE, UNPACKER) \
    do \
    { \
        if (UNPACKER.error()) \
        { \
            /* This is just to verify that MSGTYPE is correct */ \
            replicant_network_msgtype x = REPLNET_ ## MSGTYPE; \
            LOG(WARNING) << "received corrupt \"" xstr(MSGTYPE) "\" message"; \
            x = REPLNET_NOP; \
            return; \
        } \
    } while (0)

static bool s_continue = true;

static void
exit_on_signal(int /*signum*/)
{
    RAW_LOG(ERROR, "signal received; triggering exit");
    s_continue = false;
}

replicant_daemon :: replicant_daemon(po6::net::ipaddr bind_to,
                                     in_port_t incoming,
                                     in_port_t outgoing)
    : m_s()
    , m_busybee(bind_to, incoming, outgoing)
    , m_us()
    , m_confman()
    , m_commands()
    , m_clients()
    , m_objects()
    , m_periodic()
    , m_heal_next()
    , m_heal_prev(false)
    , m_disconnected_iteration(0)
    , m_disconnected()
    , m_disconnected_times()
    , m_marked_for_gc(0)
    , m_snapshots()
{
    m_us.address = bind_to;
    m_us.incoming_port = incoming;
    m_us.outgoing_port = m_busybee.outbound().port;
    trip_periodic(0, &replicant_daemon::periodic_dump_config);
    trip_periodic(0, &replicant_daemon::periodic_garbage_collect_commands_start);
    trip_periodic(0, &replicant_daemon::periodic_list_clients);
    trip_periodic(0, &replicant_daemon::periodic_detach_clients);
}

replicant_daemon :: ~replicant_daemon() throw ()
{
}

int
replicant_daemon :: run(bool d)
{
    if (!install_signal_handlers())
    {
        return EXIT_FAILURE;
    }

    if (!generate_identifier_token())
    {
        return EXIT_FAILURE;
    }

    if (!start_new_cluster())
    {
        return EXIT_FAILURE;
    }

    m_busybee.set_timeout(1);

    if (d)
    {
        if (!daemonize())
        {
            return EXIT_FAILURE;
        }
    }
    else
    {
        google::LogToStderr();
    }

    if (!loop())
    {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

int
replicant_daemon :: run(bool d, po6::net::hostname existing)
{
    if (!install_signal_handlers())
    {
        return EXIT_FAILURE;
    }

    if (!generate_identifier_token())
    {
        return EXIT_FAILURE;
    }

    if (!connect_to_cluster(existing))
    {
        return EXIT_FAILURE;
    }

    m_busybee.set_timeout(5000);
    trip_periodic(0, &replicant_daemon::periodic_set_timeout);

    if (d)
    {
        if (!daemonize())
        {
            return EXIT_FAILURE;
        }
    }
    else
    {
        google::LogToStderr();
    }

    if (!loop())
    {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

bool
replicant_daemon :: install_signal_handlers()
{
    if (!install_signal_handler(SIGHUP, exit_on_signal))
    {
        return false;
    }

    if (!install_signal_handler(SIGINT, exit_on_signal))
    {
        return false;
    }

    if (!install_signal_handler(SIGTERM, exit_on_signal))
    {
        return false;
    }

    return true;
}

bool
replicant_daemon :: install_signal_handler(int signum, void (*func)(int))
{
    struct sigaction handle;
    handle.sa_handler = func;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

bool
replicant_daemon :: generate_identifier_token()
{
    LOG(INFO) << "generating random token";
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        PLOG(ERROR) << "could not open /dev/urandom";
        return false;
    }

    if (sysrand.read(&m_us.token, sizeof(m_us.token)) != sizeof(m_us.token))
    {
        PLOG(ERROR) << "could not read from /dev/urandom";
        return false;
    }

    LOG(INFO) << "random token:  " << m_us.token;
    return true;
}

bool
replicant_daemon :: start_new_cluster()
{
    configuration zero(0, m_us);
    configuration initial(1, m_us);
    m_confman.reset(zero);
    m_confman.add_proposed(initial);
    accept_config(initial);
    return true;
}

bool
replicant_daemon :: connect_to_cluster(po6::net::hostname hn)
{
    po6::net::location loc = hn.lookup(SOCK_STREAM, IPPROTO_TCP);
    LOG(INFO) << "connecting to existing cluster " << hn
              << " which resolves to " << loc;
    std::auto_ptr<e::buffer> msg(e::buffer::create(BUSYBEE_HEADER_SIZE + sizeof(uint8_t)));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << static_cast<uint8_t>(REPLNET_JOIN);
    // Don't use "send" here because the only acceptable outcome is SUCCESS and
    // there is nothing to do but fail if this doesn't fail.
    busybee_returncode rc = m_busybee.send(loc, msg);

    if (rc != BUSYBEE_SUCCESS)
    {
        LOG(ERROR) << "failed to send \"JOIN\" request to the existing cluster:  " << rc;
        return false;
    }

    LOG(INFO) << "sent \"JOIN\" request to the existing cluster";
    return true;
}

bool
replicant_daemon :: daemonize()
{
    google::SetLogDestination(google::INFO, "replicant-");

    if (::daemon(1, 0) < 0)
    {
        PLOG(ERROR) << "could not daemonize";
        return false;
    }

    return true;
}

bool
replicant_daemon :: loop()
{
    while (s_continue)
    {
        po6::net::location from;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee.recv(&from, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                process_message(from, msg);
                break;
            case BUSYBEE_TIMEOUT:
                if (m_confman.unconfigured())
                {
                    LOG(ERROR) << "failed to \"JOIN\" the existing cluster:  connection timed out";
                    s_continue = false;
                }
                break;
            case BUSYBEE_DISCONNECT:
            case BUSYBEE_CONNECTFAIL:
                handle_disconnect(from);
                break;
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
                LOG(ERROR) << "XXX CASE " << rc; // XXX
                break;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_QUEUED:
            case BUSYBEE_BUFFERFULL:
            case BUSYBEE_EXTERNAL:
            default:
                abort();
        }

        run_periodic();
    }

    return true;
}

void
replicant_daemon :: process_message(const po6::net::location& from,
                                    std::auto_ptr<e::buffer> msg)
{
    assert(from != po6::net::location());
    assert(msg.get());
    replicant_network_msgtype mt = REPLNET_NOP;
    e::buffer::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    up = up >> mt;

    switch (mt)
    {
        case REPLNET_NOP:
            break;
        case REPLNET_JOIN:
            process_join(from, msg, up);
            break;
        case REPLNET_INFORM:
            process_inform(from, msg, up);
            break;
        case REPLNET_BECOME_SPARE:
            process_become_spare(from, msg, up);
            break;
        case REPLNET_BECOME_STANDBY:
            process_become_standby(from, msg, up);
            break;
        case REPLNET_BECOME_MEMBER:
            process_become_member(from, msg, up);
            break;
        case REPLNET_CONFIG_PROPOSE:
            process_config_propose(from, msg, up);
            break;
        case REPLNET_CONFIG_ACCEPT:
            process_config_accept(from, msg, up);
            break;
        case REPLNET_CONFIG_REJECT:
            process_config_reject(from, msg, up);
            break;
        case REPLNET_IDENTIFY:
            process_identify(from, msg, up);
            break;
        case REPLNET_IDENTIFIED:
            LOG(WARNING) << "dropping \"IDENTIFIED\" received by server";
            break;
        case REPLNET_CLIENT_LIST:
            process_client_list(from, msg, up);
            break;
        case REPLNET_COMMAND_SUBMIT:
            process_command_submit(from, msg, up);
            break;
        case REPLNET_COMMAND_ISSUE:
            process_command_issue(from, msg, up);
            break;
        case REPLNET_COMMAND_ACK:
            process_command_ack(from, msg, up);
            break;
        case REPLNET_COMMAND_RESPONSE:
            LOG(WARNING) << "dropping \"RESPONSE\" received by server";
            break;
        case REPLNET_COMMAND_RESEND:
            LOG(WARNING) << "dropping \"RESEND\" received by server";
            break;
        case REPLNET_REQ_STATE:
            process_req_state(from, msg, up);
            break;
        case REPLNET_RESP_STATE:
            process_resp_state(from, msg, up);
            break;
        case REPLNET_SNAPSHOT:
            process_snapshot(from, msg, up);
            break;
        case REPLNET_HEALED:
            process_healed(from, msg, up);
            break;
        default:
            LOG(WARNING) << "unknown message type; here's some hex:  " << msg->hex();
            break;
    }
}

void
replicant_daemon :: process_join(const po6::net::location& from,
                                 std::auto_ptr<e::buffer>,
                                 e::buffer::unpacker)
{
    const configuration& config(m_confman.get_stable());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_INFORM)
              + pack_size(config);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << config;

    if (!send(from, msg))
    {
        LOG(WARNING) << "Could not send \"INFORM\" message";
    }
}

void
replicant_daemon :: process_inform(const po6::net::location&,
                                   std::auto_ptr<e::buffer>,
                                   e::buffer::unpacker up)
{
    configuration new_config;
    up = up >> new_config;
    CHECK_UNPACK(INFORM, up);

    if (m_confman.get_latest().version() < new_config.version())
    {
        LOG(INFO) << "informed about configuration " << new_config.version()
                  << " which replaces configuration " << m_confman.get_latest().version();
        m_confman.reset(new_config);
        accept_config(new_config);
    }
}

void
replicant_daemon :: process_become_spare(const po6::net::location& from,
                                         std::auto_ptr<e::buffer>,
                                         e::buffer::unpacker up)
{
    chain_node new_spare;
    up = up >> new_spare;
    CHECK_UNPACK(BECOME_SPARE, up);

    if (from != new_spare.sender())
    {
        LOG(INFO) << "dropping \"BECOME_SPARE\" message because the sender doesn't match the node";
        return;
    }

    configuration new_config = m_confman.get_latest();

    if (new_config.in_chain(new_spare))
    {
        LOG(INFO) << "dropping \"BECOME_SPARE\" message because the requester is in the chain";
        return;
    }

    if (new_config.is_spare(new_spare))
    {
        LOG(INFO) << "dropping \"BECOME_SPARE\" message because the requester is already a spare";
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_INFORM)
                  + pack_size(m_confman.get_stable());
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << m_confman.get_stable();
        send(from, msg); // ignore return
        return;
    }

    if (new_config.spares_full())
    {
        LOG(INFO) << "dropping \"BECOME_SPARE\" message because there are too many spare nodes";
        return;
    }

    new_config.add_spare(new_spare);
    new_config.bump_version();
    propose_config(new_config); // ignore return, they'll retry
}

void
replicant_daemon :: process_become_standby(const po6::net::location& from,
                                           std::auto_ptr<e::buffer>,
                                           e::buffer::unpacker up)
{
    chain_node new_standby;
    up = up >> new_standby;
    CHECK_UNPACK(BECOME_STANDBY, up);

    if (from != new_standby.sender())
    {
        LOG(INFO) << "dropping \"BECOME_STANDBY\" message because the sender doesn't match the node";
        return;
    }

    configuration new_config = m_confman.get_latest();

    if (new_config.in_chain(new_standby))
    {
        LOG(INFO) << "dropping \"BECOME_STANDBY\" message because the requester is in the chain";
        return;
    }

    if (new_config.chain_full())
    {
        LOG(INFO) << "dropping \"BECOME_STANDBY\" message because there are too many chain nodes";
        return;
    }

    new_config.add_standby(new_standby);
    new_config.bump_version();
    propose_config(new_config); // ignore return, they'll retry
}

void
replicant_daemon :: process_become_member(const po6::net::location& from,
                                          std::auto_ptr<e::buffer>,
                                          e::buffer::unpacker up)
{
    chain_node new_member;
    up = up >> new_member;
    CHECK_UNPACK(BECOME_MEMBER, up);

    if (from != new_member.sender())
    {
        LOG(INFO) << "dropping \"BECOME_MEMBER\" message because the sender doesn't match the node";
        return;
    }

    configuration new_config = m_confman.get_latest();

    if (new_config.is_member(new_member))
    {
        LOG(INFO) << "dropping \"BECOME_MEMBER\" message because the requester is a chain member";
        return;
    }

    if (!new_config.is_standby(new_member))
    {
        LOG(INFO) << "dropping \"BECOME_MEMBER\" message because the requester is not on standby";
        return;
    }

    if (new_config.first_standby() != new_member)
    {
        LOG(INFO) << "dropping \"BECOME_MEMBER\" message because the requester is not the first on standby";
        return;
    }

    if (new_config.chain_full())
    {
        LOG(INFO) << "dropping \"BECOME_MEMBER\" message because there are too many chain nodes";
        return;
    }

    new_config.convert_standby(new_member);
    new_config.bump_version();
    propose_config(new_config); // ignore return, they'll retry
}

void
replicant_daemon :: process_config_propose(const po6::net::location& from,
                                           std::auto_ptr<e::buffer>,
                                           e::buffer::unpacker up)
{
    std::vector<configuration> config_chain;
    up = up >> config_chain;
    CHECK_UNPACK(CONFIG_PROPOSE, up);

    if (config_chain.size() < 2)
    {
        LOG(ERROR) << "received corrupt \"PROPOSE\" message (too few configurations)";
        return;
    }

    bool reject = false;
    uint64_t oldest_version = m_confman.get_stable().version();
    uint64_t latest_version = m_confman.get_latest().version();

    // If all of the configs fall outside the range we've already got, then we
    // should reject.
    if ((config_chain.front().version() < oldest_version &&
         config_chain.back().version()  < oldest_version) ||
        (config_chain.front().version() > latest_version &&
         config_chain.back().version()  > latest_version))
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "rejecting \"PROPOSE\" message that doesn't intersect our history";
        reject = true;
    }

    for (size_t i = 0; i < config_chain.size(); ++i)
    {
        uint64_t version = config_chain[i].version();

        // It should validate
        if (!config_chain[i].validate())
        {
            // This should never happen, so it's an error
            LOG(ERROR) << "rejecting \"PROPOSE\" message with corrupt config";
            reject = true;
        }

        // If we should manage this config and don't, that's grounds for
        // rejection
        if (oldest_version <= version && version <= latest_version &&
            !m_confman.manages(config_chain[i]))
        {
            // This is allowable, so it's just an INFO message
            LOG(INFO) << "reject \"PROPOSE\" message with divergent history";
            reject = true;
        }

        if (i + 1 < config_chain.size() &&
            config_chain[i].version() + 1 != config_chain[i + 1].version())
        {
            // This should never happen, so it's an error
            LOG(ERROR) << "rejecting \"PROPOSE\" message with out-of-order configs";
            reject = true;
        }

        for (size_t j = i + 1; j < config_chain.size(); ++j)
        {
            if (!config_chain[j].quorum_of(config_chain[i]))
            {
                // This should never happen, so it's an error
                LOG(ERROR) << "rejecting \"PROPOSE\" message that violates the quorum requirement";
                reject = true;
            }
        }

        // We only care about >latest_version.  If <=latest_version, then either
        // we're <oldest_version (and don't care) or >= oldest_version, and
        // covered by the m_confman.manages() check above.
        if (version > latest_version &&
            !m_confman.quorum_for_all(config_chain[i]))
        {
            // This should never happen, so it's an error
            LOG(ERROR) << "rejecting \"PROPOSE\" message that violates the quorum requirement";
            reject = true;
        }
    }

    // All proposals must come from our "prev" in the latest config
    if (config_chain.back().prev(m_us).sender() != from)
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "rejecting \"PROPOSE\" message that didn't come from the previous node";
        reject = true;
    }

    // It's at this point that we have to decide what to reject.
    // We reject everything that is >= to oldest_version that is not managed by
    // the chain.
    if (reject)
    {
        std::vector<configuration> rejectable;

        // Not worth saving this info above.  "reject" should be rare and
        // therefore it doesn't matter if it is slow the 1 in 10M+ times it
        // happens
        for (size_t i = 0; i < config_chain.size(); ++i)
        {
            if ((config_chain[i].version() >= oldest_version &&
                 !m_confman.manages(config_chain[i])) ||
                !config_chain[i].validate())
            {
                rejectable.push_back(config_chain[i]);
            }
        }

        std::sort(rejectable.begin(), rejectable.end());
        std::vector<configuration>::iterator it;
        it = std::unique(rejectable.begin(), rejectable.end());
        rejectable.resize(it - rejectable.begin());

        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_REJECT)
                  + pack_size(rejectable);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_REJECT << rejectable;

        // We can use "from" here safely.  If from == the sender we expected,
        // then the chain unwinds.  If from != the sender we expected, then we
        // allow them to clean up and hopefully heal.
        if (!send(from, msg))
        {
            LOG(WARNING) << "Could not send \"REJECT\" message";
        }

        return;
    }

    // If this is a retransmission, we need to retransmit the accept
    if (config_chain.back().version() == oldest_version)
    {
        send_config_accept(config_chain.back());
    }

    // Propose all the new configurations
    for (size_t i = 0; i < config_chain.size(); ++i)
    {
        if (latest_version < config_chain[i].version())
        {
            propose_config(config_chain[i]);
        }
    }
}

void
replicant_daemon :: process_config_accept(const po6::net::location& from,
                                          std::auto_ptr<e::buffer> msg,
                                          e::buffer::unpacker up)
{
    configuration accepted;
    up = up >> accepted;
    CHECK_UNPACK(CONFIG_ACCEPT, up);

    if (accepted.version() >= m_confman.get_stable().version() &&
        !m_confman.manages(accepted))
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "dropping \"ACCEPT\" message for a config we haven't seen";
        return;
    }

    if (accepted.next(m_us).sender() != from)
    {
        LOG(INFO) << "dropping \"ACCEPT\" message because the sender doesn't match the next node";
        return;
    }

    accept_config(accepted);
    po6::net::location prev(accepted.prev(m_us).receiver());

    if (prev != po6::net::location())
    {
        if (!send(prev, msg))
        {
            LOG(WARNING) << "Could not send \"ACCEPT\" message";
        }
    }
}

void
replicant_daemon :: process_config_reject(const po6::net::location& from,
                                          std::auto_ptr<e::buffer>,
                                          e::buffer::unpacker up)
{
    std::vector<configuration> rejected;
    up = up >> rejected;
    CHECK_UNPACK(CONFIG_REJECT, up);

    if (rejected.empty())
    {
        // This means we sent something that had all valid configs, but they
        // were out-of-order or some such nonesense.  Only with a bug present
        // would this ever happen.
        LOG(ERROR) << "dropping \"REJECT\" message with no configs";
        return;
    }

    for (size_t i = 0; i < rejected.size(); ++i)
    {
        if (rejected[i].next(m_us).sender() == from &&
            m_confman.manages(rejected[i]))
        {
            m_confman.reject(rejected[i].version());
            send_config_reject(rejected[i]);
        }
    }
}

void
replicant_daemon :: propose_config(const configuration& config)
{
    if (!config.validate())
    {
        LOG(FATAL) << "proposed configuration is invalid; file a bug";
        return;
    }

    if (!m_confman.quorum_for_all(config))
    {
        LOG(WARNING) << "cannot propose new config because it does not contain"
                     << " a quorum of proposed configurations";
        return;
    }

    LOG(INFO) << "proposing new configuration " << config.version();
    m_confman.add_proposed(config);

    if (config.config_tail() == m_us)
    {
        accept_config(config);
        send_config_accept(config);
    }
    else
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_PROPOSE)
                  + pack_size(m_confman);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_PROPOSE << m_confman;
        send(config.next(m_us).receiver(), msg);
    }
}

void
replicant_daemon :: accept_config(const configuration& new_config)
{
    assert(m_confman.manages(new_config));
    configuration old_config(m_confman.get_stable());
    LOG(INFO) << "accepting new configuration " << new_config.version()
              << " which replaces configuration " << old_config.version();
    m_confman.adopt(new_config.version());
    trip_periodic(0, &replicant_daemon::periodic_become_spare);
    accept_config_inform_spares(old_config, new_config);
    accept_config_inform_clients(old_config, new_config);
    accept_config_reset_healing(old_config, new_config);
    // XXX reset last_seen for all clients
}

void
replicant_daemon :: accept_config_inform_spares(const configuration&,
                                                const configuration& new_config)
{
    for (const chain_node* spare = new_config.spares_begin();
            spare != new_config.spares_end(); ++spare)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_INFORM)
                  + pack_size(new_config);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << new_config;
        send(spare->receiver(), msg); // ignore return
    }
}

void
replicant_daemon :: accept_config_inform_clients(const configuration&,
                                                 const configuration& new_config)
{
    std::vector<po6::net::location> clients;
    m_clients.get(&clients);

    for (size_t i = 0; i < clients.size(); ++i)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_INFORM)
                  + pack_size(m_confman.get_stable());
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << new_config;
        send(clients[i], msg); // ignore return
    }
}

void
replicant_daemon :: send_config_accept(const configuration& config)
{
    po6::net::location prev(config.prev(m_us).receiver());

    if (prev != po6::net::location())
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_ACCEPT)
                  + pack_size(config);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_ACCEPT << config;
        send(prev, msg);
    }
}

void
replicant_daemon :: send_config_reject(const configuration& config)
{
    po6::net::location prev(config.prev(m_us).receiver());

    if (prev != po6::net::location())
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_REJECT)
                  + pack_size(config);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_REJECT << config;
        send(prev, msg);
    }
}

void
replicant_daemon :: periodic_become_spare(uint64_t now)
{
    const configuration& stable(m_confman.get_stable());

    if (stable.is_member(m_us))
    {
        return;
    }

    if (stable.is_standby(m_us))
    {
        trip_periodic(0, &replicant_daemon::periodic_become_chain_member);
        return;
    }

    if (stable.is_spare(m_us))
    {
        trip_periodic(0, &replicant_daemon::periodic_become_standby);
        return;
    }

    if (m_confman.unconfigured())
    {
        return;
    }

    trip_periodic(now + m_s.BECOME_SPARE_INTERVAL, &replicant_daemon::periodic_become_spare);
    po6::net::location loc(stable.head().receiver());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_BECOME_SPARE)
              + pack_size(m_us);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BECOME_SPARE << m_us;
    send(loc, msg);
    LOG(INFO) << "asking " << loc << " to make us a spare in the existing chain";
}

void
replicant_daemon :: periodic_become_standby(uint64_t now)
{
    const configuration& stable(m_confman.get_stable());

    if (stable.is_member(m_us))
    {
        return;
    }

    if (stable.is_standby(m_us))
    {
        trip_periodic(0, &replicant_daemon::periodic_become_chain_member);
        return;
    }

    if (!stable.is_spare(m_us))
    {
        trip_periodic(0, &replicant_daemon::periodic_become_spare);
        return;
    }

    trip_periodic(now + m_s.BECOME_STANDBY_INTERVAL, &replicant_daemon::periodic_become_standby);
    po6::net::location loc(stable.head().receiver());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_BECOME_STANDBY)
              + pack_size(m_us);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BECOME_STANDBY << m_us;
    send(loc, msg);
    LOG(INFO) << "asking " << loc << " to make us a standby in the existing chain";
}

void
replicant_daemon :: periodic_become_chain_member(uint64_t now)
{
    const configuration& stable(m_confman.get_stable());

    if (stable.is_member(m_us))
    {
        return;
    }

    if (!stable.is_standby(m_us))
    {
        trip_periodic(now, &replicant_daemon::periodic_become_standby);
        return;
    }

    if (*stable.standbys_begin() != m_us)
    {
        trip_periodic(now + m_s.BECOME_MEMBER_INTERVAL, &replicant_daemon::periodic_become_chain_member);
        return;
    }

    if (!m_heal_prev)
    {
        trip_periodic(now + m_s.BECOME_MEMBER_INTERVAL, &replicant_daemon::periodic_become_chain_member);
        return;
    }

    trip_periodic(now + m_s.BECOME_MEMBER_INTERVAL, &replicant_daemon::periodic_become_chain_member);
    po6::net::location loc(stable.head().receiver());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_BECOME_MEMBER)
              + pack_size(m_us);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_BECOME_MEMBER << m_us;
    send(loc, msg);
    LOG(INFO) << "asking " << loc << " to make us a member of the existing chain";
}

void
replicant_daemon :: process_command_submit(const po6::net::location& from,
                                           std::auto_ptr<e::buffer> msg,
                                           e::buffer::unpacker up)
{
    // Here we play a dirty trick.  Because all of these variables are necessary
    // for when a command propagates, we force the user to send all of them so
    // that when we forward message, we do no allocation and almost 0 copying.
    //
    // "garbage" denotes the lowest nonce still in use by the client.  All
    // others less than this may be garbage collected.  When the message is
    // sent, the "garbage" value becomes the "slot" value.
    uint64_t client = 0;
    uint64_t nonce = 0;
    uint64_t garbage = 0;
    uint64_t object = 0;
    up = up >> client >> nonce >> garbage >> object;
    CHECK_UNPACK(COMMAND_SUBMIT, up);

    const configuration& config(m_confman.get_stable());
    client_manager::client_details* client_info = m_clients.get(client);

    // If this is not a registration request and the client doesn't match the
    // one who identified with us
    if (object != OBJECT_CLIENTS && client_info == NULL)
    {
        LOG(INFO) << "dropping \"COMMAND_SUBMIT\" from unknown client";
        return;
    }

    if (object != OBJECT_CLIENTS &&
        client_info->location() != from &&
        !config.in_chain_sender(from))
    {
        LOG(INFO) << "dropping \"COMMAND_SUBMIT\" from unknown location for a known client";
        return;
    }

    // If we are not the head, bounce the message
    if (object != OBJECT_CLIENTS && config.head() != m_us)
    {
        send(config.head().receiver(), msg);
        return;
    }
    else if (config.head() != m_us)
    {
        LOG(INFO) << "dropping \"COMMAND_SUBMIT\" because we'd bounce a client registration";
        return;
    }

    if (object == OBJECT_CLIENTS)
    {
        if (client_info != NULL || client == 0)
        {
            // immediately indicate failure
            size_t sz = BUSYBEE_HEADER_SIZE
                      + pack_size(REPLNET_COMMAND_RESPONSE)
                      + sizeof(uint8_t);
            std::auto_ptr<e::buffer> response(e::buffer::create(sz));
            response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE << uint8_t(0);
            send(from, response);
            return;
        }
        else
        {
            client_info = m_clients.create(client);
            client_info->set_location(from);
        }
    }
    else
    {
        assert(client_info != NULL);

        if (nonce < client_info->lower_bound_nonce())
        {
            LOG(INFO) << "dropping \"COMMAND_SUBMIT\" with nonce less than the client's lower bound";
            return;
        }

        uint64_t slot = client_info->slot_for(nonce);

        if (slot > 0 && m_commands.is_acknowledged(slot))
        {
            // resend the response
            e::intrusive_ptr<command> cmd = m_commands.lookup_acknowledged(slot);
            assert(cmd);
            send_command_response(cmd);
            return;
        }
        else if (slot > 0)
        {
            // drop it, it's proposed
            return;
        }

        if (nonce < client_info->upper_bound_nonce())
        {
            size_t sz = BUSYBEE_HEADER_SIZE
                      + pack_size(REPLNET_COMMAND_RESEND)
                      + 2 * sizeof(uint64_t);
            std::auto_ptr<e::buffer> response(e::buffer::create(sz));
            response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESEND
                                                   << client << nonce;
            send(client_info->location(), response);
            return;
        }
    }

    client_info->set_lower_bound(garbage);
    uint64_t slot = m_commands.upper_bound_proposed();
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ISSUE
                                      << client << nonce << slot << object;

#ifdef REPL_LOG_COMMANDS
    LOG(INFO) << "SUBMIT " << slot;
#endif

    issue_command(po6::net::location(), client, nonce, slot, object, msg);
}

void
replicant_daemon :: process_command_issue(const po6::net::location& from,
                                          std::auto_ptr<e::buffer> msg,
                                          e::buffer::unpacker up)
{
    uint64_t client = 0;
    uint64_t nonce = 0;
    uint64_t slot = 0;
    uint64_t object = 0;
    up = up >> client >> nonce >> slot >> object;
    CHECK_UNPACK(COMMAND_ISSUE, up);

    if (m_confman.get_stable().prev(m_us).sender() != from)
    {
        LOG(INFO) << "dropping \"COMMAND_ISSUE\" which didn't come from the right host";
        return;
    }

    if (slot < m_commands.upper_bound_proposed())
    {
        LOG(ERROR) << "dropping \"COMMAND_ISSUE\" which violates monotonicity (proposed="
                   << slot << ", expected=" << m_commands.upper_bound_proposed() << ")";
        return;
    }

#ifdef REPL_LOG_COMMANDS
    LOG(INFO) << "PROPOSED " << slot;
#endif

    issue_command(from, client, nonce, slot, object, msg);
}

void
replicant_daemon :: process_command_ack(const po6::net::location& from,
                                        std::auto_ptr<e::buffer>,
                                        e::buffer::unpacker up)
{
    uint64_t slot = 0;
    up = up >> slot;
    CHECK_UNPACK(COMMAND_ACK, up);
    acknowledge_command(from, slot);
}

void
replicant_daemon :: issue_command(const po6::net::location& from,
                                  uint64_t client,
                                  uint64_t nonce,
                                  uint64_t slot,
                                  uint64_t object,
                                  std::auto_ptr<e::buffer> msg)
{
    if (m_commands.is_acknowledged(slot))
    {
        send_command_ack(slot, from);
    }
    else
    {
        e::intrusive_ptr<command> cmd = m_commands.lookup_proposed(slot);
        bool preexisting = false;

        if (cmd)
        {
            preexisting = true;
        }
        else
        {
            cmd = new command(client, nonce, slot, object, msg);
        }

        if (!preexisting)
        {
            m_commands.append(cmd);

            if (client != 0)
            {
                // use "create" here because the client may not have identified
                // yet
                client_manager::client_details* client_info = m_clients.create(client);

                if (client_info != NULL)
                {
                    client_info->set_slot(nonce, slot);
                }
            }

            po6::net::location next(m_confman.get_stable().next(m_us).receiver());

            if (next != po6::net::location())
            {
                if (m_heal_next.state >= heal_next::HEALTHY_SENT)
                {
                    std::auto_ptr<e::buffer> copy(cmd->msg()->copy());

                    // If we fail to send, we need to fall back to healing the
                    // chain.  This likely means "BUFFER_FULL".
                    if (!send(next, copy))
                    {
                        LOG(INFO) << "normal chain operation failed; falling back to state transfer";
                        m_heal_next.state = heal_next::HEALING;
                        m_heal_next.acknowledged = m_commands.upper_bound_acknowledged();
                        m_heal_next.proposed = cmd->slot();
                        trip_periodic(0, &replicant_daemon::periodic_heal_next);
                    }
                }
            }

            if (m_confman.get_stable().command_tail() == m_us)
            {
                acknowledge_command(m_us.sender(), cmd->slot());
            }

            if (m_confman.get_stable().is_standby(m_us))
            {
                acknowledge_command(m_us.sender(), cmd->slot());
            }
        }
    }
}

void
replicant_daemon :: acknowledge_command(const po6::net::location& from, uint64_t slot)
{

    if (!(m_confman.get_stable().next(m_us).sender() == from) &&
        !(m_confman.get_stable().command_tail() == m_us && from == m_us.sender()) &&
        !(m_confman.get_stable().is_standby(m_us)))
    {
        LOG(INFO) << "dropping \"COMMAND_ACK\" which didn't come from the right host";
        return;
    }

    if (m_heal_next.state != heal_next::HEALTHY)
    {
        m_heal_next.acknowledged = slot;
        transfer_more_state();
    }

    if (m_commands.is_acknowledged(slot))
    {
        // eliminate the dupe silently
        return;
    }

    if (m_commands.upper_bound_acknowledged() != slot)
    {
        LOG(ERROR) << "dropping \"COMMAND_ACK\" which violates monotonicity";
        return;
    }

    e::intrusive_ptr<command> cmd = m_commands.lookup_proposed(slot);

    if (!cmd)
    {
        LOG(ERROR) << "dropping \"COMMAND_ACK\" for " << slot << " because we cannot find the command object";
        return;
    }

    m_commands.acknowledge(slot);
    apply_command(cmd);

    if (m_confman.get_stable().head() != m_us)
    {
        send_command_ack(slot, m_confman.get_stable().prev(m_us).receiver());
    }

#ifdef REPL_LOG_COMMANDS
    LOG(INFO) << "ACK " << slot;
#endif
}

void
replicant_daemon :: apply_command(e::intrusive_ptr<command> cmd)
{
    if (cmd->object() == OBJECT_CLIENTS)
    {
        apply_command_clients(cmd);
        send_command_response(cmd);
    }
    else if (cmd->object() == OBJECT_DEPART)
    {
        apply_command_depart(cmd);
        send_command_response(cmd);
    }
    else if (cmd->object() == OBJECT_GARBAGE)
    {
        apply_command_garbage(cmd);
        send_command_response(cmd);
    }
    else if (cmd->object() == OBJECT_CREATE)
    {
        apply_command_create(cmd);
        send_command_response(cmd);
    }
    else if (cmd->object() == OBJECT_SNAP)
    {
        apply_command_snapshot(cmd);
        send_command_response(cmd);
    }
    else
    {
        m_objects.append_cmd(cmd);
    }

}

void
replicant_daemon :: send_command_response(e::intrusive_ptr<command> cmd)
{
    assert(cmd->response());

    if (m_confman.get_stable().head() != m_us)
    {
        return;
    }

    client_manager::client_details* client_info = m_clients.get(cmd->client());
    po6::net::location host;

    if (client_info != NULL)
    {
        host = client_info->location();
    }

    if (host != po6::net::location())
    {
        std::auto_ptr<e::buffer> msg(cmd->response()->copy());
        send(host, msg);
    }
}

void
replicant_daemon :: send_command_ack(uint64_t slot, const po6::net::location& dst)
{
    size_t sz = BUSYBEE_HEADER_SIZE + sizeof(uint8_t) + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ACK << slot;
    send(dst, msg); // ignore return
}

void
replicant_daemon :: process_req_state(const po6::net::location& from,
                                      std::auto_ptr<e::buffer>,
                                      e::buffer::unpacker up)
{
    uint64_t version;
    up = up >> version;
    CHECK_UNPACK(REQ_STATE, up);

    if (m_confman.get_stable().version() < version)
    {
        LOG(INFO) << "dropping \"REQ_STATE\" for older version";
        return;
    }

    if (m_confman.get_stable().version() > version)
    {
        LOG(ERROR) << "dropping \"REQ_STATE\" for newer version (file a bug)";
        return;
    }

    if (m_confman.get_stable().prev(m_us).sender() != from)
    {
        LOG(INFO) << "dropping \"REQ_STATE\" that isn't from our predecessor";
        return;
    }

    uint64_t acknowledged = m_commands.upper_bound_acknowledged();
    uint64_t proposed = m_commands.upper_bound_proposed();
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_RESP_STATE)
              + 3 * sizeof(uint64_t);
    std::auto_ptr<e::buffer> resp(e::buffer::create(sz));
    resp->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_RESP_STATE
                                       << acknowledged << proposed << version;

    if (send(m_confman.get_stable().prev(m_us).receiver(), resp))
    {
        m_heal_prev = false;
    }
}

void
replicant_daemon :: process_resp_state(const po6::net::location& from,
                                       std::auto_ptr<e::buffer>,
                                       e::buffer::unpacker up)
{
    uint64_t acknowledged;
    uint64_t proposed;
    uint64_t version;
    up = up >> acknowledged >> proposed >> version;
    CHECK_UNPACK(RESP_STATE, up);

    if (version != m_confman.get_stable().version())
    {
        LOG(INFO) << "dropping \"RESP_STATE\" for an old healing process";
        return;
    }

    if (m_heal_next.state != heal_next::REQUEST_SENT)
    {
        LOG(INFO) << "dropping \"RESP_STATE\" that we weren't expecting";
        return;
    }

    if (m_confman.get_stable().next(m_us).sender() != from)
    {
        LOG(INFO) << "dropping \"RESP_STATE\" that isn't from our successor";
        return;
    }

    m_heal_next.state = heal_next::HEALING;
    m_heal_next.acknowledged = std::max(acknowledged, m_commands.lower_bound_acknowledged());
    m_heal_next.proposed = std::max(m_heal_next.acknowledged, proposed);

    if (!m_snapshots.empty() && m_heal_next.proposed < m_snapshots.back().first)
    {
        m_heal_next.acknowledged = m_snapshots.back().first;
        m_heal_next.proposed = m_snapshots.back().first;
    }

    // Process all acks up to, but not including, the one for acknowledged.
    while (m_commands.upper_bound_acknowledged() < acknowledged)
    {
        acknowledge_command(from, m_commands.upper_bound_acknowledged());
    }

    LOG(INFO) << "initiating state transfer of slot "
              << m_heal_next.acknowledged << " to slot " << m_commands.upper_bound_acknowledged();
    transfer_more_state();
}

void
replicant_daemon :: process_snapshot(const po6::net::location&,
                                     std::auto_ptr<e::buffer> msg,
                                     e::buffer::unpacker up)
{
    uint64_t slot;
    settings s;
    std::vector<uint64_t> clients;
    std::vector<std::pair<uint64_t, std::pair<e::slice, e::slice> > > objects;
    up = up >> slot >> s >> clients >> objects;
    CHECK_UNPACK(SNAPSHOT, up);
    LOG(INFO) << "restoring from a snapshot of slot " << slot;

    // Copy settings
    m_s = s;

    // Drop clients we don't care about
    std::vector<uint64_t> all_clients;
    m_clients.get(&all_clients);
    std::sort(clients.begin(), clients.end());
    std::sort(all_clients.begin(), all_clients.end());

    for (size_t i = 0; i < all_clients.size(); ++i)
    {
        if (!std::binary_search(clients.begin(), clients.end(), all_clients[i]))
        {
            client_manager::client_details* client_info;
            client_info = m_clients.get(all_clients[i]);

            if (client_info->location() != po6::net::location())
            {
                m_busybee.drop(client_info->location());
            }

            m_clients.remove(all_clients[i]);
        }
    }

    // Create the objects we need
    for (size_t i = 0; i < objects.size(); ++i)
    {
        bool success = true;
        uint64_t new_object(objects[i].first);
        const e::slice& pathstr(objects[i].second.first);
        const e::slice& snapshot(objects[i].second.second);

        if (!m_objects.valid_path(pathstr))
        {
            success = false;
            LOG(ERROR) << "cannot restore object because the path is invalid";
        }

        if (m_objects.exists(new_object))
        {
            success = false;
            LOG(ERROR) << "cannot restore object because the object already exists";
        }

        if (success)
        {
            LOG(INFO) << "restoring object " << new_object << " from " << pathstr.data();

            if (!m_objects.restore(new_object, pathstr, snapshot))
            {
                LOG(ERROR) << "could not restore object because the object itself rejected the snapshot";
                LOG(ERROR) << "Here's a hexdump of the failed snapshot: " << snapshot.hex();
                success = false;
            }
        }

        if (!success)
        {
            LOG(ERROR) << "the above error may require manual repair; in the meantime, we will fail and try again";
            // XXX fail ourselves
        }
    }

    // Wipe out the command history.  It's obsoleted by the snapshot.
    //m_commands = command_manager();

    // Setup m_marked_for_gc
    m_marked_for_gc = 0;

    // Prepare snapshots
    m_snapshots.clear();
    std::tr1::shared_ptr<e::buffer> snap(msg.release());
    m_snapshots.push_back(std::make_pair(slot, snap));
}

void
replicant_daemon :: process_healed(const po6::net::location& from,
                                   std::auto_ptr<e::buffer> msg,
                                   e::buffer::unpacker)
{
    if (m_confman.get_stable().next(m_us).sender() == from &&
        m_heal_next.state == heal_next::HEALTHY_SENT)
    {
        // we can move m_heal_next from HEALTHY_SENT to HEALTHY
        m_heal_next.state = heal_next::HEALTHY;
        LOG(INFO) << "the connection with the next node is 100% healed";
    }

    if (m_confman.get_stable().prev(m_us).sender() == from)
    {
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_HEALED;
        send(m_confman.get_stable().prev(m_us).receiver(), msg);

        if (!m_heal_prev)
        {
            trip_periodic(0, &replicant_daemon::periodic_become_chain_member);
        }

        m_heal_prev = true;
    }
}

void
replicant_daemon :: transfer_more_state()
{
    bool error = false;

    while (m_heal_next.state < heal_next::HEALTHY_SENT &&
           m_heal_next.proposed < m_commands.upper_bound_proposed() &&
           m_heal_next.proposed - m_heal_next.acknowledged <= m_s.TRANSFER_WINDOW)
    {
        e::intrusive_ptr<command> next_cmd;
        next_cmd = m_commands.lookup(m_heal_next.proposed);
        assert(next_cmd->slot() == m_heal_next.proposed);

        if (!next_cmd)
        {
            LOG(ERROR) << "cannot transfer slot " << m_heal_next.proposed << " because we cannot find the command object";
            ++m_heal_next.proposed;
            continue;
        }

        std::auto_ptr<e::buffer> copy(next_cmd->msg()->copy());
        po6::net::location next(m_confman.get_stable().next(m_us).receiver());

        if (next_cmd->object() == OBJECT_SNAP)
        {
            copy.reset(m_snapshots.back().second->copy());
        }

        if (next == po6::net::location())
        {
            LOG(ERROR) << "performing transfer, but cannot lookup our next node";
            error = true;
            break;
        }

        if (send(m_confman.get_stable().next(m_us).receiver(), copy))
        {
            ++m_heal_next.proposed;
        }
        else
        {
            break;
        }
    }

    if (m_heal_next.proposed == m_commands.upper_bound_proposed() &&
        m_heal_next.state < heal_next::HEALTHY_SENT)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_HEALED);
        std::auto_ptr<e::buffer> response(e::buffer::create(sz));
        response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_HEALED;

        if (send(m_confman.get_stable().next(m_us).receiver(), response))
        {
            LOG(INFO) << "state transfer complete; falling back to normal chain operation";
            m_heal_next.state = heal_next::HEALTHY_SENT;
        }
    }
}

void
replicant_daemon :: accept_config_reset_healing(const configuration&,
                                                const configuration& new_config)
{
    // If there's a node after us that we need to make up with
    if (new_config.next(m_us) != chain_node())
    {
        m_heal_next = heal_next();
        trip_periodic(0, &replicant_daemon::periodic_heal_next);
    }
    else
    {
        m_heal_next.state = heal_next::HEALTHY;
    }

    if (new_config.command_tail() == m_us)
    {
        while (m_commands.lower_bound_proposed() < m_commands.upper_bound_proposed())
        {
            acknowledge_command(m_us.sender(), m_commands.lower_bound_proposed());
        }
    }

    // If there's a node before us that we need to make up with
    if (new_config.prev(m_us) != chain_node())
    {
        m_heal_prev = false;
    }
    else
    {
        m_heal_prev = true;
        trip_periodic(0, &replicant_daemon::periodic_become_chain_member);
    }

    if (!new_config.is_member(m_us))
    {
        // XXX throw away all proposed commands
    }
}

void
replicant_daemon :: handle_disconnect_heal(const po6::net::location& host)
{
    const configuration& config = m_confman.get_stable();
    chain_node n = config.next(m_us);

    if (n.receiver() == host || n.sender() == host)
    {
        m_heal_next = heal_next();
        trip_periodic(0, &replicant_daemon::periodic_heal_next);
    }

    chain_node p = config.prev(m_us);

    if (p.receiver() == host || p.sender() == host)
    {
        m_heal_prev = false;
    }
}

void
replicant_daemon :: periodic_heal_next(uint64_t now)
{
    if (m_heal_next.state != heal_next::HEALTHY)
    {
        trip_periodic(now + m_s.HEAL_NEXT_INTERVAL, &replicant_daemon::periodic_heal_next);
    }

    size_t sz;
    std::auto_ptr<e::buffer> msg;
    po6::net::location next(m_confman.get_stable().next(m_us).receiver());

    switch (m_heal_next.state)
    {
        case heal_next::BROKEN:
            if (next != po6::net::location())
            {
                sz = BUSYBEE_HEADER_SIZE
                   + pack_size(REPLNET_REQ_STATE)
                   + sizeof(uint64_t);
                msg.reset(e::buffer::create(sz));
                msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_REQ_STATE
                                                  << m_confman.get_stable().version();

                if (send(next, msg))
                {
                    m_heal_next.state = heal_next::REQUEST_SENT;
                }
            }
            break;
        case heal_next::REQUEST_SENT:
            // do nothing, wait for other side
            break;
        case heal_next::HEALING:
            // do nothing, wait for other side
            break;
        case heal_next::HEALTHY_SENT:
            // do nothing, wait for other side
            break;
        case heal_next::HEALTHY:
            // do nothing, we won't run this periodic func anymore
            break;
        default:
            abort();
    }
}

void
replicant_daemon :: process_identify(const po6::net::location& from,
                                     std::auto_ptr<e::buffer> msg,
                                     e::buffer::unpacker up)
{
    uint64_t client;
    up = up >> client;

    if (up.error())
    {
        LOG(ERROR) << "received corrupt \"IDENTIFY\" message";
        return;
    }

    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_IDENTIFIED << client;

    if (send(from, msg))
    {
        client_manager::client_details* client_info = m_clients.create(client);
        assert(client_info);
        client_info->set_location(from);
    }
}

void
replicant_daemon :: process_client_list(const po6::net::location&,
                                        std::auto_ptr<e::buffer>,
                                        e::buffer::unpacker up)
{
    std::vector<uint64_t> clients;
    up = up >> clients;
    CHECK_UNPACK(CLIENT_LIST, up);
    uint64_t now = e::time();

    for (size_t i = 0; i < clients.size(); ++i)
    {
        client_manager::client_details* client_info;
        client_info = m_clients.get(clients[i]);

        if (!client_info)
        {
            continue;
        }

        client_info->set_last_seen(now);
    }
}

void
replicant_daemon :: handle_disconnect_client(const po6::net::location& host)
{
    m_clients.disassociate(host);
}

void
replicant_daemon :: periodic_list_clients(uint64_t now)
{
    trip_periodic(now + m_s.CLIENT_LIST_INTERVAL, &replicant_daemon::periodic_list_clients);
    std::vector<uint64_t> clients;
    m_clients.get_live_clients(&clients);
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_CLIENT_LIST)
              + sizeof(uint32_t) + clients.size() * sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CLIENT_LIST << clients;
    m_busybee.send(m_confman.get_stable().head().receiver(), msg);
}

void
replicant_daemon :: periodic_detach_clients(uint64_t now)
{
    // We intend to use the LIST_INTERVAL here because it's more frequent than
    // the disconnect interval.  If CLI < CDI, then we'll take at most CLI + CDI
    // to collect.  Otherwise it may take CDI * 2.  If that is a big value, this
    // is undesirable.
    trip_periodic(now + m_s.CLIENT_LIST_INTERVAL, &replicant_daemon::periodic_detach_clients);

    if (m_confman.get_stable().head() != m_us)
    {
        return;
    }

    now -= m_s.CLIENT_DISCONNECT_INTERVAL;
    std::vector<uint64_t> clients;
    m_clients.get_old_clients(now, &clients);

    for (size_t i = 0; i < clients.size(); ++i)
    {
        client_manager::client_details* client_info;
        client_info = m_clients.get(clients[i]);
        client_info->set_lower_bound(UINT64_MAX);
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_ISSUE)
                  + 4 * sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        uint64_t nonce = UINT64_MAX;
        uint64_t slot = m_commands.upper_bound_proposed();
        uint64_t object = OBJECT_DEPART;
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ISSUE
                                          << clients[i]
                                          << nonce << slot << object;

#ifdef REPL_LOG_COMMANDS
        LOG(INFO) << "DETACH CLIENT " << slot;
#endif

        issue_command(po6::net::location(), clients[i], nonce, slot, object, msg);
    }
}

void
replicant_daemon :: apply_command_clients(e::intrusive_ptr<command> cmd)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_RESPONSE)
              + sizeof(uint8_t);
    std::auto_ptr<e::buffer> response(e::buffer::create(sz));
    response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE << uint8_t(1);
    cmd->set_response(response);
}

void
replicant_daemon :: apply_command_depart(e::intrusive_ptr<command> cmd)
{
    client_manager::client_details* client_info = m_clients.get(cmd->client());

    if (client_info && client_info->location() != po6::net::location())
    {
        m_busybee.drop(client_info->location());
    }

    m_clients.remove(cmd->client());

    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_RESPONSE);
    std::auto_ptr<e::buffer> response(e::buffer::create(sz));
    response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE;
    cmd->set_response(response);
}

void
replicant_daemon :: periodic_garbage_collect_commands_start(uint64_t now)
{
    trip_periodic(now + m_s.GARBAGE_COLLECT_INTERVAL, &replicant_daemon::periodic_garbage_collect_commands_start);

    if (m_confman.get_stable().head() != m_us)
    {
        return;
    }

    if (m_commands.lower_bound_acknowledged() + m_s.GARBAGE_COLLECT_MIN_SLOTS >
            m_commands.upper_bound_acknowledged())
    {
        return;
    }

    uint64_t garbage = m_clients.lowest_slot_in_use();
    garbage = std::min(garbage, m_commands.upper_bound_acknowledged());

    if (m_heal_next.state != heal_next::HEALTHY)
    {
        garbage = std::min(garbage, m_heal_next.acknowledged);
    }

    if (garbage > m_marked_for_gc)
    {
        if (m_s.GARBAGE_COLLECT_LOG_DETAILS)
        {
            LOG(INFO) << "asking to garbage collect up to " << garbage;
        }

        uint64_t zero = 0;
        uint64_t slot = m_commands.upper_bound_proposed();
        uint64_t object = OBJECT_SNAP;
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_ISSUE)
                  + 4 * sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ISSUE
                                          << zero << zero << slot << object;
#ifdef REPL_LOG_COMMANDS
        LOG(INFO) << "SNAPSHOT " << slot;
#endif
        issue_command(po6::net::location(), 0, 0, slot, object, msg);

        zero = 0;
        slot = m_commands.upper_bound_proposed();
        object = OBJECT_GARBAGE;
        sz = BUSYBEE_HEADER_SIZE
           + pack_size(REPLNET_COMMAND_ISSUE)
           + 5 * sizeof(uint64_t);
        msg.reset(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ISSUE
                                          << zero << zero << slot << object << garbage;
#ifdef REPL_LOG_COMMANDS
        LOG(INFO) << "GARBAGE COLLECT " << slot;
#endif
        issue_command(po6::net::location(), 0, 0, slot, object, msg);
    }
    else if (m_s.GARBAGE_COLLECT_LOG_DETAILS)
    {
        LOG(INFO) << "garbage collection up to " << garbage << " is blocked";
    }
}

void
replicant_daemon :: periodic_garbage_collect_commands_collect(uint64_t now)
{
    // XXX revisit this logic
    if (m_heal_next.state != heal_next::HEALTHY &&
        m_marked_for_gc < m_heal_next.acknowledged &&
        m_confman.get_stable().config_tail() != m_us)
    {
        trip_periodic(now + m_s.GARBAGE_COLLECT_INTERVAL, &replicant_daemon::periodic_garbage_collect_commands_collect);
        return;
    }

    uint64_t garbage = 0;

    for (snapshot_list::iterator it = m_snapshots.begin();
            it != m_snapshots.end(); ++it)
    {
        if (it->first <= m_marked_for_gc)
        {
            garbage = it->first;
        }
    }

    if (garbage == 0)
    {
        trip_periodic(now + m_s.GARBAGE_COLLECT_INTERVAL, &replicant_daemon::periodic_garbage_collect_commands_collect);
        return;
    }

    assert(!m_snapshots.empty());

    while (m_snapshots.front().first < garbage)
    {
        m_snapshots.pop_front();
    }

    assert(!m_snapshots.empty());

    if (garbage != m_marked_for_gc)
    {
        trip_periodic(now + m_s.GARBAGE_COLLECT_INTERVAL, &replicant_daemon::periodic_garbage_collect_commands_collect);
    }

    if (m_s.GARBAGE_COLLECT_LOG_DETAILS)
    {
        LOG(INFO) << "garbage collecting everything up to slot " << garbage;
    }

    m_commands.garbage_collect(garbage);
    m_clients.garbage_collect(garbage);
}

void
replicant_daemon :: apply_command_garbage(e::intrusive_ptr<command> cmd)
{
    // Grab the upper bound for garbage collection
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_ISSUE)
              + 4 * sizeof(uint64_t);
    e::buffer::unpacker up = cmd->msg()->unpack_from(sz);
    char buf[sizeof(uint64_t)];
    memset(buf, 0, sizeof(uint64_t));
    e::slice rem = up.as_slice();
    memmove(buf, rem.data(), std::min(sizeof(uint64_t), rem.size()));
    uint64_t garbage;
    e::unpack64be(buf, &garbage);

    // Get ready for garbage collection
    m_marked_for_gc = std::max(garbage, m_marked_for_gc);
    trip_periodic(0, &replicant_daemon::periodic_garbage_collect_commands_collect);

    // Craft a response
    sz = BUSYBEE_HEADER_SIZE
       + pack_size(REPLNET_COMMAND_RESPONSE)
       + sizeof(uint64_t);
    std::auto_ptr<e::buffer> response(e::buffer::create(sz));
    response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE << garbage;
    cmd->set_response(response);
}

void
replicant_daemon :: apply_command_snapshot(e::intrusive_ptr<command> cmd)
{
    // Create the information for the snapshot
    // Get a list of client tokens
    std::vector<uint64_t> clients;
    m_clients.get(&clients);
    // Get snapshots of all objects
    std::vector<std::pair<uint64_t, std::pair<e::slice, e::slice> > > objects;
    object_manager::snapshot snapshot;
    m_objects.take_snapshot(&snapshot, &objects);

    // Construct a blob representing this snapshot
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_SNAPSHOT)
              + sizeof(uint64_t)
              + pack_size(m_s)
              + sizeof(uint32_t) + clients.size() * sizeof(uint64_t)
              + sizeof(uint32_t);

    for (size_t i = 0; i < objects.size(); ++i)
    {
        sz += sizeof(uint64_t) + 2 * sizeof(uint32_t)
            + objects[i].second.first.size()
            + objects[i].second.second.size();
    }

    std::tr1::shared_ptr<e::buffer> snap(e::buffer::create(sz));
    snap->pack_at(BUSYBEE_HEADER_SIZE)
        << REPLNET_SNAPSHOT << cmd->slot() << m_s << clients << objects;
    m_snapshots.push_back(std::make_pair(cmd->slot(), snap));

    // Craft a response to satisfy invariants
    sz = BUSYBEE_HEADER_SIZE
       + pack_size(REPLNET_COMMAND_RESPONSE);
    std::auto_ptr<e::buffer> response(e::buffer::create(sz));
    response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE;
    cmd->set_response(response);
}

bool
replicant_daemon :: send(const po6::net::location& to,
                         std::auto_ptr<e::buffer> msg)
{
    switch (m_busybee.send(to, msg))
    {
        case BUSYBEE_SUCCESS:
        case BUSYBEE_QUEUED:
            return true;
        case BUSYBEE_DISCONNECT:
        case BUSYBEE_CONNECTFAIL:
            handle_disconnect(to);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_BUFFERFULL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        default:
            return false;
    }
}

void
replicant_daemon :: apply_command_create(e::intrusive_ptr<command> cmd)
{
    bool success = true;
    // Unpack the message
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_ISSUE)
              + 4 * sizeof(uint64_t);
    e::buffer::unpacker up = cmd->msg()->unpack_from(sz);
    uint64_t new_object;
    e::slice pathstr;
    up = up >> new_object >> pathstr;

    if (up.error())
    {
        success = false;
        LOG(ERROR) << "aborting object create because it doesn't unpack";
    }

    if (!m_objects.valid_path(pathstr))
    {
        success = false;
        LOG(ERROR) << "aborting object create because the path is not valid";
    }

    if (m_objects.exists(new_object))
    {
        success = false;
        LOG(ERROR) << "aborting object create because the object already exists";
    }

    if (success)
    {
        LOG(INFO) << "creating object " << new_object << " from " << pathstr.data();

        if (!m_objects.create(new_object, pathstr, this))
        {
            LOG(ERROR) << "could not create object because the object itself failed to create";
            success = false;
        }
    }

    if (!success)
    {
        LOG(ERROR) << "the above error requires manual repair; please fix the issue and remove/recreate the object";
    }

    // Craft a response
    sz = BUSYBEE_HEADER_SIZE
       + pack_size(REPLNET_COMMAND_RESPONSE)
       + sizeof(uint64_t) + sizeof(uint8_t);
    std::auto_ptr<e::buffer> response(e::buffer::create(sz));
    response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE
                                           << cmd->nonce()
                                           << uint8_t(success ? 1 : 0);
    cmd->set_response(response);
}

void
replicant_daemon :: handle_disconnect(const po6::net::location& host)
{
    m_disconnected.push(std::make_pair(m_disconnected_iteration, host));
    trip_periodic(0, &replicant_daemon::periodic_handle_disconnect);
}

void
replicant_daemon :: periodic_handle_disconnect(uint64_t)
{
    uint64_t now = e::time();
    uint64_t start_iter = m_disconnected_iteration;
    ++m_disconnected_iteration;

    while (!m_disconnected.empty())
    {
        uint64_t iter = m_disconnected.front().first;
        po6::net::location host = m_disconnected.front().second;
        m_disconnected.pop();

        uint64_t last = 0;
        std::map<po6::net::location, uint64_t>::iterator it;
        it = m_disconnected_times.find(host);

        if (it != m_disconnected_times.end())
        {
            last = it->second;
            it->second = now;
        }
        else
        {
            m_disconnected_times[host] = now;
        }

        if (iter <= start_iter && last + m_s.CONNECTION_RETRY < now)
        {
            handle_disconnect_propose_new_config(host); // XXX obsolete when phifd is added
            handle_disconnect_client(host);
            handle_disconnect_heal(host);
        }
    }

    for (std::map<po6::net::location, uint64_t>::iterator it = m_disconnected_times.begin();
            it != m_disconnected_times.end(); )
    {
        if (it->second + m_s.CONNECTION_RETRY < now)
        {
            m_disconnected_times.erase(it);
            it = m_disconnected_times.begin();
        }
        else
        {
            ++it;
        }
    }
}

void
replicant_daemon :: handle_disconnect_propose_new_config(const po6::net::location& host)
{
    // Figure out a new configuration we could send if we were the head
    configuration new_config = m_confman.get_latest();
    new_config.bump_version();
    size_t count = 0;

    for (const chain_node* n = new_config.members_begin();
            n != new_config.members_end(); )
    {
        if (n->receiver() == host || n->sender() == host)
        {
            new_config.remove_member(*n);
            n = new_config.members_begin();
            ++count;
        }
        else
        {
            ++n;
        }
    }

    for (const chain_node* n = new_config.standbys_begin();
            n != new_config.standbys_end(); )
    {
        if (n->receiver() == host || n->sender() == host)
        {
            new_config.remove_standby(*n);
            n = new_config.standbys_begin();
            ++count;
        }
        else
        {
            ++n;
        }
    }

    for (const chain_node* n = new_config.spares_begin();
            n != new_config.spares_end(); )
    {
        if (n->receiver() == host || n->sender() == host)
        {
            new_config.remove_spare(*n);
            n = new_config.spares_begin();
            ++count;
        }
        else
        {
            ++n;
        }
    }

    if (count > 0 && new_config.head() == m_us)
    {
        propose_config(new_config);
    }
}

typedef void (replicant_daemon::*_periodic_fptr)(uint64_t now);
typedef std::pair<uint64_t, _periodic_fptr> _periodic;

static bool
compare_periodic(const _periodic& lhs, const _periodic& rhs)
{
    return lhs.first > rhs.first;
}

void
replicant_daemon :: trip_periodic(uint64_t when, periodic_fptr fp)
{
    for (size_t i = 0; i < m_periodic.size(); ++i)
    {
        if (m_periodic[i].second == fp)
        {
            m_periodic[i].second = &replicant_daemon::periodic_nop;
        }
    }

    m_periodic.push_back(std::make_pair(when, fp));
    std::push_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
}

void
replicant_daemon :: run_periodic()
{
    uint64_t now = e::time();

    while (!m_periodic.empty() && m_periodic[0].first <= now)
    {
        if (m_periodic.size() > m_s.PERIODIC_SIZE_WARNING)
        {
            LOG(WARNING) << "there are " << m_periodic.size()
                         << " functions scheduled which exceeds the threshold of "
                         << m_s.PERIODIC_SIZE_WARNING << " functions";
        }

        periodic_fptr fp;
        std::pop_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
        fp = m_periodic.back().second;
        m_periodic.pop_back();
        (this->*fp)(now);
    }
}

void
replicant_daemon :: periodic_nop(uint64_t)
{
}

void
replicant_daemon :: periodic_set_timeout(uint64_t)
{
    m_busybee.set_timeout(1);
}

void
replicant_daemon :: periodic_dump_config(uint64_t now)
{
    trip_periodic(now + m_s.REPORT_INTERVAL, &replicant_daemon::periodic_dump_config);
    LOG(INFO) << "agreed upon " << (m_commands.upper_bound_acknowledged() - 1)
              << " commands (the first "
              << m_commands.lower_bound_acknowledged() << " are garbage-collected)"
              << " with " << (m_commands.upper_bound_proposed() - m_commands.upper_bound_acknowledged())
              << " outstanding\n" << m_confman;
}


