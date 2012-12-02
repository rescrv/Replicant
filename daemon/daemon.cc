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

// C
#include <cmath>

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
#include <busybee_mta.h>

// Replicant
#include "common/bootstrap.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/special_objects.h"
#include "daemon/daemon.h"
#include "daemon/heal_next.h"

#define CHECK_UNPACK(MSGTYPE, UNPACKER) \
    do \
    { \
        if (UNPACKER.error()) \
        { \
            replicant_network_msgtype CONCAT(_anon, __LINE__)(REPLNET_ ## MSGTYPE); \
            LOG(WARNING) << "received corrupt \"" \
                         << CONCAT(_anon, __LINE__) << "\" message"; \
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

static uint64_t
monotonic_time()
{
    timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    {
        abort();
    }

    return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

replicant_daemon :: ~replicant_daemon() throw ()
{
}

replicant_daemon :: replicant_daemon()
    : m_s()
    , m_busybee_mapper()
    , m_busybee()
    , m_us()
    , m_config_manager()
    , m_failure_manager()
    , m_object_manager()
    , m_periodic()
    , m_heal_next()
    , m_disrupted_unhandled()
    , m_disrupted_backoff()
    , m_disrupted_times()
    , m_fs()
{
    m_object_manager.set_callback(this, &replicant_daemon::record_execution);
    trip_periodic(0, &replicant_daemon::periodic_join_cluster);
    trip_periodic(0, &replicant_daemon::periodic_describe_cluster);
    trip_periodic(0, &replicant_daemon::periodic_retry_reconfiguration);
    trip_periodic(0, &replicant_daemon::periodic_exchange);
}

static bool
install_signal_handler(int signum)
{
    struct sigaction handle;
    handle.sa_handler = exit_on_signal;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

int
replicant_daemon :: run(bool daemonize,
                        po6::pathname data,
                        bool set_bind_to,
                        po6::net::location bind_to,
                        bool set_existing,
                        po6::net::hostname existing)
{
    if (!install_signal_handler(SIGHUP))
    {
        std::cerr << "could not install SIGHUP handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGINT))
    {
        std::cerr << "could not install SIGINT handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGTERM))
    {
        std::cerr << "could not install SIGTERM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    google::LogToStderr();
    bool saved = false;
    chain_node saved_us;
    configuration_manager saved_config_manager;
    LOG(INFO) << "initializing persistent storage";

    if (!m_fs.open(data, &saved, &saved_us, &saved_config_manager))
    {
        return EXIT_FAILURE;
    }

    if (saved)
    {
        LOG(INFO) << "starting daemon from state found in the persistent storage";

        if (set_bind_to && bind_to != saved_us.address)
        {
            LOG(ERROR) << "cannot bind to address; it conflicts with our previous address at " << saved_us.address;
            return EXIT_FAILURE;
        }

        m_us = saved_us;
        m_config_manager = saved_config_manager;
    }
    else
    {
        LOG(INFO) << "starting daemon new state from command-line arguments";

        if (!generate_token(&m_us.token))
        {
            PLOG(ERROR) << "could not read random token from /dev/urandom";
            return EXIT_FAILURE;
        }

        LOG(INFO) << "generated new random token:  " << m_us.token;
        m_us.address = bind_to;
    }

    m_busybee.reset(new busybee_mta(&m_busybee_mapper, m_us.address, m_us.token, 0/*we don't use pause/unpause*/));
    m_busybee->set_timeout(1);

    if (daemonize)
    {
        LOG(INFO) << "forking off to the background; goodbye!";
        google::SetLogDestination(google::INFO, "replicant-");

        if (::daemon(1, 0) < 0)
        {
            PLOG(ERROR) << "could not daemonize";
            return EXIT_FAILURE;
        }
    }

    if (saved)
    {
        accept_config(m_config_manager.stable());
    }
    else if (set_existing)
    {
        configuration initial;
        replicant::bootstrap_returncode rc = replicant::bootstrap(existing, &initial);

        switch (rc)
        {
            case replicant::BOOTSTRAP_SUCCESS:
                break;
            case replicant::BOOTSTRAP_SEE_ERRNO:
                PLOG(ERROR) << "could not join existing cluster at " << existing;
                return false;
            case replicant::BOOTSTRAP_COMM_FAIL:
                LOG(ERROR) << "could not join existing cluster at " << existing << ":  could not communicate with server";
                return false;
            case replicant::BOOTSTRAP_CORRUPT_INFORM:
                LOG(ERROR) << "could not join existing cluster at " << existing << ":  the remote host sent a bad \"INFORM\" message";
                return false;
            case replicant::BOOTSTRAP_TIMEOUT:
                LOG(ERROR) << "could not join existing cluster at " << existing << ":  the connection timed out";
                return false;
            case replicant::BOOTSTRAP_NOT_CLUSTER_MEMBER:
                LOG(ERROR) << "could not join existing cluster at " << existing << ":  the remote host is not a cluster member";
                return false;
            default:
                LOG(ERROR) << "could not join existing cluster at " << existing << ":  an unknown error occurred";
                return false;
        }

        m_config_manager.reset(initial);
        m_fs.inform_configuration(initial);
        LOG(INFO) << "joined existing cluster " << initial;
        accept_config(initial);
    }
    else
    {
        configuration initial(1, m_us);
        m_config_manager.reset(initial);
        m_fs.inform_configuration(initial);
        LOG(INFO) << "started new cluster " << initial;
        accept_config(initial);
    }

    m_fs.remove_saved_state();

    for (size_t slot = 1; slot < m_fs.next_slot_to_ack(); ++slot)
    {
        uint64_t object;
        uint64_t client;
        uint64_t nonce;
        e::slice dat;
        std::string backing;

        if (!m_fs.get_slot(slot, &object, &client, &nonce, &dat, &backing))
        {
            LOG(ERROR) << "gap in the history; missing slot " << slot;
            return EXIT_FAILURE;
        }

        m_object_manager.enqueue(slot, object, client, nonce, dat, &backing);
    }

    replicant::connection conn;
    std::auto_ptr<e::buffer> msg;

    while (recv(&conn, &msg))
    {
        assert(msg.get());
        replicant_network_msgtype mt = REPLNET_NOP;
        e::buffer::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> mt;

        switch (mt)
        {
            case REPLNET_NOP:
                break;
            case REPLNET_BOOTSTRAP:
                process_bootstrap(conn, msg, up);
                break;
            case REPLNET_INFORM:
                process_inform(conn, msg, up);
                break;
            case REPLNET_JOIN:
                process_join(conn, msg, up);
                break;
            case REPLNET_CONFIG_PROPOSE:
                process_config_propose(conn, msg, up);
                break;
            case REPLNET_CONFIG_ACCEPT:
                process_config_accept(conn, msg, up);
                break;
            case REPLNET_CONFIG_REJECT:
                process_config_reject(conn, msg, up);
                break;
            case REPLNET_CLIENT_REGISTER:
                process_client_register(conn, msg, up);
                break;
            case REPLNET_CLIENT_DISCONNECT:
                process_client_disconnect(conn, msg, up);
                break;
            case REPLNET_COMMAND_SUBMIT:
                process_command_submit(conn, msg, up);
                break;
            case REPLNET_COMMAND_ISSUE:
                process_command_issue(conn, msg, up);
                break;
            case REPLNET_COMMAND_ACK:
                process_command_ack(conn, msg, up);
                break;
            case REPLNET_COMMAND_RESPONSE:
                LOG(WARNING) << "dropping \"RESPONSE\" received by server";
                break;
            case REPLNET_HEAL_REQ:
                process_heal_req(conn, msg, up);
                break;
            case REPLNET_HEAL_RESP:
                process_heal_resp(conn, msg, up);
                break;
            case REPLNET_HEAL_DONE:
                process_heal_done(conn, msg, up);
                break;
            case REPLNET_PING:
                process_ping(conn, msg, up);
                break;
            case REPLNET_PONG:
                process_pong(conn, msg, up);
                break;
            default:
                LOG(WARNING) << "unknown message type; here's some hex:  " << msg->hex();
                break;
        }
    }

    LOG(INFO) << "replicant is gracefully shutting down";
    m_fs.close(m_us, m_config_manager);
    LOG(INFO) << "replicant will now terminate";
    return EXIT_SUCCESS;
}

void
replicant_daemon :: process_bootstrap(const replicant::connection& conn,
                                      std::auto_ptr<e::buffer>,
                                      e::buffer::unpacker)
{
    LOG(INFO) << "providing configuration to " << conn.token
              << " as part of the bootstrap process";
    const configuration& config(m_config_manager.stable());
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_INFORM)
              + pack_size(config);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << config;
    send(conn, msg);
}

void
replicant_daemon :: process_inform(const replicant::connection&,
                                   std::auto_ptr<e::buffer>,
                                   e::buffer::unpacker up)
{
    configuration new_config;
    up = up >> new_config;
    CHECK_UNPACK(INFORM, up);

    if (m_config_manager.latest().version() < new_config.version())
    {
        LOG(INFO) << "informed about configuration "
                  << new_config.version()
                  << " which replaces configuration "
                  << m_config_manager.latest().version();
        m_config_manager.reset(new_config);
        m_fs.inform_configuration(new_config);
        accept_config(new_config);
    }
    else
    {
        LOG(INFO) << "received stale \"INFORM\" message for " << new_config.version()
                  << "; our current configurations include " << m_config_manager.stable().version()
                  << " through " << m_config_manager.latest().version();
    }
}

void
replicant_daemon :: process_join(const replicant::connection& conn,
                                 std::auto_ptr<e::buffer>,
                                 e::buffer::unpacker up)
{
    chain_node sender;
    up = up >> sender;
    CHECK_UNPACK(CONFIG_PROPOSE, up);
    LOG(INFO) << "received \"JOIN\" message from " << conn.token
              << " claiming to be " << sender;

    if (!conn.matches(sender))
    {
        LOG(ERROR) << "not processing the \"JOIN\" message because the tokens "
                   << "don't match (file a bug)";
        return;
    }

    if (m_config_manager.in_any_cluster(sender))
    {
        LOG(INFO) << "not processing the \"JOIN\" message because " << sender
                  << " is already in a cluster configuration";
        return;
    }

    if (m_config_manager.is_any_spare(sender))
    {
        LOG(INFO) << "not processing the \"JOIN\" message because " << sender
                  << " is already a spare in a cluster configuration";
        return;
    }

    if (m_config_manager.stable().head() != m_us)
    {
        LOG(INFO) << "not processing the \"JOIN\" message because we are not the head";
        return;
    }

    configuration new_config = m_config_manager.latest();

    if (new_config.may_add_spare())
    {
        chain_node n = new_config.get(sender.token);

        if (n == chain_node() || n == sender)
        {
            LOG(INFO) << "adding " << sender << " as a spare node";
            new_config.add_spare(sender);
            propose_config(new_config);
        }
        else
        {
            LOG(INFO) << "not processing the \"JOIN\" message from " << sender
                      << " because of a duplicate token";
        }
    }
    else
    {
        LOG(INFO) << "not processing the \"JOIN\" message from " << sender
                  << " because we have no space for spare servers";
    }
}

#define SEND_CONFIG_RESP(NODE, ACTION, ID, TIME, US) \
    do \
    { \
        size_t CONCAT(_sz, __LINE__) = BUSYBEE_HEADER_SIZE \
                                     + pack_size(REPLNET_CONFIG_ ## ACTION) \
                                     + 2 * sizeof(uint64_t) \
                                     + pack_size(US); \
        std::auto_ptr<e::buffer> CONCAT(_msg, __LINE__)( \
                e::buffer::create(CONCAT(_sz, __LINE__))); \
        CONCAT(_msg, __LINE__)->pack_at(BUSYBEE_HEADER_SIZE) \
            << (REPLNET_CONFIG_ ## ACTION) << ID << TIME << US; \
        send(NODE, CONCAT(_msg, __LINE__)); \
    } \
    while(0)

void
replicant_daemon :: process_config_propose(const replicant::connection& conn,
                                           std::auto_ptr<e::buffer>,
                                           e::buffer::unpacker up)
{
    uint64_t proposal_id;
    uint64_t proposal_time;
    chain_node sender;
    std::vector<configuration> config_chain;
    up = up >> proposal_id >> proposal_time >> sender >> config_chain;
    CHECK_UNPACK(CONFIG_PROPOSE, up);
    LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " received";

    if (config_chain.empty())
    {
        LOG(ERROR) << "dropping proposal " << proposal_id << ":" << proposal_time
                   << " because it is empty (file a bug)";
        return;
    }

    if (!conn.matches(sender))
    {
        LOG(ERROR) << "dropping \"CONFIG_PROPOSE\" message because the tokens "
                   << "don't match (file a bug)";
        return;
    }

    if (m_fs.is_rejected_configuration(proposal_id, proposal_time))
    {
        SEND_CONFIG_RESP(sender, REJECT, proposal_id, proposal_time, m_us);
        LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " previously rejected; responding";
        return;
    }

    if (m_fs.is_accepted_configuration(proposal_id, proposal_time))
    {
        SEND_CONFIG_RESP(sender, ACCEPT, proposal_id, proposal_time, m_us);
        LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " previously accpted; responding";
        return;
    }

    if (m_fs.is_proposed_configuration(proposal_id, proposal_time))
    {
        LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " previously proposed; waiting";
        return;
    }

    size_t idx_stable = 0;

    while (idx_stable < config_chain.size() &&
           config_chain[idx_stable] != m_config_manager.stable())
    {
        ++idx_stable;
    }

    bool reject = false;

    if (idx_stable == config_chain.size() && config_chain[0].version() > m_config_manager.stable().version())
    {
        LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time
                  << " is rooted in a stable configuration that supercedes our own;"
                  << " treating it as an inform message";
        m_config_manager.reset(config_chain[0]);
        m_fs.inform_configuration(config_chain[0]);
        accept_config(config_chain[0]);
        idx_stable = 0;
    }
    else if (idx_stable == config_chain.size())
    {
        LOG(ERROR) << "rejecting proposal " << proposal_id << ":" << proposal_time
                   << " that does not contain our stable configuration (proposed="
                   << m_config_manager.stable().version() << ","
                   << m_config_manager.latest().version() << "; proposal="
                   << config_chain[0].version() << "," << config_chain[config_chain.size() - 1].version()
                   << ")";
        reject = true;
    }

    configuration* configs = &config_chain.front() + idx_stable;
    size_t configs_sz = config_chain.size() - idx_stable;
    m_fs.propose_configuration(proposal_id, proposal_time, configs, configs_sz);

    // Make sure that we could propose it
    if (!reject && !m_config_manager.is_compatible(configs, configs_sz))
    {
        LOG(INFO) << "rejecting proposal " << proposal_id << ":" << proposal_time
                  << " that does not merge with current proposals";
        reject = true;
    }

    for (size_t i = 0; !reject && i < configs_sz; ++i)
    {
        // Check that the configs are valid
        if (!reject && !configs[i].validate())
        {
            LOG(ERROR) << "rejecting proposal " << proposal_id << ":" << proposal_time
                       << " that contains a corrupt configuration";
            reject = true;
        }

        // Check the sequential links
        if (!reject && i + 1 < configs_sz &&
            (configs[i].version() + 1 != configs[i + 1].version() ||
             configs[i].this_token() != configs[i + 1].prev_token()))
        {
            LOG(ERROR) << "rejecting proposal " << proposal_id << ":" << proposal_time
                       << " that violates the configuration chain invariant";
            reject = true;
        }

        // Check that the proposed chain meets the quorum requirement
        for (size_t j = i + 1; !reject && j < configs_sz; ++j)
        {
            if (!reject && !configs[j].quorum_of(configs[i]))
            {
                // This should never happen, so it's an error
                LOG(ERROR) << "rejecting proposal " << proposal_id << ":" << proposal_time
                           << " that violates the configuration quorum invariant";
                reject = true;
            }
        }
    }

    if (!reject && configs[configs_sz - 1].prev(m_us) != sender)
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "rejecting proposal " << proposal_id << ":" << proposal_time
                   << " that did not propagate along the chain";
        reject = true;
    }

    if (reject)
    {
        LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " rejected";
        m_fs.reject_configuration(proposal_id, proposal_time);
        SEND_CONFIG_RESP(sender, REJECT, proposal_id, proposal_time, m_us);
        return;
    }
    else
    {
        if (configs_sz == 1)
        {
            LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " adopted by fiat";
            m_fs.accept_configuration(proposal_id, proposal_time);
            // no need to run hooks
            SEND_CONFIG_RESP(sender, ACCEPT, proposal_id, proposal_time, m_us);
        }
        else if (m_config_manager.latest().version() < configs[configs_sz - 1].version())
        {
            m_config_manager.merge(proposal_id, proposal_time, configs, configs_sz);

            if (configs[configs_sz - 1].config_tail() == m_us)
            {
                LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " adopted because we're the last node";
                m_fs.accept_configuration(proposal_id, proposal_time);
                accept_config(configs[configs_sz - 1]);
                SEND_CONFIG_RESP(sender, ACCEPT, proposal_id, proposal_time, m_us);
            }
            else
            {
                LOG(INFO) << "proposal " << proposal_id << ":" << proposal_time << " set as pending";

                // We must send the whole config_chain and cannot fall back on
                // configs.
                size_t sz = BUSYBEE_HEADER_SIZE
                          + pack_size(REPLNET_CONFIG_PROPOSE)
                          + 2 * sizeof(uint64_t)
                          + pack_size(m_us)
                          + pack_size(config_chain);
                std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_PROPOSE
                                                  << proposal_id << proposal_time
                                                  << m_us << config_chain;
                send(config_chain.back().next(m_us), msg);
            }
        }
    }
}

void
replicant_daemon :: process_config_accept(const replicant::connection& conn,
                                          std::auto_ptr<e::buffer>,
                                          e::buffer::unpacker up)
{
    uint64_t proposal_id;
    uint64_t proposal_time;
    chain_node sender;
    up = up >> proposal_id >> proposal_time >> sender;
    CHECK_UNPACK(CONFIG_ACCEPT, up);

    if (!conn.matches(sender))
    {
        LOG(ERROR) << "dropping \"CONFIG_ACCEPT\" message because the tokens "
                   << "don't match (file a bug)";
        return;
    }

    if (!m_fs.is_proposed_configuration(proposal_id, proposal_time))
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "dropping \"CONFIG_ACCEPT\" message for proposal we haven't seen";
        return;
    }

    if (m_fs.is_accepted_configuration(proposal_id, proposal_time))
    {
        // This is a duplicate accept, so we can drop it
        return;
    }

    if (m_fs.is_rejected_configuration(proposal_id, proposal_time))
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "dropping \"CONFIG_ACCEPT\" message for proposal we rejected";
        return;
    }

    configuration new_config;

    if (!m_config_manager.get_proposal(proposal_id, proposal_time, &new_config))
    {
        LOG(ERROR) << "could not get proposal despite knowing it was proposed (file a bug)";
        return;
    }

    if (!new_config.has_next(m_us) || new_config.next(m_us) != sender)
    {
        LOG(ERROR) << "dropping \"CONFIG_ACCEPT\" message that comes from the wrong place";
        return;
    }

    LOG(INFO) << "accepting proposal " << proposal_id << ":" << proposal_time;
    m_fs.accept_configuration(proposal_id, proposal_time);
    accept_config(new_config);

    if (new_config.has_prev(m_us))
    {
        SEND_CONFIG_RESP(new_config.prev(m_us), ACCEPT, proposal_id, proposal_time, m_us);
    }
}

void
replicant_daemon :: process_config_reject(const replicant::connection& conn,
                                          std::auto_ptr<e::buffer>,
                                          e::buffer::unpacker up)
{
    uint64_t proposal_id;
    uint64_t proposal_time;
    chain_node sender;
    up = up >> proposal_id >> proposal_time >> sender;
    CHECK_UNPACK(CONFIG_REJECT, up);

    if (!conn.matches(sender))
    {
        LOG(ERROR) << "dropping \"CONFIG_REJECT\" message because the tokens "
                   << "don't match (file a bug)";
        return;
    }

    if (!m_fs.is_proposed_configuration(proposal_id, proposal_time))
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "dropping \"CONFIG_REJECT\" message for proposal we haven't seen";
        return;
    }

    if (m_fs.is_accepted_configuration(proposal_id, proposal_time))
    {
        // This should never happen, so it's an error
        LOG(ERROR) << "dropping \"CONFIG_REJECT\" message for proposal we accepted";
        return;
    }

    if (m_fs.is_rejected_configuration(proposal_id, proposal_time))
    {
        // This is a duplicate accept, so we can drop it
        return;
    }

    configuration new_config;

    if (!m_config_manager.get_proposal(proposal_id, proposal_time, &new_config))
    {
        LOG(ERROR) << "could not get proposal despite knowing it was proposed (file a bug)";
        return;
    }

    if (!new_config.has_next(m_us) || new_config.next(m_us) != sender)
    {
        LOG(ERROR) << "dropping \"CONFIG_REJECT\" message that comes from the wrong place";
        return;
    }

    LOG(INFO) << "rejecting proposal " << proposal_id << ":" << proposal_time;
    m_fs.reject_configuration(proposal_id, proposal_time);
    m_config_manager.reject(proposal_id, proposal_time);

    if (new_config.has_prev(m_us))
    {
        SEND_CONFIG_RESP(new_config.prev(m_us), REJECT, proposal_id, proposal_time, m_us);
    }
}

// caller must enforce quorum and chaining invariants
void
replicant_daemon :: propose_config(const configuration& config)
{
    assert(config.validate());
    uint64_t proposal_id = 0xdeadbeefcafebabeULL;
    uint64_t proposal_time = e::time();
    generate_token(&proposal_id);
    std::vector<configuration> config_chain;
    m_config_manager.get_config_chain(&config_chain);
    config_chain.push_back(config);

    configuration* configs = &config_chain.front();
    size_t configs_sz = config_chain.size();
    assert(configs_sz > 1);
    assert(configs[configs_sz - 1] == config);
    assert(configs[configs_sz - 1].head() == m_us);
    m_fs.propose_configuration(proposal_id, proposal_time, configs, configs_sz);
    m_config_manager.merge(proposal_id, proposal_time, configs, configs_sz);
    LOG(INFO) << "proposing " << proposal_id << ":" << proposal_time << " " << config;

    if (configs[configs_sz - 1].config_tail() == m_us)
    {
        m_fs.accept_configuration(proposal_id, proposal_time);
        accept_config(configs[configs_sz - 1]);
    }
    else
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_PROPOSE)
                  + 2 * sizeof(uint64_t)
                  + pack_size(m_us)
                  + pack_size(config_chain);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_PROPOSE
                                          << proposal_id << proposal_time
                                          << m_us << config_chain;
        send(config_chain.back().next(m_us), msg);
    }
}

void
replicant_daemon :: accept_config(const configuration& new_config)
{
    LOG(INFO) << "deploying configuration " << new_config;
    trip_periodic(0, &replicant_daemon::periodic_join_cluster);
    trip_periodic(0, &replicant_daemon::periodic_retry_reconfiguration);
    trip_periodic(0, &replicant_daemon::periodic_maintain_cluster);
    trip_periodic(0, &replicant_daemon::periodic_describe_cluster);
    configuration old_config(m_config_manager.stable());
    m_config_manager.advance(new_config);
    accept_config_inform_spares(old_config);
    accept_config_inform_clients(old_config);
    accept_config_reset_healing(old_config);
    std::vector<chain_node> nodes;
    m_config_manager.get_all_nodes(&nodes);
    m_failure_manager.reset(nodes);
}

void
replicant_daemon :: accept_config_inform_spares(const configuration& /*old_config*/)
{
    const configuration& new_config(m_config_manager.stable());

    for (const chain_node* spare = new_config.spares_begin();
            spare != new_config.spares_end(); ++spare)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_INFORM)
                  + pack_size(new_config);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << new_config;
        send(*spare, msg);
    }
}

void
replicant_daemon :: periodic_join_cluster(uint64_t now)
{
    if (m_config_manager.in_any_cluster(m_us))
    {
        return;
    }

    trip_periodic(now + m_s.JOIN_INTERVAL, &replicant_daemon::periodic_join_cluster);

    for (const chain_node* n = m_config_manager.stable().members_begin();
            n != m_config_manager.stable().members_end(); ++n)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_JOIN)
                  + pack_size(m_us);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_JOIN << m_us;

        if (send(*n, msg))
        {
            return;
        };
    }

    LOG(WARNING) << "having a little trouble joining the cluster; it seems no one wants to talk to us";
}

static void
mean_and_stdev(const std::map<uint64_t, double>& suspicions, double* mean, double* stdev)
{
    double n = 0;
    *mean = 0;
    double M2 = 0;

    for (std::map<uint64_t, double>::const_iterator it = suspicions.begin();
            it != suspicions.end(); ++it)
    {
        if (isinf(it->second))
        {
            continue;
        }

        ++n;
        double delta = it->second - *mean;
        *mean = *mean + delta / n;
        M2 = M2 + delta * (it->second - *mean);
    }

    *stdev = n > 1 ? sqrt(M2 / (n - 1)) : 0;
}

void
replicant_daemon :: periodic_maintain_cluster(uint64_t now)
{
    trip_periodic(now + m_s.MAINTAIN_INTERVAL, &replicant_daemon::periodic_maintain_cluster);
    // desired fault tolerance level
    uint64_t f_d = m_s.FAULT_TOLERANCE;
    // current fault tolerance level
    uint64_t f_c = m_config_manager.latest().fault_tolerance();

    // identify nodes that the failure-detector claims failed
    std::map<uint64_t, double> suspicions;
    m_failure_manager.get_suspicions(now, m_config_manager.latest(), &suspicions);
    double mean = 0;
    double stdev = 0;
    mean_and_stdev(suspicions, &mean, &stdev);
    double thresh = std::max(mean + 2 * stdev, 10.0);
    configuration fail_config(m_config_manager.latest());
    uint64_t failed = 0;

    for (std::map<uint64_t, double>::const_iterator it = suspicions.begin();
            it != suspicions.end(); ++it)
    {
        if (it->second > thresh)
        {
            chain_node n = fail_config.get(it->first);
            fail_config.remove(n);
            ++failed;
        }
    }

    if (failed && failed <= f_c && fail_config.head() == m_us &&
        fail_config.validate() &&
        m_config_manager.is_quorum_for_all(fail_config))
    {
        LOG(INFO) << "proposing new config which routes around failed nodes";
        fail_config.bump_version();
        propose_config(fail_config);
        f_c = m_config_manager.latest().fault_tolerance();
    }

    // add nodes to restore fault tolerance
    while (f_c < f_d &&
           m_config_manager.latest().head() == m_us &&
           m_config_manager.latest().may_promote_spare())
    {
        configuration new_config(m_config_manager.latest());
        LOG(INFO) << "promoting spare " << *new_config.spares_begin() << " to restore fault tolerance";
        new_config.promote_spare(*new_config.spares_begin());
        propose_config(new_config);
        f_c = m_config_manager.latest().fault_tolerance();
    }

    if (m_config_manager.stable().version() == m_config_manager.latest().version())
    {
        while (m_config_manager.latest().head() == m_us &&
               m_config_manager.latest().may_promote_standby())
        {
            configuration new_config(m_config_manager.latest());
            LOG(INFO) << "promoting standby " << *new_config.standbys_begin() << " to restore fault tolerance";
            new_config.promote_standby();
            propose_config(new_config);
            f_c = m_config_manager.latest().fault_tolerance();
        }
    }
}

void
replicant_daemon :: periodic_describe_cluster(uint64_t now)
{
    trip_periodic(now + m_s.REPORT_INTERVAL, &replicant_daemon::periodic_describe_cluster);
    LOG(INFO) << "the latest stable configuration is " << m_config_manager.stable();
    LOG(INFO) << "the latest proposed configuration is " << m_config_manager.latest();
    uint64_t f_d = m_s.FAULT_TOLERANCE;
    uint64_t f_c = m_config_manager.stable().fault_tolerance();

    if (f_c < f_d)
    {
        LOG(WARNING) << "the most recently deployed configuration can tolerate at most "
                     << f_c << " failures which is less than the " << f_d
                     << " failures the cluster is expected to tolerate; "
                     << "bring " << m_config_manager.stable().servers_needed_for(f_d)
                     << " more servers online to restore "
                     << f_d << "-fault tolerance";
    }
    else
    {
        LOG(INFO) << "the most recently deployed configuration can tolerate the expected " << f_d << " failures";
    }
}

void
replicant_daemon :: periodic_retry_reconfiguration(uint64_t now)
{
    trip_periodic(now + m_s.RETRY_RECONFIGURE_INTERVAL, &replicant_daemon::periodic_retry_reconfiguration);

    if (m_config_manager.stable().version() == m_config_manager.latest().version())
    {
        return;
    }

    std::vector<configuration> config_chain;
    std::vector<configuration_manager::proposal> proposals;
    m_config_manager.get_config_chain(&config_chain);
    m_config_manager.get_proposals(&proposals);

    for (size_t i = 0; i < config_chain.size(); ++ i)
    {
        if (!config_chain[i].has_next(m_us))
        {
            continue;
        }

        configuration_manager::proposal* prop = NULL;

        for (size_t j = 0; j < proposals.size(); ++j)
        {
            if (proposals[j].version == config_chain[i].version())
            {
                prop = &proposals[j];
                break;
            }
        }

        if (!prop)
        {
            continue;
        }

        std::vector<configuration> cc(config_chain.begin(), config_chain.begin() + i + 1);
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_PROPOSE)
                  + 2 * sizeof(uint64_t)
                  + pack_size(m_us)
                  + pack_size(cc);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_PROPOSE
                                          << prop->id << prop->time
                                          << m_us << cc;
        send(config_chain[i].next(m_us), msg);
    }
}

void
replicant_daemon :: handle_disruption_reset_reconfiguration(uint64_t token)
{
    std::vector<configuration> config_chain;
    std::vector<configuration_manager::proposal> proposals;
    m_config_manager.get_config_chain(&config_chain);
    m_config_manager.get_proposals(&proposals);

    for (size_t i = 0; i < config_chain.size(); ++ i)
    {
        if (!config_chain[i].has_next(m_us) || config_chain[i].next(m_us).token != token)
        {
            continue;
        }

        configuration_manager::proposal* prop = NULL;

        for (size_t j = 0; j < proposals.size(); ++j)
        {
            if (proposals[j].version == config_chain[i].version())
            {
                prop = &proposals[j];
                break;
            }
        }

        if (!prop)
        {
            continue;
        }

        std::vector<configuration> cc(config_chain.begin(), config_chain.begin() + i + 1);
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_CONFIG_PROPOSE)
                  + 2 * sizeof(uint64_t)
                  + pack_size(m_us)
                  + pack_size(cc);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CONFIG_PROPOSE
                                          << prop->id << prop->time
                                          << m_us << cc;
        send(config_chain[i].next(m_us), msg);
    }
}

void
replicant_daemon :: process_client_register(const replicant::connection& conn,
                                            std::auto_ptr<e::buffer>,
                                            e::buffer::unpacker up)
{
    uint64_t client;
    up = up >> client;
    CHECK_UNPACK(CLIENT_REGISTER, up);
    bool success = true;

    if (conn.is_cluster_member)
    {
        LOG(WARNING) << "rejecting registration for client that comes from a cluster member";
        success = false;
    }

    if (m_fs.is_client(client))
    {
        LOG(WARNING) << "rejecting registration for client that comes from a dead client";
        success = false;
    }

    if (conn.token != client)
    {
        LOG(WARNING) << "rejecting registration for client (" << client
                     << ") that does not match its token (" << conn.token << ")";
        success = false;
    }

    if (m_config_manager.stable().head() != m_us)
    {
        LOG(WARNING) << "rejecting registration for client because we are not the head";
        success = false;
    }

    if (!success)
    {
        replicant::response_returncode rc = replicant::RESPONSE_REGISTRATION_FAIL;
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_RESPONSE)
                  + sizeof(uint64_t)
                  + pack_size(rc);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE << uint64_t(0) << rc;
        send(conn, msg);
        return;
    }

    uint64_t slot = m_fs.next_slot_to_issue();
    issue_command(slot, OBJECT_CLI_REG, client, 0, e::slice("", 0));
}

void
replicant_daemon :: process_client_disconnect(const replicant::connection& conn,
                                              std::auto_ptr<e::buffer>,
                                              e::buffer::unpacker up)
{
    uint64_t nonce;
    up = up >> nonce;
    CHECK_UNPACK(CLIENT_REGISTER, up);

    if (!conn.is_client)
    {
        LOG(WARNING) << "rejecting \"CLIENT_DISCONNECT\" that doesn't come from a client";
        return;
    }

    if (m_config_manager.stable().head() != m_us)
    {
        LOG(WARNING) << "rejecting \"CLIENT_DISCONNECT\" because we are not the head";
        return;
    }

    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_CLIENT_DISCONNECT);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_CLIENT_DISCONNECT;
    uint64_t slot = m_fs.next_slot_to_issue();
    issue_command(slot, OBJECT_CLI_DIE, conn.token, nonce, e::slice("", 0));
}

void
replicant_daemon :: accept_config_inform_clients(const configuration& /*old_config*/)
{
    const configuration& new_config(m_config_manager.stable());
    std::vector<uint64_t> clients;
    m_fs.get_all_clients(&clients);

    for (size_t i = 0; i < clients.size(); ++i)
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_INFORM)
                  + pack_size(m_config_manager.stable());
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << new_config;
        send(clients[i], msg);
    }
}

void
replicant_daemon :: process_command_submit(const replicant::connection& conn,
                                           std::auto_ptr<e::buffer> msg,
                                           e::buffer::unpacker up)
{
    uint64_t object;
    uint64_t client;
    uint64_t nonce;
    up = up >> object >> client >> nonce;
    CHECK_UNPACK(COMMAND_SUBMIT, up);
    e::slice data = up.as_slice();

    // Check for special objects that a client tries to affect directly
    if (object != OBJECT_OBJ_NEW && object != OBJECT_OBJ_DEL &&
        IS_SPECIAL_OBJECT(object) && !conn.is_cluster_member)
    {
        LOG(INFO) << "dropping \"COMMAND_SUBMIT\" for special object that "
                  << "was not sent by a cluster member";
        return;
    }

    if (!conn.is_cluster_member && !conn.is_client)
    {
        LOG(INFO) << "dropping \"COMMAND_SUBMIT\" from " << conn.token
                  << " because it is not a client or cluster member";
        return;
    }

    if (conn.is_client && conn.token != client)
    {
        LOG(INFO) << "dropping \"COMMAND_SUBMIT\" from " << conn.token
                  << " because it uses the wrong token";
        return;
    }

    uint64_t slot = 0;

    if (m_fs.get_slot(client, nonce, &slot))
    {
        assert(slot > 0);
        replicant::response_returncode rc;
        std::string backing;

        if (m_fs.get_exec(slot, &rc, &data, &backing))
        {
            size_t sz = BUSYBEE_HEADER_SIZE
                      + pack_size(REPLNET_COMMAND_RESPONSE)
                      + sizeof(uint64_t)
                      + pack_size(rc)
                      + data.size();
            msg.reset(e::buffer::create(sz));
            e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
            pa = pa << REPLNET_COMMAND_RESPONSE << nonce << rc;
            pa = pa.copy(data);
            send(client, msg);
        }
        // else: drop it, it's proposed, but not executed

        return;
    }

    // If we are not the head
    if (m_config_manager.stable().head() != m_us)
    {
        // bounce the message
        send(m_config_manager.stable().head(), msg);
        return;
    }

    slot = m_fs.next_slot_to_issue();
    issue_command(slot, object, client, nonce, data);
}

void
replicant_daemon :: process_command_issue(const replicant::connection& conn,
                                          std::auto_ptr<e::buffer>,
                                          e::buffer::unpacker up)
{
    uint64_t slot = 0;
    uint64_t object = 0;
    uint64_t client = 0;
    uint64_t nonce = 0;
    up = up >> slot >> object >> client >> nonce;
    CHECK_UNPACK(COMMAND_ISSUE, up);
    e::slice data = up.as_slice();

    if (!conn.is_prev)
    {
        LOG(INFO) << "dropping \"COMMAND_ISSUE\" which didn't come from the right host";
        return;
    }

    if (m_fs.is_acknowledged_slot(slot))
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_ACK)
                  + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ACK << slot;
        send(conn, msg);
        return;
    }

    if (m_fs.is_issued_slot(slot))
    {
        // just drop it, we're waiting for an ACK ourselves
        return;
    }

    issue_command(slot, object, client, nonce, data);
}

void
replicant_daemon :: process_command_ack(const replicant::connection& conn,
                                        std::auto_ptr<e::buffer>,
                                        e::buffer::unpacker up)
{
    uint64_t slot = 0;
    up = up >> slot;
    CHECK_UNPACK(COMMAND_ACK, up);

    if (!conn.is_next)
    {
        LOG(INFO) << "dropping \"COMMAND_ACK\" which didn't come from the right host";
        return;
    }

    if (!m_fs.is_issued_slot(slot))
    {
        LOG(WARNING) << "dropping \"COMMAND_ACK\" for slot that was not issued";
        return;
    }

    acknowledge_command(slot);
}

void
replicant_daemon :: issue_command(uint64_t slot,
                                  uint64_t object,
                                  uint64_t client,
                                  uint64_t nonce,
                                  const e::slice& data)
{
    if (slot != m_fs.next_slot_to_issue())
    {
        LOG(WARNING) << "dropping command issue that violates monotonicity "
                     << "slot=" << slot << " expected=" << m_fs.next_slot_to_issue();
        return;
    }

#ifdef REPL_LOG_COMMANDS
    LOG(INFO) << "ISSUE " << slot << " " << data.hex();
#endif

    m_fs.issue_slot(slot, object, client, nonce, data);

    if (m_config_manager.stable().has_next(m_us))
    {
        if (m_heal_next.state >= heal_next::HEALTHY_SENT)
        {
            size_t sz = BUSYBEE_HEADER_SIZE
                      + pack_size(REPLNET_COMMAND_ISSUE)
                      + 4 * sizeof(uint64_t)
                      + data.size();
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
            pa = pa << REPLNET_COMMAND_ISSUE
                    << slot << object << client << nonce;
            pa.copy(data);
            send(m_config_manager.stable().next(m_us), msg);
        }
    }

    if (m_config_manager.stable().command_tail() == m_us)
    {
        acknowledge_command(slot);
    }
}

void
replicant_daemon :: acknowledge_command(uint64_t slot)
{
    if (m_heal_next.state != heal_next::HEALTHY)
    {
        m_heal_next.acknowledged = std::max(m_heal_next.acknowledged, slot + 1);
        transfer_more_state();
    }

    if (m_fs.is_acknowledged_slot(slot))
    {
        // eliminate the dupe silently
        return;
    }

    if (slot != m_fs.next_slot_to_ack())
    {
        LOG(WARNING) << "dropping command ACK that violates monotonicity "
                     << "slot=" << slot << " expected=" << m_fs.next_slot_to_issue();
        return;
    }

    uint64_t object;
    uint64_t client;
    uint64_t nonce;
    e::slice data;
    std::string backing;

    if (!m_fs.get_slot(slot, &object, &client, &nonce, &data, &backing))
    {
        LOG(ERROR) << "cannot ack slot " << slot << " because there are gaps in our history (file a bug)";
        abort();
        return;
    }

#ifdef REPL_LOG_COMMANDS
    LOG(INFO) << "ACK " << slot;
#endif

    m_fs.ack_slot(slot);

    if (m_config_manager.stable().has_prev(m_us))
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_ACK)
                  + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_ACK << slot;
        send(m_config_manager.stable().prev(m_us), msg);
    }

    if (object == OBJECT_CLI_REG || object == OBJECT_CLI_DIE)
    {
        if (object == OBJECT_CLI_REG)
        {
            m_fs.reg_client(client);
        }
        else
        {
            m_fs.die_client(client);
        }

        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_RESPONSE)
                  + sizeof(uint64_t) + pack_size(replicant::RESPONSE_SUCCESS);
        std::auto_ptr<e::buffer> response(e::buffer::create(sz));
        response->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_COMMAND_RESPONSE << nonce << replicant::RESPONSE_SUCCESS;
        send(client, response);
    }
    else
    {
        m_object_manager.enqueue(slot, object, client, nonce, data, &backing);
    }
}

void
replicant_daemon :: record_execution(uint64_t slot, uint64_t client, uint64_t nonce, replicant::response_returncode rc, const e::slice& data)
{
    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_COMMAND_RESPONSE)
              + sizeof(uint64_t)
              + pack_size(rc)
              + data.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << REPLNET_COMMAND_RESPONSE << nonce << rc;
    pa = pa.copy(data);
    send(client, msg);
    m_fs.exec_slot(slot, rc, data);
}

void
replicant_daemon :: process_heal_req(const replicant::connection& conn,
                                     std::auto_ptr<e::buffer>,
                                     e::buffer::unpacker up)
{
    uint64_t version;
    up = up >> version;
    CHECK_UNPACK(HEAL_REQ, up);

    if (m_config_manager.stable().version() > version)
    {
        LOG(INFO) << "dropping \"HEAL_REQ\" for older version=" << version;
        return;
    }

    if (m_config_manager.stable().version() < version)
    {
        LOG(ERROR) << "dropping \"HEAL_REQ\" for newer version=" << version << " (file a bug)";
        return;
    }

    if (!m_config_manager.stable().has_prev(m_us) ||
        m_config_manager.stable().prev(m_us).token != conn.token)
    {
        LOG(INFO) << "dropping \"HEAL_REQ\" that isn't from our predecessor";
        return;
    }

    uint64_t to_ack = m_fs.next_slot_to_ack();

    size_t sz = BUSYBEE_HEADER_SIZE
              + pack_size(REPLNET_HEAL_RESP)
              + 2 * sizeof(uint64_t);
    std::auto_ptr<e::buffer> resp(e::buffer::create(sz));
    resp->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_HEAL_RESP << version << to_ack;
    send(conn, resp);
}

void
replicant_daemon :: process_heal_resp(const replicant::connection& conn,
                                      std::auto_ptr<e::buffer>,
                                      e::buffer::unpacker up)
{
    uint64_t version;
    uint64_t to_ack;
    up = up >> version >> to_ack;
    CHECK_UNPACK(HEAL_RESP, up);

    if (version != m_config_manager.stable().version())
    {
        LOG(INFO) << "dropping \"HEAL_RESP\" for an old healing process";
        return;
    }

    if (m_heal_next.state != heal_next::REQUEST_SENT)
    {
        LOG(INFO) << "dropping \"HEAL_RESP\" that we weren't expecting";
        return;
    }

    if (!m_config_manager.stable().has_next(m_us) ||
        m_config_manager.stable().next(m_us).token != conn.token)
    {
        LOG(INFO) << "dropping \"RESP_STATE\" that isn't from our successor";
        return;
    }

    // Process all acks up to, but not including, next to_ack
    while (m_fs.next_slot_to_ack() < m_fs.next_slot_to_issue() &&
           m_fs.next_slot_to_ack() < to_ack)
    {
        acknowledge_command(m_fs.next_slot_to_ack());
    }

    // take the min in case the next host is way ahead of us
    to_ack = std::min(to_ack, m_fs.next_slot_to_ack());
    m_heal_next.state = heal_next::HEALING;
    m_heal_next.acknowledged = to_ack;
    m_heal_next.proposed = to_ack;

    LOG(INFO) << "initiating state transfer starting at slot " << to_ack;
    transfer_more_state();
}

void
replicant_daemon :: process_heal_done(const replicant::connection& conn,
                                      std::auto_ptr<e::buffer> msg,
                                      e::buffer::unpacker)
{
    if (conn.is_next && m_heal_next.state == heal_next::HEALTHY_SENT)
    {
        // we can move m_heal_next from HEALTHY_SENT to HEALTHY
        m_heal_next.state = heal_next::HEALTHY;
        LOG(INFO) << "the connection with the next node is 100% healed";
    }

    if (conn.is_prev)
    {
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_HEAL_DONE;
        send(conn, msg);
    }
}

void
replicant_daemon :: transfer_more_state()
{
    while (m_heal_next.state < heal_next::HEALTHY_SENT &&
           m_heal_next.proposed < m_fs.next_slot_to_issue() &&
           m_heal_next.proposed - m_heal_next.acknowledged <= m_s.TRANSFER_WINDOW)
    {
        uint64_t slot = m_heal_next.proposed;
        uint64_t object;
        uint64_t client;
        uint64_t nonce;
        e::slice data;
        std::string backing;

        if (!m_fs.get_slot(slot, &object, &client, &nonce, &data, &backing))
        {
            LOG(ERROR) << "cannot transfer slot " << m_heal_next.proposed << " because there are gaps in our history (file a bug)";
            abort();
        }

        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_COMMAND_ISSUE)
                  + 4 * sizeof(uint64_t)
                  + data.size();
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
        pa = pa << REPLNET_COMMAND_ISSUE << slot << object << client << nonce;
        pa.copy(data);

        if (!m_config_manager.stable().has_next(m_us))
        {
            LOG(ERROR) << "cannot heal because there is no next node";
            abort();
        }

        if (send(m_config_manager.stable().next(m_us), msg))
        {
            ++m_heal_next.proposed;
        }
        else
        {
            break;
        }
    }

    if (m_heal_next.state < heal_next::HEALTHY_SENT &&
        m_heal_next.proposed == m_fs.next_slot_to_issue())
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_HEAL_DONE);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_HEAL_DONE;

        if (send(m_config_manager.stable().next(m_us), msg))
        {
            LOG(INFO) << "state transfer complete; falling back to normal chain operation";
            m_heal_next.state = heal_next::HEALTHY_SENT;
        }
    }
}

void
replicant_daemon :: accept_config_reset_healing(const configuration& /*old_config*/)
{
    const configuration& new_config(m_config_manager.stable());

    if (new_config.has_next(m_us))
    {
        m_heal_next = heal_next();
        trip_periodic(0, &replicant_daemon::periodic_heal_next);
    }
    else
    {
        m_heal_next = heal_next();
        m_heal_next.state = heal_next::HEALTHY;
    }

    if (new_config.config_tail() == m_us)
    {
        while (m_fs.next_slot_to_ack() < m_fs.next_slot_to_issue())
        {
            acknowledge_command(m_fs.next_slot_to_ack());
        }
    }

    if (!new_config.is_member(m_us))
    {
        m_fs.clear_unacked_slots();
    }
}

void
replicant_daemon :: periodic_heal_next(uint64_t now)
{
    // keep running this function until we are healed
    if (m_heal_next.state != heal_next::HEALTHY)
    {
        trip_periodic(now + m_s.HEAL_NEXT_INTERVAL, &replicant_daemon::periodic_heal_next);
    }

    size_t sz;
    std::auto_ptr<e::buffer> msg;

    switch (m_heal_next.state)
    {
        case heal_next::BROKEN:
            assert(m_config_manager.stable().has_next(m_us));
            sz = BUSYBEE_HEADER_SIZE
               + pack_size(REPLNET_HEAL_REQ)
               + sizeof(uint64_t);
            msg.reset(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_HEAL_REQ
                                              << m_config_manager.stable().version();

            if (send(m_config_manager.stable().next(m_us), msg))
            {
                m_heal_next.state = heal_next::REQUEST_SENT;
            }
            break;
        case heal_next::REQUEST_SENT:
        case heal_next::HEALING:
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
replicant_daemon :: handle_disruption_reset_healing(uint64_t token)
{
    if (m_config_manager.stable().has_next(m_us))
    {
        chain_node n = m_config_manager.stable().next(m_us);

        if (token == n.token)
        {
            m_heal_next = heal_next();
            trip_periodic(0, &replicant_daemon::periodic_heal_next);
        }
    }
}

void
replicant_daemon :: process_ping(const replicant::connection& conn,
                                 std::auto_ptr<e::buffer> msg,
                                 e::buffer::unpacker up)
{
    uint64_t version = 0;
    up = up >> version;
    CHECK_UNPACK(PING, up);

    if (version < m_config_manager.stable().version())
    {
        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_INFORM)
                  + pack_size(m_config_manager.stable());
        std::auto_ptr<e::buffer> inf(e::buffer::create(sz));
        inf->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_INFORM << m_config_manager.stable();
        send(conn, inf);
    }

    msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PONG;
    send(conn, msg);
}

void
replicant_daemon :: process_pong(const replicant::connection& conn,
                                 std::auto_ptr<e::buffer>,
                                 e::buffer::unpacker)
{
    m_failure_manager.heartbeat(conn.token, monotonic_time());
}

void
replicant_daemon :: periodic_exchange(uint64_t now)
{
    trip_periodic(now + m_s.PING_INTERVAL, &replicant_daemon::periodic_exchange);
    std::vector<chain_node> nodes;
    m_config_manager.get_all_nodes(&nodes);
    uint64_t version = m_config_manager.stable().version();

    for (size_t i = 0; i < nodes.size(); ++i)
    {
        if (nodes[i] == m_us)
        {
            continue;
        }

        size_t sz = BUSYBEE_HEADER_SIZE
                  + pack_size(REPLNET_PING)
                  + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << REPLNET_PING << version;
        send(nodes[i], msg);
    }
}

bool
replicant_daemon :: recv(replicant::connection* conn, std::auto_ptr<e::buffer>* msg)
{
    while (s_continue)
    {
        run_periodic();
        busybee_returncode rc = m_busybee->recv(&conn->token, msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
                continue;
            case BUSYBEE_DISRUPTED:
                handle_disruption(conn->token);
                continue;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            default:
                LOG(ERROR) << "BusyBee returned " << rc << " during a \"recv\" call";
                return false;
        }

        const configuration& config(m_config_manager.stable());

        for (const chain_node* n = config.members_begin();
                n != config.members_end(); ++n)
        {
            if (n->token == conn->token)
            {
                conn->is_cluster_member = true;
                conn->is_client = false;
                conn->is_prev = config.has_prev(m_us) && config.prev(m_us) == *n;
                conn->is_next = config.has_next(m_us) && config.next(m_us) == *n;
                return true;
            }
        }

        for (const chain_node* n = config.standbys_begin();
                n != config.standbys_end(); ++n)
        {
            if (n->token == conn->token)
            {
                conn->is_cluster_member = true;
                conn->is_client = false;
                conn->is_prev = config.has_prev(m_us) && config.prev(m_us) == *n;
                conn->is_next = config.has_next(m_us) && config.next(m_us) == *n;
                return true;
            }
        }

        for (const chain_node* n = config.spares_begin();
                n != config.spares_end(); ++n)
        {
            if (n->token == conn->token)
            {
                conn->is_cluster_member = true;
                conn->is_client = false;
                conn->is_prev = false;
                conn->is_next = false;
                return true;
            }
        }

        conn->is_cluster_member = false;
        conn->is_client = m_fs.is_live_client(conn->token);
        conn->is_prev = false;
        conn->is_next = false;
        return true;
    }

    return false;
}

bool
replicant_daemon :: send(const replicant::connection& conn, std::auto_ptr<e::buffer> msg)
{
    if (m_disrupted_backoff.find(conn.token) != m_disrupted_backoff.end())
    {
        return false;
    }

    switch (m_busybee->send(conn.token, msg))
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(conn.token);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        default:
            return false;
    }
}

bool
replicant_daemon :: send(const chain_node& node, std::auto_ptr<e::buffer> msg)
{
    if (m_disrupted_backoff.find(node.token) != m_disrupted_backoff.end())
    {
        return false;
    }

    m_busybee_mapper.set(node);

    switch (m_busybee->send(node.token, msg))
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(node.token);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        default:
            return false;
    }
}

bool
replicant_daemon :: send(uint64_t token, std::auto_ptr<e::buffer> msg)
{
    if (m_disrupted_backoff.find(token) != m_disrupted_backoff.end())
    {
        return false;
    }

    switch (m_busybee->send(token, msg))
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(token);
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        default:
            return false;
    }
}

void
replicant_daemon :: handle_disruption(uint64_t token)
{
    m_disrupted_unhandled.push(token);
    trip_periodic(0, &replicant_daemon::periodic_handle_disruption);
}

static bool
compare_disrupted(const std::pair<uint64_t, uint64_t>& lhs, const std::pair<uint64_t, uint64_t>& rhs)
{
    return lhs.first > rhs.first;
}

void
replicant_daemon :: periodic_handle_disruption(uint64_t now)
{
    uint64_t start = m_disrupted_times.empty() ? UINT64_MAX : m_disrupted_times[0].first;

    while (!m_disrupted_unhandled.empty())
    {
        uint64_t token = m_disrupted_unhandled.front();
        m_disrupted_unhandled.pop();

        if (m_disrupted_backoff.find(token) == m_disrupted_backoff.end())
        {
            m_disrupted_backoff.insert(token);
            m_disrupted_times.push_back(std::make_pair(now + m_s.CONNECTION_RETRY, token));
            std::push_heap(m_disrupted_times.begin(), m_disrupted_times.end(), compare_disrupted);
        }
    }

    uint64_t end = m_disrupted_times.empty() ? UINT64_MAX : m_disrupted_times[0].first;

    if (end < start)
    {
        trip_periodic(end, &replicant_daemon::periodic_retry_disruption);
    }
}

void
replicant_daemon :: periodic_retry_disruption(uint64_t now)
{
    while (!m_disrupted_times.empty() && m_disrupted_times[0].first <= now)
    {
        std::pop_heap(m_disrupted_times.begin(), m_disrupted_times.end(), compare_disrupted);
        uint64_t token = m_disrupted_times.back().second;
        m_disrupted_times.pop_back();
        handle_disruption_reset_reconfiguration(token);
        handle_disruption_reset_healing(token);
        m_disrupted_backoff.erase(token);
    }

    if (!m_disrupted_times.empty())
    {
        trip_periodic(m_disrupted_times[0].first, &replicant_daemon::periodic_retry_disruption);
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

    // Clean up dead functions from the front
    while (!m_periodic.empty() && m_periodic[0].second == &replicant_daemon::periodic_nop)
    {
        std::pop_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
        m_periodic.pop_back();
    }

    // And from the back
    while (!m_periodic.empty() && m_periodic.back().second == &replicant_daemon::periodic_nop)
    {
        m_periodic.pop_back();
    }

    m_periodic.push_back(std::make_pair(when, fp));
    std::push_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
}

void
replicant_daemon :: run_periodic()
{
    uint64_t now = monotonic_time();

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

bool
replicant_daemon :: generate_token(uint64_t* token)
{
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        return false;
    }

    if (sysrand.read(token, sizeof(*token)) != sizeof(*token))
    {
        return false;
    }

    return true;
}
