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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// STL
#include <memory>

// Google Log
#include <glog/logging.h>

// LevelDB
#include <leveldb/filter_policy.h>
#include <leveldb/iterator.h>

// e
#include <e/endian.h>

// Replicant
#include "daemon/fact_store.h"

using replicant::fact_store;

///////////////////////////////////// Utils ////////////////////////////////////

#define KEY_SIZE_PROPOSAL (4 /*strlen("prop")*/ + 8 /*sizeof(uint64_t)*/ + 1 /*strlen(":")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_ACCEPTED_PROPOSAL (3 /*strlen("acc")*/ + 8 /*sizeof(uint64_t)*/ + 1 /*strlen(":")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_REJECTED_PROPOSAL (3 /*strlen("rej")*/ + 8 /*sizeof(uint64_t)*/ + 1 /*strlen(":")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_INFORM_CONFIG (3 /*strlen("inf")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_CLIENT (6 /*strlen("client")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_SLOT (4 /*strlen("slot")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_ACK  (3 /*strlen("ack")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_EXEC (4 /*strlen("exec")*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_NONCE (5 /*strlen("nonce")*/ + 8 /*sizeof(uint64_t)*/ + 8 /*sizeof(uint64_t)*/)

static void
pack_key_proposal(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    memmove(key, "prop", 4);
    e::pack64be(proposal_id, key + 4);
    key[12] = ':';
    e::pack64be(proposal_time, key + 13);
}

static void
pack_key_accepted_proposal(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    memmove(key, "acc", 3);
    e::pack64be(proposal_id, key + 3);
    key[11] = ':';
    e::pack64be(proposal_time, key + 12);
}

static void
pack_key_rejected_proposal(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    memmove(key, "rej", 3);
    e::pack64be(proposal_id, key + 3);
    key[11] = ':';
    e::pack64be(proposal_time, key + 12);
}

static void
pack_key_inform_config(uint64_t version, char* key)
{
    memmove(key, "inf", 3);
    e::pack64be(version, key + 3);
}

static void
pack_key_client(uint64_t client, char* key)
{
    memmove(key, "client", 6);
    e::pack64be(client, key + 6);
}

static void
pack_key_slot(uint64_t slot, char* key)
{
    memmove(key, "slot", 4);
    e::pack64be(slot, key + 4);
}

static void
pack_key_ack(uint64_t slot, char* key)
{
    memmove(key, "ack", 3);
    e::pack64be(slot, key + 3);
}

static void
pack_key_exec(uint64_t slot, char* key)
{
    memmove(key, "exec", 4);
    e::pack64be(slot, key + 4);
}

static void
pack_key_nonce(uint64_t client, uint64_t nonce, char* key)
{
    memmove(key, "nonce", 5);
    e::pack64be(client, key + 5);
    e::pack64be(nonce, key + 13);
}

////////////////////////////////// Fact Store //////////////////////////////////

fact_store :: fact_store()
    : m_db(NULL)
    , m_cache_next_slot_issue(0)
    , m_cache_next_slot_ack(0)
{
}

fact_store :: ~fact_store() throw ()
{
    if (m_db)
    {
        delete m_db;
        m_db = NULL;
    }
}

bool
fact_store :: open(const po6::pathname& path,
                   bool* saved,
                   chain_node* saved_us,
                   configuration_manager* saved_config_manager)
{
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    std::string name(path.get());
    leveldb::Status st = leveldb::DB::Open(opts, name, &m_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
        return false;
    }

    leveldb::ReadOptions ropts;
    ropts.fill_cache = true;
    ropts.verify_checksums = true;
    leveldb::WriteOptions wopts;
    wopts.sync = true;

    leveldb::Slice rk("replicant", 9);
    std::string rbacking;
    st = m_db->Get(ropts, rk, &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;

        if (rbacking != PACKAGE_VERSION &&
            rbacking != "0.1.1" &&
            rbacking != "0.1.0")
        {
            LOG(ERROR) << "could not restore from LevelDB because "
                       << "the existing data was created by "
                       << "replicant " << rbacking << " but "
                       << "this is version " << PACKAGE_VERSION;
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        first_time = true;
        leveldb::Slice k("replicant", 9);
        leveldb::Slice v(PACKAGE_VERSION, strlen(PACKAGE_VERSION));
        st = m_db->Put(wopts, k, v);

        if (st.ok())
        {
            // fall through
        }
        else if (st.IsNotFound())
        {
            LOG(ERROR) << "could not restore from LevelDB because Put returned NotFound:  "
                       << st.ToString();
            return false;
        }
        else if (st.IsCorruption())
        {
            LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                       << st.ToString();
            return false;
        }
        else if (st.IsIOError())
        {
            LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                       << st.ToString();
            return false;
        }
        else
        {
            LOG(ERROR) << "could not restore from LevelDB because it returned an "
                       << "unknown error that we don't know how to handle:  "
                       << st.ToString();
            return false;
        }
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not restore from LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }

    leveldb::Slice sk("state", 5);
    std::string sbacking;
    st = m_db->Get(ropts, sk, &sbacking);

    if (st.ok())
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you're on your own with this one";
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        if (!first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed; run the recovery program and try again";
            return false;
        }
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not restore from LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }

    if (first_time)
    {
        *saved = false;
        return true;
    }

    *saved = true;
    // XXX inefficient, lazy hack
    std::auto_ptr<e::buffer> buf(e::buffer::create(sbacking.size()));
    memmove(buf->data(), sbacking.data(), sbacking.size());
    buf->resize(sbacking.size());
    e::unpacker up = buf->unpack_from(0);
    up = up >> *saved_us >> *saved_config_manager;

    if (up.error())
    {
        LOG(ERROR) << "could not restore from LevelDB because a previous "
                   << "execution saved invalid state; run the recovery program and try again";
        return false;
    }

    return true;
}

bool
fact_store :: close(const chain_node& us_to_save,
                    const configuration_manager& config_manager_to_save)
{
    if (m_db)
    {
        leveldb::WriteOptions wopts;
        wopts.sync = true;
        size_t sz = pack_size(us_to_save) + pack_size(config_manager_to_save);
        std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
        buf->pack_at(0) << us_to_save << config_manager_to_save;
        leveldb::Slice k("state", 5);
        leveldb::Slice v(reinterpret_cast<const char*>(buf->data()), buf->size());
        leveldb::Status st = m_db->Put(wopts, k, v);

        if (st.ok())
        {
            // fall through
        }
        else if (st.IsNotFound())
        {
            LOG(ERROR) << "could not save state to LevelDB because Put returned NotFound:  "
                       << st.ToString();
            return false;
        }
        else if (st.IsCorruption())
        {
            LOG(ERROR) << "could not save state to LevelDB because of corruption:  "
                       << st.ToString();
            return false;
        }
        else if (st.IsIOError())
        {
            LOG(ERROR) << "could not save state to LevelDB because of an IO error:  "
                       << st.ToString();
            return false;
        }
        else
        {
            LOG(ERROR) << "could not save state to LevelDB because it returned an "
                       << "unknown error that we don't know how to handle:  "
                       << st.ToString();
            return false;
        }


        delete m_db;
        m_db = NULL;
    }

    return true;
}

void
fact_store :: remove_saved_state()
{
    delete_key("state", 5);
}

bool
fact_store :: is_proposed_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[KEY_SIZE_PROPOSAL];
    pack_key_proposal(proposal_id, proposal_time, key);
    return check_key_exists(key, KEY_SIZE_PROPOSAL);
}

bool
fact_store :: is_accepted_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[KEY_SIZE_ACCEPTED_PROPOSAL];
    pack_key_accepted_proposal(proposal_id, proposal_time, key);
    return check_key_exists(key, KEY_SIZE_ACCEPTED_PROPOSAL);
}

bool
fact_store :: is_rejected_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[KEY_SIZE_REJECTED_PROPOSAL];
    pack_key_rejected_proposal(proposal_id, proposal_time, key);
    return check_key_exists(key, KEY_SIZE_REJECTED_PROPOSAL);
}

void
fact_store :: propose_configuration(uint64_t proposal_id, uint64_t proposal_time,
                                    const configuration* configs, size_t configs_sz)
{
    assert(!is_proposed_configuration(proposal_id, proposal_time));
    char key[KEY_SIZE_PROPOSAL];
    pack_key_proposal(proposal_id, proposal_time, key);
    store_key_value(key, KEY_SIZE_PROPOSAL, "", 0);
    // XXX store the configs
}

void
fact_store :: accept_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    assert(is_proposed_configuration(proposal_id, proposal_time));
    assert(!is_accepted_configuration(proposal_id, proposal_time));
    assert(!is_rejected_configuration(proposal_id, proposal_time));
    char key[KEY_SIZE_ACCEPTED_PROPOSAL];
    pack_key_accepted_proposal(proposal_id, proposal_time, key);
    store_key_value(key, KEY_SIZE_ACCEPTED_PROPOSAL, "", 0);
}

void
fact_store :: reject_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    assert(is_proposed_configuration(proposal_id, proposal_time));
    assert(!is_accepted_configuration(proposal_id, proposal_time));
    assert(!is_rejected_configuration(proposal_id, proposal_time));
    char key[KEY_SIZE_REJECTED_PROPOSAL];
    pack_key_rejected_proposal(proposal_id, proposal_time, key);
    store_key_value(key, KEY_SIZE_REJECTED_PROPOSAL, "", 0);
}

void
fact_store :: inform_configuration(const configuration& config)
{
    char key[KEY_SIZE_INFORM_CONFIG];
    pack_key_inform_config(config.version(), key);
    std::vector<char> value(pack_size(config));
    pack_config(config, &value.front());
    store_key_value(key, KEY_SIZE_INFORM_CONFIG, &value.front(), value.size());
}

bool
fact_store :: is_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
    pack_key_client(client, key);
    return check_key_exists(key, KEY_SIZE_CLIENT);
}

bool
fact_store :: is_live_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
    pack_key_client(client, key);
    std::string backing;

    if (!retrieve_value(key, KEY_SIZE_CLIENT, &backing))
    {
        return false;
    }

    if (backing.size() != 3)
    {
        abort();
    }

    if (backing == "reg")
    {
        return true;
    }
    else if (backing == "die")
    {
        return false;
    }
    else
    {
        abort();
    }
}

void
fact_store :: get_all_clients(std::vector<uint64_t>* clients)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    assert(it.get());

    it->Seek(leveldb::Slice("client\x00\x00\x00\x00\x00\x00\x00\x00", 14));

    while (it->Valid())
    {
        leveldb::Slice key(it->key());

        if (strncmp(key.data(), "client", 6) == 0 && key.size() == 14)
        {
            uint64_t tmp;
            e::unpack64be(key.data() + 6, &tmp);
            clients->push_back(tmp);
        }
        else
        {
            break;
        }

        it->Next();
    }
}

void
fact_store :: reg_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
    pack_key_client(client, key);
    store_key_value(key, KEY_SIZE_CLIENT, "reg", 3);
}

void
fact_store :: die_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
    pack_key_client(client, key);
    store_key_value(key, KEY_SIZE_CLIENT, "die", 3);
}

bool
fact_store :: get_slot(uint64_t slot,
                       uint64_t* object,
                       uint64_t* client,
                       uint64_t* nonce,
                       e::slice* data,
                       std::string* backing)
{
    char key[KEY_SIZE_SLOT];
    pack_key_slot(slot, key);

    if (!retrieve_value(key, KEY_SIZE_SLOT, backing))
    {
        return false;
    }

    if (backing->size() < 3 * sizeof(uint64_t))
    {
        abort();
    }

    const char* ptr = backing->data();
    ptr = e::unpack64be(ptr, object);
    ptr = e::unpack64be(ptr, client);
    ptr = e::unpack64be(ptr, nonce);
    *data = e::slice(ptr, backing->size() - 3 * sizeof(uint64_t));
    return true;
}

bool
fact_store :: get_slot(uint64_t client,
                       uint64_t nonce,
                       uint64_t* slot)
{
    char key[KEY_SIZE_NONCE];
    pack_key_nonce(client, nonce, key);
    std::string backing;

    if (!retrieve_value(key, KEY_SIZE_NONCE, &backing))
    {
        return false;
    }

    if (backing.size() < sizeof(uint64_t))
    {
        abort();
    }

    e::unpack64be(backing.data(), slot);
    return true;
}

bool
fact_store :: get_exec(uint64_t slot,
                       replicant::response_returncode* rc,
                       e::slice* data,
                       std::string* backing)
{
    char key[KEY_SIZE_EXEC];
    pack_key_exec(slot, key);

    if (!retrieve_value(key, KEY_SIZE_EXEC, backing))
    {
        return false;
    }

    if (backing->size() == 0)
    {
        abort();
    }

    *rc = static_cast<replicant::response_returncode>((*backing)[0]);
    *data = e::slice(backing->data() + 1, backing->size() - 1);
    return true;
}

bool
fact_store :: is_acknowledged_slot(uint64_t slot)
{
    char key[KEY_SIZE_ACK];
    pack_key_ack(slot, key);
    return check_key_exists(key, KEY_SIZE_ACK);
}

bool
fact_store :: is_issued_slot(uint64_t slot)
{
    char key[KEY_SIZE_SLOT];
    pack_key_slot(slot, key);
    return check_key_exists(key, KEY_SIZE_SLOT);
}

uint64_t
fact_store :: next_slot_to_issue()
{
    if (m_cache_next_slot_issue != 0)
    {
        return m_cache_next_slot_issue;
    }

    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    assert(it.get());

    it->Seek(leveldb::Slice("slot\x00\x00\x00\x00\x00\x00\x00\x00", 12));
    uint64_t next_to_issue = 1;

    while (it->Valid())
    {
        leveldb::Slice key(it->key());

        if (strncmp(key.data(), "slot", 4) == 0 && key.size() == 12)
        {
            uint64_t tmp;
            e::unpack64be(key.data() + 4, &tmp);
            next_to_issue = std::max(next_to_issue, tmp + 1);
        }
        else
        {
            break;
        }

        it->Next();
    }

    m_cache_next_slot_issue = next_to_issue;
    return next_to_issue;
}

uint64_t
fact_store :: next_slot_to_ack()
{
    if (m_cache_next_slot_ack != 0)
    {
        return m_cache_next_slot_ack;
    }

    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    assert(it.get());

    it->Seek(leveldb::Slice("ack\x00\x00\x00\x00\x00\x00\x00\x00", 11));
    uint64_t next_to_ack = 1;

    while (it->Valid())
    {
        leveldb::Slice key(it->key());

        if (strncmp(key.data(), "ack", 3) == 0 && key.size() == 11)
        {
            uint64_t tmp;
            e::unpack64be(key.data() + 3, &tmp);
            next_to_ack = std::max(next_to_ack, tmp + 1);
        }
        else
        {
            break;
        }

        it->Next();
    }

    m_cache_next_slot_ack = next_to_ack;
    return next_to_ack;
}

void
fact_store :: issue_slot(uint64_t slot,
                         uint64_t object,
                         uint64_t client,
                         uint64_t nonce,
                         const e::slice& data)
{
    assert(slot == m_cache_next_slot_issue || m_cache_next_slot_issue == 0);
    char key[KEY_SIZE_SLOT];
    pack_key_slot(slot, key);
    std::vector<char> value(3 * sizeof(uint64_t) + data.size());
    char* ptr = &value.front();
    ptr = e::pack64be(object, ptr);
    ptr = e::pack64be(client, ptr);
    ptr = e::pack64be(nonce, ptr);
    memmove(ptr, data.data(), data.size());
    store_key_value(key, KEY_SIZE_SLOT, &value.front(), value.size());

    if (slot == m_cache_next_slot_issue && m_cache_next_slot_issue != 0)
    {
        ++m_cache_next_slot_issue;
    }

    char keyn[KEY_SIZE_NONCE];
    pack_key_nonce(client, nonce, keyn);
    char valn[sizeof(uint64_t)];
    e::pack64be(slot, valn);
    store_key_value(keyn, KEY_SIZE_NONCE, valn, sizeof(uint64_t));
}

void
fact_store :: ack_slot(uint64_t slot)
{
    assert(slot == m_cache_next_slot_ack || m_cache_next_slot_ack == 0);
    char key[KEY_SIZE_ACK];
    pack_key_ack(slot, key);
    store_key_value(key, KEY_SIZE_ACK, "", 0);

    if (slot == m_cache_next_slot_ack && m_cache_next_slot_ack != 0)
    {
        ++m_cache_next_slot_ack;
    }
}

void
fact_store :: exec_slot(uint64_t slot,
                        replicant::response_returncode rc,
                        const e::slice& data)
{
    char key[KEY_SIZE_EXEC];
    pack_key_exec(slot, key);
    std::vector<char> value(sizeof(uint8_t) + data.size());
    char* ptr = &value.front();
    *ptr = static_cast<uint8_t>(rc);
    ++ptr;
    memmove(ptr, data.data(), data.size());
    store_key_value(key, KEY_SIZE_EXEC, &value.front(), value.size());
}

void
fact_store :: clear_unacked_slots()
{
    uint64_t acked = next_slot_to_ack();
    uint64_t issued = next_slot_to_issue();

    while (issued > acked)
    {
        --issued;
        char key[KEY_SIZE_SLOT];
        pack_key_slot(issued, key);
        delete_key(key, KEY_SIZE_SLOT);
    }

    m_cache_next_slot_issue = 0;
    m_cache_next_slot_ack = 0;
}

bool
fact_store :: check_key_exists(const char* key, size_t key_sz)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    leveldb::Slice k(key, key_sz);
    std::string backing;
    leveldb::Status st = m_db->Get(opts, k, &backing);

    if (st.ok())
    {
        return true;
    }
    else if (st.IsNotFound())
    {
        return false;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption:  " << st.ToString();
        abort();
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error:  " << st.ToString();
        abort();
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        abort();
    }
}

bool
fact_store :: retrieve_value(const char* key, size_t key_sz,
                             std::string* backing)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    leveldb::Slice k(key, key_sz);
    leveldb::Status st = m_db->Get(opts, k, backing);

    if (st.ok())
    {
        return true;
    }
    else if (st.IsNotFound())
    {
        return false;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption:  " << st.ToString();
        abort();
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error:  " << st.ToString();
        abort();
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        abort();
    }
}

void
fact_store :: store_key_value(const char* key, size_t key_sz,
                              const char* value, size_t value_sz)
{
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Slice k(key, key_sz);
    leveldb::Slice v(value, value_sz);
    leveldb::Status st = m_db->Put(opts, k, v);

    if (st.ok())
    {
        return;
    }
    else if (st.IsNotFound())
    {
        LOG(ERROR) << "PUT returned not NotFound; this doesn't make sense";
        abort();
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption:  " << st.ToString();
        abort();
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error:  " << st.ToString();
        abort();
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        abort();
    }
}

void
fact_store :: delete_key(const char* key, size_t key_sz)
{
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Slice k(key, key_sz);
    leveldb::Status st = m_db->Delete(opts, k);

    if (st.ok() || st.IsNotFound())
    {
        return;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption:  " << st.ToString();
        abort();
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error:  " << st.ToString();
        abort();
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        abort();
    }
}
