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
#include <algorithm>
#include <map>
#include <memory>
#include <set>

// Google Log
#include <glog/logging.h>

// LevelDB
#include <hyperleveldb/filter_policy.h>
#include <hyperleveldb/iterator.h>

// e
#include <e/endian.h>

// Replicant
#include "daemon/fact_store.h"

using replicant::fact_store;

namespace
{

class prefix_iterator
{
    public:
        prefix_iterator(const leveldb::Slice& prefix, leveldb::DB* db, size_t sz);
        ~prefix_iterator() throw ();

    public:
        bool error() { return m_error; }
        bool valid();
        void next();
        leveldb::Slice key();
        leveldb::Slice val();

    private:
        prefix_iterator(const prefix_iterator&);
        prefix_iterator& operator = (const prefix_iterator&);

    private:
        leveldb::Slice m_prefix;
        std::auto_ptr<leveldb::Iterator> m_it;
        size_t m_sz;
        bool m_error;
};

prefix_iterator :: prefix_iterator(const leveldb::Slice& prefix, leveldb::DB* db, size_t sz)
    : m_prefix(prefix)
    , m_it()
    , m_sz(sz)
    , m_error(false)
{
    leveldb::ReadOptions opts;
    opts.verify_checksums = true;
    m_it.reset(db->NewIterator(opts));
    m_it->Seek(prefix);
}

prefix_iterator :: ~prefix_iterator() throw ()
{
}

bool
prefix_iterator :: valid()
{
    if (m_error)
    {
        return false;
    }

    if (m_it->Valid())
    {
        if (m_it->key().starts_with(m_prefix) && m_it->key().size() != m_sz)
        {
            m_error = true;
            return false;
        }
        else if (!m_it->key().starts_with(m_prefix))
        {
            m_it->SeekToLast();
            return false;
        }
    }
    else
    {
        return false;
    }

    return true;
}

void
prefix_iterator :: next()
{
    m_it->Next();
}

leveldb::Slice
prefix_iterator :: key()
{
    return m_it->key();
}

leveldb::Slice
prefix_iterator :: val()
{
    return m_it->value();
}

} // namespace

///////////////////////////////////// Utils ////////////////////////////////////

#define KEY_SIZE_PROPOSAL (4 /*strlen("prop")*/ + 8 /*sizeof(uint64_t)*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_ACCEPTED_PROPOSAL (3 /*strlen("acc")*/ + 8 /*sizeof(uint64_t)*/ + 8 /*sizeof(uint64_t)*/)
#define KEY_SIZE_REJECTED_PROPOSAL (3 /*strlen("rej")*/ + 8 /*sizeof(uint64_t)*/ + 8 /*sizeof(uint64_t)*/)
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
    e::pack64be(proposal_time, key + 12);
}

static void
pack_key_accepted_proposal(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    memmove(key, "acc", 3);
    e::pack64be(proposal_id, key + 3);
    e::pack64be(proposal_time, key + 11);
}

static void
pack_key_rejected_proposal(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    memmove(key, "rej", 3);
    e::pack64be(proposal_id, key + 3);
    e::pack64be(proposal_time, key + 11);
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
                   bool* restored,
                   chain_node* restored_us,
                   configuration_manager* restored_config_manager)
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

    // read the "replicant" key and check the version
    std::string rbacking;
    st = m_db->Get(ropts, leveldb::Slice("replicant", 9), &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;

        if (rbacking != PACKAGE_VERSION)
        {
            LOG(ERROR) << "could not restore from LevelDB because "
                       << "the existing data was created by "
                       << "replicant " << rbacking << " but "
                       << "this is version " << PACKAGE_VERSION << " which is not compatible";
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        first_time = true;
        leveldb::Slice k("replicant", 9);
        leveldb::Slice v(PACKAGE_VERSION, strlen(PACKAGE_VERSION));
        st = m_db->Put(wopts, k, v);

        if (!st.ok())
        {
            LOG(ERROR) << "could not save \"replicant\" key into LevelDB: " << st.ToString();
            return false;
        }
    }
    else
    {
        LOG(ERROR) << "could not read \"replicant\" key from LevelDB: " << st.ToString();
        return false;
    }

    // read the "state" key and parse it
    std::string sbacking;
    st = m_db->Get(ropts, leveldb::Slice("us", 2), &sbacking);

    if (st.ok())
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you'll need to manually erase this DB and create a new one";
            return false;
        }

        e::unpacker up(sbacking.data(), sbacking.size());
        up = up >> *restored_us;

        if (up.error())
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution wrote an invalid node identity; "
                       << "you'll need to manually erase this DB and create a new one";
            return false;
        }

        *restored = true;
    }
    else if (st.IsNotFound())
    {
        if (!only_key_is_replicant_key())
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution didn't save a node identity, and did write "
                       << "other data; "
                       << "you'll need to manually erase this DB and create a new one";
            return false;
        }

        *restored = false;
    }

    if (!fsck(false, false, restored_config_manager))
    {
        LOG(ERROR) << "found integrity errors while scanning LevelDB";
        LOG(ERROR) << "because the fix to these errors may be destructive, we "
                   << "require that you manually run \"replicant repair\" to continue";
        LOG(ERROR) << "you're advised to make a backup of the data directory first";
        return false;
    }

    return true;
}

bool
fact_store :: repair(const po6::pathname& path)
{
    leveldb::Options opts;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    std::string name(path.get());
    leveldb::Status st = leveldb::DB::Open(opts, name, &m_db);

    if (!st.ok())
    {
        std::cerr << "could not open LevelDB: " << st.ToString() << std::endl;
        return false;
    }

    leveldb::ReadOptions ropts;
    ropts.fill_cache = true;
    ropts.verify_checksums = true;
    leveldb::WriteOptions wopts;
    wopts.sync = true;

    // read the "replicant" key and check the version
    std::string rbacking;
    st = m_db->Get(ropts, leveldb::Slice("replicant", 9), &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;

        if (rbacking != PACKAGE_VERSION)
        {
            std::cerr << "could not restore from LevelDB because "
                      << "the existing data was created by "
                      << "replicant " << rbacking << " but "
                      << "this is version " << PACKAGE_VERSION << " which is not compatible" << std::endl;
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        first_time = true;
        leveldb::Slice k("replicant", 9);
        leveldb::Slice v(PACKAGE_VERSION, strlen(PACKAGE_VERSION));
        st = m_db->Put(wopts, k, v);

        if (!st.ok())
        {
            std::cerr << "could not save \"replicant\" key into LevelDB: " << st.ToString() << std::endl;
            return false;
        }
    }
    else
    {
        std::cerr << "could not read \"replicant\" key from LevelDB: " << st.ToString() << std::endl;
        return false;
    }

    // read the "state" key and parse it
    std::string sbacking;
    st = m_db->Get(ropts, leveldb::Slice("us", 2), &sbacking);

    if (st.ok())
    {
        if (first_time)
        {
            std::cerr << "could not restore from LevelDB because a previous "
                      << "execution crashed and the database was tampered with; "
                      << "you'll need to manually erase this DB and create a new one" << std::endl;
            return false;
        }

        chain_node us;
        e::unpacker up(sbacking.data(), sbacking.size());
        up = up >> us;

        if (up.error())
        {
            std::cerr << "could not restore from LevelDB because a previous "
                      << "execution wrote an invalid node identity; "
                      << "you'll need to manually erase this DB and create a new one" << std::endl;
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        if (!only_key_is_replicant_key())
        {
            std::cerr << "could not restore from LevelDB because a previous "
                      << "execution didn't save a node identity and wrote "
                      << "other data; "
                      << "you'll need to manually erase this DB and create a new one" << std::endl;
            return false;
        }
    }

    configuration_manager config_manager;

    if (!fsck(true, false, &config_manager))
    {
        std::cerr << "If any of the above messages are fatal, we cannot repair "
                  << "the database without possibly violating safety of the "
                  << "system.  To restore this node, remove the data and start "
                  << "fresh.  If a majority of nodes are in a bad state, file "
                  << "a bug and mention this note." << std::endl;
        return false;
    }

    return true;
}

bool
fact_store :: save(const chain_node& us)
{
    leveldb::WriteOptions wopts;
    wopts.sync = true;
    size_t sz = pack_size(us);
    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
    buf->pack_at(0) << us;
    leveldb::Slice k("us", 2);
    leveldb::Slice v(reinterpret_cast<const char*>(buf->data()), buf->size());
    leveldb::Status st = m_db->Put(wopts, k, v);

    if (!st.ok())
    {
        LOG(ERROR) << "could not record node identity as " << us << ": " << st.ToString();
        return false;
    }

    return true;
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
    size_t sz = 0;

    for (size_t i = 0; i < configs_sz; ++i)
    {
        sz += pack_size(configs[i]);
    }

    std::vector<char> value(sz);
    char* ptr = &value.front();

    for (size_t i = 0; i < configs_sz; ++i)
    {
        ptr = pack_config(configs[i], ptr);
    }

    store_key_value(key, KEY_SIZE_PROPOSAL, &value.front(), value.size());
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

    prefix_iterator nonces(leveldb::Slice("nonce", 5), m_db, 21);

    for (; nonces.valid(); nonces.next())
    {
        const char* ptr = nonces.key().data();
        uint64_t client;
        uint64_t nonce;
        e::unpack64be(ptr + 5, &client);
        e::unpack64be(ptr + 13, &nonce);
        ptr = nonces.val().data();

        if (nonces.val().size() != sizeof(uint64_t))
        {
            continue;
        }

        uint64_t slot;
        e::unpack64be(ptr, &slot);

        if (slot > acked)
        {
            delete_key(nonces.key().data(), nonces.key().size());
        }
    }

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

bool
fact_store :: only_key_is_replicant_key()
{
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = NULL;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    it->SeekToFirst();
    bool seen = false;

    while (it->Valid())
    {
        if (it->key().compare(leveldb::Slice("replicant", 9)) != 0)
        {
            return false;
        }

        it->Next();
        seen = true;
    }

    return seen;
}

#define FSCK_LOG if (verbose) std::cerr

bool
fact_store :: fsck(bool verbose, bool destructive, configuration_manager* config_manager)
{
    if (!fsck_meta_state(verbose, destructive, config_manager))
    {
        return false;
    }

    if (!fsck_clients(verbose, destructive))
    {
        return false;
    }

    if (!fsck_slots(verbose, destructive))
    {
        return false;
    }

    return true;
}

#define XCONCAT(x, y) x ## y
#define CONCAT(x, y) XCONCAT(x, y)
#define ADD_CONFIGURATION(CS, C, D) \
    std::map<uint64_t, configuration>::iterator CONCAT(it, __LINE__) = (CS).find((C).version()); \
    if (CONCAT(it, __LINE__) == (CS).end()) \
    { \
        (CS)[(C).version()] = (C); \
    } \
    else if (CONCAT(it, __LINE__)->second != (C)) \
    { \
        FSCK_LOG << "FATAL conflicting " D " configurations:\n" \
                 << "        " << CONCAT(it, __LINE__)->second << "\n" \
                 << "        " << (C) << std::endl; \
        return false; \
    }

bool
fact_store :: fsck_meta_state(bool verbose,
                              bool destructive,
                              configuration_manager* config_manager)
{
    typedef std::pair<uint64_t, uint64_t> uup;
    std::vector<uup> accepted_proposals;
    std::vector<uup> rejected_proposals;
    std::vector<configuration_manager::proposal> proposals;
    std::map<uint64_t, configuration> proposed;
    std::map<uint64_t, configuration> accepted;

    if (!scan_accepted_proposals(verbose, destructive, &accepted_proposals))
    {
        return false;
    }

    if (!scan_rejected_proposals(verbose, destructive, &rejected_proposals))
    {
        return false;
    }

    for (size_t i = 0; i < accepted_proposals.size(); ++i)
    {
        for (size_t j = 0; j < rejected_proposals.size(); ++j)
        {
            if (accepted_proposals[i] == rejected_proposals[j])
            {
                uint64_t proposal_id = accepted_proposals[i].first;
                uint64_t proposal_time = accepted_proposals[i].second;
                FSCK_LOG << "proposal " << proposal_id << ":" << proposal_time
                         << " rejected and accepted simultaneously" << std::endl;
                return false;
            }
        }
    }

    if (!scan_informed_configurations(verbose, destructive, &accepted))
    {
        return false;
    }

    if (!scan_proposals(verbose, destructive,
                        accepted_proposals, rejected_proposals,
                        &proposals, &proposed, &accepted))
    {
        return false;
    }

    if (accepted.empty())
    {
        return true;
    }

    std::map<uint64_t, configuration>::reverse_iterator rit = accepted.rbegin();
    config_manager->reset(rit->second);
    std::vector<configuration> latest_proposal;
    latest_proposal.push_back(rit->second);

    if (!proposed.empty())
    {
        uint64_t latest_proposed_version = proposed.rbegin()->first;

        for (uint64_t version = rit->first + 1;
                version <= latest_proposed_version; ++version)
        {
            std::map<uint64_t, configuration>::iterator it = proposed.find(version);

            if (it == proposed.end())
            {
                FSCK_LOG << "FATAL discontinuity in proposed configurations" << std::endl;
                return false;
            }

            latest_proposal.push_back(it->second);
        }
    }

    if (!config_manager->is_compatible(&latest_proposal.front(), latest_proposal.size()))
    {
        FSCK_LOG << "FATAL proposed configurations are not compatible" << std::endl;
        return false;
    }

    for (size_t i = 0; i < proposals.size(); ++i)
    {
        if (proposals[i].version > rit->first)
        {
            size_t length = proposals[i].version - rit->first + 1;
            config_manager->merge(proposals[i].id, proposals[i].time,
                                  &latest_proposal.front(), length);
        }
    }

    return true;
}

bool
fact_store :: scan_accepted_proposals(bool verbose,
                                      bool,
                                      std::vector<std::pair<uint64_t, uint64_t> >* accepted_proposals)
{
    prefix_iterator accected(leveldb::Slice("acc", 3), m_db, 19);

    for (; accected.valid(); accected.next())
    {
        const char* ptr = accected.key().data();
        uint64_t proposal_id;
        uint64_t proposal_time;
        e::unpack64be(ptr + 3, &proposal_id);
        e::unpack64be(ptr + 11, &proposal_time);
        accepted_proposals->push_back(std::make_pair(proposal_id, proposal_time));
    }

    if (accected.error())
    {
        FSCK_LOG << "FATAL scanning accepted proposals encountered bad key" << std::endl;
        return false;
    }

    std::sort(accepted_proposals->begin(), accepted_proposals->end());
    return true;
}

bool
fact_store :: scan_rejected_proposals(bool verbose,
                                      bool,
                                      std::vector<std::pair<uint64_t, uint64_t> >* rejected_proposals)
{
    prefix_iterator rejected(leveldb::Slice("rej", 3), m_db, 19);

    for (; rejected.valid(); rejected.next())
    {
        const char* ptr = rejected.key().data();
        uint64_t proposal_id;
        uint64_t proposal_time;
        e::unpack64be(ptr + 3, &proposal_id);
        e::unpack64be(ptr + 11, &proposal_time);
        rejected_proposals->push_back(std::make_pair(proposal_id, proposal_time));
    }

    if (rejected.error())
    {
        FSCK_LOG << "FATAL scanning rejected proposals encountered bad key" << std::endl;
        return false;
    }

    std::sort(rejected_proposals->begin(), rejected_proposals->end());
    return true;
}

bool
fact_store :: scan_informed_configurations(bool verbose,
                                           bool,
                                           std::map<uint64_t, configuration>* configurations)
{
    prefix_iterator informs(leveldb::Slice("inf", 3), m_db, 11);

    for (; informs.valid(); informs.next())
    {
        const char* ptr = informs.key().data();
        uint64_t version;
        e::unpack64be(ptr + 3, &version);
        configuration tmp;
        e::unpacker up(informs.val().data(), informs.val().size());
        up = up >> tmp;

        if (up.error())
        {
            FSCK_LOG << "FATAL encountered bad inform message: " << informs.val().ToString() << std::endl;
            return false;
        }

        if (tmp.version() != version)
        {
            FSCK_LOG << "FATAL informed configuration has mismatched version: " << version << " != " << tmp.version() << std::endl;
            return false;
        }

        if (!tmp.validate())
        {
            FSCK_LOG << "FATAL informed configuration does not validate: " << tmp << std::endl;
            return false;
        }

        ADD_CONFIGURATION(*configurations, tmp, "informed");
    }

    if (informs.error())
    {
        FSCK_LOG << "FATAL scanning informs encountered bad key" << std::endl;
        return false;
    }

    return true;
}

bool
fact_store :: scan_proposals(bool verbose,
                             bool,
                             const std::vector<std::pair<uint64_t, uint64_t> >& accepted_proposals,
                             const std::vector<std::pair<uint64_t, uint64_t> >& rejected_proposals,
                             std::vector<configuration_manager::proposal>* proposals,
                             std::map<uint64_t, configuration>* proposed,
                             std::map<uint64_t, configuration>* accepted)
{
    prefix_iterator props(leveldb::Slice("prop", 4), m_db, 20);

    for (; props.valid(); props.next())
    {
        const char* ptr = props.key().data();
        uint64_t proposal_id;
        uint64_t proposal_time;
        e::unpack64be(ptr + 4, &proposal_id);
        e::unpack64be(ptr + 12, &proposal_time);
        std::vector<configuration> configs;
        e::unpacker up(props.val().data(), props.val().size());

        while (!up.error() && !up.empty())
        {
            configuration tmp;
            up = up >> tmp;

            if (up.error() || !tmp.validate())
            {
                up = up.as_error();
                continue;
            }

            configs.push_back(tmp);
        }

        if (up.error() || configs.empty())
        {
            FSCK_LOG << "ERROR corrupt proposal: " << proposal_id << ":" << proposal_time << std::endl;
            return false;
        }

        if (std::binary_search(rejected_proposals.begin(),
                               rejected_proposals.end(),
                               std::make_pair(proposal_id, proposal_time)))
        {
            continue;
        }
        else if (std::binary_search(accepted_proposals.begin(),
                                    accepted_proposals.end(),
                                    std::make_pair(proposal_id, proposal_time)))
        {
            ADD_CONFIGURATION(*accepted, configs.back(), "accepted");
        }
        else
        {
            proposals->push_back(configuration_manager::proposal(proposal_id, proposal_time, configs.back().version()));

            for (size_t i = 0; i < configs.size(); ++i)
            {
                ADD_CONFIGURATION(*proposed, configs[i], "proposed");
            }

            ADD_CONFIGURATION(*accepted, configs.back(), "accepted");
        }
    }

    if (props.error())
    {
        FSCK_LOG << "FATAL scanning proposals encountered bad key" << std::endl;
        return false;
    }

    return true;
}

bool
fact_store :: fsck_clients(bool verbose,
                           bool)
{
    prefix_iterator clients(leveldb::Slice("client", 6), m_db, 14);

    for (; clients.valid(); clients.next())
    {
        const char* ptr = clients.key().data();
        uint64_t client;
        e::unpack64be(ptr + 4, &client);

        if (clients.val().compare(leveldb::Slice("reg", 3)) != 0 &&
            clients.val().compare(leveldb::Slice("die", 3)) != 0)
        {
            FSCK_LOG << "FATAL client " << client << " is neither registered nor dead" << std::endl;
            return false;
        }
    }

    if (clients.error())
    {
        FSCK_LOG << "FATAL scanning clients encountered bad key" << std::endl;
        return false;
    }

    return true;
}

bool
fact_store :: fsck_slots(bool verbose,
                         bool destructive)
{
    // check slots ////////////////////////////////////////////////////////////
    prefix_iterator slots(leveldb::Slice("slot", 4), m_db, 12);
    uint64_t prev_slot = 0;
    bool slot_jumps = false;

    for (; slots.valid(); slots.next())
    {
        const char* ptr = slots.key().data();
        uint64_t slot;
        e::unpack64be(ptr + 4, &slot);

        if (!slot_jumps && prev_slot + 1 != slot)
        {
            FSCK_LOG << "ERROR discontinuity in slots:  jumps from " << prev_slot << " to " << slot << std::endl;
            slot_jumps = true;
        }

        if (slot_jumps)
        {
            FSCK_LOG << "deleting key for slot " << slot << " to remedy the discontinuity" << std::endl;

            if (destructive)
            {
                delete_key(slots.key().data(), slots.key().size());
            }
            else
            {
                return false;
            }
        }

        prev_slot = slot;
    }

    if (slots.error())
    {
        FSCK_LOG << "FATAL scanning slots encountered bad key" << std::endl;
        return false;
    }

    // check acks /////////////////////////////////////////////////////////////
    prefix_iterator acks(leveldb::Slice("ack", 3), m_db, 11);

    for (; acks.valid(); acks.next())
    {
        const char* ptr = acks.key().data();
        uint64_t ack;
        e::unpack64be(ptr + 3, &ack);
        char skey[KEY_SIZE_SLOT];
        pack_key_slot(ack, skey);

        if (!check_key_exists(skey, KEY_SIZE_SLOT))
        {
            FSCK_LOG << "ERROR deleting key for ack " << ack << " because it has no corresponding slot" << std::endl;

            if (destructive)
            {
                delete_key(acks.key().data(), acks.key().size());
            }
            else
            {
                return false;
            }
        }
    }

    if (acks.error())
    {
        FSCK_LOG << "FATAL scanning acks encountered bad key" << std::endl;
        return false;
    }

    // check execs ////////////////////////////////////////////////////////////
    prefix_iterator execs(leveldb::Slice("exec", 4), m_db, 12);

    for (; execs.valid(); execs.next())
    {
        const char* ptr = execs.key().data();
        uint64_t exec;
        e::unpack64be(ptr + 4, &exec);
        char akey[KEY_SIZE_ACK];
        pack_key_ack(exec, akey);

        if (!check_key_exists(akey, KEY_SIZE_ACK))
        {
            FSCK_LOG << "ERROR deleting key for exec " << exec << " because it has no corresponding ack" << std::endl;

            if (destructive)
            {
                delete_key(execs.key().data(), execs.key().size());
            }
            else
            {
                return false;
            }
        }
    }

    if (execs.error())
    {
        FSCK_LOG << "FATAL scanning execs encountered bad key" << std::endl;
        return false;
    }

    // check nonces ///////////////////////////////////////////////////////////
    prefix_iterator nonces(leveldb::Slice("nonce", 5), m_db, 21);

    for (; nonces.valid(); nonces.next())
    {
        const char* ptr = nonces.key().data();
        uint64_t client;
        uint64_t nonce;
        e::unpack64be(ptr + 5, &client);
        e::unpack64be(ptr + 13, &nonce);
        ptr = nonces.val().data();

        if (nonces.val().size() != sizeof(uint64_t))
        {
            FSCK_LOG << "FATAL (client=" << client << ", nonce=" << nonce << ") does not map to a valid slot" << std::endl;
            return false;
        }

        uint64_t slot;
        e::unpack64be(ptr, &slot);
        char skey[KEY_SIZE_SLOT];
        pack_key_slot(slot, skey);
        std::string sbacking;

        if (!retrieve_value(skey, KEY_SIZE_SLOT, &sbacking) ||
            sbacking.size() < 3 * sizeof(uint64_t))
        {
            FSCK_LOG << "ERROR deleting key (client=" << client << ", nonce=" << nonce << ") because it has no valid slot" << std::endl;

            if (destructive)
            {
                delete_key(skey, KEY_SIZE_SLOT);
            }
            else
            {
                return false;
            }
        }

        uint64_t sclient;
        uint64_t snonce;
        ptr = sbacking.data();
        e::unpack64be(ptr + 8, &sclient);
        e::unpack64be(ptr + 16, &snonce);

        if (client != sclient || nonce != snonce)
        {
            FSCK_LOG << "FATAL key (client=" << client << ", nonce=" << nonce << ") points at slot " << slot << " which does not reciprocate" << std::endl;
            FSCK_LOG << "          (sclient=" << sclient << ", snonce=" << snonce << ")" << std::endl;
            return false;
        }

        char ckey[KEY_SIZE_CLIENT];
        pack_key_client(client, ckey);

        if (!check_key_exists(ckey, KEY_SIZE_CLIENT))
        {
            FSCK_LOG << "ERROR deleting key (client=" << client << ", nonce=" << nonce << ") for unknown client" << std::endl;

            if (destructive)
            {
                delete_key(ckey, KEY_SIZE_CLIENT);
            }
            else
            {
                return false;
            }
        }
    }

    if (nonces.error())
    {
        FSCK_LOG << "FATAL scanning nonces encountered bad key" << std::endl;
        return false;
    }

    return true;
}
