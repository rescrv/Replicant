// Copyright (c) 2012, Robert Escriva
// Copyright (c) 2013, Howard Chu, Symas Corp.
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

// e
#include <e/endian.h>

// Replicant
#include "daemon/fact_store.h"

using replicant::fact_store;

///////////////////////////////////// Utils ////////////////////////////////////

#define STRLENOF(s)    (sizeof(s)-1)
#define KEY_SIZE_PROPOSAL (STRLENOF("prop") + sizeof(uint64_t) + STRLENOF(":") + sizeof(uint64_t))
#define KEY_SIZE_ACCEPTED_PROPOSAL (STRLENOF("acc") + sizeof(uint64_t) + STRLENOF(":") + sizeof(uint64_t))
#define KEY_SIZE_REJECTED_PROPOSAL (STRLENOF("rej") + sizeof(uint64_t) + STRLENOF(":") + sizeof(uint64_t))
#define KEY_SIZE_INFORM_CONFIG (STRLENOF("inf") + sizeof(uint64_t))
#define KEY_SIZE_CLIENT (STRLENOF("client") + sizeof(uint64_t))
#define KEY_SIZE_SLOT (STRLENOF("slot") + sizeof(uint64_t))
#define KEY_SIZE_ACK  (STRLENOF("ack") + sizeof(uint64_t))
#define KEY_SIZE_EXEC (STRLENOF("exec") + sizeof(uint64_t))
#define KEY_SIZE_NONCE (STRLENOF("nonce") + sizeof(uint64_t) + sizeof(uint64_t))

#define MVS(v,s)    v.mv_data = (void *)s; v.mv_size = STRLENOF(s)

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
        mdb_env_close(m_db);
        m_db = NULL;
    }
}

bool
fact_store :: open(const po6::pathname& path,
                   bool* saved,
                   chain_node* saved_us,
                   configuration_manager* saved_config_manager)
{
    MDB_txn *txn;
    MDB_val key, data;
    int rc;
    bool ret = false;

    rc = mdb_env_create(&m_db);
    if (rc)
    {
        LOG(ERROR) << "could not create LMDB env: " << mdb_strerror(rc);
        return false;
    }
    rc = mdb_env_set_mapsize(m_db, 10485760);    /* 10MB default */
    rc = mdb_env_open(m_db, path.get(), MDB_WRITEMAP|MDB_NOSYNC, 0600);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB env: " << mdb_strerror(rc);
        return false;
    }
    rc = mdb_txn_begin(m_db, NULL, 0, &txn);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB txn: " << mdb_strerror(rc);
        return false;
    }
    rc = mdb_open(txn, NULL, 0, &m_dbi);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB dbi: " << mdb_strerror(rc);
        mdb_txn_abort(txn);
        return false;
    }

	/* Set this up for readers to use later */
	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &m_rtxn);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB read txn: " << mdb_strerror(rc);
        return false;
    }
	rc = mdb_cursor_open(m_rtxn, m_dbi, &m_rcsr);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB read cursor: " << mdb_strerror(rc);
        return false;
    }
	mdb_txn_reset(m_rtxn);

    bool first_time = false;

    MVS(key, "replicant");
    rc = mdb_get(txn, m_dbi, &key, &data);

    if (rc == MDB_SUCCESS)
    {
        if (memcmp(data.mv_data, PACKAGE_VERSION, STRLENOF(PACKAGE_VERSION)) &&
            memcmp(data.mv_data, "0.1.1", STRLENOF("0.1.1")) &&
            memcmp(data.mv_data, "0.1.0", STRLENOF("0.1.0")))
        {
            LOG(ERROR) << "could not restore from LMDB because "
                       << "the existing data was created by "
                       << "replicant " << data.mv_data << " but "
                       << "this is version " << PACKAGE_VERSION;
            goto leave;
        }
    }
    else if (rc == MDB_NOTFOUND)
    {
        first_time = true;
        MVS(data, PACKAGE_VERSION);
        rc = mdb_put(txn, m_dbi, &key, &data, 0);

        if (rc)
        {
            LOG(ERROR) << "could not restore from LMDB because Put failed:  "
                       << mdb_strerror(rc);
            goto leave;
        }
    }
    else
    {
        LOG(ERROR) << "could not restore from LMDB:  "
                   << mdb_strerror(rc);
        goto leave;
    }

    MVS(key, "state");
    rc = mdb_get(txn, m_dbi, &key, &data);

    if (rc == MDB_SUCCESS)
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LMDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you're on your own with this one";
            goto leave;
        }
    }
    else if (rc == MDB_NOTFOUND)
    {
        if (!first_time)
        {
            LOG(ERROR) << "could not restore from LMDB because a previous "
                       << "execution crashed; run the recovery program and try again";
            goto leave;
        }
    }
    else
    {
        LOG(ERROR) << "could not restore from LMDB:  "
                   << mdb_strerror(rc);
        goto leave;
    }

    rc = mdb_txn_commit(txn);
    txn = NULL;
    if (rc)
    {
            LOG(ERROR) << "could not restore from LMDB because txn_commit failed:  "
                       << mdb_strerror(rc);
            goto leave;
    }

    if (first_time)
    {
        *saved = false;
        return true;
    }

    *saved = true;
    {
    // XXX inefficient, lazy hack
    std::auto_ptr<e::buffer> buf(e::buffer::create(data.mv_size));
    memmove(buf->data(), data.mv_data, data.mv_size);
    buf->resize(data.mv_size);
    e::unpacker up = buf->unpack_from(0);
    up = up >> *saved_us >> *saved_config_manager;

    if (up.error())
    {
        LOG(ERROR) << "could not restore from LevelDB because a previous "
                   << "execution saved invalid state; run the recovery program and try again";
    } else
    {
        ret = true;
    }
    }

leave:
    mdb_txn_abort(txn);
    return ret;
}

bool
fact_store :: close(const chain_node& us_to_save,
                    const configuration_manager& config_manager_to_save)
{
    if (m_db)
    {
        MDB_val key, data;
        MDB_txn *txn;
        int rc;
        size_t sz = pack_size(us_to_save) + pack_size(config_manager_to_save);
        std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
        buf->pack_at(0) << us_to_save << config_manager_to_save;

        rc = mdb_txn_begin(m_db, NULL, 0, &txn);
        if (rc)
        {
            LOG(ERROR) << "could not save state to LMDB because txn_begin failed:  "
                       << mdb_strerror(rc);
            return false;
        }
        MVS(key, "state");
        data.mv_data = buf->data();
        data.mv_size = buf->size();
        rc = mdb_put(txn, m_dbi, &key, &data, 0);
        if (rc == MDB_SUCCESS)
        {
            rc = mdb_txn_commit(txn);
        }
        else
        {
            mdb_txn_abort(txn);
        }
        if (rc)
        {
            LOG(ERROR) << "could not save state to LMDB:  "
                       << mdb_strerror(rc);
            return false;
        }
        mdb_env_close(m_db);
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
	MDB_val backing;
	bool ret = false;

    if (!retrieve_value(key, KEY_SIZE_CLIENT, &backing))
    {
        return false;
    }

    if (backing.mv_size != 3)
    {
        abort();
    }

    if (!memcmp(backing.mv_data, "reg", 3))
    {
        ret = true;
    }
    else if (memcmp(backing.mv_data, "die", 3))
    {
        abort();
    }
	mdb_txn_reset(m_rtxn);
	return ret;
}

void
fact_store :: get_all_clients(std::vector<uint64_t>* clients)
{
    MDB_val key;
    int rc;

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
            LOG(ERROR) << "could not read client list because txn_renew failed:  "
                       << mdb_strerror(rc);
            return;
    }
    rc = mdb_cursor_renew(m_rtxn, m_rcsr);
    if (rc)
    {
            LOG(ERROR) << "could not read client list because cursor_open failed:  "
                       << mdb_strerror(rc);
            mdb_txn_reset(m_rtxn);
            return;
    }
    MVS(key, "client\x00\x00\x00\x00\x00\x00\x00\x00");

    rc = mdb_cursor_get(m_rcsr, &key, NULL, MDB_SET_RANGE);
    while (rc == MDB_SUCCESS)
    {
        if (strncmp((const char *)key.mv_data, "client", 6) == 0 && key.mv_size == 14)
        {
            uint64_t tmp;
            e::unpack64be((char *)key.mv_data + 6, &tmp);
            clients->push_back(tmp);
        }
        else
        {
            break;
        }
        rc = mdb_cursor_get(m_rcsr, &key, NULL, MDB_NEXT);
    }
    mdb_txn_reset(m_rtxn);
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
	MDB_val bdata;
    pack_key_slot(slot, key);

    if (!retrieve_value(key, KEY_SIZE_SLOT, &bdata))
    {
        return false;
    }

    if (bdata.mv_size < 3 * sizeof(uint64_t))
    {
        abort();
    }

    backing->assign((const char *)bdata.mv_data, bdata.mv_size);
	mdb_txn_reset(m_rtxn);
    const char* ptr = backing->data();
    ptr = e::unpack64be(ptr, object);
    ptr = e::unpack64be(ptr, client);
    ptr = e::unpack64be(ptr, nonce);
    *data = e::slice(ptr, bdata.mv_size - 3 * sizeof(uint64_t));
    return true;
}

bool
fact_store :: get_slot(uint64_t client,
                       uint64_t nonce,
                       uint64_t* slot)
{
    char key[KEY_SIZE_NONCE];
    pack_key_nonce(client, nonce, key);
    MDB_val backing;

    if (!retrieve_value(key, KEY_SIZE_NONCE, &backing))
    {
        return false;
    }

    if (backing.mv_size < sizeof(uint64_t))
    {
        abort();
    }

    e::unpack64be((const char *)backing.mv_data, slot);
	mdb_txn_reset(m_rtxn);
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
	MDB_val bdata;

    if (!retrieve_value(key, KEY_SIZE_EXEC, &bdata))
    {
        return false;
    }

    if (bdata.mv_size == 0)
    {
        abort();
    }

    backing->assign((const char *)bdata.mv_data, bdata.mv_size);
	mdb_txn_reset(m_rtxn);
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
    MDB_val key;
    int rc;

    if (m_cache_next_slot_issue != 0)
    {
        return m_cache_next_slot_issue;
    }

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
            LOG(ERROR) << "could not read slot list because txn_renew failed:  "
                       << mdb_strerror(rc);
            abort();
    }
    rc = mdb_cursor_renew(m_rtxn, m_rcsr);
    if (rc)
    {
            LOG(ERROR) << "could not read slot list because cursor_open failed:  "
                       << mdb_strerror(rc);
            mdb_txn_reset(m_rtxn);
            abort();
    }

    MVS(key, "slot\x00\x00\x00\x00\x00\x00\x00\x00");

    uint64_t next_to_issue = 1;

    rc = mdb_cursor_get(m_rcsr, &key, NULL, MDB_SET_RANGE);
    while (rc == MDB_SUCCESS)
    {
        if (strncmp((const char *)key.mv_data, "slot", 4) == 0 && key.mv_size == 12)
        {
            uint64_t tmp;
            e::unpack64be((char *)key.mv_data + 4, &tmp);
            next_to_issue = std::max(next_to_issue, tmp + 1);
        }
        else
        {
            break;
        }
        rc = mdb_cursor_get(m_rcsr, &key, NULL, MDB_NEXT);
    }
    mdb_txn_reset(m_rtxn);

    m_cache_next_slot_issue = next_to_issue;
    return next_to_issue;
}

uint64_t
fact_store :: next_slot_to_ack()
{
    MDB_val key;
    int rc;

    if (m_cache_next_slot_ack != 0)
    {
        return m_cache_next_slot_ack;
    }

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
            LOG(ERROR) << "could not read ack list because txn_begin failed:  "
                       << mdb_strerror(rc);
            abort();
    }
    rc = mdb_cursor_renew(m_rtxn, m_rcsr);
    if (rc)
    {
            LOG(ERROR) << "could not read ack list because cursor_open failed:  "
                       << mdb_strerror(rc);
            mdb_txn_reset(m_rtxn);
            abort();
    }

    MVS(key, "ack\x00\x00\x00\x00\x00\x00\x00\x00");

    uint64_t next_to_ack = 1;

    rc = mdb_cursor_get(m_rcsr, &key, NULL, MDB_SET_RANGE);
    while (rc == MDB_SUCCESS)
    {
        if (strncmp((const char *)key.mv_data, "ack", 3) == 0 && key.mv_size == 11)
        {
            uint64_t tmp;
            e::unpack64be((char *)key.mv_data + 3, &tmp);
            next_to_ack = std::max(next_to_ack, tmp + 1);
        }
        else
        {
            break;
        }
        rc = mdb_cursor_get(m_rcsr, &key, NULL, MDB_NEXT);
    }
    mdb_txn_reset(m_rtxn);

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
    MDB_val k;
    int rc;

    k.mv_data = (void *)key;
    k.mv_size = key_sz;

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
        LOG(ERROR) << "txn_renew failed:  " << mdb_strerror(rc);
        abort();
    }
    rc = mdb_get(m_rtxn, m_dbi, &k, NULL);
    mdb_txn_reset(m_rtxn);
    if (rc == MDB_SUCCESS)
    {
        return true;
    }
    else if (rc == MDB_NOTFOUND)
    {
        return false;
    }
    else
    {
        LOG(ERROR) << "LMDB failed: " << mdb_strerror(rc);
        abort();
    }
}

bool
fact_store :: retrieve_value(const char* key, size_t key_sz,
                             MDB_val *backing)
{
    MDB_val k, data;
    int rc;

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
        LOG(ERROR) << "txn_renew failed:  " << mdb_strerror(rc);
        abort();
    }
    rc = mdb_get(m_rtxn, m_dbi, &k, backing);
    if (rc == MDB_SUCCESS)
    {
        return true;
    }
    else if (rc == MDB_NOTFOUND)
    {
		mdb_txn_reset(m_rtxn);
        return false;
    }
    else
    {
        LOG(ERROR) << "LMDB failed: " << mdb_strerror(rc);
        abort();
    }
}

void
fact_store :: store_key_value(const char* key, size_t key_sz,
                              const char* value, size_t value_sz)
{
    MDB_txn *txn;
    MDB_val k, data;
    int rc;

    rc = mdb_txn_begin(m_db, NULL, 0, &txn);
    if (rc)
    {
        LOG(ERROR) << "txn_begin failed:  " << mdb_strerror(rc);
        abort();
    }
    k.mv_data = (void *)key;
    k.mv_size = key_sz;
    data.mv_data = (void *)value;
    data.mv_size = value_sz;
    rc = mdb_put(txn, m_dbi, &k, &data, 0);
    if (rc == MDB_SUCCESS)
    {
        rc = mdb_txn_commit(txn);
    }
    if (rc == MDB_SUCCESS)
    {
        return;
    }
    else
    {
        LOG(ERROR) << "LMDB put failed: " << mdb_strerror(rc);
        mdb_txn_abort(txn);
        abort();
    }
}

void
fact_store :: delete_key(const char* key, size_t key_sz)
{
    MDB_txn *txn;
    MDB_val k;
    int rc;

    rc = mdb_txn_begin(m_db, NULL, 0, &txn);
    if (rc)
    {
        LOG(ERROR) << "txn_begin failed:  " << mdb_strerror(rc);
        abort();
    }
    k.mv_data = (void *)key;
    k.mv_size = key_sz;
    rc = mdb_del(txn, m_dbi, &k, NULL);
    if (rc == MDB_NOTFOUND)
    {
        mdb_txn_abort(txn);
        return;
    }
    if (rc == MDB_SUCCESS)
    {
        rc = mdb_txn_commit(txn);
    }
    if (rc == MDB_SUCCESS)
    {
        return;
    }
    else
    {
        LOG(ERROR) << "LMDB delete failed: " << mdb_strerror(rc);
        abort();
    }
}
