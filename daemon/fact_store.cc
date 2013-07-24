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
#include <algorithm>
#include <map>
#include <memory>
#include <set>

// Google Log
#include <glog/logging.h>

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
        prefix_iterator(const MDB_val *prefix, MDB_txn *rtxn, MDB_dbi dbi, size_t sz);
        ~prefix_iterator() throw ();

    public:
        bool error() { return m_error; }
        bool valid();
        void next();
        MDB_val *key();
        MDB_val *val();

    private:
        prefix_iterator(const prefix_iterator&);
        prefix_iterator& operator = (const prefix_iterator&);

    private:
        MDB_val m_prefix;
		MDB_val m_key;
		MDB_val m_val;
		MDB_txn *m_rtxn;
        MDB_cursor *m_it;
        size_t m_sz;
        bool m_error;
		bool m_valid;
};

prefix_iterator :: prefix_iterator(const MDB_val *prefix, MDB_txn *rtxn, MDB_dbi dbi, size_t sz)
    : m_prefix()
	, m_rtxn(rtxn)
    , m_it()
    , m_sz(sz)
    , m_error(false)
	, m_valid(false)
{
	int rc;
	rc = mdb_cursor_open(m_rtxn, dbi, &m_it);
	if (rc)
	{
		abort();
	}
	m_prefix = *prefix;
	m_key = *prefix;
	rc = mdb_cursor_get(m_it, &m_key, &m_val, MDB_SET_RANGE);
	if (rc == MDB_SUCCESS)
		m_valid = true;
}

prefix_iterator :: ~prefix_iterator() throw ()
{
	mdb_cursor_close(m_it);
}

bool
prefix_iterator :: valid()
{
    if (m_error)
    {
        return false;
    }

    if (m_valid)
    {
		if (memcmp(m_prefix.mv_data, m_key.mv_data, m_prefix.mv_size))
		{
			m_valid = false;
			return false;
		}
		if (m_key.mv_size != m_sz)
        {
            m_error = true;
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
	int rc;
	rc = mdb_cursor_get(m_it, &m_key, &m_val, MDB_NEXT);
	if (rc == MDB_NOTFOUND) {
		m_valid = false;
	}
}

MDB_val *
prefix_iterator :: key()
{
    return &m_key;
}

MDB_val *
prefix_iterator :: val()
{
    return &m_val;
}

} // namespace

///////////////////////////////////// Utils ////////////////////////////////////

#define STRLENOF(s)    (sizeof(s)-1)
#define KEY_SIZE_PROPOSAL (STRLENOF("prop") + sizeof(uint64_t) + sizeof(uint64_t))
#define KEY_SIZE_ACCEPTED_PROPOSAL (STRLENOF("acc") + sizeof(uint64_t) + sizeof(uint64_t))
#define KEY_SIZE_REJECTED_PROPOSAL (STRLENOF("rej") + sizeof(uint64_t) + sizeof(uint64_t))
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
        mdb_env_close(m_db);
        m_db = NULL;
    }
}

bool
fact_store :: do_open(const po6::pathname& path,
                   bool* restored,
                   chain_node* restored_us,
                   configuration_manager* restored_config_manager, bool isOpen)
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
		mdb_txn_abort(txn);
        return false;
    }
	rc = mdb_cursor_open(m_rtxn, m_dbi, &m_rcsr);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB read cursor: " << mdb_strerror(rc);
		mdb_txn_abort(m_rtxn);
		mdb_txn_abort(txn);
        return false;
    }
	mdb_txn_reset(m_rtxn);

    // read the "replicant" key and check the version
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
                       << "replicant " << (char *)data.mv_data << " but "
                       << "this is version " << PACKAGE_VERSION << " which is not compatible";
            goto leave;
        }
    }
    else if (rc == MDB_NOTFOUND)
    {
        first_time = true;
        MVS(data, PACKAGE_VERSION);
        rc = mdb_put(txn, m_dbi, &key, &data, 0);
		if (rc == MDB_SUCCESS)
		{
			rc = mdb_txn_commit(txn);
			if (rc == MDB_SUCCESS)
				rc = mdb_txn_begin(m_db, NULL, 0, &txn);
		}

        if (rc)
        {
            LOG(ERROR) << "could not save \"replicant\" key into LMDB: " << mdb_strerror(rc);
            goto leave;
        }
    }
    else
    {
        LOG(ERROR) << "could not read \"replicant\" key from LMDB: " << mdb_strerror(rc);
        goto leave;
    }

    // read the "state" key and parse it
    MVS(key, "us");
    rc = mdb_get(txn, m_dbi, &key, &data);

    if (rc == MDB_SUCCESS)
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LMDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you'll need to manually erase this DB and create a new one";
            goto leave;
        }

        e::unpacker up((const char *)data.mv_data, data.mv_size);
        up = up >> *restored_us;

        if (up.error())
        {
            LOG(ERROR) << "could not restore from LMDB because a previous "
                       << "execution wrote an invalid node identity; "
                       << "you'll need to manually erase this DB and create a new one";
            goto leave;
        }

        *restored = true;
    }
    else if (rc == MDB_NOTFOUND)
    {
        if (!only_key_is_replicant_key())
        {
            LOG(ERROR) << "could not restore from LMDB because a previous "
                       << "execution didn't save a node identity, and did write "
                       << "other data; "
                       << "you'll need to manually erase this DB and create a new one";
            goto leave;
        }

        *restored = false;
    }

    if (!fsck(!isOpen, false, restored_config_manager))
    {
		if (isOpen)
		{
        LOG(ERROR) << "found integrity errors while scanning LMDB";
        LOG(ERROR) << "because the fix to these errors may be destructive, we "
                   << "require that you manually run \"replicant repair\" to continue";
        LOG(ERROR) << "you're advised to make a backup of the data directory first";
		} else {
        std::cerr << "If any of the above messages are fatal, we cannot repair "
                  << "the database without possibly violating safety of the "
                  << "system.  To restore this node, remove the data and start "
                  << "fresh.  If a majority of nodes are in a bad state, file "
                  << "a bug and mention this note." << std::endl;
		}
        goto leave;
    }
	ret = true;

leave:
	rc = mdb_txn_commit(txn);
	if (rc)
	{
            LOG(ERROR) << "could not commit \"replicant\" key into LMDB: " << mdb_strerror(rc);
			ret = false;
	}
    return ret;
}

bool
fact_store :: open(const po6::pathname& path,
                   bool* restored,
                   chain_node* restored_us,
                   configuration_manager* restored_config_manager)
{
	return do_open(path, restored, restored_us, restored_config_manager, true);
}

bool
fact_store :: repair(const po6::pathname& path)
{
    configuration_manager config_manager;
	bool restored;
	chain_node restored_us;

	return do_open(path, &restored, &restored_us, &config_manager, false);
}

bool
fact_store :: save(const chain_node& us)
{
	MDB_val key, data;
	MDB_txn *txn;
	int rc;
    size_t sz = pack_size(us);
    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
    buf->pack_at(0) << us;

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
        LOG(ERROR) << "could not record node identity as " << us << ": " << mdb_strerror(rc);
        return false;
	}
	MVS(key, "us");
	data.mv_data = buf->data();
	data.mv_size = buf->size();
	rc = mdb_put(txn, m_dbi, &key, &data, 0);
    if (rc)
    {
        LOG(ERROR) << "could not record node identity as " << us << ": " << mdb_strerror(rc);
		mdb_txn_abort(txn);
        return false;
    }
	rc = mdb_txn_commit(txn);
    if (rc)
    {
        LOG(ERROR) << "could not record node identity as " << us << ": " << mdb_strerror(rc);
        return false;
    }

    return true;
}

bool
fact_store :: is_proposed_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[KEY_SIZE_PROPOSAL];
	MDB_val k = { sizeof(key), key };
    pack_key_proposal(proposal_id, proposal_time, key);
    return check_key_exists(&k);
}

bool
fact_store :: is_accepted_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[KEY_SIZE_ACCEPTED_PROPOSAL];
	MDB_val k = { sizeof(key), key };
    pack_key_accepted_proposal(proposal_id, proposal_time, key);
    return check_key_exists(&k);
}

bool
fact_store :: is_rejected_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[KEY_SIZE_REJECTED_PROPOSAL];
	MDB_val k = { sizeof(key), key };
    pack_key_rejected_proposal(proposal_id, proposal_time, key);
    return check_key_exists(&k);
}

void
fact_store :: propose_configuration(uint64_t proposal_id, uint64_t proposal_time,
                                    const configuration* configs, size_t configs_sz)
{
    assert(!is_proposed_configuration(proposal_id, proposal_time));
    char key[KEY_SIZE_PROPOSAL];
	MDB_val k = { sizeof(key), key };
	MDB_val val;
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

	val.mv_data = &value.front();
	val.mv_size = value.size();
    store_key_value(&k, &val);
}

void
fact_store :: accept_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    assert(is_proposed_configuration(proposal_id, proposal_time));
    assert(!is_accepted_configuration(proposal_id, proposal_time));
    assert(!is_rejected_configuration(proposal_id, proposal_time));
    char key[KEY_SIZE_ACCEPTED_PROPOSAL];
	MDB_val k = { sizeof(key), key };
	MDB_val val = { 0, (void *)"" };
    pack_key_accepted_proposal(proposal_id, proposal_time, key);
    store_key_value(&k, &val);
}

void
fact_store :: reject_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    assert(is_proposed_configuration(proposal_id, proposal_time));
    assert(!is_accepted_configuration(proposal_id, proposal_time));
    assert(!is_rejected_configuration(proposal_id, proposal_time));
    char key[KEY_SIZE_REJECTED_PROPOSAL];
	MDB_val k = { sizeof(key), key };
	MDB_val val = { 0, (void *)"" };
    pack_key_rejected_proposal(proposal_id, proposal_time, key);
    store_key_value(&k, &val);
}

void
fact_store :: inform_configuration(const configuration& config)
{
    char key[KEY_SIZE_INFORM_CONFIG];
	MDB_val k = { sizeof(key), key };
	MDB_val val;
    pack_key_inform_config(config.version(), key);
    std::vector<char> value(pack_size(config));
    pack_config(config, &value.front());
	val.mv_data = &value.front();
	val.mv_size = value.size();
    store_key_value(&k, &val);
}

bool
fact_store :: is_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
	MDB_val k = { sizeof(key), key };
    pack_key_client(client, key);
    return check_key_exists(&k);
}

bool
fact_store :: is_live_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
	MDB_val k = { sizeof(key), key };
    pack_key_client(client, key);
    MDB_val backing;
	bool ret = false;

    if (!retrieve_value(&k, &backing))
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
    else if (!memcmp(backing.mv_data, "die", 3))
    {
        ret = false;
    }
    else
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
		return;
	}
	rc = mdb_cursor_renew(m_rtxn, m_rcsr);
	if (rc)
	{
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
	MDB_val k = { sizeof(key), key };
	MDB_val val;
    pack_key_client(client, key);
	MVS(val, "reg");
    store_key_value(&k, &val);
}

void
fact_store :: die_client(uint64_t client)
{
    char key[KEY_SIZE_CLIENT];
	MDB_val k = { sizeof(key), key };
	MDB_val val;
    pack_key_client(client, key);
	MVS(val, "die");
    store_key_value(&k, &val);
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
	MDB_val k = { sizeof(key), key };
	MDB_val bdata;
    pack_key_slot(slot, key);

    if (!retrieve_value(&k, &bdata))
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
    *data = e::slice(ptr, backing->size() - 3 * sizeof(uint64_t));
    return true;
}

bool
fact_store :: get_slot(uint64_t client,
                       uint64_t nonce,
                       uint64_t* slot)
{
    char key[KEY_SIZE_NONCE];
	MDB_val k = { sizeof(key), key };
    pack_key_nonce(client, nonce, key);
    MDB_val backing;

    if (!retrieve_value(&k, &backing))
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
	MDB_val k = { sizeof(key), key };
	MDB_val bdata;
    pack_key_exec(slot, key);

    if (!retrieve_value(&k, &bdata))
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
	MDB_val k = { sizeof(key), key };
    pack_key_ack(slot, key);
    return check_key_exists(&k);
}

bool
fact_store :: is_issued_slot(uint64_t slot)
{
    char key[KEY_SIZE_SLOT];
	MDB_val k = { sizeof(key), key };
    pack_key_slot(slot, key);
    return check_key_exists(&k);
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
	MDB_val k = { sizeof(key), key };
	MDB_val val;
    pack_key_slot(slot, key);
    std::vector<char> value(3 * sizeof(uint64_t) + data.size());
    char* ptr = &value.front();
    ptr = e::pack64be(object, ptr);
    ptr = e::pack64be(client, ptr);
    ptr = e::pack64be(nonce, ptr);
    memmove(ptr, data.data(), data.size());
	val.mv_data = &value.front();
	val.mv_size = value.size();
    store_key_value(&k, &val);

    if (slot == m_cache_next_slot_issue && m_cache_next_slot_issue != 0)
    {
        ++m_cache_next_slot_issue;
    }

    char keyn[KEY_SIZE_NONCE];
    pack_key_nonce(client, nonce, keyn);
    char valn[sizeof(uint64_t)];
    e::pack64be(slot, valn);
	k.mv_data = keyn;
	k.mv_size = sizeof(keyn);
	val.mv_data = valn;
	val.mv_size = sizeof(valn);
    store_key_value(&k, &val);
}

void
fact_store :: ack_slot(uint64_t slot)
{
    assert(slot == m_cache_next_slot_ack || m_cache_next_slot_ack == 0);
    char key[KEY_SIZE_ACK];
	MDB_val k = { sizeof(key), key };
	MDB_val val = { 0, (void *)"" };
    pack_key_ack(slot, key);
    store_key_value(&k, &val);

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
	MDB_val k = { sizeof(key), key };
	MDB_val val;
    pack_key_exec(slot, key);
    std::vector<char> value(sizeof(uint8_t) + data.size());
    char* ptr = &value.front();
    *ptr = static_cast<uint8_t>(rc);
    ++ptr;
    memmove(ptr, data.data(), data.size());
	val.mv_data = &value.front();
	val.mv_size = value.size();
    store_key_value(&k, &val);
}

void
fact_store :: clear_unacked_slots()
{
    uint64_t acked = next_slot_to_ack();
    uint64_t issued = next_slot_to_issue();
	MDB_val pref;

	MVS(pref, "nonce");
	mdb_txn_renew(m_rtxn);
    prefix_iterator nonces(&pref, m_rtxn, m_dbi, 21);

    for (; nonces.valid(); nonces.next())
    {
        const char* ptr = (const char *)nonces.key()->mv_data;
        uint64_t client;
        uint64_t nonce;
        e::unpack64be(ptr + 5, &client);
        e::unpack64be(ptr + 13, &nonce);
        ptr = (const char *)nonces.val()->mv_data;

        if (nonces.val()->mv_size != sizeof(uint64_t))
        {
            continue;
        }

        uint64_t slot;
        e::unpack64be(ptr, &slot);

        if (slot > acked)
        {
            delete_key(nonces.key());
        }
    }
	mdb_txn_reset(m_rtxn);

    while (issued > acked)
    {
        char key[KEY_SIZE_SLOT];
		MDB_val k = { sizeof(key), key };
        --issued;
        pack_key_slot(issued, key);

        delete_key(&k);
    }

    m_cache_next_slot_issue = 0;
    m_cache_next_slot_ack = 0;
}

bool
fact_store :: check_key_exists(MDB_val *key)
{
	MDB_val val;
	bool do_reset = true;
	int rc;

	rc = mdb_txn_renew(m_rtxn);
	if (rc)
	{
		do_reset = false;
	}
	rc = mdb_get(m_rtxn, m_dbi, key, &val);
	if (do_reset)
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
fact_store :: retrieve_value(MDB_val *key, MDB_val *value)
{
	int rc;
	bool do_reset = true;

	rc = mdb_txn_renew(m_rtxn);
	if (rc)
	{
		do_reset = false;
	}
	rc = mdb_get(m_rtxn, m_dbi, key, value);
    if (rc == MDB_SUCCESS)
    {
        return true;
    }
    else if (rc == MDB_NOTFOUND)
    {
		if (do_reset)
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
fact_store :: store_key_value(MDB_val *key, MDB_val *value)
{
	MDB_txn *txn;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		abort();
	}
	rc = mdb_put(txn, m_dbi, key, value, 0);
    if (rc == MDB_SUCCESS)
    {
        rc = mdb_txn_commit(txn);
    }
    else
    {
        LOG(ERROR) << "LMDB put failed: " << mdb_strerror(rc);
		mdb_txn_abort(txn);
        abort();
    }
}

void
fact_store :: delete_key(MDB_val *key)
{
	MDB_txn *txn;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, 0, &txn);
	if (rc)
	{
		abort();
	}
	rc = mdb_del(txn, m_dbi, key, NULL);
	if (rc == MDB_NOTFOUND)
	{
		mdb_txn_abort(txn);
		return;
	}
	if (rc == MDB_SUCCESS)
	{
		rc = mdb_txn_commit(txn);
		if (rc == MDB_SUCCESS)
			return;
	}
    else
    {
        LOG(ERROR) << "LMDB delete failed: " << mdb_strerror(rc);
        abort();
    }
}

bool
fact_store :: only_key_is_replicant_key()
{
	MDB_cursor *mc;
	MDB_txn *txn;
	MDB_val key;
	int rc;

	rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &txn);
	if (rc)
	{
		abort();
	}
	rc = mdb_cursor_open(txn, m_dbi, &mc);
	if (rc)
	{
		abort();
	}

    bool seen = false;

    while ((rc = mdb_cursor_get(mc, &key, NULL, MDB_NEXT)) == MDB_SUCCESS)
    {
		if (key.mv_size != 9 || memcmp(key.mv_data, "replicant", 9))
        {
			mdb_cursor_close(mc);
			mdb_txn_abort(txn);
            return false;
        }

        seen = true;
    }
	mdb_cursor_close(mc);
	mdb_txn_abort(txn);
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
	MDB_val pref;
	MVS(pref, "acc");
	mdb_txn_renew(m_rtxn);
    prefix_iterator accected(&pref, m_rtxn, m_dbi, 19);

    for (; accected.valid(); accected.next())
    {
        const char* ptr = (const char *)accected.key()->mv_data;
        uint64_t proposal_id;
        uint64_t proposal_time;
        e::unpack64be(ptr + 3, &proposal_id);
        e::unpack64be(ptr + 11, &proposal_time);
        accepted_proposals->push_back(std::make_pair(proposal_id, proposal_time));
    }
	mdb_txn_reset(m_rtxn);

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
	MDB_val pref;
	MVS(pref, "rej");
	mdb_txn_renew(m_rtxn);
    prefix_iterator rejected(&pref, m_rtxn, m_dbi, 19);

    for (; rejected.valid(); rejected.next())
    {
        const char* ptr = (const char *)rejected.key()->mv_data;
        uint64_t proposal_id;
        uint64_t proposal_time;
        e::unpack64be(ptr + 3, &proposal_id);
        e::unpack64be(ptr + 11, &proposal_time);
        rejected_proposals->push_back(std::make_pair(proposal_id, proposal_time));
    }
	mdb_txn_reset(m_rtxn);

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
	MDB_val pref;
	bool ret = false;
	MVS(pref, "inf");
	mdb_txn_renew(m_rtxn);
    prefix_iterator informs(&pref, m_rtxn, m_dbi, 11);

    for (; informs.valid(); informs.next())
    {
        const char* ptr = (const char *)informs.key()->mv_data;
        uint64_t version;
        e::unpack64be(ptr + 3, &version);
        configuration tmp;
        e::unpacker up((char *)informs.val()->mv_data, informs.val()->mv_size);
        up = up >> tmp;

        if (up.error())
        {
            FSCK_LOG << "FATAL encountered bad inform message: " << (const char *)informs.val()->mv_data << std::endl;
            goto fail;
        }

        if (tmp.version() != version)
        {
            FSCK_LOG << "FATAL informed configuration has mismatched version: " << version << " != " << tmp.version() << std::endl;
            goto fail;
        }

        if (!tmp.validate())
        {
            FSCK_LOG << "FATAL informed configuration does not validate: " << tmp << std::endl;
            goto fail;
        }

        ADD_CONFIGURATION(*configurations, tmp, "informed");
    }

    if (informs.error())
    {
        FSCK_LOG << "FATAL scanning informs encountered bad key" << std::endl;
        goto fail;
    }

	ret = true;
fail:
	mdb_txn_reset(m_rtxn);
    return ret;
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
	MDB_val pref;
	bool ret = false;
	MVS(pref, "prop");
	mdb_txn_renew(m_rtxn);
    prefix_iterator props(&pref, m_rtxn, m_dbi, 20);

    for (; props.valid(); props.next())
    {
        const char* ptr = (const char *)props.key()->mv_data;
        uint64_t proposal_id;
        uint64_t proposal_time;
        e::unpack64be(ptr + 4, &proposal_id);
        e::unpack64be(ptr + 12, &proposal_time);
        std::vector<configuration> configs;
        e::unpacker up((const char *)props.val()->mv_data, props.val()->mv_size);

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
            goto fail;
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
        goto fail;
    }
	ret = true;

fail:
	mdb_txn_reset(m_rtxn);
    return ret;
}

bool
fact_store :: fsck_clients(bool verbose,
                           bool)
{
	MDB_val pref;
	bool ret = false;
	MVS(pref, "client");
	mdb_txn_renew(m_rtxn);
    prefix_iterator clients(&pref, m_rtxn, m_dbi, 14);

    for (; clients.valid(); clients.next())
    {
        const char* ptr = (const char *)clients.key()->mv_data;
        uint64_t client;
        e::unpack64be(ptr + 4, &client);

		if (memcmp(clients.val()->mv_data, "reg", 3) &&
		    memcmp(clients.val()->mv_data, "die", 3))
        {
            FSCK_LOG << "FATAL client " << client << " is neither registered nor dead" << std::endl;
            goto fail;
        }
    }

    if (clients.error())
    {
        FSCK_LOG << "FATAL scanning clients encountered bad key" << std::endl;
        goto fail;
    }
	ret = true;
fail:
	mdb_txn_reset(m_rtxn);
    return ret;
}

bool
fact_store :: fsck_slots(bool verbose,
                         bool destructive)
{
    // check slots ////////////////////////////////////////////////////////////
	MDB_val pref;
	MVS(pref, "slots");
	mdb_txn_renew(m_rtxn);
    prefix_iterator slots(&pref, m_rtxn, m_dbi, 12);
    uint64_t prev_slot = 0;
    bool slot_jumps = false;
	bool ret = false;

    for (; slots.valid(); slots.next())
    {
        const char* ptr = (const char *)slots.key()->mv_data;
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
                delete_key(slots.key());
            }
            else
            {
                goto fail;
            }
        }

        prev_slot = slot;
    }

    if (slots.error())
    {
        FSCK_LOG << "FATAL scanning slots encountered bad key" << std::endl;
        goto fail;
    }

    // check acks /////////////////////////////////////////////////////////////
	MVS(pref, "ack");
	{
    prefix_iterator acks(&pref, m_rtxn, m_dbi, 11);

    for (; acks.valid(); acks.next())
    {
        const char* ptr = (const char *)acks.key()->mv_data;
        uint64_t ack;
        e::unpack64be(ptr + 3, &ack);
        char skey[KEY_SIZE_SLOT];
		MDB_val sk = {sizeof(skey), skey};
        pack_key_slot(ack, skey);

        if (!check_key_exists(&sk))
        {
            FSCK_LOG << "ERROR deleting key for ack " << ack << " because it has no corresponding slot" << std::endl;

            if (destructive)
            {
                delete_key(acks.key());
            }
            else
            {
                goto fail;
            }
        }
    }

    if (acks.error())
    {
        FSCK_LOG << "FATAL scanning acks encountered bad key" << std::endl;
        goto fail;
    }
	}

    // check execs ////////////////////////////////////////////////////////////
	MVS(pref, "exec");
	{
    prefix_iterator execs(&pref, m_rtxn, m_dbi, 12);

    for (; execs.valid(); execs.next())
    {
        const char* ptr = (const char *)execs.key()->mv_data;
        uint64_t exec;
        e::unpack64be(ptr + 4, &exec);
        char akey[KEY_SIZE_ACK];
		MDB_val ak = {sizeof(akey), akey};
        pack_key_ack(exec, akey);

        if (!check_key_exists(&ak))
        {
            FSCK_LOG << "ERROR deleting key for exec " << exec << " because it has no corresponding ack" << std::endl;

            if (destructive)
            {
                delete_key(execs.key());
            }
            else
            {
                goto fail;
            }
        }
    }

    if (execs.error())
    {
        FSCK_LOG << "FATAL scanning execs encountered bad key" << std::endl;
        goto fail;
    }
	}

    // check nonces ///////////////////////////////////////////////////////////
	MVS(pref, "nonce");
	{
    prefix_iterator nonces(&pref, m_rtxn, m_dbi, 21);

    for (; nonces.valid(); nonces.next())
    {
        const char* ptr = (const char *)nonces.key()->mv_data;
        uint64_t client;
        uint64_t nonce;
        e::unpack64be(ptr + 5, &client);
        e::unpack64be(ptr + 13, &nonce);
        ptr = (const char *)nonces.val()->mv_data;

        if (nonces.val()->mv_size != sizeof(uint64_t))
        {
            FSCK_LOG << "FATAL (client=" << client << ", nonce=" << nonce << ") does not map to a valid slot" << std::endl;
            goto fail;
        }

        uint64_t slot;
        e::unpack64be(ptr, &slot);
        char skey[KEY_SIZE_SLOT];
		MDB_val sk = {sizeof(skey), skey};
		MDB_val sbacking;
        pack_key_slot(slot, skey);

        if (!retrieve_value(&sk, &sbacking) ||
            sbacking.mv_size < 3 * sizeof(uint64_t))
        {
            FSCK_LOG << "ERROR deleting key (client=" << client << ", nonce=" << nonce << ") because it has no valid slot" << std::endl;

            if (destructive)
            {
                delete_key(&sk);
            }
            else
            {
                goto fail;
            }
        }

        uint64_t sclient;
        uint64_t snonce;
        ptr = (const char *)sbacking.mv_data;
        e::unpack64be(ptr + 8, &sclient);
        e::unpack64be(ptr + 16, &snonce);

        if (client != sclient || nonce != snonce)
        {
            FSCK_LOG << "FATAL key (client=" << client << ", nonce=" << nonce << ") points at slot " << slot << " which does not reciprocate" << std::endl;
            FSCK_LOG << "          (sclient=" << sclient << ", snonce=" << snonce << ")" << std::endl;
            goto fail;
        }

        char ckey[KEY_SIZE_CLIENT];
        pack_key_client(client, ckey);
		MDB_val ck = {sizeof(ckey), ckey};

        if (!check_key_exists(&ck))
        {
            FSCK_LOG << "ERROR deleting key (client=" << client << ", nonce=" << nonce << ") for unknown client" << std::endl;

            if (destructive)
            {
                delete_key(&ck);
            }
            else
            {
                goto fail;
            }
        }
    }

    if (nonces.error())
    {
        FSCK_LOG << "FATAL scanning nonces encountered bad key" << std::endl;
        goto fail;
    }
	}
	ret = true;

fail:
	mdb_txn_reset(m_rtxn);

    return ret;
}
