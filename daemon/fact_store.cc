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

#define __STDC_LIMIT_MACROS

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// STL
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>
#include <e/strescape.h>
#include <e/tuple_compare.h>

// Replicant
#include "daemon/fact_store.h"

using replicant::fact_store;

///////////////////////////////////// Utils ////////////////////////////////////

namespace
{

class prefix_iterator
{
    public:
        prefix_iterator(const MDB_val *prefix, MDB_txn *rtxn, MDB_dbi dbi);
        ~prefix_iterator() throw () { mdb_cursor_close(m_it); }

    public:
        bool valid();
        void next();
        MDB_val *key() { return &m_key; }
        MDB_val *val() { return &m_val; }
        int status() { return m_status; }

    private:
        prefix_iterator(const prefix_iterator&);
        prefix_iterator& operator = (const prefix_iterator&);

    private:
        MDB_val m_prefix;
        MDB_val m_key;
        MDB_val m_val;
        MDB_txn *m_rtxn;
        MDB_cursor *m_it;
        int m_status;
        bool m_valid;
};

prefix_iterator :: prefix_iterator(const MDB_val *prefix, MDB_txn *rtxn, MDB_dbi dbi)
    : m_prefix(*prefix)
    , m_rtxn(rtxn)
    , m_it()
    , m_valid(false)
{
    m_status = mdb_cursor_open(m_rtxn, dbi, &m_it);
    if (m_status)
    {
        abort();
    }
    m_key = *prefix;
    m_status = mdb_cursor_get(m_it, &m_key, &m_val, MDB_SET_RANGE);
    if (m_status == MDB_SUCCESS)
        m_valid = true;
}

bool
prefix_iterator :: valid()
{
    if (m_status)
    {
        return false;
    }

    while (m_valid)
    {
        if (m_key.mv_size >= m_prefix.mv_size &&
            !memcmp(m_prefix.mv_data, m_key.mv_data, m_prefix.mv_size))
        {
            return true;
        }

        m_valid = false;
    }

    return false;
}

void
prefix_iterator :: next()
{
    if (m_valid) {
        m_status = mdb_cursor_get(m_it, &m_key, &m_val, MDB_NEXT);
        if (m_status) {
            m_valid = false;
            if (m_status == MDB_NOTFOUND)
                m_status = MDB_SUCCESS;
        }
    }
}

#define STRLENOF(x)    (sizeof(x)-1)
#define PROPOSAL_PREFIX "prop"
#define PROPOSAL_KEY_SIZE (STRLENOF(PROPOSAL_PREFIX)  + sizeof(uint64_t) + sizeof(uint64_t))
/* Assign a string constant to an MDB_val */
#define MVS(v,s)    v.mv_data = (void *)s; v.mv_size = STRLENOF(s)
/* Assign a slice or string to an MDB_val */
#define MVSL(v,sl)  v.mv_data = (void *)(sl).data(); v.mv_size = (sl).size()

static void
pack_proposal_key(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    const size_t sz = STRLENOF(PROPOSAL_PREFIX);
    memmove(key, PROPOSAL_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(proposal_id, ptr);
    ptr = e::pack64be(proposal_time, ptr);
}

static void
unpack_proposal_key(const char* key, uint64_t* proposal_id, uint64_t* proposal_time)
{
    const size_t sz = STRLENOF(PROPOSAL_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, proposal_id);
    ptr = e::unpack64be(ptr, proposal_time);
}

#define ACCEPTED_PROPOSAL_PREFIX "acc"
#define ACCEPTED_PROPOSAL_KEY_SIZE (STRLENOF(ACCEPTED_PROPOSAL_PREFIX) + sizeof(uint64_t) + sizeof(uint64_t))

static void
pack_accepted_proposal_key(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    const size_t sz = STRLENOF(ACCEPTED_PROPOSAL_PREFIX);
    memmove(key, ACCEPTED_PROPOSAL_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(proposal_id, ptr);
    ptr = e::pack64be(proposal_time, ptr);
}

static void
unpack_accepted_proposal_key(const char* key, uint64_t* proposal_id, uint64_t* proposal_time)
{
    const size_t sz = STRLENOF(ACCEPTED_PROPOSAL_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, proposal_id);
    ptr = e::unpack64be(ptr, proposal_time);
}

#define REJECTED_PROPOSAL_PREFIX "rej"
#define REJECTED_PROPOSAL_KEY_SIZE (STRLENOF(REJECTED_PROPOSAL_PREFIX) + sizeof(uint64_t) + sizeof(uint64_t))

static void
pack_rejected_proposal_key(uint64_t proposal_id, uint64_t proposal_time, char* key)
{
    const size_t sz = STRLENOF(REJECTED_PROPOSAL_PREFIX);
    memmove(key, REJECTED_PROPOSAL_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(proposal_id, ptr);
    ptr = e::pack64be(proposal_time, ptr);
}

static void
unpack_rejected_proposal_key(const char* key, uint64_t* proposal_id, uint64_t* proposal_time)
{
    const size_t sz = STRLENOF(REJECTED_PROPOSAL_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, proposal_id);
    ptr = e::unpack64be(ptr, proposal_time);
}

#define INFORM_CONFIG_PREFIX "inf"
#define INFORM_CONFIG_KEY_SIZE (STRLENOF(INFORM_CONFIG_PREFIX) + sizeof(uint64_t))

static void
pack_inform_config_key(uint64_t version, char* key)
{
    const size_t sz = STRLENOF(INFORM_CONFIG_PREFIX);
    memmove(key, INFORM_CONFIG_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(version, ptr);
}

static void
unpack_inform_config_key(const char* key, uint64_t* version)
{
    const size_t sz = STRLENOF(INFORM_CONFIG_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, version);
}

#define CLIENT_PREFIX "client"
#define CLIENT_KEY_SIZE (STRLENOF(CLIENT_PREFIX) + sizeof(uint64_t))

static void
pack_client_key(uint64_t client, char* key)
{
    const size_t sz = STRLENOF(CLIENT_PREFIX);
    memmove(key, CLIENT_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(client, ptr);
}

static void
unpack_client_key(const char* key, uint64_t* client)
{
    const size_t sz = STRLENOF(CLIENT_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, client);
}

#define SLOT_PREFIX "slot"
#define SLOT_KEY_SIZE (STRLENOF(SLOT_PREFIX) + sizeof(uint64_t))

static void
pack_slot_key(uint64_t slot, char* key)
{
    const size_t sz = STRLENOF(SLOT_PREFIX);
    memmove(key, SLOT_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(slot, ptr);
}

static void
unpack_slot_key(const char* key, uint64_t* slot)
{
    const size_t sz = STRLENOF(SLOT_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, slot);
}

static void
pack_slot_val(uint64_t object,
              uint64_t client,
              uint64_t nonce,
              const e::slice& data,
              std::vector<char>* val)
{
    val->resize(3 * sizeof(uint64_t) + data.size());
    char* ptr = &val->front();
    ptr = e::pack64be(object, ptr);
    ptr = e::pack64be(client, ptr);
    ptr = e::pack64be(nonce, ptr);
    memmove(ptr, data.data(), data.size());
}

static bool
unpack_slot_val(const MDB_val* val,
                uint64_t* object,
                uint64_t* client,
                uint64_t* nonce,
                e::slice* data)
{
    if (val->mv_size < 3 * sizeof(uint64_t))
    {
        return false;
    }

    const char* ptr = (const char *)val->mv_data;
    ptr = e::unpack64be(ptr, object);
    ptr = e::unpack64be(ptr, client);
    ptr = e::unpack64be(ptr, nonce);
    *data = e::slice(ptr, val->mv_size - (ptr - (const char *)val->mv_data));
    return true;
}

static bool
unpack_slot_val(const MDB_val* val,
                uint64_t* object,
                uint64_t* client,
                uint64_t* nonce,
                std::string* func,
                std::string* data)
{
    e::slice tmp;

    if (!unpack_slot_val(val, object, client, nonce, &tmp))
    {
        return false;
    }

    const char* ptr = reinterpret_cast<const char*>(memchr(tmp.data(), '\0', tmp.size()));

    if (ptr == NULL)
    {
        return true;
    }

    *func = std::string(reinterpret_cast<const char*>(tmp.data()), ptr);
    ++ptr;
    const char* end = reinterpret_cast<const char*>(tmp.data() + tmp.size());

    if (ptr < end)
    {
        *data = std::string(ptr, end);
    }
    else
    {
        *data = "";
    }

    return true;
}

#define ACK_PREFIX "ack"
#define ACK_KEY_SIZE (STRLENOF(ACK_PREFIX) + sizeof(uint64_t))

static void
pack_ack_key(uint64_t slot, char* key)
{
    const size_t sz = STRLENOF(ACK_PREFIX);
    memmove(key, ACK_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(slot, ptr);
}

static void
unpack_ack_key(const char* key, uint64_t* slot)
{
    const size_t sz = STRLENOF(ACK_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, slot);
}

#define EXEC_PREFIX "exec"
#define EXEC_KEY_SIZE (STRLENOF(EXEC_PREFIX) + sizeof(uint64_t))

static void
pack_exec_key(uint64_t slot, char* key)
{
    const size_t sz = STRLENOF(EXEC_PREFIX);
    memmove(key, EXEC_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(slot, ptr);
}

static void
unpack_exec_key(const char* key, uint64_t* slot)
{
    const size_t sz = STRLENOF(EXEC_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, slot);
}

static void
pack_exec_val(replicant::response_returncode rc,
              const e::slice& response,
              std::vector<char>* val)
{
    val->resize(sizeof(uint8_t) + response.size());
    char* ptr = &val->front();
    *ptr = static_cast<uint8_t>(rc);
    ++ptr;
    memmove(ptr, response.data(), response.size());
}

static bool
unpack_exec_val(const MDB_val* val,
                replicant::response_returncode* rc,
                e::slice* response)
{
    if (val->mv_size < 1)
    {
        return false;
    }
    const char *ptr = (const char *)val->mv_data;

    *rc = static_cast<replicant::response_returncode>(ptr[0]);
    *response = e::slice(ptr + 1, val->mv_size - 1);
    return true;
}

static bool
unpack_exec_val(const MDB_val* val,
                replicant::response_returncode* rc,
                std::string* response)
{
    e::slice resp;

    if (!unpack_exec_val(val, rc, &resp))
    {
        return false;
    }

    *response = std::string(reinterpret_cast<const char*>(resp.data()), resp.size());
    return true;
}

#define NONCE_PREFIX "nonce"
#define NONCE_KEY_SIZE (STRLENOF(NONCE_PREFIX) + sizeof(uint64_t) + sizeof(uint64_t))
#define NONCE_VAL_SIZE (sizeof(uint64_t))

static void
pack_nonce_key(uint64_t client, uint64_t nonce, char* key)
{
    const size_t sz = STRLENOF(NONCE_PREFIX);
    memmove(key, NONCE_PREFIX, sz);
    char* ptr = key + sz;
    ptr = e::pack64be(client, ptr);
    ptr = e::pack64be(nonce, ptr);
}

static void
unpack_nonce_key(const char* key, uint64_t* client, uint64_t* nonce)
{
    const size_t sz = STRLENOF(NONCE_PREFIX);
    const char* ptr = key + sz;
    ptr = e::unpack64be(ptr, client);
    ptr = e::unpack64be(ptr, nonce);
}

static void
pack_nonce_val(uint64_t number, char* key)
{
    e::pack64be(number, key);
}

static void
unpack_nonce_val(const char* key, uint64_t* slot)
{
    e::unpack64be(key, slot);
}

} // namespace

//////////////////////////////// Internal Types ////////////////////////////////

struct fact_store :: slot
{
    slot() : number(), object(), client(), nonce(), func(), data() {}
    uint64_t number;
    uint64_t object;
    uint64_t client;
    uint64_t nonce;
    std::string func;
    std::string data;
    bool operator < (const slot& rhs) const
    { return e::tuple_compare(number, object, client, nonce,
                              rhs.number, rhs.object, rhs.client, rhs.nonce) < 0; }
};

struct fact_store :: exec
{
    exec() : number(), rc(), response() {}
    uint64_t number;
    response_returncode rc;
    std::string response;
    bool operator < (const exec& rhs) const
    { return number < rhs.number; }
};

struct fact_store :: slot_mapping
{
    uint64_t client;
    uint64_t nonce;
    uint64_t slot;
    bool operator < (const slot_mapping& rhs) const
    { return e::tuple_compare(client, nonce, slot,
                              rhs.client, rhs.nonce, rhs.slot) < 0; }
    bool operator == (const slot_mapping& rhs) const
    { return e::tuple_compare(client, nonce, slot,
                              rhs.client, rhs.nonce, rhs.slot) == 0; }
};

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
                   bool* restored,
                   chain_node* us,
                   configuration_manager* config_manager)
{
    int st;
    st = open_db(path, true);

    if (st)
    {
        LOG(ERROR) << "could not open DB: " << mdb_strerror(st);
        return false;
    }

    std::ostringstream ostr;

    if (!initialize(ostr, restored, us))
    {
        LOG(ERROR) << ostr;
        return false;
    }

    if (*restored && !integrity_check(1, false, false, config_manager))
    {
        LOG(ERROR) << "Integrity check failed.";
        LOG(ERROR) << "Some of the above errors may be fixable automatically using the integrity-check tool";
        LOG(ERROR) << "For more information try \"man replicant-integrity-check\"";
        return false;
    }

    return true;
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
    char key[PROPOSAL_KEY_SIZE];
    pack_proposal_key(proposal_id, proposal_time, key);
    return check_key_exists(key, PROPOSAL_KEY_SIZE);
}

bool
fact_store :: is_accepted_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[ACCEPTED_PROPOSAL_KEY_SIZE];
    pack_accepted_proposal_key(proposal_id, proposal_time, key);
    return check_key_exists(key, ACCEPTED_PROPOSAL_KEY_SIZE);
}

bool
fact_store :: is_rejected_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    char key[REJECTED_PROPOSAL_KEY_SIZE];
    pack_rejected_proposal_key(proposal_id, proposal_time, key);
    return check_key_exists(key, REJECTED_PROPOSAL_KEY_SIZE);
}

void
fact_store :: propose_configuration(uint64_t proposal_id, uint64_t proposal_time,
                                    const configuration* configs, size_t configs_sz)
{
    assert(!is_proposed_configuration(proposal_id, proposal_time));
    char key[PROPOSAL_KEY_SIZE];
    pack_proposal_key(proposal_id, proposal_time, key);
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

    store_key_value(key, PROPOSAL_KEY_SIZE, &value.front(), value.size());
}

void
fact_store :: accept_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    assert(is_proposed_configuration(proposal_id, proposal_time));
    assert(!is_accepted_configuration(proposal_id, proposal_time));
    assert(!is_rejected_configuration(proposal_id, proposal_time));
    char key[ACCEPTED_PROPOSAL_KEY_SIZE];
    pack_accepted_proposal_key(proposal_id, proposal_time, key);
    store_key_value(key, ACCEPTED_PROPOSAL_KEY_SIZE, "", 0);
}

void
fact_store :: reject_configuration(uint64_t proposal_id, uint64_t proposal_time)
{
    assert(is_proposed_configuration(proposal_id, proposal_time));
    assert(!is_accepted_configuration(proposal_id, proposal_time));
    assert(!is_rejected_configuration(proposal_id, proposal_time));
    char key[REJECTED_PROPOSAL_KEY_SIZE];
    pack_rejected_proposal_key(proposal_id, proposal_time, key);
    store_key_value(key, REJECTED_PROPOSAL_KEY_SIZE, "", 0);
}

void
fact_store :: inform_configuration(const configuration& config)
{
    char key[INFORM_CONFIG_KEY_SIZE];
    pack_inform_config_key(config.version(), key);
    std::vector<char> value(pack_size(config));
    pack_config(config, &value.front());
    store_key_value(key, INFORM_CONFIG_KEY_SIZE, &value.front(), value.size());
}

bool
fact_store :: is_client(uint64_t client)
{
    char key[CLIENT_KEY_SIZE];
    pack_client_key(client, key);
    return check_key_exists(key, CLIENT_KEY_SIZE);
}

bool
fact_store :: is_live_client(uint64_t client)
{
    char key[CLIENT_KEY_SIZE];
    pack_client_key(client, key);
    std::string backing;

    if (!retrieve_value(key, CLIENT_KEY_SIZE, &backing))
    {
        return false;
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
    MDB_val pref;
    MVS(pref,CLIENT_PREFIX);
    mdb_txn_renew(m_rtxn);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != CLIENT_KEY_SIZE)
        {
            continue;
        }

        uint64_t id;
        unpack_client_key((const char *)iter.key()->mv_data, &id);
        clients->push_back(id);
    }
    mdb_txn_reset(m_rtxn);
}

void
fact_store :: reg_client(uint64_t client)
{
    char key[CLIENT_KEY_SIZE];
    pack_client_key(client, key);
    store_key_value(key, CLIENT_KEY_SIZE, "reg", 3);
}

void
fact_store :: die_client(uint64_t client)
{
    char key[CLIENT_KEY_SIZE];
    pack_client_key(client, key);
    store_key_value(key, CLIENT_KEY_SIZE, "die", 3);
}

bool
fact_store :: get_slot(uint64_t number,
                       uint64_t* object,
                       uint64_t* client,
                       uint64_t* nonce,
                       e::slice* data,
                       std::string* backing)
{
    char key[SLOT_KEY_SIZE];
    pack_slot_key(number, key);

    if (!retrieve_value(key, SLOT_KEY_SIZE, backing))
    {
        return false;
    }

    MDB_val v;
    MVSL(v,*backing);
    return unpack_slot_val(&v, object, client, nonce, data);
}

bool
fact_store :: get_slot(uint64_t client,
                       uint64_t nonce,
                       uint64_t* number)
{
    char key[NONCE_KEY_SIZE];
    pack_nonce_key(client, nonce, key);
    std::string backing;

    if (!retrieve_value(key, NONCE_KEY_SIZE, &backing) ||
        backing.size() != NONCE_VAL_SIZE)
    {
        return false;
    }

    unpack_nonce_val(backing.data(), number);
    return true;
}

bool
fact_store :: get_exec(uint64_t number,
                       replicant::response_returncode* rc,
                       e::slice* data,
                       std::string* backing)
{
    char key[EXEC_KEY_SIZE];
    pack_exec_key(number, key);

    if (!retrieve_value(key, EXEC_KEY_SIZE, backing))
    {
        return false;
    }

    MDB_val v;
    MVSL(v,*backing);
    return unpack_exec_val(&v, rc, data);
}

bool
fact_store :: is_acknowledged_slot(uint64_t number)
{
    char key[ACK_KEY_SIZE];
    pack_ack_key(number, key);
    return check_key_exists(key, ACK_KEY_SIZE);
}

bool
fact_store :: is_issued_slot(uint64_t number)
{
    char key[SLOT_KEY_SIZE];
    pack_slot_key(number, key);
    return check_key_exists(key, SLOT_KEY_SIZE);
}

uint64_t
fact_store :: next_slot_to_issue()
{
    if (m_cache_next_slot_issue != 0)
    {
        return m_cache_next_slot_issue;
    }

    MDB_val pref;
    MVS(pref,SLOT_PREFIX);
    mdb_txn_renew(m_rtxn);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);
    uint64_t next_to_issue = 1;

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != SLOT_KEY_SIZE)
        {
            continue;
        }

        uint64_t n;
        unpack_slot_key((const char *)iter.key()->mv_data, &n);
        next_to_issue = std::max(next_to_issue, n + 1);
    }
    mdb_txn_reset(m_rtxn);

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

    MDB_val pref;
    MVS(pref,ACK_PREFIX);
    mdb_txn_renew(m_rtxn);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);
    uint64_t next_to_ack = 1;

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != ACK_KEY_SIZE)
        {
            continue;
        }

        uint64_t n;
        unpack_ack_key((const char *)iter.key()->mv_data, &n);
        next_to_ack = std::max(next_to_ack, n + 1);
    }
    mdb_txn_reset(m_rtxn);

    m_cache_next_slot_ack = next_to_ack;
    return next_to_ack;
}

void
fact_store :: issue_slot(uint64_t number,
                         uint64_t object,
                         uint64_t client,
                         uint64_t nonce,
                         const e::slice& data)
{
    assert(number == m_cache_next_slot_issue || m_cache_next_slot_issue == 0);
    char key[SLOT_KEY_SIZE];
    pack_slot_key(number, key);
    std::vector<char> val;
    pack_slot_val(object, client, nonce, data, &val);
    store_key_value(key, SLOT_KEY_SIZE, &val.front(), val.size());

    if (number == m_cache_next_slot_issue && m_cache_next_slot_issue != 0)
    {
        ++m_cache_next_slot_issue;
    }

    char keyn[NONCE_KEY_SIZE];
    pack_nonce_key(client, nonce, keyn);
    char valn[NONCE_VAL_SIZE];
    pack_nonce_val(number, valn);
    store_key_value(keyn, NONCE_KEY_SIZE, valn, NONCE_VAL_SIZE);
}

void
fact_store :: ack_slot(uint64_t number)
{
    assert(number == m_cache_next_slot_ack || m_cache_next_slot_ack == 0);
    char key[ACK_KEY_SIZE];
    pack_ack_key(number, key);
    store_key_value(key, ACK_KEY_SIZE, "", 0);

    if (number == m_cache_next_slot_ack && m_cache_next_slot_ack != 0)
    {
        ++m_cache_next_slot_ack;
    }
}

void
fact_store :: exec_slot(uint64_t number,
                        replicant::response_returncode rc,
                        const e::slice& data)
{
    char key[EXEC_KEY_SIZE];
    pack_exec_key(number, key);
    std::vector<char> value;
    pack_exec_val(rc, data, &value);
    store_key_value(key, EXEC_KEY_SIZE, &value.front(), value.size());
}

void
fact_store :: clear_unacked_slots()
{
    uint64_t next_to_ack = next_slot_to_ack();
    uint64_t next_to_issue = next_slot_to_issue();

    while (next_to_issue >= next_to_ack)
    {
        char key[SLOT_KEY_SIZE];
        pack_slot_key(next_to_issue, key);
        std::string backing;
        uint64_t object;
        uint64_t client;
        uint64_t nonce;
        e::slice data;
        MDB_val v;

        if (retrieve_value(key, SLOT_KEY_SIZE, &backing)) {
            MDB_val v;
            MVSL(v,backing);
            if (unpack_slot_val(&v, &object, &client, &nonce, &data))
        {
            char keyn[NONCE_KEY_SIZE];
            pack_nonce_key(client, nonce, keyn);
            delete_key(keyn, NONCE_KEY_SIZE);
        }
        }

        delete_key(key, SLOT_KEY_SIZE);
        --next_to_issue;
    }

    m_cache_next_slot_issue = 0;
    m_cache_next_slot_ack = 0;
}

bool
fact_store :: debug_dump(const po6::pathname& path)
{
    int st;
    st = open_db(path, false);

    if (st)
    {
        std::cerr << "could not open DB: " << mdb_strerror(st) << std::endl;
        return false;
    }

    // dump information relating to reconfiguration
    typedef std::pair<uint64_t, uint64_t> uup;
    std::vector<uup> proposals;
    std::vector<std::vector<configuration> > proposed_configs;
    std::vector<uup> accepted_proposals;
    std::vector<uup> rejected_proposals;
    std::vector<std::pair<uint64_t, configuration> > informed_configs;
    std::vector<std::pair<uint64_t, const char*> > clients;
    std::vector<slot> slots_issued;
    std::vector<uint64_t> slots_acked;
    std::vector<exec> slots_execd;
    std::vector<slot_mapping> slot_mappings;

    if (!scan_all(&proposals, &proposed_configs, &accepted_proposals,
                  &rejected_proposals, &informed_configs, &clients,
                  &slots_issued, &slots_acked, &slots_execd, &slot_mappings))
    {
        return false;
    }

    assert(proposals.size() == proposed_configs.size());

    for (size_t i = 0; i < proposals.size(); ++i)
    {
        std::cout << "proposal " << proposals[i].first << ":" << proposals[i].second << "\n";

        for (size_t j = 0; j < proposed_configs[i].size(); ++j)
        {
            std::cout << "         " << proposed_configs[i][j] << "\n";
        }
    }

    for (size_t i = 0; i < accepted_proposals.size(); ++i)
    {
        std::cout << "accepted " << accepted_proposals[i].first << ":" << accepted_proposals[i].second << "\n";
    }

    for (size_t i = 0; i < rejected_proposals.size(); ++i)
    {
        std::cout << "rejected " << rejected_proposals[i].first << ":" << rejected_proposals[i].second << "\n";
    }

    for (size_t i = 0; i < informed_configs.size(); ++i)
    {
        std::cout << "informed " << informed_configs[i].first << ": " << informed_configs[i].second << "\n";
    }

    for (size_t i = 0; i < clients.size(); ++i)
    {
        std::cout << "client " << clients[i].first << " state=" << clients[i].second << "\n";
    }

    for (size_t i = 0; i < slots_issued.size(); ++i)
    {
        std::cout << "issued slot " << slots_issued[i].number << ":"
                  << " object=" << slots_issued[i].object
                  << " client=" << slots_issued[i].client
                  << " nonce=" << slots_issued[i].nonce
                  << " func=" << e::strescape(slots_issued[i].func)
                  << " data=\"" << e::strescape(slots_issued[i].data) << "\"\n";
    }

    for (size_t i = 0; i < slots_acked.size(); ++i)
    {
        std::cout << "acked slot " << slots_acked[i] << "\n";
    }

    for (size_t i = 0; i < slots_execd.size(); ++i)
    {
        std::cout << "execd slot " << slots_execd[i].number
                  << " status=" << slots_execd[i].rc 
                  << " response=\"" << e::strescape(slots_execd[i].response) << "\"\n";
    }

    for (size_t i = 0; i < slot_mappings.size(); ++i)
    {
        std::cout << "slot mapping client=" << slot_mappings[i].client
                  << " nonce=" << slot_mappings[i].nonce << " -> "
                  << "slot=" << slot_mappings[i].slot << "\n";
    }

    return true;
}

bool
fact_store :: integrity_check(const po6::pathname& path, bool destructive)
{
    int st;
    st = open_db(path, false);

    if (st)
    {
        std::cerr << "could not open LevelDB: " << mdb_strerror(st) << std::endl;
        return false;
    }

    std::ostringstream ostr;
    bool restored;
    chain_node us;

    if (!initialize(ostr, &restored, &us))
    {
        std::cerr << ostr.str() << std::endl;
        return false;
    }

    if (restored && !integrity_check(5, true, destructive, NULL))
    {
        std::cerr << "Integrity check failed.\n"
                  << "Fix the above errors and re-run the integrity check.\n"
                  << "For more information try \"man replicant-integrity-check\"\n";
        return false;
    }

    return true;
}

int
fact_store :: open_db(const po6::pathname& path, bool create)
{
    MDB_txn *txn;
    int rc;

    rc = mdb_env_create(&m_db);
    if (rc)
    {
        LOG(ERROR) << "could not create LMDB env: " << mdb_strerror(rc);
        return rc;
    }
    rc = mdb_env_set_mapsize(m_db, 10485760);    /* 10MB default */
    rc = mdb_env_open(m_db, path.get(), MDB_WRITEMAP, 0600);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB env: " << mdb_strerror(rc);
        return rc;
    }
    rc = mdb_txn_begin(m_db, NULL, 0, &txn);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB txn: " << mdb_strerror(rc);
        return rc;
    }
    rc = mdb_open(txn, NULL, 0, &m_dbi);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB dbi: " << mdb_strerror(rc);
        mdb_txn_abort(txn);
        return rc;
    }
    mdb_txn_commit(txn);

    /* Set this up for readers to use later */
    rc = mdb_txn_begin(m_db, NULL, MDB_RDONLY, &m_rtxn);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB read txn: " << mdb_strerror(rc);
        return rc;
    }
    rc = mdb_cursor_open(m_rtxn, m_dbi, &m_rcsr);
    if (rc)
    {
        LOG(ERROR) << "could not open LMDB read cursor: " << mdb_strerror(rc);
        mdb_txn_abort(m_rtxn);
        return rc;
    }
    mdb_txn_reset(m_rtxn);
    return rc;
}

bool
fact_store :: initialize(std::ostream& ostr, bool* restored, chain_node* us)
{
    MDB_txn *txn;
    MDB_val key, data;
    int rc;
    bool ret = false;

    rc = mdb_txn_begin(m_db, NULL, 0, &txn);
    if (rc)
    {
        ostr << "could not open LMDB txn: " << mdb_strerror(rc);
        return false;
    }

    // read the "replicant" key and check the version
    bool first_time = false;

    MVS(key, "replicant");
    rc = mdb_get(txn, m_dbi, &key, &data);
 
    if (rc == MDB_SUCCESS)
    {
        if (data.mv_size != STRLENOF(PACKAGE_VERSION) ||
            memcmp(data.mv_data, PACKAGE_VERSION, STRLENOF(PACKAGE_VERSION)))
        {
            ostr << "could not restore from DB because "
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
            ostr << "could not save \"replicant\" key into DB: " << mdb_strerror(rc);
            goto leave;
        }
    }
    else
    {
        ostr << "could not read \"replicant\" key from DB: " << mdb_strerror(rc);
        goto leave;
    }

    // read the "state" key and parse it
    MVS(key, "us");
    rc = mdb_get(txn, m_dbi, &key, &data);

    if (rc == MDB_SUCCESS)
    {
        if (first_time)
        {
            ostr << "could not restore from DB because a previous "
                 << "execution crashed and the database was tampered with; "
                 << "you'll need to manually erase this DB and create a new one";
            goto leave;
        }

        e::unpacker up((const char *)data.mv_data, data.mv_size);
        up = up >> *us;

        if (up.error())
        {
            ostr << "could not restore from DB because a previous "
                 << "execution wrote an invalid node identity; "
                 << "you'll need to manually erase this DB and create a new one";
            goto leave;
        }
    }
    else if (rc == MDB_NOTFOUND)
    {
        if (!only_key_is_replicant_key())
        {
            ostr << "could not restore from DB because a previous "
                 << "execution didn't save a node identity, and wrote "
                 << "other data; "
                 << "you'll need to manually erase this DB and create a new one";
            goto leave;
        }
    }

    ret = true;
leave:
    mdb_txn_abort(txn);
    *restored = !first_time;
    return ret;
}

#define REPORT_ERROR(X) do { if (output) std::cout << "integrity error: " << X << std::endl; error = true; } while (0)

bool
fact_store :: integrity_check(int tries_remaining, bool output, bool destructive, configuration_manager* config_manager)
{
    // pull all of the information from the database
    int st;
    typedef std::pair<uint64_t, uint64_t> uup;
    std::vector<uup> proposals;
    std::vector<std::vector<configuration> > proposed_configs;
    std::vector<uup> accepted_proposals;
    std::vector<uup> rejected_proposals;
    std::vector<std::pair<uint64_t, configuration> > informed_configs;
    std::vector<std::pair<uint64_t, const char*> > clients;
    std::vector<slot> slots_issued;
    std::vector<uint64_t> slots_acked;
    std::vector<exec> slots_execd;
    std::vector<slot_mapping> slot_mappings;
    bool tried_something = false;

    if (!scan_all(&proposals, &proposed_configs, &accepted_proposals,
                  &rejected_proposals, &informed_configs, &clients,
                  &slots_issued, &slots_acked, &slots_execd, &slot_mappings))
    {
        return false;
    }

    bool error = false;

    // every proposal may either be accepted or rejected, not both
    for (size_t i = 0; i < proposals.size(); ++i)
    {
        bool accepted = std::binary_search(accepted_proposals.begin(),
                                           accepted_proposals.end(),
                                           proposals[i]);
        bool rejected = std::binary_search(rejected_proposals.begin(),
                                           rejected_proposals.end(),
                                           proposals[i]);

        if (accepted && rejected)
        {
            REPORT_ERROR("proposal " << proposals[i].first << ":" << proposals[i].second
                         << " rejected and accepted simultaneously");
        }

        if (proposed_configs[i].empty())
        {
            REPORT_ERROR("proposal " << proposals[i].first << ":" << proposals[i].second
                         << " contains no configurations");
        } 

        // every configuration in a proposal must validate
        for (size_t j = 0; j < proposed_configs[i].size(); ++j)
        {
            if (!proposed_configs[i][j].validate())
            {
                REPORT_ERROR("proposal " << proposals[i].first << ":" << proposals[i].second
                             << " contains invalid configuration " << proposed_configs[i][j]);
            }

            if (j > 0 && 
                proposed_configs[i][j - 1].version() + 1 != proposed_configs[i][j].version())
            {
                REPORT_ERROR("proposal " << proposals[i].first << ":" << proposals[i].second
                             << " contains contains discontinous configurations");
            }
        }
    }

    // every accepted proposal must be proposed as well
    for (size_t i = 0; i < accepted_proposals.size(); ++i)
    {
        if (!std::binary_search(proposals.begin(),
                                proposals.end(),
                                accepted_proposals[i]))
        {
            REPORT_ERROR("unknown proposal " << accepted_proposals[i].first << ":" << accepted_proposals[i].second << " accepted");
        }
    }

    // every rejected proposal must be proposed as well
    for (size_t i = 0; i < rejected_proposals.size(); ++i)
    {
        if (!std::binary_search(proposals.begin(),
                                proposals.end(),
                                rejected_proposals[i]))
        {
            REPORT_ERROR("unknown proposal " << rejected_proposals[i].first << ":" << rejected_proposals[i].second << " rejected");
        }
    }

    // there must be at least one informed config
    if (informed_configs.empty())
    {
        REPORT_ERROR("no \"informed\" configs");
    }

    // every informed config must validate
    for (size_t i = 0; i < informed_configs.size(); ++i)
    {
        if (informed_configs[i].first != informed_configs[i].second.version() ||
            !informed_configs[i].second.validate())
        {
            REPORT_ERROR("informed config " << informed_configs[i].first
                         << " is invalid " << informed_configs[i].second);
        }
    }

    // select every accepted configuration
    std::vector<std::pair<uint64_t, configuration> > accepted_configs(informed_configs);

    for (size_t i = 0; i < proposals.size(); ++i)
    {
        if (std::binary_search(accepted_proposals.begin(),
                               accepted_proposals.end(),
                               proposals[i]))
        {
            for (size_t j = 0; j < proposed_configs[i].size(); ++j)
            {
                const configuration& c(proposed_configs[i][j]);
                uint64_t v = c.version();
                accepted_configs.push_back(std::make_pair(v, c));
            }
        }
        else if (!std::binary_search(rejected_proposals.begin(),
                                     rejected_proposals.end(),
                                     proposals[i]))
        {
            const configuration& c(proposed_configs[i][0]);
            uint64_t v = c.version();
            accepted_configs.push_back(std::make_pair(v, c));
        }
    }

    std::sort(accepted_configs.begin(), accepted_configs.end());
    std::vector<std::pair<uint64_t, configuration> >::iterator cit;
    cit = std::unique(accepted_configs.begin(), accepted_configs.end());
    accepted_configs.resize(cit - accepted_configs.begin());

    for (size_t i = 1; i < accepted_configs.size(); ++i)
    {
        if (accepted_configs[i - 1].first == accepted_configs[i].first)
        {
            REPORT_ERROR("conflicting configurations with the same version "
                         << accepted_configs[i - 1].second << " != "
                         << accepted_configs[i].second);
        }
    }

    // construct a config manager from the state pulled from the fact store
    if (!accepted_configs.empty())
    {
        configuration_manager cm;
        uint64_t min_version = accepted_configs.back().second.version();
        cm.reset(accepted_configs.back().second);

        for (size_t i = 0; i < proposals.size(); ++i)
        {
            if (std::binary_search(accepted_proposals.begin(),
                                   accepted_proposals.end(),
                                   proposals[i]) ||
                std::binary_search(rejected_proposals.begin(),
                                   rejected_proposals.end(),
                                   proposals[i]) ||
                proposed_configs[i].back().version() <= min_version)
            {
                continue;
            }

            configuration* configs = &proposed_configs[i][0];
            size_t configs_sz = proposed_configs[i].size();

            while (configs->version() < min_version)
            {
                ++configs;
                --configs_sz;
            }

            assert(configs->version() == min_version);
            assert(configs_sz > 1);

            if (!cm.is_compatible(configs, configs_sz))
            {
                REPORT_ERROR("proposed configurations are incompatible");
            }

            cm.merge(proposals[i].first,
                     proposals[i].second,
                     configs, configs_sz);
        }

        if (config_manager)
        {
            *config_manager = cm;
        }
    }
    else
    {
        REPORT_ERROR("not checking proposals further; no accepted configurations found");
    }

    uint64_t erase_slots_above = UINT64_MAX;

    // check that slots are continuous
    for (size_t i = 1; i < slots_issued.size(); ++i)
    {
        if (slots_issued[i - 1].number + 1 != slots_issued[i].number)
        {
            REPORT_ERROR("discontinuity in issued slots: jumps from "
                         << slots_issued[i - 1].number << " to "
                         << slots_issued[i].number);

            // if not acked, this is recoverable
            if (slots_issued[i].number > slots_acked.empty() ? 0 : slots_acked.back())
            {
                erase_slots_above = std::min(erase_slots_above,
                                             slots_issued[i].number);
            }
        }
    }

    if (erase_slots_above < UINT64_MAX)
    {
        if (!destructive)
        {
            REPORT_ERROR("must erase slots above " << erase_slots_above
                         << " (this error may be fixed automatically by a destructive integrity check)");
        }
        else
        {
            REPORT_ERROR("must erase slots above " << erase_slots_above
                         << " (running in destructive mode: trying to fix automatically)");

            for (size_t i = 0; i < slots_issued.size(); ++i)
            {
                uint64_t number = slots_issued[slots_issued.size() - i - 1].number;

                if (number >= erase_slots_above)
                {
                    char key[SLOT_KEY_SIZE];
                    pack_slot_key(number, key);
                    delete_key(key, SLOT_KEY_SIZE);
                }
            }

            tried_something = true;
        }
    }

    // check that acks are continous and refer to issued slots
    for (size_t i = 0; i < slots_acked.size(); ++i)
    {
        if (i > 0 && slots_acked[i - 1] + 1 != slots_acked[i])
        {
            REPORT_ERROR("discontinuity in acked slots: jumps from "
                         << slots_acked[i - 1] << " to " << slots_acked[i]);
        }

        if (slots_issued.empty() ||
            slots_issued.front().number > slots_acked[i] ||
            slots_issued.back().number < slots_acked[i])
        {
            REPORT_ERROR("acked slot " << slots_acked[i]
                         << " does not refer to issued slot");
        }
    }

    // check that execs refer to acked slots
    for (size_t i = 0; i < slots_execd.size(); ++i)
    {
        if (slots_acked.empty() ||
            slots_acked.front() > slots_execd[i].number ||
            slots_acked.back() < slots_execd[i].number)
        {
            REPORT_ERROR("exec'd slot " << slots_execd[i].number
                         << " does not refer to acked slot");
        }
    }

    // check the bidirection mapping (client, nonce) <-> slot
    for (size_t i = 0; i < slot_mappings.size(); ++i)
    {
        if (slots_issued.empty() ||
            slots_issued.front().number > slot_mappings[i].slot ||
            slots_issued.back().number < slot_mappings[i].slot)
        {
            if (!destructive)
            {
                REPORT_ERROR("slot mapping (" << slot_mappings[i].client << ", "
                             << slot_mappings[i].nonce << ") -> "
                             << slot_mappings[i].slot << " points to invalid slot"
                             << " (this error may be fixed automatically by a destructive integrity check)");
            }
            else
            {
                REPORT_ERROR("slot mapping (" << slot_mappings[i].client << ", "
                             << slot_mappings[i].nonce << ") -> "
                             << slot_mappings[i].slot << " points to invalid slot"
                             << " (running in destructive mode: trying to fix automatically)");
                char key[NONCE_KEY_SIZE];
                pack_nonce_key(slot_mappings[i].client, slot_mappings[i].nonce, key);
                delete_key(key, NONCE_KEY_SIZE);
                tried_something = true;
            }

            continue;
        }

        slot s;
        s.number = slot_mappings[i].slot;
        std::vector<slot>::iterator it;
        it = std::lower_bound(slots_issued.begin(),
                              slots_issued.end(), s);
        assert(it != slots_issued.end());

        if (it->client != slot_mappings[i].client ||
            it->nonce != slot_mappings[i].nonce)
        {
            REPORT_ERROR("unreciprocal slot mapping ("
                         << slot_mappings[i].client << ", "
                         << slot_mappings[i].nonce << ") -> "
                         << slot_mappings[i].slot << " -> ("
                         << slots_issued[i].client << ", "
                         << slots_issued[i].nonce << ")");
        }
    }

    for (size_t i = 0; i < slots_issued.size(); ++i)
    {
        slot_mapping sm;
        sm.client = slots_issued[i].client;
        sm.nonce = slots_issued[i].nonce;
        sm.slot = 0;
        std::vector<slot_mapping>::iterator it;
        it = std::lower_bound(slot_mappings.begin(),
                              slot_mappings.end(),
                              sm);

        if (it == slot_mappings.end() ||
            it->client != sm.client ||
            it->nonce != sm.nonce)
        {
            if (!destructive)
            {
                REPORT_ERROR("slot " << slots_issued[i].number
                             << " claims to originate from ("
                             << slots_issued[i].client << ", "
                             << slots_issued[i].nonce
                             << "), but said slot (client, nonce) pair doesn't exist"
                             << " (this error may be fixed automatically by a destructive integrity check)");
            }
            else
            {
                REPORT_ERROR("slot " << slots_issued[i].number
                             << " claims to originate from ("
                             << slots_issued[i].client << ", "
                             << slots_issued[i].nonce
                             << "), but said slot (client, nonce) pair doesn't exist"
                             << " (running in destructive mode: trying to fix automatically)");
                char key[NONCE_KEY_SIZE];
                pack_nonce_key(slots_issued[i].client, slots_issued[i].nonce, key);
                char val[NONCE_VAL_SIZE];
                pack_nonce_val(slots_issued[i].number, val);
                store_key_value(key, NONCE_KEY_SIZE, val, NONCE_VAL_SIZE);
                tried_something = true;
            }
        }
        else if (it->slot != slots_issued[i].number)
        {
            if (!destructive)
            {
                REPORT_ERROR("unreciprocal slot mapping "
                             << slots_issued[i].number << " -> ("
                             << slots_issued[i].client << ", "
                             << slots_issued[i].nonce << ") -> "
                             << it->slot
                             << " (this error may be fixed automatically by a destructive integrity check)");
            }
            else
            {
                REPORT_ERROR("unreciprocal slot mapping "
                             << slots_issued[i].number << " -> ("
                             << slots_issued[i].client << ", "
                             << slots_issued[i].nonce << ") -> "
                             << it->slot
                             << " (running in destructive mode: trying to fix automatically)");
                char key[NONCE_KEY_SIZE];
                pack_nonce_key(slots_issued[i].client, slots_issued[i].nonce, key);
                char val[NONCE_VAL_SIZE];
                pack_nonce_val(slots_issued[i].number, val);
                store_key_value(key, NONCE_KEY_SIZE, val, NONCE_VAL_SIZE);
                tried_something = true;
            }
        }
    }

    if (tried_something && tries_remaining > 0)
    {
        std::cout << "tried some fixes... making another pass" << std::endl;
        return integrity_check(tries_remaining - 1, output, destructive, config_manager);
    }

    return !error;
}

#undef REPORT_ERROR

bool
fact_store :: check_key_exists(const char* key, size_t key_sz)
{
    MDB_val k, val;
    bool do_reset = true;
    int rc;

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
        do_reset = false;
    }
    k.mv_data = (void *)key; k.mv_size = key_sz;
    rc = mdb_get(m_rtxn, m_dbi, &k, &val);
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
        LOG(ERROR) << "DB failed: " << mdb_strerror(rc);
        abort();
    }
}

void
fact_store :: store_key_value(const char* key, size_t key_sz,
                              const char* value, size_t value_sz)
{
    MDB_txn *txn;
    MDB_val k, v;
    int rc;

    k.mv_data = (void *)key; k.mv_size = key_sz;
    v.mv_data = (void *)value; v.mv_size = value_sz;

    rc = mdb_txn_begin(m_db, NULL, 0, &txn);
    if (rc)
    {
        abort();
    }
    rc = mdb_put(txn, m_dbi, &k, &v, 0);
    if (rc == MDB_SUCCESS)
    {
        rc = mdb_txn_commit(txn);
    }
    else
    {
        mdb_txn_abort(txn);
        LOG(ERROR) << "DB put failed: " << mdb_strerror(rc);
        abort();
    }
}

bool
fact_store :: retrieve_value(const char* key, size_t key_sz,
                             std::string* backing)
{
    MDB_val k, v;
    int rc;
    bool do_reset = true;

    rc = mdb_txn_renew(m_rtxn);
    if (rc)
    {
        do_reset = false;
    }
    k.mv_data = (void *)key; k.mv_size = key_sz;
    rc = mdb_get(m_rtxn, m_dbi, &k, &v);
    if (rc == MDB_SUCCESS)
    {
        backing->assign((const char *)v.mv_data, v.mv_size);
        if (do_reset)
            mdb_txn_reset(m_rtxn);
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
        LOG(ERROR) << "DB failed: " << mdb_strerror(rc);
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
        abort();
    }
    k.mv_data = (void *)key; k.mv_size = key_sz;
    rc = mdb_del(txn, m_dbi, &k, NULL);
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
    LOG(ERROR) << "DB delete failed: " << mdb_strerror(rc);
    abort();
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

bool
fact_store :: scan_all(std::vector<std::pair<uint64_t, uint64_t> >* proposals,
                       std::vector<std::vector<configuration> >* proposed_configs,
                       std::vector<std::pair<uint64_t, uint64_t> >* accepted_proposals,
                       std::vector<std::pair<uint64_t, uint64_t> >* rejected_proposals,
                       std::vector<std::pair<uint64_t, configuration> >* informed_configs,
                       std::vector<std::pair<uint64_t, const char*> >* clients,
                       std::vector<slot>* slots_issued,
                       std::vector<uint64_t>* slots_acked,
                       std::vector<exec>* slots_execd,
                       std::vector<slot_mapping>* slot_mappings)
{
    int st;
    bool ret = true;
    mdb_txn_renew(m_rtxn);

    do {
    st = scan_proposals(proposals, proposed_configs);

    if (st)
    {
        std::cerr << "could not scan proposed proposals: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_accepted_proposals(accepted_proposals);

    if (st)
    {
        std::cerr << "could not scan accepted proposals: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_rejected_proposals(rejected_proposals);

    if (st)
    {
        std::cerr << "could not scan rejected proposals: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_informed_configurations(informed_configs);

    if (st)
    {
        std::cerr << "could not scan inform configurations: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_clients(clients);

    if (st)
    {
        std::cerr << "could not scan clients: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_issue_slots(slots_issued);

    if (st)
    {
        std::cerr << "could not scan issued slots: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_ack_slots(slots_acked);

    if (st)
    {
        std::cerr << "could not scan acked slots: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_exec_slots(slots_execd);

    if (st)
    {
        std::cerr << "could not scan exec'd slots: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }

    st = scan_slot_mappings(slot_mappings);

    if (st)
    {
        std::cerr << "could not scan slot mappings: " << mdb_strerror(st) << std::endl;
        ret = false;
        break;
    }
    } while(0);
    mdb_txn_reset(m_rtxn);
    return ret;
}

int
fact_store :: scan_proposals(std::vector<std::pair<uint64_t, uint64_t> >* proposals,
                             std::vector<std::vector<configuration> >* proposed_configurations)
{
    MDB_val pref;
    assert(proposals->size() == proposed_configurations->size());
    MVS(pref,PROPOSAL_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != PROPOSAL_KEY_SIZE)
        {
            // ("key with proposed prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        uint64_t proposal_id;
        uint64_t proposal_time;
        unpack_proposal_key((const char *)iter.key()->mv_data, &proposal_id, &proposal_time);
        std::vector<configuration> configs;
        e::unpacker up((const char *)iter.val()->mv_data, iter.val()->mv_size);

        while (up.remain() && !up.error())
        {
            configuration c;
            up = up >> c;

            if (!up.error())
            {
                configs.push_back(c);
            }
        }

        if (up.error())
        {
            // ("could not unpack proposed configurations");
            return MDB_INCOMPATIBLE;
        }

        proposals->push_back(std::make_pair(proposal_id, proposal_time));
        proposed_configurations->push_back(configs);
    }
    assert(proposals->size() == proposed_configurations->size());
    return iter.status();
}

int
fact_store :: scan_accepted_proposals(std::vector<std::pair<uint64_t, uint64_t> >* accepted_proposals)
{
    MDB_val pref;
    MVS(pref,ACCEPTED_PROPOSAL_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != ACCEPTED_PROPOSAL_KEY_SIZE)
        {
            // ("key with accept prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        uint64_t proposal_id;
        uint64_t proposal_time;
        unpack_accepted_proposal_key((const char *)iter.key()->mv_data, &proposal_id, &proposal_time);
        accepted_proposals->push_back(std::make_pair(proposal_id, proposal_time));
    }

    return iter.status();
}

int
fact_store :: scan_rejected_proposals(std::vector<std::pair<uint64_t, uint64_t> >* rejected_proposals)
{
    MDB_val pref;
    MVS(pref,REJECTED_PROPOSAL_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != REJECTED_PROPOSAL_KEY_SIZE)
        {
            // ("key with reject prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        uint64_t proposal_id;
        uint64_t proposal_time;
        unpack_rejected_proposal_key((const char *)iter.key()->mv_data, &proposal_id, &proposal_time);
        rejected_proposals->push_back(std::make_pair(proposal_id, proposal_time));
    }

    return iter.status();
}

int
fact_store :: scan_informed_configurations(std::vector<std::pair<uint64_t, configuration> >* configurations)
{
    MDB_val pref;
    MVS(pref,INFORM_CONFIG_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != INFORM_CONFIG_KEY_SIZE)
        {
            // ("key with inform prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        uint64_t version;
        unpack_inform_config_key((const char *)iter.key()->mv_data, &version);
        configuration tmp;
        e::unpacker up((const char *)iter.val()->mv_data, iter.val()->mv_size);
        up = up >> tmp;

        if (up.error())
        {
             // ("could not unpack informed configuration");
            return MDB_INCOMPATIBLE;
        }

        configurations->push_back(std::make_pair(version, tmp));
    }

    return iter.status();
}

int
fact_store :: scan_clients(std::vector<std::pair<uint64_t, const char*> >* clients)
{
    MDB_val pref;
    MVS(pref,CLIENT_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != CLIENT_KEY_SIZE)
        {
            // ("key with client prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        uint64_t id;
        unpack_client_key((const char *)iter.key()->mv_data, &id);

        if (iter.val()->mv_size != 3)
        {
            // ("could not unpack client status");
            return MDB_INCOMPATIBLE;
        }

        const char* c = NULL;

        if (strncmp((const char *)iter.val()->mv_data, "reg", 3) == 0)
        {
            c = "reg";
        }
        else if (strncmp((const char *)iter.val()->mv_data, "die", 3) == 0)
        {
            c = "die";
        }
        else
        {
            // ("could not unpack client status");
            return MDB_INCOMPATIBLE;
        }

        clients->push_back(std::make_pair(id, c));
    }

    return iter.status();
}

int
fact_store :: scan_issue_slots(std::vector<slot>* slots)
{
    MDB_val pref;
    MVS(pref,SLOT_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != SLOT_KEY_SIZE)
        {
            // ("key with slot prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        slot s;
        unpack_slot_key((const char *)iter.key()->mv_data, &s.number);

        if (!unpack_slot_val(iter.val(), &s.object, &s.client, &s.nonce,
                             &s.func, &s.data))
        {
            // ("could not unpack slot data");
            return MDB_INCOMPATIBLE;
        }

        slots->push_back(s);
    }

    return iter.status();
}

int
fact_store :: scan_ack_slots(std::vector<uint64_t>* slots)
{
    MDB_val pref;
    MVS(pref,ACK_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != ACK_KEY_SIZE)
        {
            // ("key with ack prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        uint64_t s;
        unpack_ack_key((const char *)iter.key()->mv_data, &s);
        slots->push_back(s);
    }

    return iter.status();
}

int
fact_store :: scan_exec_slots(std::vector<exec>* slots)
{
    MDB_val pref;
    MVS(pref,EXEC_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != EXEC_KEY_SIZE)
        {
            // ("key with exec prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        exec e;
        unpack_exec_key((const char *)iter.key()->mv_data, &e.number);

        if (!unpack_exec_val(iter.val(), &e.rc, &e.response))
        {
            // ("could not unpack exec data");
            return MDB_INCOMPATIBLE;
        }

        slots->push_back(e);
    }

    return iter.status();
}

int
fact_store :: scan_slot_mappings(std::vector<slot_mapping>* mappings)
{
    MDB_val pref;
    MVS(pref,NONCE_PREFIX);
    prefix_iterator iter(&pref, m_rtxn, m_dbi);

    for (; iter.valid(); iter.next())
    {
        if (iter.key()->mv_size != NONCE_KEY_SIZE ||
            iter.val()->mv_size != NONCE_VAL_SIZE)
        {
            // ("key with exec prefix and improper length");
            return MDB_INCOMPATIBLE;
        }

        slot_mapping sm;
        unpack_nonce_key((const char *)iter.key()->mv_data, &sm.client, &sm.nonce);
        unpack_nonce_val((const char *)iter.val()->mv_data, &sm.slot);
        mappings->push_back(sm);
    }

    return iter.status();
}
