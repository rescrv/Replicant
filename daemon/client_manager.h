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

#ifndef replicant_client_manager_h_
#define replicant_client_manager_h_

// STL
#include <map>
#include <vector>

// po6
#include <po6/net/location.h>

// e
#include <e/buffer.h>

class client_manager
{
    public:
        class client_details;

    public:
        client_manager();
        ~client_manager() throw ();

    public:
        void get(std::vector<po6::net::location>* clients) const;
        void get(std::vector<uint64_t>* clients) const;
        const client_details* get(uint64_t client) const;
        void get_live_clients(std::vector<uint64_t>* clients) const;
        uint64_t lowest_slot_in_use() const;

    public:
        client_details* create(uint64_t client); // or get
        void disassociate(const po6::net::location& host);
        void garbage_collect(uint64_t slot);
        client_details* get(uint64_t client);
        void get_old_clients(uint64_t time, std::vector<uint64_t>* clients);
        void remove(uint64_t client);

    private:
        typedef std::map<uint64_t, client_details> client_map;

    private:
        client_map m_clients;
};

e::buffer::packer
operator << (e::buffer::packer lhs, const client_manager& rhs);

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, client_manager& rhs);

size_t
pack_size(const client_manager& rhs);

class client_manager::client_details
{
    public:
        client_details();
        ~client_details() throw ();

    public:
        const po6::net::location& location() const;
        uint64_t last_seen() const;
        uint64_t slot_for(uint64_t nonce) const;
        uint64_t lower_bound_nonce() const;
        uint64_t upper_bound_nonce() const;
        uint64_t lower_bound_slot() const;
        uint64_t upper_bound_slot() const;

    public:
        void garbage_collect(uint64_t slot);
        void set_last_seen(uint64_t time);
        void set_location(const po6::net::location& location);
        void set_lower_bound(uint64_t nonce);
        void set_slot(uint64_t nonce, uint64_t slot);

    private:
        typedef std::vector<std::pair<uint64_t, uint64_t> > slot_list;

    private:
        po6::net::location m_location;
        uint64_t m_lower_bound;
        uint64_t m_last_seen;
        slot_list m_slots;
};

#endif // replicant_client_manager_h_
