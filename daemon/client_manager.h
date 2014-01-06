// Copyright (c) 2014, Robert Escriva
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

#ifndef replicant_daemon_client_manager_h_
#define replicant_daemon_client_manager_h_

// C
#include <stdint.h>

// STL
#include <vector>

namespace replicant
{

class client_manager
{
    public:
        client_manager();
        ~client_manager() throw ();

    public:
        void register_client(uint64_t client);
        void deregister_client(uint64_t client);
        void list_clients(std::vector<uint64_t>* clients);
        void owned_clients(uint64_t chain_index,
                           uint64_t chain_length,
                           std::vector<uint64_t>* clients);
        void last_seen_before(uint64_t when, std::vector<uint64_t>* clients);
        void proof_of_life(uint64_t now);
        void proof_of_life(uint64_t client, uint64_t now);

    private:
        class client_metadata;
        std::vector<client_metadata> m_clients;
};

} // namespace replicant

#endif // replicant_daemon_client_manager_h_
