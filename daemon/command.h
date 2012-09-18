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

#ifndef replicant_command_h_
#define replicant_command_h_

// STL
#include <memory>

// e
#include <e/intrusive_ptr.h>

// Replicant
#include "common/chain_node.h"

class command
{
    public:
        command(uint64_t client, uint64_t nonce,
                uint64_t slot, uint64_t object,
                std::auto_ptr<e::buffer> msg);
        ~command() throw ();

    public:
        uint64_t client() const { return m_client; }
        uint64_t nonce() const { return m_nonce; }
        uint64_t slot() const { return m_slot; }
        uint64_t object() const { return m_object; }
        e::buffer* msg() const;
        e::buffer* response() const;

    public:
        void set_response(std::auto_ptr<e::buffer> response);

    private:
        friend class e::intrusive_ptr<command>;

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
        uint64_t m_client;
        uint64_t m_nonce;
        uint64_t m_slot;
        uint64_t m_object;
        std::auto_ptr<e::buffer> m_msg;
        std::auto_ptr<e::buffer> m_response;
};

#endif // replicant_command_h_
