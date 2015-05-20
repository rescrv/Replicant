// Copyright (c) 2015, Robert Escriva
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

#ifndef replicant_client_pending_cond_follow_h_
#define replicant_client_pending_cond_follow_h_

// Replicant
#include "client/pending.h"

BEGIN_REPLICANT_NAMESPACE

class pending_cond_follow : public pending
{
    public:
        pending_cond_follow(int64_t client_visible_id,
                            const char* object, const char* cond,
                            replicant_returncode* status,
                            uint64_t* state,
                            char** data, size_t* data_sz);
        virtual ~pending_cond_follow() throw ();

    public:
        virtual std::auto_ptr<e::buffer> request(uint64_t nonce);
        virtual bool resend_on_failure();
        virtual void handle_response(client* cl,
                                     std::auto_ptr<e::buffer> msg,
                                     e::unpacker up);

    private:
        const std::string m_object;
        const std::string m_cond;
        uint64_t* const m_state;
        char** const m_data;
        size_t* const m_data_sz;

    private:
        pending_cond_follow(const pending_cond_follow&);
        pending_cond_follow& operator = (const pending_cond_follow&);
};

END_REPLICANT_NAMESPACE

#endif // replicant_client_pending_cond_follow_h_
