// Copyright (c) 2012-2015, Robert Escriva
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

#ifndef replicant_client_pending_h_
#define replicant_client_pending_h_

// STL
#include <memory>

// e
#include <e/buffer.h>
#include <e/error.h>
#include <e/intrusive_ptr.h>

// Replicant
#include <replicant.h>
#include "namespace.h"

BEGIN_REPLICANT_NAMESPACE

class pending
{
    public:
        pending(int64_t client_visible_id,
                replicant_returncode* status);
        virtual ~pending() throw ();

    public:
        int64_t client_visible_id() const { return m_client_visible_id; }
        void set_status(replicant_returncode st) { *m_status = st; }
        replicant_returncode status() const { return *m_status; }
        replicant_returncode* status_ptr() const { return m_status; }
        e::error error() const { return m_error; }

    public:
        virtual std::auto_ptr<e::buffer> request(uint64_t nonce) = 0;
        virtual bool resend_on_failure() = 0;
        virtual void handle_response(std::auto_ptr<e::buffer> msg,
                                     e::unpacker up) = 0;

    public:
        std::ostream& error(const char* file, size_t line);
        void set_error(const e::error& err);
        void success();

    // refcount
    protected:
        friend class e::intrusive_ptr<pending>;
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        size_t m_ref;

    // operation state
    private:
        int64_t m_client_visible_id;
        replicant_returncode* m_status;
        e::error m_error;

    // noncopyable
    private:
        pending(const pending&);
        pending& operator = (const pending&);
};

#define PENDING_ERROR(CODE) \
    this->set_status(REPLICANT_ ## CODE); \
    this->error(__FILE__, __LINE__)

END_REPLICANT_NAMESPACE

#endif // replicant_client_pending_h_
