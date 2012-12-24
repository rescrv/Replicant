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

#ifndef replicant_daemon_conditions_wrapper_h_
#define replicant_daemon_conditions_wrapper_h_

// Replicant
#include "daemon/object_manager.h"

namespace replicant
{

class conditions_wrapper
{
    public:
        conditions_wrapper() : m_om(NULL), m_o(NULL) {}
        conditions_wrapper(object_manager* om, void* o) : m_om(om), m_o(o) {}
        conditions_wrapper(conditions_wrapper& other) : m_om(other.m_om), m_o(other.m_o) {}
        ~conditions_wrapper() throw () {}

    public:
        int create(uint64_t cond)
        { return m_om->condition_create(m_o, cond); }
        int destroy(uint64_t cond)
        { return m_om->condition_destroy(m_o, cond); }
        int broadcast(uint64_t cond, uint64_t* state)
        { return m_om->condition_broadcast(m_o, cond, state); }

    public:
        conditions_wrapper& operator = (const conditions_wrapper& rhs)
        { m_om = rhs.m_om; m_o = rhs.m_o; return *this; }

    private:
        object_manager* m_om;
        void* m_o;
};

} // namespace replicant

#endif // replicant_daemon_conditions_wrapper_h_
