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

#ifndef replicant_state_machine_context_h_
#define replicant_state_machine_context_h_

// Replicant
#include "daemon/object_manager.h"
#include "daemon/replicant_state_machine.h"

struct replicant_state_machine_context
{
    public:
        replicant_state_machine_context(uint64_t slot, uint64_t object, uint64_t client,
                                        replicant::object_manager* om,
                                        replicant::object_manager::object* ob);
        ~replicant_state_machine_context() throw ();

    public:
        uint64_t slot;
        uint64_t object;
        uint64_t client;
        char* log_output;
        size_t log_output_sz;
        FILE* output;
        replicant::object_manager* obj_man;
        replicant::object_manager::object* obj;
        const char* response;
        size_t response_sz;

    private:
        replicant_state_machine_context(const replicant_state_machine_context&);
        replicant_state_machine_context& operator = (const replicant_state_machine_context&);
};

#endif // replicant_state_machine_context_h_
