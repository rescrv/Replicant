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

#ifndef replicant_daemon_slot_type_h_
#define replicant_daemon_slot_type_h_

// e
#include <e/buffer.h>

// Replicant
#include "namespace.h"

BEGIN_REPLICANT_NAMESPACE

enum slot_type
{
    SLOT_SERVER_BECOME_MEMBER = 1,
    SLOT_SERVER_SET_GC_THRESH = 2,
    SLOT_SERVER_CHANGE_ADDRESS = 10,
    SLOT_SERVER_RECORD_STRIKE = 11,
    SLOT_INCREMENT_COUNTER = 3,
    SLOT_OBJECT_FAILED = 9,
    SLOT_OBJECT_REPAIR = 8,
    SLOT_TICK = 7,
    SLOT_POKE = 4,
    SLOT_CALL = 5,
    SLOT_NOP = 0
};

std::ostream&
operator << (std::ostream& lhs, slot_type rhs);

e::packer
operator << (e::packer lhs, const slot_type& rhs);
e::unpacker
operator >> (e::unpacker lhs, slot_type& rhs);
size_t
pack_size(const slot_type& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_daemon_slot_type_h_
