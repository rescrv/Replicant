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

#ifndef replicant_common_response_flags_h_
#define replicant_common_response_flags_h_

// C++
#include <iostream>

// e
#include <e/buffer.h>

namespace replicant
{

enum response_returncode
{
    RESPONSE_SUCCESS,
    RESPONSE_REGISTRATION_FAIL,
    RESPONSE_OBJ_EXIST,
    RESPONSE_OBJ_NOT_EXIST,
    RESPONSE_SERVER_ERROR,
    RESPONSE_DLOPEN_FAIL,
    RESPONSE_DLSYM_FAIL,
    RESPONSE_NO_CTOR,
    RESPONSE_NO_RTOR,
    RESPONSE_NO_DTOR,
    RESPONSE_NO_SNAP,
    RESPONSE_NO_FUNC,
    RESPONSE_MALFORMED
};

std::ostream&
operator << (std::ostream& lhs, response_returncode rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const response_returncode& rhs);

e::unpacker
operator >> (e::unpacker lhs, response_returncode& rhs);

size_t
pack_size(const response_returncode& rhs);

} // namespace replicant

#endif // replicant_common_response_flags_h_
