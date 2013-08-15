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

// Replicant
#include "common/macros.h"
#include "common/response_returncode.h"

using replicant::response_returncode;

std::ostream&
replicant :: operator << (std::ostream& lhs, response_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(RESPONSE_SUCCESS);
        STRINGIFY(RESPONSE_REGISTRATION_FAIL);
        STRINGIFY(RESPONSE_OBJ_EXIST);
        STRINGIFY(RESPONSE_OBJ_NOT_EXIST);
        STRINGIFY(RESPONSE_COND_NOT_EXIST);
        STRINGIFY(RESPONSE_COND_DESTROYED);
        STRINGIFY(RESPONSE_SERVER_ERROR);
        STRINGIFY(RESPONSE_DLOPEN_FAIL);
        STRINGIFY(RESPONSE_DLSYM_FAIL);
        STRINGIFY(RESPONSE_NO_CTOR);
        STRINGIFY(RESPONSE_NO_RTOR);
        STRINGIFY(RESPONSE_NO_DTOR);
        STRINGIFY(RESPONSE_NO_SNAP);
        STRINGIFY(RESPONSE_NO_FUNC);
        STRINGIFY(RESPONSE_CTOR_FAILED);
        STRINGIFY(RESPONSE_MALFORMED);
        default:
            lhs << "unknown returncode";
    }

    return lhs;
}

e::buffer::packer
replicant :: operator << (e::buffer::packer lhs, const response_returncode& rhs)
{
    uint8_t mt = static_cast<uint8_t>(rhs);
    return lhs << mt;
}

e::unpacker
replicant :: operator >> (e::unpacker lhs, response_returncode& rhs)
{
    uint8_t mt;
    lhs = lhs >> mt;
    rhs = static_cast<response_returncode>(mt);
    return lhs;
}

size_t
replicant :: pack_size(const response_returncode&)
{
    return sizeof(uint8_t);
}
