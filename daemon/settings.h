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

#ifndef replicant_settings_h_
#define replicant_settings_h_

// e
#include <e/buffer.h>

#define NANOS 1ULL
#define MICROS (1000ULL * NANOS)
#define MILLIS (1000ULL * MICROS)
#define SECONDS (1000ULL * MILLIS)

class settings
{
    public:
        settings();

    public:
        uint64_t REPORT_INTERVAL;
        uint64_t BECOME_SPARE_INTERVAL;
        uint64_t BECOME_STANDBY_INTERVAL;
        uint64_t BECOME_MEMBER_INTERVAL;
        uint64_t HEAL_PREV_INTERVAL;
        uint64_t HEAL_NEXT_INTERVAL;
        uint64_t GARBAGE_COLLECT_INTERVAL;
        bool GARBAGE_COLLECT_LOG_DETAILS;
        uint64_t GARBAGE_COLLECT_MIN_SLOTS;
        uint64_t CLIENT_LIST_INTERVAL;
        uint64_t CLIENT_DISCONNECT_INTERVAL;
        uint64_t TRANSFER_WINDOW;
        uint64_t CONNECTION_RETRY;
        uint64_t PERIODIC_SIZE_WARNING;
};

inline
settings :: settings()
    : REPORT_INTERVAL(10 * SECONDS)
    , BECOME_SPARE_INTERVAL(1 * SECONDS)
    , BECOME_STANDBY_INTERVAL(1 * SECONDS)
    , BECOME_MEMBER_INTERVAL(1 * SECONDS)
    , HEAL_PREV_INTERVAL(1 * SECONDS)
    , HEAL_NEXT_INTERVAL(1 * SECONDS)
    , GARBAGE_COLLECT_INTERVAL(1 * SECONDS)
    , GARBAGE_COLLECT_LOG_DETAILS(false)
    , GARBAGE_COLLECT_MIN_SLOTS(1024)
    , CLIENT_LIST_INTERVAL(5 * SECONDS)
    , CLIENT_DISCONNECT_INTERVAL(30 * SECONDS)
    , TRANSFER_WINDOW(512)
    , CONNECTION_RETRY(1 * SECONDS)
    , PERIODIC_SIZE_WARNING(16)
{
}

#undef SECONDS
#undef MILLIS
#undef MICROS
#undef NANOS

e::buffer::packer
operator << (e::buffer::packer lhs, const settings& rhs);

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, settings& rhs);

size_t
pack_size(const settings& rhs);

#endif // replicant_settings_h_
