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

namespace replicant
{

class settings
{
    public:
        settings();

    public:
        uint64_t FAULT_TOLERANCE;
        uint64_t PERIODIC_SIZE_WARNING;
        uint64_t REPORT_EVERY;
        uint64_t CLIENT_DISCONNECT_EVERY;
        uint64_t CLIENT_DISCONNECT_TIMEOUT;
        uint64_t FAILURE_DETECT_INTERVAL;
        uint64_t FAILURE_DETECT_PING_OFFSET;
        uint64_t FAILURE_DETECT_SUSPECT_OFFSET;
        uint64_t FAILURE_DETECT_WINDOW_SIZE;
        uint64_t HEAL_NEXT_INTERVAL;
        uint64_t TRANSFER_WINDOW_LOWER_BOUND;
        uint64_t TRANSFER_WINDOW_UPPER_BOUND;
        uint64_t CONNECTION_RETRY;
};

inline
settings :: settings()
    : FAULT_TOLERANCE(2)
    , PERIODIC_SIZE_WARNING(16)
    , REPORT_EVERY(60 * SECONDS)
    , CLIENT_DISCONNECT_EVERY(1 * SECONDS)
    , CLIENT_DISCONNECT_TIMEOUT(30 * SECONDS)
    , FAILURE_DETECT_INTERVAL(250 * MILLIS)
    , FAILURE_DETECT_PING_OFFSET(5 * MILLIS)
    , FAILURE_DETECT_SUSPECT_OFFSET(245 * MILLIS)
    , FAILURE_DETECT_WINDOW_SIZE(6000)
    , HEAL_NEXT_INTERVAL(1 * SECONDS)
    , TRANSFER_WINDOW_LOWER_BOUND(16)
    , TRANSFER_WINDOW_UPPER_BOUND(4096)
    , CONNECTION_RETRY(50 * MILLIS)
{
}

} // namespace replicant

#endif // replicant_settings_h_
