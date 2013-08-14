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
        uint64_t REPORT_INTERVAL;
        uint64_t JOIN_INTERVAL;
        uint64_t MAINTAIN_INTERVAL;
        uint64_t RETRY_RECONFIGURE_INTERVAL;
        uint64_t PING_INTERVAL;
        uint64_t FAULT_TOLERANCE;
        uint64_t HEAL_NEXT_INTERVAL;
        uint64_t TRANSFER_WINDOW_LOWER_BOUND;
        uint64_t TRANSFER_WINDOW_UPPER_BOUND;
        uint64_t CONNECTION_RETRY;
        uint64_t PERIODIC_SIZE_WARNING;
};

inline
settings :: settings()
    : REPORT_INTERVAL(60 * SECONDS)
    , JOIN_INTERVAL(1 * SECONDS)
    , MAINTAIN_INTERVAL(250 * MILLIS)
    , RETRY_RECONFIGURE_INTERVAL(1 * SECONDS)
    , PING_INTERVAL(50 * MILLIS)
    , FAULT_TOLERANCE(2)
    , HEAL_NEXT_INTERVAL(1 * SECONDS)
    , TRANSFER_WINDOW_LOWER_BOUND(16)
    , TRANSFER_WINDOW_UPPER_BOUND(4096)
    , CONNECTION_RETRY(50 * MILLIS)
    , PERIODIC_SIZE_WARNING(16)
{
}

} // namespace replicant

#endif // replicant_settings_h_
