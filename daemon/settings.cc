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
#include "daemon/settings.h"

e::buffer::packer
operator << (e::buffer::packer lhs, const settings& rhs)
{
    uint8_t garbage_collect_log_details = rhs.GARBAGE_COLLECT_LOG_DETAILS ? 1 : 0;
    return lhs << rhs.REPORT_INTERVAL
               << rhs.BECOME_SPARE_INTERVAL
               << rhs.BECOME_STANDBY_INTERVAL
               << rhs.BECOME_MEMBER_INTERVAL
               << rhs.HEAL_PREV_INTERVAL
               << rhs.HEAL_NEXT_INTERVAL
               << rhs.GARBAGE_COLLECT_INTERVAL
               << garbage_collect_log_details
               << rhs.GARBAGE_COLLECT_MIN_SLOTS
               << rhs.CLIENT_LIST_INTERVAL
               << rhs.CLIENT_DISCONNECT_INTERVAL
               << rhs.TRANSFER_WINDOW
               << rhs.CONNECTION_RETRY
               << rhs.PERIODIC_SIZE_WARNING;
}

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, settings& rhs)
{
    uint8_t garbage_collect_log_details;
    lhs = lhs >> rhs.REPORT_INTERVAL
              >> rhs.BECOME_SPARE_INTERVAL
              >> rhs.BECOME_STANDBY_INTERVAL
              >> rhs.BECOME_MEMBER_INTERVAL
              >> rhs.HEAL_PREV_INTERVAL
              >> rhs.HEAL_NEXT_INTERVAL
              >> rhs.GARBAGE_COLLECT_INTERVAL
              >> garbage_collect_log_details
              >> rhs.GARBAGE_COLLECT_MIN_SLOTS
              >> rhs.CLIENT_LIST_INTERVAL
              >> rhs.CLIENT_DISCONNECT_INTERVAL
              >> rhs.TRANSFER_WINDOW
              >> rhs.CONNECTION_RETRY
              >> rhs.PERIODIC_SIZE_WARNING;
    rhs.GARBAGE_COLLECT_LOG_DETAILS = garbage_collect_log_details != 0;
    return lhs;
}

size_t
pack_size(const settings&)
{
    return 13 * sizeof(uint64_t) + sizeof(uint8_t);
}
