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

#ifndef replicant_command_manager_h_
#define replicant_command_manager_h_

// STL
#include <list>
#include <map>

// Replicant
#include "daemon/command.h"

class command_manager
{
    public:
        command_manager();
        ~command_manager() throw ();

    public:
        bool is_acknowledged(uint64_t slot) const;
        e::intrusive_ptr<command> lookup(uint64_t slot) const;
        e::intrusive_ptr<command> lookup_acknowledged(uint64_t slot) const;
        e::intrusive_ptr<command> lookup_proposed(uint64_t slot) const;
        uint64_t lower_bound_acknowledged() const;
        uint64_t upper_bound_acknowledged() const;
        uint64_t lower_bound_proposed() const;
        uint64_t upper_bound_proposed() const;

    public:
        void append(e::intrusive_ptr<command> cmd);
        void acknowledge(uint64_t slot);
        void garbage_collect(uint64_t slot);

    private:
        typedef std::list<e::intrusive_ptr<command> > command_list;
        typedef std::map<uint64_t, command_list::const_iterator> command_index;

    private:
        command_list m_cmds;
        command_index m_idx;
        // This is points to (one past the last acknowledged)/(the first proposed)
        command_list::iterator m_cutoff;
};

#endif // replicant_command_manager_h_
