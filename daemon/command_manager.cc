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
#include "daemon/command_manager.h"

command_manager :: command_manager()
    : m_cmds()
    , m_idx()
    , m_cutoff(m_cmds.begin())
{
}

command_manager :: ~command_manager() throw ()
{
}

bool
command_manager :: is_acknowledged(uint64_t slot) const
{
    command_list::const_iterator it = m_cutoff;

    // There are no acknowledged slots
    if (it == m_cmds.begin())
    {
        return false;
    }

    --it;
    return (*it)->slot() >= slot;
}

e::intrusive_ptr<command>
command_manager :: lookup(uint64_t slot) const
{
    uint64_t idx = 1023;
    idx = ~idx;
    idx &= slot;

    command_index::const_iterator i = m_idx.find(idx);
    command_list::const_iterator it = m_cmds.begin();

    if (i != m_idx.end())
    {
        it = i->second;
    }

    while (it != m_cmds.end() && (*it)->slot() < slot)
    {
        ++it;
    }

    if (it != m_cmds.end() && (*it)->slot() == slot)
    {
        return *it;
    }
    else
    {
        return e::intrusive_ptr<command>();
    }
}

e::intrusive_ptr<command>
command_manager :: lookup_acknowledged(uint64_t slot) const
{
    if (lower_bound_acknowledged() <= slot &&
        slot < upper_bound_acknowledged())
    {
        return lookup(slot);
    }
    else
    {
        return e::intrusive_ptr<command>();
    }
}

e::intrusive_ptr<command>
command_manager :: lookup_proposed(uint64_t slot) const
{
    if (lower_bound_proposed() <= slot &&
        slot < upper_bound_proposed())
    {
        return lookup(slot);
    }
    else
    {
        return e::intrusive_ptr<command>();
    }
}

uint64_t
command_manager :: lower_bound_acknowledged() const
{
    if (m_cmds.empty())
    {
        return 1;
    }

    return m_cmds.front()->slot();
}

uint64_t
command_manager :: upper_bound_acknowledged() const
{
    if (m_cutoff == m_cmds.end())
    {
        return upper_bound_proposed();
    }
    else
    {
        return (*m_cutoff)->slot();
    }
}

uint64_t
command_manager :: lower_bound_proposed() const
{
    return upper_bound_acknowledged();
}

uint64_t
command_manager :: upper_bound_proposed() const
{
    if (m_cmds.empty())
    {
        return 1;
    }

    return m_cmds.back()->slot() + 1;
}

void
command_manager :: append(e::intrusive_ptr<command> cmd)
{
    assert(cmd);
    assert(m_cmds.empty() || cmd->slot() > m_cmds.back()->slot());

    if (m_cutoff == m_cmds.end())
    {
        m_cutoff = m_cmds.insert(m_cutoff, cmd);

        if (cmd->slot() % 1024 == 0)
        {
            m_idx[cmd->slot()] = m_cutoff;
        }
    }
    else
    {
        m_cmds.push_back(cmd);

        if (cmd->slot() % 1024 == 0)
        {
            m_idx[cmd->slot()] = --m_cmds.end();
        }
    }
}

void
command_manager :: acknowledge(uint64_t slot)
{
    assert(m_cutoff != m_cmds.end() && (*m_cutoff)->slot() == slot);
    ++m_cutoff;
}

void
command_manager :: garbage_collect(uint64_t slot)
{
    assert(slot <= upper_bound_acknowledged());
    command_list::iterator lit = m_cmds.begin();

    while (lit != m_cmds.end() && (*lit)->slot() < slot)
    {
        lit = m_cmds.erase(lit);
    }

    command_index::iterator iit = m_idx.begin();

    while (iit != m_idx.end() && iit->first < slot)
    {
        m_idx.erase(iit);
        iit = m_idx.begin();
    }
}
