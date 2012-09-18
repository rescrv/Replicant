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

// STL
#include <algorithm>

#include <glog/logging.h>

// Replicant
#include "daemon/configuration_manager.h"

configuration_manager :: configuration_manager()
    : m_config()
    , m_proposed()
    , m_rejected()
{
}

configuration_manager :: ~configuration_manager() throw ()
{
}

const configuration&
configuration_manager :: get_latest() const
{
    if (m_proposed.empty())
    {
        return m_config;
    }
    else
    {
        return m_proposed.back();
    }
}

const configuration&
configuration_manager :: get_stable() const
{
    return m_config;
}

bool
configuration_manager :: manages(const configuration& config) const
{
    // Otherwise, we need to find the config of the same version.
    if (m_config == config)
    {
        return true;
    }

    for (std::list<configuration>::const_iterator pit = m_proposed.begin();
            pit != m_proposed.end(); ++pit)
    {
        if (config == *pit)
        {
            return true;
        }
    }

    return false;
}

bool
configuration_manager :: quorum_for_all(const configuration& config) const
{
    if (!config.quorum_of(m_config))
    {
        return false;
    }

    for (std::list<configuration>::const_iterator pit = m_proposed.begin();
            pit != m_proposed.end(); ++pit)
    {
        if (!config.quorum_of(*pit))
        {
            return false;
        }
    }

    return true;
}

bool
configuration_manager :: unconfigured() const
{
    return m_config.version() == 0 && m_proposed.empty();
}

void
configuration_manager :: add_proposed(const configuration& config)
{
    m_proposed.push_back(config);
}

void
configuration_manager :: adopt(uint64_t version)
{
    bool found = m_config.version() == version;

    for (std::list<configuration>::const_iterator pit = m_proposed.begin();
            pit != m_proposed.end(); ++pit)
    {
        if (pit->version() == version)
        {
            found = true;
        }
    }

    assert(found);

    while (m_config.version() < version)
    {
        m_config = m_proposed.front();
        m_proposed.pop_front();
    }
}

void
configuration_manager :: reject(uint64_t version)
{
    m_rejected.push_back(version);
    std::push_heap(m_rejected.begin(), m_rejected.end());

    while (!m_rejected.empty() && !m_proposed.empty() &&
           m_rejected[0] >= m_proposed.back().version())
    {
        if (m_rejected[0] == m_proposed.back().version())
        {
            m_proposed.pop_back();
        }

        std::pop_heap(m_rejected.begin(), m_rejected.end());
        m_rejected.pop_back();
    }

    if (m_proposed.empty())
    {
        m_rejected.clear();
    }
}

void
configuration_manager :: reset(const configuration& us)
{
    m_config = us;
    m_proposed.clear();
}

std::ostream&
operator << (std::ostream& lhs, const configuration_manager& rhs)
{
    lhs << "configuration dump: \n"
        << "STABLE " << rhs.m_config;

    for (std::list<configuration>::const_iterator pit = rhs.m_proposed.begin();
            pit != rhs.m_proposed.end(); ++pit)
    {
        lhs << "\nPROPOSED " << *pit;
    }

    return lhs;
}

e::buffer::packer
operator << (e::buffer::packer lhs, const configuration_manager& rhs)
{
    lhs = lhs << static_cast<uint32_t>(1 + rhs.m_proposed.size())
              << rhs.m_config;

    for (std::list<configuration>::const_iterator pit = rhs.m_proposed.begin();
            pit != rhs.m_proposed.end(); ++pit)
    {
        lhs = lhs << *pit;
    }

    return lhs;
}

size_t
pack_size(const configuration_manager& rhs)
{
    size_t sz = sizeof(uint32_t) + pack_size(rhs.m_config);

    for (std::list<configuration>::const_iterator pit = rhs.m_proposed.begin();
            pit != rhs.m_proposed.end(); ++pit)
    {
        sz += pack_size(*pit);
    }

    return sz;
}
