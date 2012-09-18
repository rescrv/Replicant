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

#ifndef replicant_configuration_manager_h_
#define replicant_configuration_manager_h_

// STL
#include <list>

// Replicant
#include "common/configuration.h"

class configuration_manager
{
    public:
        configuration_manager();
        ~configuration_manager() throw ();

    public:
        const configuration& get_latest() const;
        const configuration& get_stable() const;
        bool manages(const configuration& config) const;
        bool quorum_for_all(const configuration& config) const;
        bool unconfigured() const;

    public:
        void add_proposed(const configuration& config);
        void adopt(uint64_t version);
        void reject(uint64_t version);
        void reset(const configuration& us);

    private:
        friend std::ostream& operator << (std::ostream& lhs, const configuration_manager& rhs);
        friend e::buffer::packer operator << (e::buffer::packer lhs, const configuration_manager& rhs);
        friend size_t pack_size(const configuration_manager& rhs);

    private:
        configuration m_config;
        std::list<configuration> m_proposed;
        std::vector<uint64_t> m_rejected;
};

std::ostream&
operator << (std::ostream& lhs, const configuration_manager& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const configuration_manager& rhs);

size_t
pack_size(const configuration_manager& rhs);

#endif // replicant_configuration_manager_h_
