// Copyright (c) 2015, Robert Escriva
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

#ifndef replicant_common_bootstrap_h_
#define replicant_common_bootstrap_h_

// STL
#include <vector>

// po6
#include <po6/net/hostname.h>

// e
#include <e/error.h>

// Replicant
#include <replicant.h>
#include "namespace.h"

BEGIN_REPLICANT_NAMESPACE
class configuration;

class bootstrap
{
    public:
        static bool parse_hosts(const char* conn_str,
                                std::vector<po6::net::hostname>* hosts);
        static std::string conn_str(const po6::net::hostname* hns, size_t hns_sz);

    public:
        bootstrap();
        bootstrap(const char* host, uint16_t port);
        bootstrap(const char* conn_str);
        bootstrap(const char* host, uint16_t port, const char* conn_str);
        bootstrap(const std::vector<po6::net::hostname>& hosts);
        bootstrap(const bootstrap& other);
        ~bootstrap() throw ();

    public:
        bool valid() const;
        replicant_returncode do_it(int timeout, configuration* config, e::error* err) const;
        std::string conn_str() const;
        const std::vector<po6::net::hostname>& hosts() const { return m_hosts; }

    public:
        bootstrap& operator = (const bootstrap& rhs);

    private:
        std::vector<po6::net::hostname> m_hosts;
        bool m_valid;
};

std::ostream&
operator << (std::ostream& lhs, const bootstrap& rhs);

END_REPLICANT_NAMESPACE

#endif // replicant_common_bootstrap_h_
