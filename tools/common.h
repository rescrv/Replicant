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

#ifndef replicant_tools_common_h_
#define replicant_tools_common_h_

// e
#include <e/popt.h>

class connect_opts
{
    public:
        connect_opts()
            : m_ap(), m_host("127.0.0.1"), m_port(1982)
        {
            m_ap.arg().name('h', "host")
                      .description("connect to an IP address or hostname (default: 127.0.0.1)")
                      .metavar("addr").as_string(&m_host);
            m_ap.arg().name('p', "port")
                      .description("connect to an alternative port (default: 1982)")
                      .metavar("port").as_long(&m_port);
        }
        ~connect_opts() throw () {}

    public:
        const e::argparser& parser() { return m_ap; }
        const char* host() { return m_host; }
        uint16_t port() { return m_port; }
        bool validate()
        {
            if (m_port <= 0 || m_port >= (1 << 16))
            {
                std::cerr << "port number to connect to is out of range" << std::endl;
                return false;
            }

            return true;
        }

    private:
        connect_opts(const connect_opts&);
        connect_opts& operator = (const connect_opts&);

    private:
        e::argparser m_ap;
        const char* m_host;
        long m_port;
};

void
cli_log_error(struct replicant_client* r, replicant_returncode status)
{
    std::cerr << __FILE__ << ":" << __LINE__ << " "
              << replicant_returncode_to_string(status) << " "
              << replicant_client_error_message(r) << " @ "
              << replicant_client_error_location(r) << std::endl;
}

bool
cli_finish(struct replicant_client* r, int64_t ret, enum replicant_returncode* status)
{
    if (ret < 0)
    {
        cli_log_error(r, *status);
        return false;
    }

    replicant_returncode lrc;
    int64_t lid = replicant_client_wait(r, ret, -1, &lrc);

    if (lid < 0)
    {
        cli_log_error(r, lrc);
        return false;
    }

    assert(lid == ret);

    if (*status != REPLICANT_SUCCESS)
    {
        cli_log_error(r, *status);
        return false;
    }

    return true;
}

#endif // replicant_tools_common_h_
