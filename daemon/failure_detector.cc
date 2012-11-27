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

#define __STDC_LIMIT_MACROS

// C
#include <cassert>
#include <cmath>

// Replicant
#include "daemon/failure_detector.h"

using replicant::failure_detector;

failure_detector :: failure_detector()
    : m_window()
    , m_window_sz(1000)
{
}

failure_detector :: ~failure_detector() throw ()
{
}

void
failure_detector :: heartbeat(uint64_t now)
{
    assert(m_window.empty() || m_window.back() < now);
    m_window.push_back(now);

    if (m_window.size() > m_window_sz)
    {
        m_window.pop_front();
    }
}

static double
phi(double x)
{
    // constants
    double a1 =  0.254829592;
    double a2 = -0.284496736;
    double a3 =  1.421413741;
    double a4 = -1.453152027;
    double a5 =  1.061405429;
    double p  =  0.3275911;

    // Save the sign of x
    int sign = 1;
    if (x < 0)
        sign = -1;
    x = fabs(x)/sqrt(2.0);

    // A&S formula 7.1.26
    double t = 1.0/(1.0 + p*x);
    double y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t*exp(-x*x);

    return 0.5*(1.0 + sign*y);
}

double
failure_detector :: suspicion(uint64_t now)
{
    if (m_window.empty())
    {
        return 1.0;
    }

    // Calculate the mean and standard deviation
    double n = 0;
    double mean = 0;
    double M2 = 0;

    std::deque<uint64_t>::iterator a = m_window.begin();
    std::deque<uint64_t>::iterator b = m_window.begin() + 1;

    while (b != m_window.end())
    {
        ++n;
        double diff = *b - *a;
        double delta = diff - mean;
        mean = mean + delta / n;
        M2 = M2 + delta * (diff - mean);
        ++a;
        ++b;
    }

    double stdev = sqrt(M2 / (n - 1));

    // A hack to initialize the failure detector.
    if (m_window.size() * 10 < m_window_sz && now - m_window.back() < 1000000000ULL)
    {
        return 1.0;
    }

    // Run that through phi
    double f = phi(((now - m_window.back()) - mean) / stdev);

    if (f < 1.0)
    {
        return 0 - log10(1.0 - f);
    }
    else
    {
        return HUGE_VAL;
    }
}
