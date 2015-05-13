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

// POSIX
#include <fcntl.h>
#include <stdio.h>

// po6
#include <po6/io/fd.h>

// Replicant
#include "common/atomic_io.h"

bool
replicant :: atomic_read(int dir, const char* path, std::string* contents)
{
    contents->clear();
    po6::io::fd fd(openat(dir, path, O_RDONLY));

    if (fd.get() < 0)
    {
        return false;
    }

    char buf[512];
    ssize_t amt;

    while ((amt = fd.xread(buf, 512)) > 0)
    {
        contents->append(buf, amt);
    }

    if (amt < 0)
    {
        return false;
    }

    return true;
}

bool
replicant :: atomic_write(int dir, const char* path, const std::string& contents)
{
    po6::io::fd fd(openat(dir, ".atomic.tmp", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));
    return fd.get() >= 0 &&
           fd.xwrite(contents.data(), contents.size()) == ssize_t(contents.size()) &&
           fsync(fd.get()) >= 0 &&
           (dir == AT_FDCWD || fsync(dir) >= 0) &&
           renameat(dir, ".atomic.tmp", dir, path) >= 0 &&
           (dir == AT_FDCWD || fsync(dir) >= 0);
}
