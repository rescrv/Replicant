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
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// C
#include <stdarg.h>
#include <stdio.h>

// POSIX
#include <unistd.h>

// STL
#include <new>
#include <vector>

// po6
#include <po6/io/fd.h>

// e
#include <e/endian.h>
#include <e/error.h>
#include <e/guard.h>

// Replicant
#include "visibility.h"
#include "daemon/object_interface.h"

#pragma GCC diagnostic ignored "-Wsuggest-attribute=format"

struct object_interface
{
    object_interface(int fd);
    ~object_interface() throw ();

    void read(char* data, size_t sz);
    void write(const char* data, size_t sz);

    po6::io::fd fd;
    FILE* debug_stream;
    bool shutdown;

    std::string tmp1;
    std::string tmp2;

    private:
        object_interface(const object_interface&);
        object_interface& operator = (const object_interface&);
};

object_interface :: object_interface(int f)
    : fd(f)
    , debug_stream(NULL)
    , shutdown(false)
    , tmp1()
    , tmp2()
{
    int tty = open("/dev/tty", O_RDWR);

    if (tty >= 0)
    {
        debug_stream = fdopen(tty, "a+");
    }
}

object_interface :: ~object_interface() throw ()
{
    if (debug_stream)
    {
        fclose(debug_stream);
    }
}

void
object_interface :: read(char* data, size_t sz)
{
    if (fd.xread(data, sz) != ssize_t(sz))
    {
        object_permanent_error(this, "short read: %s", e::error::strerror(errno).c_str());
    }
}

void
object_interface :: write(const char* data, size_t sz)
{
    if (fd.xwrite(data, sz) != ssize_t(sz))
    {
        object_permanent_error(this, "short write: %s", e::error::strerror(errno).c_str());
    }
}

extern "C"
{

REPLICANT_API object_interface*
object_interface_create(int fd)
{
    e::guard g_fd = e::makeguard(close, fd);
    object_interface* obj_int = new (std::nothrow) object_interface(fd);

    if (!obj_int)
    {
        return NULL;
    }

    g_fd.dismiss();
    return obj_int;
}

REPLICANT_API void
object_interface_destroy(object_interface* obj_int)
{
    if (obj_int)
    {
        delete obj_int;
    }
}

REPLICANT_API void
object_permanent_error(object_interface* obj_int, const char* format, ...)
{
    if (obj_int->debug_stream)
    {
        va_list args;
        va_start(args, format);
        vfprintf(obj_int->debug_stream, format, args);
        va_end(args);
    }

    obj_int->fd.close();
    abort();
}

REPLICANT_API int
object_next_action(object_interface* obj_int, action_t* action)
{
    if (obj_int->shutdown)
    {
        return -1;
    }

    char act;
    obj_int->read(&act, 1);
    *action = static_cast<action_t>(act);

    switch (*action)
    {
        case ACTION_CTOR:
        case ACTION_RTOR:
        case ACTION_COMMAND:
        case ACTION_SNAPSHOT:
            return 0;
        case ACTION_SHUTDOWN:
            obj_int->shutdown = true;
            return 0;
        default:
            object_permanent_error(obj_int, "bad action %d", *action);
    }
}

REPLICANT_API void
object_read_snapshot(object_interface* obj_int, const char** data, size_t* data_sz)
{
    char buf[4];
    obj_int->read(buf, 4);
    uint32_t size;
    e::unpack32be(buf, &size);
    std::vector<char> snap(size);
    obj_int->read(&snap[0], size);
    obj_int->tmp1.assign(&snap[0], size);
    *data = obj_int->tmp1.data();
    *data_sz = obj_int->tmp1.size();
}

REPLICANT_API void
object_read_command(object_interface* obj_int, command* cmd)
{
    // read the size of the command
    char buf[8];
    obj_int->read(buf, 8);
    uint64_t size;
    e::unpack64be(buf, &size);

    if (size < 16)
    {
        object_permanent_error(obj_int, "received corrupt command");
    }

    size -= 8;
    std::vector<char> msg(size);
    obj_int->read(&msg[0], size);
    uint32_t func_size;
    e::unpack32be(&msg[0], &func_size);

    if (8 + func_size > size)
    {
        object_permanent_error(obj_int, "received corrupt command");
    }

    uint32_t input_size;
    e::unpack32be(&msg[4 + func_size], &input_size);

    if (8 + func_size + input_size > size)
    {
        object_permanent_error(obj_int, "received corrupt command");
    }

    obj_int->tmp1.assign(&msg[4], func_size);
    obj_int->tmp2.assign(&msg[8 + func_size], input_size);
    cmd->func = obj_int->tmp1.c_str();
    cmd->input = obj_int->tmp2.data();
    cmd->input_sz = obj_int->tmp2.size();
}

REPLICANT_API void
object_command_log(object_interface* obj_int,
                   const char *format, va_list ap)
{
    char* buf = NULL;
    int ret = 0;

    if ((ret = vasprintf(&buf, format, ap)) < 0)
    {
        object_permanent_error(obj_int, "failed to allocate memory");
    }

    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_LOG);
    uint32_t o = ret;
    char head[5];
    e::pack8be(c, head);
    e::pack32be(o, head + 1);
    obj_int->write(head, 5);
    obj_int->write(buf, ret);
    free(buf);
}

REPLICANT_API void
object_command_output(object_interface* obj_int,
                      replicant_returncode status,
                      const char* data, size_t data_sz)
{
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_OUTPUT);
    uint16_t s = static_cast<uint16_t>(status);
    uint32_t o = data_sz;
    char buf[7];
    e::pack8be(c, buf);
    e::pack16be(s, buf + 1);
    e::pack32be(o, buf + 3);
    obj_int->write(buf, 7);
    obj_int->write(data, data_sz);
}

REPLICANT_API void
object_cond_create(object_interface* obj_int, const char* cond)
{
    const size_t cond_sz = strlen(cond);
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_COND_CREATE);
    uint32_t o = cond_sz;
    char buf[5];
    e::pack8be(c, buf);
    e::pack32be(o, buf + 1);
    obj_int->write(buf, 5);
    obj_int->write(cond, cond_sz);
}

REPLICANT_API void
object_cond_destroy(object_interface* obj_int, const char* cond)
{
    const size_t cond_sz = strlen(cond);
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_COND_DESTROY);
    uint32_t o = cond_sz;
    char buf[5];
    e::pack8be(c, buf);
    e::pack32be(o, buf + 1);
    obj_int->write(buf, 5);
    obj_int->write(cond, cond_sz);
}

REPLICANT_API int
object_cond_broadcast(object_interface* obj_int, const char* cond)
{
    const size_t cond_sz = strlen(cond);
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_COND_BROADCAST);
    uint32_t o = cond_sz;
    char buf[5];
    e::pack8be(c, buf);
    e::pack32be(o, buf + 1);
    obj_int->write(buf, 5);
    obj_int->write(cond, cond_sz);
    obj_int->read(buf, 1);
    return buf[0] == 0 ? 0 : -1;
}

REPLICANT_API int
object_cond_broadcast_data(object_interface* obj_int,
                                             const char* cond,
                                             const char* data, size_t data_sz)
{
    const size_t cond_sz = strlen(cond);
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_COND_BROADCAST_DATA);
    uint32_t o = cond_sz;
    char buf[9];
    e::pack8be(c, buf);
    e::pack32be(o, buf + 1);
    e::pack32be(data_sz, buf + 5);
    obj_int->write(buf, 5);
    obj_int->write(cond, cond_sz);
    obj_int->write(buf + 5, 4);
    obj_int->write(data, uint32_t(data_sz));
    obj_int->read(buf, 1);
    return buf[0] == 0 ? 0 : -1;
}

REPLICANT_API int
object_cond_current_value(object_interface* obj_int,
                                            const char* cond, uint64_t* state,
                                            const char** data, size_t* data_sz)
{
    const size_t cond_sz = strlen(cond);
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_COND_CURRENT_VALUE);
    uint32_t o = cond_sz;
    char buf[12];
    e::pack8be(c, buf);
    e::pack32be(o, buf + 1);
    obj_int->write(buf, 5);
    obj_int->write(cond, cond_sz);
    obj_int->read(buf, 1);

    if (buf[0] != 0)
    {
        return -1;
    }

    obj_int->read(buf, 12);
    e::unpack64be(buf, state);
    e::unpack32be(buf + 8, &o);
    char* ptr = static_cast<char*>(malloc(o));

    if (!ptr)
    {
        object_permanent_error(obj_int, "out of memory");
    }

    obj_int->read(ptr, o);
    *data = ptr;
    *data_sz = o;
    return 0;
}

REPLICANT_API void
object_tick_interval(struct object_interface* obj_int,
                     const char* func,
                     uint64_t seconds)
{
    const size_t func_sz = strlen(func);
    uint8_t c = static_cast<uint8_t>(COMMAND_RESPONSE_TICK_INTERVAL);
    uint32_t o = func_sz;
    char buf[8];
    e::pack8be(c, buf);
    e::pack32be(o, buf + 1);
    obj_int->write(buf, 5);
    obj_int->write(func, o);
    e::pack64be(seconds, buf);
    obj_int->write(buf, 8);
}

REPLICANT_API void
object_snapshot(object_interface* obj_int,
                const char* data, size_t data_sz)
{
    uint32_t o = data_sz;
    char buf[4];
    e::pack32be(o, buf);
    obj_int->write(buf, 4);
    obj_int->write(data, o);
}

} // extern "C"
