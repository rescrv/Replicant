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

#ifndef replicant_object_manager_h_
#define replicant_object_manager_h_

// C
#include <stdint.h>

// STL
#include <map>

// po6
#include <po6/io/fd.h>
#include <po6/pathname.h>

// e
#include <e/intrusive_ptr.h>

// Replicant
#include "daemon/command.h"

#include <tr1/memory>

//PO6
#include <po6/threads/thread.h>

class replicant_daemon;
class object_manager
{
    public:
        class snapshot
        {
            public:
                snapshot();
                ~snapshot() throw ();

            private:
                friend class object_manager;

            private:
                std::vector<char*> m_backings;
        };

    public:
        object_manager();
        ~object_manager() throw ();

    public:
        bool exists(uint64_t object) const;
        bool valid_path(const e::slice& pathstr) const;

    public:
        void apply(e::intrusive_ptr<command> cmd);
        void append_cmd(e::intrusive_ptr<command> cmd);
        bool create(uint64_t object, const e::slice& path, replicant_daemon* daemon);
        bool restore(uint64_t object, const e::slice& path, const e::slice& snapshot);
        void take_snapshot(snapshot* snap,
                           std::vector<std::pair<uint64_t, std::pair<e::slice, e::slice> > >* objects);

    private:
        class object;
        typedef std::map<uint64_t, e::intrusive_ptr<object> > object_map;

    private:
        e::intrusive_ptr<object> open_library(const e::slice& path);

    private:
        typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;
        object_map m_objects;
        std::vector<thread_ptr> m_threads;
};

e::buffer::packer
operator << (e::buffer::packer lhs, const object_manager& rhs);

e::buffer::unpacker
operator >> (e::buffer::unpacker lhs, object_manager& rhs);

size_t
pack_size(const object_manager& rhs);

#endif // replicant_object_manager_h_
