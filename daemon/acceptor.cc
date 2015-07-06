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
#include <aio.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/io/fd.h>
#include <po6/io/mmap.h>
#include <po6/net/hostname.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/guard.h>

// Replicant
#include "daemon/acceptor.h"

using replicant::acceptor;

struct acceptor::log_segment
{
    log_segment();
    ~log_segment() throw ();

    bool open(int fd, uint64_t lognum);

    bool write_ballot(const ballot& b);
    bool write_pval(const pvalue& pval);
    bool write_gc(uint64_t below);
    bool write(std::auto_ptr<e::buffer> buf);

    bool all_synced();
    void maybe_sync(uint64_t opnum);
    uint64_t sync_cut();

    uint64_t lognum;
    uint64_t written;
    po6::io::fd fd;
    bool permafail;
    uint64_t synced;
    uint64_t sync_op;
    bool sync_in_progress;
    struct aiocb afsync;
    uint64_t in_progress_synced;
    uint64_t in_progress_sync_op;

    private:
        log_segment(const log_segment&);
        log_segment& operator = (const log_segment&);
};

acceptor :: log_segment :: log_segment()
    : lognum()
    , written(0)
    , fd()
    , permafail(false)
    , synced(0)
    , sync_op(0)
    , sync_in_progress(false)
    , afsync()
    , in_progress_synced(0)
    , in_progress_sync_op(0)
{
}

acceptor :: log_segment :: ~log_segment() throw ()
{
}

bool
acceptor :: log_segment :: open(int dir, uint64_t s)
{
    lognum = s;
    std::ostringstream ostr;
    ostr << "log." << s;
    fd = ::openat(dir, ostr.str().c_str(), O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
    return fd.get() >= 0;
}

bool
acceptor :: log_segment :: write_ballot(const ballot& b)
{
    std::auto_ptr<e::buffer> buf(e::buffer::create(1 + pack_size(b)));
    buf->pack_at(0) << uint8_t('A') << b;
    return write(buf);
}

bool
acceptor :: log_segment :: write_pval(const pvalue& pval)
{
    std::auto_ptr<e::buffer> buf(e::buffer::create(1 + pack_size(pval)));
    buf->pack_at(0) << uint8_t('B') << pval;
    return write(buf);
}

bool
acceptor :: log_segment :: write_gc(uint64_t below)
{
    std::auto_ptr<e::buffer> buf(e::buffer::create(1 + sizeof(uint64_t)));
    buf->pack_at(0) << uint8_t('G') << below;
    return write(buf);
}

bool
acceptor :: log_segment :: write(std::auto_ptr<e::buffer> buf)
{
    written += buf->size();

    if (fd.xwrite(buf->data(), buf->size()) != ssize_t(buf->size()))
    {
        permafail = true;
        return false;
    }

    return true;
}

bool
acceptor :: log_segment :: all_synced()
{
    return !permafail && written == synced;
}

void
acceptor :: log_segment :: maybe_sync(uint64_t opnum)
{
    if (permafail)
    {
        return;
    }

    if (sync_in_progress && aio_error(&afsync) == EINPROGRESS)
    {
        return;
    }

    sync_in_progress = false;

    if (aio_return(&afsync) != 0)
    {
        LOG(ERROR) << "acceptor failing permanently: " << e::error::strerror(errno);
        permafail = true;
        return;
    }

    synced = in_progress_synced;
    sync_op = in_progress_sync_op;

    if (written <= synced)
    {
        return;
    }

    memset(&afsync, 0, sizeof(afsync));
    afsync.aio_fildes = fd.get();
    afsync.aio_sigevent.sigev_notify = SIGEV_NONE;
    int ret = aio_fsync(O_SYNC, &afsync);

    if (ret != 0)
    {
        permafail = true;
        return;
    }

    sync_in_progress = true;
    in_progress_synced = written;
    in_progress_sync_op = opnum;
}

uint64_t
acceptor :: log_segment :: sync_cut()
{
    return sync_op;
}

class acceptor::garbage_collector
{
    public:
        garbage_collector(acceptor* a);
        ~garbage_collector() throw ();

    public:
        void gc(uint64_t lognum, uint64_t slot);
        void kill();
        void run();
        void collect(const uint64_t lognum, const uint64_t below);

    private:
        acceptor* const m_acceptor;
        po6::threads::thread m_thread;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cnd;
        uint64_t m_below_lognum;
        uint64_t m_below_slot;
        bool m_killed;

    private:
        garbage_collector(const garbage_collector&);
        garbage_collector& operator = (const garbage_collector&);
};

acceptor :: garbage_collector :: garbage_collector(acceptor* a)
    : m_acceptor(a)
    , m_thread(po6::threads::make_thread_wrapper(&garbage_collector::run, this))
    , m_mtx()
    , m_cnd(&m_mtx)
    , m_below_lognum(0)
    , m_below_slot(0)
    , m_killed(false)
{
    m_thread.start();
}

acceptor :: garbage_collector :: ~garbage_collector() throw ()
{
    m_thread.join();
}

void
acceptor :: garbage_collector :: gc(uint64_t lognum, uint64_t slot)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_below_lognum = lognum;
    m_below_slot = slot;
    m_cnd.signal();
}

void
acceptor :: garbage_collector :: kill()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_killed = true;
    m_cnd.signal();
}

void
acceptor :: garbage_collector :: run()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    m_mtx.lock();
    uint64_t gced = 0;

    while (true)
    {
        while (gced >= m_below_slot && !m_killed)
        {
            m_cnd.wait();
        }

        if (m_killed)
        {
            break;
        }

        uint64_t lognum = m_below_lognum;
        uint64_t below = m_below_slot;
        m_mtx.unlock();
        collect(lognum, below);
        m_mtx.lock();
        gced = below;
    }

    m_mtx.unlock();
}

void
acceptor :: garbage_collector :: collect(const uint64_t lognum, const uint64_t below)
{
    std::vector<uint64_t> lognums;
    std::vector<uint64_t> replicas;
    DIR* d = opendir(".");

    if (!d)
    {
        return;
    }

    e::guard g_d = e::makeguard(closedir, d);
    struct dirent* de = NULL;
    errno = 0;

    while ((de = readdir(d)))
    {
        if (strncmp(de->d_name, "log.", 4) == 0)
        {
            char* end = NULL;
            uint64_t log = strtoull(de->d_name + 4, &end, 10);

            if (*end == '\0')
            {
                lognums.push_back(log);
            }
        }

        if (strncmp(de->d_name, "replica.", 8) == 0)
        {
            char* end = NULL;
            uint64_t replica = strtoull(de->d_name + 8, &end, 10);

            if (*end == '\0')
            {
                replicas.push_back(replica);
            }
        }
    }

    std::sort(lognums.begin(), lognums.end());
    std::sort(replicas.begin(), replicas.end());

    for (size_t i = 0; i + 2 < lognums.size(); ++i)
    {
        if (lognums[i] >= lognum)
        {
            break;
        }

        ballot ballot;
        std::vector<pvalue> pvals;
        uint64_t lowest_acceptable_slot;

        if (!replay_log(m_acceptor->m_dir.get(), lognums[i], &ballot, &pvals, &lowest_acceptable_slot))
        {
            return;
        }

        uint64_t highest_slot = 0;

        for (size_t p = 0; p < pvals.size(); ++p)
        {
            highest_slot = std::max(highest_slot, pvals[p].s);
        }

        if (highest_slot >= below)
        {
            break;
        }

        std::ostringstream ostr;
        ostr << "log." << lognums[i];

        if (unlink(ostr.str().c_str()) < 0)
        {
            return;
        }
    }

    for (size_t i = 0; i + 5 < replicas.size(); ++i)
    {
        if (replicas[i] >= below)
        {
            break;
        }

        std::ostringstream ostr;
        ostr << "replica." << replicas[i];

        if (unlink(ostr.str().c_str()) < 0)
        {
            return;
        }
    }
}

acceptor :: acceptor()
    : m_ballot()
    , m_pvals()
    , m_lowest_acceptable_slot(0)
    , m_dir()
    , m_lock()
    , m_opcount(0)
    , m_permafail(true)
    , m_current(new log_segment())
    , m_previous()
    , m_gc(new garbage_collector(this))
{
}

acceptor :: ~acceptor() throw ()
{
    m_gc->kill();
}

bool
acceptor :: open(const po6::pathname& dir,
                 bool* saved, server* saved_us,
                 bootstrap* saved_bootstrap)
{
    struct stat stbuf;
    int ret = stat(dir.get(), &stbuf);

    if (ret < 0 && errno == ENOENT)
    {
        if (mkdir(dir.get(), S_IRWXU) < 0)
        {
            LOG(ERROR) << "could not create data directory: " << e::error::strerror(errno);
            return false;
        }

        ret = stat(dir.get(), &stbuf);
    }

    if (ret < 0)
    {
        LOG(ERROR) << "could not initialize data directory: " << e::error::strerror(errno);
        return false;
    }
    else if (!S_ISDIR(stbuf.st_mode))
    {
        LOG(ERROR) << "the data directory is not, in fact, a directory";
        return false;
    }

    if (chdir(dir.get()) < 0)
    {
        LOG(ERROR) << "could not initialize data directory: " << e::error::strerror(errno);
        return false;
    }

    m_dir = ::open(".", O_RDONLY);

    if (m_dir.get() < 0)
    {
        LOG(ERROR) << "could not initialize data directory: " << e::error::strerror(errno);
        return false;
    }

    m_lock = openat(m_dir.get(), "LOCK", O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);

    if (m_lock.get() < 0)
    {
        LOG(ERROR) << "could not create lock file: " << e::error::strerror(errno);
        return false;
    }

    struct flock f;
    memset(&f, 0, sizeof(f));
    f.l_type = F_WRLCK;
    f.l_whence = SEEK_SET;
    f.l_start = 0;
    f.l_len = 0;

    if (fcntl(m_lock.get(), F_SETLK, &f) < 0)
    {
        LOG(ERROR) << "could not lock data directory";
        return false;
    }

    std::string ident;
    *saved = true;

    if (!atomic_read("IDENTITY", &ident))
    {
        if (errno != ENOENT)
        {
            LOG(INFO) << "could not read identity: " << e::error::strerror(errno);
            return false;
        }

        *saved = false;
    }

    if (*saved && !parse_identity(ident, saved_us, saved_bootstrap))
    {
        LOG(INFO) << "could not read pre-existing server identity on disk";
        return false;
    }

    std::vector<uint64_t> lognums;
    DIR* d = opendir(".");

    if (!d)
    {
        LOG(ERROR) << "error reading acceptor state from disk";
        return false;
    }

    e::guard g_d = e::makeguard(closedir, d);
    struct dirent* de = NULL;
    errno = 0;

    while ((de = readdir(d)))
    {
        if (strncmp(de->d_name, "log.", 4) == 0)
        {
            char* end = NULL;
            uint64_t log = strtoull(de->d_name + 4, &end, 10);

            if (*end == '\0')
            {
                lognums.push_back(log);
            }
        }
    }

    std::sort(lognums.begin(), lognums.end());

    for (size_t i = 0; i < lognums.size(); ++i)
    {
        if (!replay_log(m_dir.get(), lognums[i], &m_ballot, &m_pvals, &m_lowest_acceptable_slot))
        {
            LOG(ERROR) << "error reading acceptor state from disk";
            return false;
        }

        pvals();
    }

    if (!m_current->open(m_dir.get(), !lognums.empty() ? lognums.back() + 1 : 0))
    {
        LOG(ERROR) << "could not open persistent log";
        return false;
    }

    m_permafail = false;
    return true;
}

bool
acceptor :: save(server saved_us,
                 const bootstrap& saved_bootstrap)
{
    std::ostringstream ostr;
    ostr << saved_us << "\n" << saved_bootstrap << "\n";

    if (!atomic_write("IDENTITY", ostr.str()))
    {
        LOG(ERROR) << "could not write identity to disk: " << e::error::strerror(errno);
        return false;
    }

    return true;
}

static bool
compare_pvalue_slot_then_highest_ballot(const replicant::pvalue& lhs, const replicant::pvalue& rhs)
{
    if (lhs.s < rhs.s)
    {
        return true;
    }
    else if (lhs.s == rhs.s)
    {
        return lhs.b > rhs.b;
    }
    else
    {
        return false;
    }
}

const std::vector<replicant::pvalue>&
acceptor :: pvals()
{
    std::vector<pvalue> tmp;
    std::sort(m_pvals.begin(), m_pvals.end(), compare_pvalue_slot_then_highest_ballot);
    size_t idx = 0;

    while (idx < m_pvals.size() && m_pvals[idx].s < m_lowest_acceptable_slot)
    {
        ++idx;
    }

    while (idx < m_pvals.size())
    {
        tmp.push_back(m_pvals[idx]);

        while (idx < m_pvals.size() && m_pvals[idx].s == tmp.back().s)
        {
            ++idx;
        }
    }

    m_pvals.swap(tmp);
    return m_pvals;
}

void
acceptor :: adopt(const ballot& b)
{
    assert(!m_permafail);
    assert(b > m_ballot);
    log_segment* log = get_writable_log();

    if (log && log->write_ballot(b))
    {
        m_ballot = b;
        log->maybe_sync(++m_opcount);
    }
    else
    {
        m_permafail = true;
    }
}

void
acceptor :: accept(const pvalue& pval)
{
    assert(!m_permafail);
    assert(pval.b == m_ballot);
    log_segment* log = get_writable_log();

    if (log && log->write_pval(pval))
    {
        m_pvals.push_back(pval);
        log->maybe_sync(++m_opcount);
    }
    else
    {
        m_permafail = true;
    }
}

void
acceptor :: garbage_collect(uint64_t below)
{
    assert(!m_permafail);
    below = std::max(m_lowest_acceptable_slot, below);
    log_segment* log = get_writable_log();

    if (log && log->write_gc(below))
    {
        m_lowest_acceptable_slot = below;
        log->maybe_sync(++m_opcount);
        uint64_t lognum = log->lognum;

        if (m_previous.get())
        {
            lognum = std::min(lognum, m_previous->lognum);
        }

        m_gc->gc(lognum, below);
        pvals();
    }
    else
    {
        m_permafail = true;
    }
}

uint64_t
acceptor :: sync_cut()
{
    if (m_previous.get())
    {
        m_previous->maybe_sync(++m_opcount);

        if (m_previous->all_synced())
        {
            m_previous.reset();
        }
        else
        {
            return m_previous->sync_cut();
        }
    }

    assert(m_current.get());
    m_current->maybe_sync(++m_opcount);
    return m_current->sync_cut();
}

bool
acceptor :: record_snapshot(uint64_t slot, const e::slice& snapshot)
{
    std::ostringstream ostr;
    ostr << "replica." << slot;
    std::string tmp(snapshot.cdata(), snapshot.size());
    return atomic_write(ostr.str().c_str(), tmp);
}

bool
acceptor :: load_latest_snapshot(e::slice* snapshot,
                                 std::auto_ptr<e::buffer>* snapshot_backing)
{
    uint64_t max_replica = 0;
    std::string path;
    DIR* d = opendir(".");

    if (!d)
    {
        LOG(ERROR) << "error reading acceptor state from disk";
        return false;
    }

    e::guard g_d = e::makeguard(closedir, d);
    struct dirent* de = NULL;
    errno = 0;

    while ((de = readdir(d)))
    {
        if (strncmp(de->d_name, "replica.", 8) == 0)
        {
            char* end = NULL;
            uint64_t replica = strtoull(de->d_name + 8, &end, 10);

            if (*end == '\0')
            {
                if (replica > max_replica || max_replica == 0)
                {
                    max_replica = replica;
                    path = de->d_name;
                }
            }
        }
    }

    std::string replica;

    if (!atomic_read(path.c_str(), &replica))
    {
        return false;
    }

    snapshot_backing->reset(e::buffer::create(replica.size()));
    (*snapshot_backing)->resize(replica.size());
    memmove((*snapshot_backing)->data(), replica.data(), replica.size());
    *snapshot = (*snapshot_backing)->as_slice();
    return true;
}

bool
acceptor :: atomic_read(const char* path, std::string* contents)
{
    contents->clear();
    po6::io::fd fd(openat(m_dir.get(), path, O_RDONLY));

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
acceptor :: atomic_write(const char* path, const std::string& contents)
{
    po6::io::fd fd(openat(m_dir.get(), ".atomic.tmp", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));
    return fd.get() >= 0 &&
           fd.xwrite(contents.data(), contents.size()) == ssize_t(contents.size()) &&
           fsync(fd.get()) >= 0 &&
           fsync(m_dir.get()) >= 0 &&
           renameat(m_dir.get(), ".atomic.tmp", m_dir.get(), path) >= 0 &&
           fsync(m_dir.get()) >= 0;
}

bool
acceptor :: parse_identity(const std::string& ident,
                           server* saved_us, bootstrap* saved_bootstrap)
{
    const char* ptr = ident.c_str();
    const char* const end = ptr + ident.size();

    if (strncmp(ptr, "server(id=", 10) != 0)
    {
        LOG(ERROR) << "bad server in stored identity";
        return false;
    }

    ptr += 10;
    char* comma;
    const uint64_t id = strtoull(ptr, &comma, 10);

    if (*comma != ',')
    {
        LOG(ERROR) << "bad server in stored identity";
        return false;
    }

    ptr = comma;
    assert(ptr <= end);

    if (strncmp(ptr, ", bind_to=", 10) != 0)
    {
        LOG(ERROR) << "bad server in stored identity";
        return false;
    }

    ptr += 10;
    const char* eol = strchr(ptr, '\n');
    const char* paren = strchr(ptr, ')');

    if (!paren || !eol || paren + 1 != eol)
    {
        LOG(ERROR) << "bad server in stored identity";
        return false;
    }

    std::string bind_to_str(ptr, paren - ptr);
    std::vector<po6::net::hostname> hns;

    if (!bootstrap::parse_hosts(bind_to_str.c_str(), &hns) || hns.size() != 1)
    {
        LOG(ERROR) << "bad server in stored identity";
        return false;
    }

    po6::net::location bind_to = hns[0].lookup(AF_UNSPEC, IPPROTO_TCP);

    if (eol + 1 > end)
    {
        LOG(ERROR) << "bad server in stored identity";
        return false;
    }

    *saved_us = server(server_id(id), bind_to);
    *saved_bootstrap = bootstrap(eol + 1);
    return saved_bootstrap->valid();
}

acceptor::log_segment*
acceptor :: get_writable_log()
{
    if (m_permafail)
    {
        return NULL;
    }

    if (m_current->permafail)
    {
        m_permafail = true;
        return NULL;
    }

    if (m_previous.get())
    {
        m_previous->maybe_sync(++m_opcount);

        if (m_previous->all_synced())
        {
            m_previous.reset();
        }
        else if (m_previous->permafail)
        {
            m_permafail = true;
            return NULL;
        }
    }

    if (m_current->written >= 1ULL << 26 && !m_previous.get())
    {
        std::auto_ptr<log_segment> next(new log_segment());

        if (!next->open(m_dir.get(), m_current->lognum + 1))
        {
            LOG(ERROR) << "acceptor failing permanently while creating new log file: " << e::error::strerror(errno);
            m_permafail = true;
            return NULL;
        }

        m_previous = m_current;
        m_previous->maybe_sync(++m_opcount);
        m_current = next;
    }

    return m_current.get();
}

bool
acceptor :: replay_log(int dir,
                       uint64_t lognum,
                       ballot* highest_ballot,
                       std::vector<pvalue>* pvals,
                       uint64_t* lowest_acceptable_slot)
{
    std::ostringstream ostr;
    ostr << "log." << lognum;
    po6::io::fd fd(openat(dir, ostr.str().c_str(), O_RDONLY));

    if (fd.get() < 0)
    {
        return false;
    }

    struct stat st;

    if (fstat(fd.get(), &st) < 0)
    {
        return false;
    }

    if (st.st_size == 0)
    {
        return true;
    }

    po6::io::mmap map(NULL, st.st_size, PROT_READ, MAP_SHARED, fd.get(), 0);

    if (!map.valid())
    {
        errno = map.error();
        return false;
    }

    e::unpacker up(static_cast<const char*>(map.get()), st.st_size);

    while (up.remain() && !up.error())
    {
        uint8_t t;
        up = up >> t;

        if (up.error())
        {
            LOG(ERROR) << "log errneously truncated";
            return false;
        }

        if (t == 'A')
        {
            ballot b;
            up = up >> b;

            if (!up.error())
            {
                *highest_ballot = std::max(*highest_ballot, b);
            }
        }

        if (t == 'B')
        {
            pvalue pval;
            up = up >> pval;

            if (!up.error())
            {
                *highest_ballot = std::max(*highest_ballot, pval.b);
                pvals->push_back(pval);
            }
        }

        if (t == 'G')
        {
            uint64_t slot;
            up = up >> slot;

            if (!up.error())
            {
                *lowest_acceptable_slot = std::max(*lowest_acceptable_slot, slot);
            }
        }
    }

    if (up.error() || up.remain())
    {
        LOG(ERROR) << "log errneously truncated";
        return false;
    }

    return true;
}
