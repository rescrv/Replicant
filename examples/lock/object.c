/* Copyright (c) 2013-2016, Robert Escriva
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Replicant nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* C */
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* replicant */
#include <rsm.h>

#include "common.h"

void*
malloc_or_abort(size_t sz)
{
    void* tmp = malloc(sz);
    if (!tmp) abort();
    return tmp;
}

struct lock
{
    uint64_t next_seqno;
    struct lock_node* q_head;
    struct lock_node** q_tail;
};

struct lock_node
{
    struct lock_node* next;
    uint64_t seqno;
    char* name;
};

void*
lock_create(struct rsm_context* ctx)
{
    struct lock* lock = malloc_or_abort(sizeof(struct lock));
    rsm_cond_create(ctx, "holder");
    lock->next_seqno = 0;
    lock->q_head = NULL;
    lock->q_tail = &lock->q_head;
    return lock;
}

void*
lock_recreate(struct rsm_context* ctx, const char* data, size_t data_sz)
{
    const char* ptr = data;
    const char* const end = ptr + data_sz;
    struct lock_node* node;
    uint64_t seqno;
    uint64_t size;
    struct lock* lock = malloc_or_abort(sizeof(struct lock));
    if (ptr + sizeof(uint64_t) > end) goto invalid;
    ptr = unpack64be(ptr, &lock->next_seqno);

    while (ptr < end)
    {
        if (ptr + 2 * sizeof(uint64_t) > end) goto invalid;
        ptr = unpack64be(ptr, &seqno);
        ptr = unpack64be(ptr, &size);
        if (ptr + size > end) goto invalid;
        node = malloc_or_abort(sizeof(struct lock_node) + size + 1);
        node->next = NULL;
        node->seqno = seqno;
        node->name = (char*)node + sizeof(struct lock_node);
        memmove(node->name, ptr, size);
        node->name[size] = '\0';
        *lock->q_tail = node;
        lock->q_tail = &node->next;
        ptr += size;
    }

    return NULL;

invalid:
    rsm_log(ctx, "invalid object; cannot restore");
    return NULL;
}

int
lock_snapshot(struct rsm_context* ctx, void* obj, char** data, size_t* data_sz)
{
    struct lock* lock = (struct lock*)obj;
    struct lock_node* node = lock->q_head;
    size_t sz = sizeof(uint64_t);
    size_t len = 0;
    char* buf = NULL;

    while (node)
    {
        sz += sizeof(uint64_t) * 2 + strlen(node->name);
        node = node->next;
    }

    buf = malloc_or_abort(sz);
    *data = buf;
    *data_sz = sz;
    buf = pack64be(lock->next_seqno, buf);
    node = lock->q_head;

    while (node)
    {
        len = strlen(node->name);
        buf = pack64be(node->seqno, buf);
        buf = pack64be(len, buf);
        memmove(buf, node->name, len);
        node = node->next;
    }

    return 0;
}

void
lock_lock(struct rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    char buf[sizeof(uint64_t)];
    struct lock* lock = (struct lock*)obj;
    struct lock_node* node = malloc_or_abort(sizeof(struct lock_node) + data_sz + 1);
    /* initialize node */
    node->seqno = lock->next_seqno;
    ++lock->next_seqno;
    node->name = (char*)node + sizeof(struct lock_node);
    memmove(node->name, data, data_sz);
    node->name[data_sz] = '\0';
    /* enqueue */
    *lock->q_tail = node;
    lock->q_tail = &node->next;
    /* respond */
    pack64be(node->seqno, buf);
    rsm_set_output(ctx, buf, sizeof(uint64_t));

    if (lock->q_head == node)
    {
        rsm_log(ctx, "[%d] \"%s\" acquires lock\n", node->seqno, node->name);
    }
    else
    {
        rsm_log(ctx, "[%d] \"%s\" waits for lock\n", node->seqno, node->name);
    }
}

void
lock_unlock(struct rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    struct lock* lock = (struct lock*)obj;
    struct lock_node* node = NULL;
    uint64_t seqno = 0;
    uint64_t diff = 0;
    uint64_t i = 0;
    int ret = 0;

    if (data_sz < sizeof(uint64_t))
    {
        rsm_log(ctx, "error: invalid unlock request\n");
        return;
    }

    unpack64be(data, &seqno);
    data += sizeof(uint64_t);
    data_sz -= sizeof(uint64_t);

    if (!lock->q_head)
    {
        rsm_log(ctx, "error: cannot unlock an unlocked lock\n");
    }
    else if (seqno < lock->q_head->seqno)
    {
        rsm_log(ctx, "info: double-unlock for %d\n", seqno);
    }
    else if (seqno > lock->q_head->seqno)
    {
        rsm_log(ctx, "warning: out-of-order unlock for %d\n", seqno);
    }
    else if (data_sz < strlen(lock->q_head->name) ||
             strncmp(data, lock->q_head->name, data_sz) != 0)
    {
        rsm_log(ctx, "error: invalid name for unlock\n");
    }
    else
    {
        rsm_log(ctx, "[%d] \"%s\" releases lock\n", lock->q_head->seqno, lock->q_head->name);
        node = lock->q_head;
        lock->q_head = lock->q_head->next;
        diff = (lock->q_head ? lock->q_head->seqno : lock->next_seqno) - node->seqno;
        free(node);

        if (lock->q_head == NULL)
        {
            lock->q_tail = &lock->q_head;
        }
        else
        {
            rsm_log(ctx, "[%d] \"%s\" acquires lock\n", lock->q_head->seqno, lock->q_head->name);
        }

        for (i = 0; i < diff; ++i)
        {
            ret = rsm_cond_broadcast(ctx, "holder");
            assert(ret == 0);
        }
    }
}

struct state_machine rsm = {
    lock_create,
    lock_recreate,
    lock_snapshot,
    {{"lock", lock_lock},
     {"unlock", lock_unlock},
     {NULL, NULL}}
};
