/* Copyright (c) 2016, Robert Escriva
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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/* replicant */
#include <replicant.h>

#define REPLICANT_FINISH
#include "common.h"

int
main(int argc, char* argv[])
{
    int opt;
    const char* connect = "127.0.0.1:1982";
    struct replicant_client* repl = NULL;
    int64_t id;
    enum replicant_returncode rc;
    char* input = NULL;
    size_t input_sz = 0;
    uint64_t seqno = 0;

    while ((opt = getopt(argc, argv, "c:")) != -1)
    {
        switch (opt)
        {
            case 'c':
                connect = strdup(optarg);
                break;
            default:
                goto usage;
        }
    }

    if (optind + 2 != argc)
    {
        fprintf(stderr, "error: incorrect number of arguments\n\n");
        goto usage;
    }

    repl = replicant_client_create_conn_str(connect);

    if (!repl)
    {
        fprintf(stderr, "error: could not create replicant client\n");
        return EXIT_FAILURE;
    }

    seqno = atoi(argv[optind]);
    input_sz = sizeof(uint64_t) + strlen(argv[optind + 1]) + 1;
    input = malloc(input_sz);

    if (!input)
    {
        fprintf(stderr, "out of memory\n");
        return EXIT_FAILURE;
    }

    pack64be(seqno, input);
    strcpy(input + sizeof(uint64_t), argv[optind + 1]);
    id = replicant_client_call(repl, "lock", "unlock", input, input_sz,
                               REPLICANT_CALL_ROBUST, &rc, NULL, NULL);
    replicant_finish(repl, id, &rc);
    return EXIT_SUCCESS;

usage:
    fprintf(stderr, "usage: %s [-c connect-string] lock-holder-name\n", argv[0]);
    exit(EXIT_FAILURE);
}
