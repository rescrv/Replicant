// Copyright (c) 2012-2015, Robert Escriva
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// STL
#include <vector>

// e
#include <e/subcommand.h>

int
main(int argc, const char* argv[])
{
    std::vector<e::subcommand> cmds;
    cmds.push_back(e::subcommand("daemon",            "Start a new Replicant daemon"));
    cmds.push_back(e::subcommand("new-object",        "Create a new replicated object"));
    cmds.push_back(e::subcommand("del-object",        "Destroy an existing replicated object"));
    cmds.push_back(e::subcommand("kill-object",       "Kill an existing object so it will be restarted"));
    cmds.push_back(e::subcommand("backup-object",     "Create a backup of a replicated object"));
    cmds.push_back(e::subcommand("restore-object",    "Restore a replicated object from backup"));
    cmds.push_back(e::subcommand("list-objects",      "List all objects hosted by the cluster"));
    cmds.push_back(e::subcommand("poke",              "Poke the cluster to test for liveness"));
    cmds.push_back(e::subcommand("conn-str",          "Output a connection string for the current cluster"));
    cmds.push_back(e::subcommand("kill-server",       "Remove a server from the cluster"));
    cmds.push_back(e::subcommand("server-status",     "Directly check the status of a server"));
    cmds.push_back(e::subcommand("availability-check","Check if the cluster consists of N or more servers"));
    cmds.push_back(e::subcommand("generate-unique-number", "Generate a unique number, using the cluster to guarantee its uniqueness"));
    cmds.push_back(e::subcommand("debug",             "Debug tools for replicant developers"));
    return dispatch_to_subcommands(argc, argv,
                                   "replicant", "Replicant",
                                   PACKAGE_VERSION,
                                   "replicant-",
                                   "REPLICANT_EXEC_PATH", REPLICANT_EXEC_DIR,
                                   &cmds.front(), cmds.size());
}
