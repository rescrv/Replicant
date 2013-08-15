# Copyright (c) 2013, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Replicant nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement


import collections
import os
import os.path
import random
import shutil
import subprocess
import sys
import tempfile
import time

import argparse


PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


class ReplicantCluster(object):

    def __init__(self, replicants, clean=False, base=None):
        self.processes = []
        self.replicants = replicants
        self.clean = clean
        self.base = base

    def setup(self):
        if self.base is None:
            self.base = tempfile.mkdtemp(prefix='replicant-test-')
        env = {'GLOG_logtostderr': '',
               'GLOG_minloglevel': '0',
               'REPLICANT_EXEC_PATH': PATH,
               'PATH': ((os.getenv('PATH') or '') + ':' + PATH).strip(':')}
        for i in range(self.replicants):
            cmd = ['/usr/bin/env', 'replicant', 'daemon',
                   '--foreground', '--listen', 'localhost', '--listen-port', str(1982 + i)]
            if i > 0:
                cmd += ['--connect', 'localhost', '--connect-port', '1982']
            cwd = os.path.join(self.base, 'daemon%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'replicant-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)

    def cleanup(self):
        for p in self.processes:
            p.kill()
            p.wait()
        if self.clean and self.base is not None:
            shutil.rmtree(self.base)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--replicants', default=1, type=int)
    parser.add_argument('args', nargs='*')
    args = parser.parse_args(argv)
    rc = ReplicantCluster(args.replicants)
    try:
        rc.setup()
        time.sleep(1) # XXX use a barrier tool on cluster
        ctx = {'PATH': PATH, 'HOST': 'localhost', 'PORT': 1982}
        cmd_args = [arg.format(**ctx) for arg in args.args]
        return subprocess.call(cmd_args)
    finally:
        rc.cleanup()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
