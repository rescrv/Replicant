import paramiko
import argparse
import random
import time
import getpass

class Remote(object):
    def __init__(self,host,user,path,coord=None,coordport=None):
        print("Connecting to %s " % host)
        self.path = path
        self.host, self.port = str.split(host,':')
        self.coordip = coord
        self.coordport = coordport
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(self.host, username=user)
        except:
            print("Unable to connect to host %s" % host)
            exit()
        self.start()

    def pause(self):
        self.ssh.exec_command('kill -SIGSTOP %d' % self.pid)

    def resume(self):
        self.ssh.exec_command('kill -SIGCONT %d' % self.pid)

    def kill(self):
        self.ssh.exec_command('kill -SIGKILL %d' % self.pid)

    def start(self):
        # hack to get the exact pid of the process rather than grep
        if not self.coordip:
            cmd = "%s/replicant-daemon -l %s -p %s & echo $!" % \
                     (self.path,self.host,self.port)
        else:
            cmd = "%s/replicant-daemon -l %s -p %s -c %s -P %s & echo $!" % \
                     (self.path,self.host,self.port,self.coordip,self.coordport)
        print(cmd)
        self.stdin, self.stdout, self.stderr = self.ssh.exec_command(cmd)
        self.stdout.flush()
        self.pid = int(self.stdout.read()) + 1
        print(self.pid)

def choose_n(replist,N):
    chosen = set()
    while len(chosen)<N:
        chosen.add(random.choice(replist))
    return chosen


parser = argparse.ArgumentParser(description='Start a cluster and kill random nodes.')
parser.add_argument('--hosts', metavar='HOST', nargs='*',
                   help='a list of hosts',default = ['127.0.0.1:1982','127.0.0.1:1983'])
parser.add_argument('--user', metavar='NAME', nargs='?',
                   help='ssh username',default = getpass.getuser())
parser.add_argument('--path', metavar='/path/to/replicant', nargs='?',
                   help='path to the replicant-daemon binary',default = '${HOME}/replicant')
parser.add_argument('--interval', metavar='I', nargs='?',
                   help='time between killing nodes',default = 10, type=int)
parser.add_argument('--duration', metavar='D', nargs='?',
                   help='duration of sigstop-sigcont',default = 0,type=int)
parser.add_argument('--count', metavar='N', nargs='?',
                   help='duration of sigstop-sigcont',default = 1, type=int)
parser.add_argument('--sigkill', 
                   help='use sigkill instead of sigstop',action='store_true',default = False)





args = parser.parse_args()
user = args.user
path = args.path
interval = args.interval
duration = args.duration
count = args.count
sigkill = args.sigkill

print(user)
print(args.hosts)
if sigkill:
    print("Using sigkill.")
print("Killing %d nodes for %d seconds every %d seconds." % (count,duration,interval))

remotes = list()
for h in args.hosts:
    if(len(remotes)>0):
        remotes.append(Remote(h,user,path,remotes[0].host,remotes[0].port))
    else:
        remotes.append(Remote(h,user,path))
    time.sleep(3)


while True:
    time.sleep(interval)
    rep = choose_n(remotes,count)
    for r in rep:
        print("Killing %s for %d seconds" % (r.host, duration))
        if sigkill:
            r.kill()
        else:
            r.pause()

    time.sleep(duration)

    for r in rep:
        print("Resuming %s" % r.host)
        if sigkill:
            r.start()
        else:
            r.resume()

