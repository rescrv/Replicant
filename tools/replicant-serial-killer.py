import paramiko
import argparse
import random
import time

class Remote(object):
    def __init__(self,host,user,path,coord=None,coordport=None):
        print("Connecting to %s " % host)
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
    
        # hack to get the exact pid of the process rather than grep
        if not coord:
            cmd = "echo \"from subprocess import Popen; \
                    print Popen(['%s/replicant-daemon','-f','&']).pid;\" \
                    | python" % path 
        else:
            cmd = "echo \"from subprocess import Popen; \
                    print Popen(['%s/replicant-daemon','-f','-c %s','-P %s','&']).pid;\" \
                    | python" % (path, self.coordip, self.coordport)
        self.stdin, self.stdout, self.stderr = self.ssh.exec_command(cmd)
        self.pid = int(self.stdout.readline())

    def pause(self):
        self.ssh.exec_command('kill -SIGSTOP %d' % self.pid)

    def resume(self):
        self.ssh.exec_command('kill -SIGCONT %d' % self.pid)

    def kill(self):
        self.ssh.exec_command('kill -SIGKILL%d' % self.pid)


parser = argparse.ArgumentParser(description='Start a cluster and kill random nodes.')
parser.add_argument('--hosts', metavar='HOST', nargs='*',
                   help='a list of hosts',default = ['127.0.0.1:1982','127.0.0.2:1982'])
parser.add_argument('--user', metavar='NAME', nargs='?',
                   help='ssh username',default = 'roybatty')
parser.add_argument('--path', metavar='/path/to/replicant', nargs='?',
                   help='path to the replicant-daemon binary',default = '${HOME}/replicant')
parser.add_argument('--interval', metavar='I', nargs='?',
                   help='time between killing nodes',default = 10)
parser.add_argument('--duration', metavar='D', nargs='?',
                   help='duration of sigstop-sigcont',default = 0)



args = parser.parse_args()
user = args.user
path = args.path
interval = args.interval
duration = args.duration

print(user)
print(args.hosts)

remotes = list()
for h in args.hosts:
    if(len(remotes)>0):
        remotes.append(Remote(h,user,path,remotes[0].host,remotes[0].port))
    else:
        remotes.append(Remote(h,user,path))

while True:
    time.sleep(interval)
    r = random.choice(remotes)
    print("Killing %s for %d seconds" % (r.host, duration))
    r.pause()
    time.sleep(duration)
    print("Resuming %s" % r.host)
    r.resume()

