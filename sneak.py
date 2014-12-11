#!/usr/bin/python

import os, sys, getopt, pxssh, pexpect, socket, datetime, Queue, threading
sneak_err = 'sneak.py -i <hosts> -j <jobs> -k <project>'

exitFlag = 0
jobs_lock = threading.Lock()


# read i-th line of file
def readline(file, i):
    j = 0
    for line in open(file, 'r'):
        j = j + 1
        if (j==i):
            return line.strip()
    if (i>j):
        return "line number error"


# assign the job to host
def assign_job(jobs_queue, host):
    res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
    if (res == 0):
        jobs_lock.acquire()
        #update jobs_queue
        if not jobs_queue.empty():
            try:
                job = jobs_queue.get()
                jobs_lock.release()
                s = pxssh.pxssh()
                s.login (host[0], host[1], host[2])
                s.sendline("echo '#!/usr/bin/python' > run.py")
                s.prompt()
                s.sendline("echo 'import os, socket' >> run.py")
                s.prompt()
                s.sendline("echo 'os.system(\"" + job + "\")' >> run.py")
                s.prompt()
                s.sendline("echo 'HOST = \"150.203.210.120\"' >> run.py")
                s.prompt()
                s.sendline("echo 'PORT = 8000' >> run.py")
                s.prompt()
                s.sendline("echo 's = socket.socket(socket.AF_INET, socket.SOCK_STREAM)' >> run.py")
                s.prompt()
                s.sendline("echo 's.connect((HOST, PORT))' >> run.py")
                s.prompt()
                s.sendline("echo 's.sendall(\"Hello, world\")' >> run.py")
                s.prompt()
                s.sendline("echo 's.close()' >> run.py")
                s.prompt()
                s.sendline("nohup " +  + " & > pid")
                s.prompt()
                #print s.before
                pid = s.before.split()
                pid = pid[len(pid)-1]
                #print pid
                now = datetime.datetime.now()
                os.system("echo " + job + " " + host[0] + " " + host[1] + " " + host[2] + " " + pid + " %d:%d:%d:" % (now.year, now.month, now.day) + "%d:%d:%d" % (now.hour, now.minute, now.second) + " Running >> .log")
                s.logout()
            except pxssh.ExceptionPxssh, e:
                print "pxssh failed on login."
                print str(e)


class rThread (threading.Thread):
    def __init__(self, jobs_queue):
        self.jobs_queue = jobs_queue
    def run(self):
        while True:
            print "sth"
            # listen to the port and if (received a signal from worker) assign_job(self.jobs_queue, worker, self.defaults, self.dir)


def main(argv):
   hosts = ''
   jobs = ''
   dir = ''
   try:
      opts, args = getopt.getopt(argv,"hi:j:k:",["hosts=","jobs=","dir="])
   except getopt.GetoptError:
      print sneak_err
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print sneak_err
         sys.exit()
      if opt in ("-i", "--hosts"):
         hosts = arg
      if opt in ("-j", "--jobs"):
         jobs = arg
      if opt in ("-k", "--dir"):
         dir = arg

   if ((hosts=='') or (jobs=='') or (dir=='')):
       print sneak_err
       sys.exit(2)


   # main listener that listens to workers for an empty spot and sends a job to them
   #recruit = rThread(jobs_queue)
   #recruit.start()

    # put all the jobs in the queue
   jobs_data = [line.strip() for line in open(jobs, 'r')]
   jobs_queue = Queue.Queue(len(jobs_data))
   jobs_lock.acquire()
   for job in jobs_data:
       jobs_queue.put(job)
   jobs_lock.release()

   # find all the workers that are reachable and login-able and ask them to send a signal
   # default command that needs to be run on workers
   defaults = "cd dir && g++ -o test test.cpp"
   data = [line.strip() for line in open(hosts, 'r')]
   hosts_data = [line.split() for line in data]
   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
           # copy the file
           scp = pexpect.spawn("scp -r " + dir + " " + host[1] + "@" + host[0] + ":")
           if (scp.expect(["password:", pexpect.EOF])==0):
               scp.sendline(host[2])
               scp.expect(pexpect.EOF)
           # run a simple job such as uname and send the finished signal to get an actual job
           try:
               s = pxssh.pxssh()
               s.login (host[0], host[1], host[2])
               s.sendline(defaults) # also adding the signal stuff
               s.prompt()
               s.logout()
           except pxssh.ExceptionPxssh, e:
               print "pxssh failed on login."
               #print str(e)


   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
           try:
               s = pxssh.pxssh()
               s.login (host[0], host[1], host[2])
               s.sendline("echo '#!/usr/bin/python' > run.py")
               s.prompt()
               s.sendline("echo 'import os, socket' >> run.py")
               s.prompt()
               s.sendline("echo 'os.system(\"cd dir && ./test\")' >> run.py")
               s.prompt()
               s.sendline("echo 'HOST = \"150.203.210.120\"' >> run.py")
               s.prompt()
               s.sendline("echo 'PORT = 8000' >> run.py")
               s.prompt()
               s.sendline("echo 's = socket.socket(socket.AF_INET, socket.SOCK_STREAM)' >> run.py")
               s.prompt()
               s.sendline("echo 's.connect((HOST, PORT))' >> run.py")
               s.prompt()
               s.sendline("echo 's.sendall(\"Hello, world\")' >> run.py")
               s.prompt()
               s.sendline("echo 's.close()' >> run.py")
               s.prompt()
               #s.sendline("nohup python run.py")
               #s.prompt()
               #print s.before
               #s.logout()
           except pxssh.ExceptionPxssh, e:
               print "pxssh failed on login."
               #print str(e)

   print "finished running"




if __name__ == "__main__":
   main(sys.argv[1:])


