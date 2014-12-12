#!/usr/bin/python

import os, sys, getopt, pxssh, pexpect, socket, datetime, Queue, threading
sneak_err = 'sneak.py -i <hosts> -j <jobs> -k <project>'

exitFlag = False
jobs_lock = threading.Lock()
jobs_data = []

# read i-th line of file
def readline(file, i):
    j = 0
    for line in open(file, 'r'):
        j = j + 1
        if (j==i):
            return line.strip()
    if (i>j):
        return "line number error"

#added for commit!
def create_job_runner_file(s, job):
    s.sendline("echo '#!/usr/bin/python' > run.py")
    s.prompt()
    s.sendline("echo 'import os, socket, sys' >> run.py")
    s.prompt()
    s.sendline("echo 'os.system(\"" + job + " > _job_output\")' >> run.py")
    s.prompt()
    s.sendline("echo 'HOST = \"150.203.210.120\"' >> run.py")
    s.prompt()
    s.sendline("echo 'PORT = 8000' >> run.py")
    s.prompt()
    s.sendline("echo 's = socket.socket(socket.AF_INET, socket.SOCK_STREAM)' >> run.py")
    s.prompt()
    s.sendline("echo 's.connect((HOST, PORT))' >> run.py")
    s.prompt()
    s.sendline("echo 'f=open (\"_job_output\", \"rb\")' >> run.py")
    s.prompt()
    s.sendline("echo 'l = f.read(1024)' >> run.py")
    s.prompt()
    s.sendline("echo 'while (l):' >> run.py")
    s.prompt()
    s.sendline("echo '    s.send(l):' >> run.py")
    s.prompt()
    s.sendline("echo '    l = f.read(1024)' >> run.py")
    s.prompt()
    s.sendline("echo 's.close()' >> run.py")
    s.prompt()


# assign the job to host
def assign_job(jobs_queue, host):
    res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
    if (res == 0):
        jobs_lock.acquire()
        ################################ -> update jobs_queue
        for i in range(0, len(jobs_data)):
            if (jobs_data[i][3] > 0):
                # check if the job is still running
                s = pxssh.pxssh()
                s.sendline("echo $(kill -s 0 " + jobs_data[i][3] + ")")
                s.prompt()
                if (s.before == '\n'):
                    jobs_data[i][3] = -2
                    jobs_queue.put(jobs_data[1])
                i = i + 1
        if not jobs_queue.empty():
            try:
                job = jobs_queue.get()
                jobs_lock.release()
                s = pxssh.pxssh()
                s.login (host[0], host[1], host[2])
                create_job_runner_file(s, job)
                s.sendline("nohup python run.py & > pid")
                s.prompt()
                #print s.before
                pid = s.before.split()
                pid = pid[len(pid)-1]
                #print pid
                now = datetime.datetime.now()
                #os.system("echo " + job + " " + host[0] + " " + host[1] + " " + host[2] + " " + pid + " %d:%d:%d:" % (now.year, now.month, now.day) + "%d:%d:%d" % (now.hour, now.minute, now.second) + " Running >> .log")
                s.logout()
                for job_id in range(0, len(jobs_data)):
                    if (jobs_data[job_id][1]==job):
                        jobs_data[job_id][2] = host
                        jobs_data[job_id][3] = pid
            except pxssh.ExceptionPxssh, e:
                print "pxssh failed on login."
                print str(e)


class rThread (threading.Thread):
    def __init__(self, jobs_queue):
        self.jobs_queue = jobs_queue
    def run(self):
        # listen to the port and if (received a signal from worker) assign_job(self.jobs_queue, worker, self.defaults, self.dir)
        while not exitFlag:
            HOST = ''
            PORT = 8000
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((HOST, PORT))
            s.listen(1)
            conn, addr = s.accept()
            print 'Connected by', addr
            job_id = 0
            for job_id in range(0, len(jobs_data)):
                if (jobs_data[job_id][2] == addr):
                    jobs_data[job_id][3] = -2
                    break
            f = open("_" + job_id + ".out", 'wb')
            while True:
                data = conn.recv(1024)
                f.write(data)
                if not data: break
            conn.close()
            assign_job(jobs_queue, addr)



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

   # put all the jobs in the queue and create the database
   i = 0
   for line in open(jobs, 'r'):
       # append job_id, job, server_id, process_id
       jobs_data.append([i, line.strip(), -1, -1])
       i = i + 1
   jobs_queue = Queue.Queue(len(jobs_data))
   jobs_lock.acquire()
   for job in jobs_data:
       jobs_queue.put(job[1])
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

   # Wait for queue to empty
   while not jobs_data.empty():
       pass

   # Notify threads it's time to exit
   exitFlag = True


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


