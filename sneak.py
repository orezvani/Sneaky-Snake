#!/usr/bin/python

import os, sys, getopt, pxssh, pexpect, datetime
sneak_err = 'sneak.py -i <hosts> -j <jobs> -k <project>'


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
def assign_job(host, job, dir, defaults):
    try:
        scp = pexpect.spawn("scp -r " + dir + " " + host[1] + "@" + host[0] + ":")
        if (scp.expect(["password:", pexpect.EOF])==0):
            scp.sendline(host[2])
            scp.expect(pexpect.EOF)
        s = pxssh.pxssh()
        s.login (host[0], host[1], host[2])
        s.sendline(defaults)
        s.prompt()
        s.sendline("nohup " + job + " & > pid")
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

   # Load Hosts
   defaults = "cd dir && g++ -o test test.cpp"
   data = [line.strip() for line in open(hosts, 'r')]
   hosts_data = [line.split() for line in data]
   n = 0
   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
            n = n + 1
            job = readline(jobs, n)
            if (job != "line number error"):
                assign_job(host, job, dir, defaults)









if __name__ == "__main__":
   main(sys.argv[1:])


