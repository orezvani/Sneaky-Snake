#!/usr/bin/python

import os, sys, getopt, pxssh, pexpect
sneak_err = 'sneak.py -i <hosts> -j <jobs> -k <project>'

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
   data = [line.strip() for line in open(hosts, 'r')]
   hosts_data = [line.split() for line in data]
   s = []
   n = 0
   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
           try:
                s.append(pxssh.pxssh())
                s[n].login (host[0], host[1], host[2])
                scp = pexpect.spawn("scp -r " + dir + " " + host[1] + "@" + host[0] + ":")
                if (scp.expect(["password:", pexpect.EOF])==0):
                    scp.sendline(host[2])
                    scp.expect(pexpect.EOF)
                #s[i].sendline ('uptime')   # run a command
                #s[i].prompt()             # match the prompt
                #print s[i].before          # print everything before the prompt.
                n = n + 1
           except pxssh.ExceptionPxssh, e:
               print "pxssh failed on login."
               print str(e)
   print 'Successfully connected to %d host(s).\n' % n

   # Run Jobs
   jobs_data = [line.strip() for line in open(jobs, 'r')]
   while (len(jobs_data)>0):
       for ss in s:
           ss.sendline(jobs_data[0])
           ss.prompt()
           print ss.before
           jobs_data.pop(0)



   # Logging out
   for ss in s:
       ss.logout()
   print "Successfully disconnected from %d host(s)." % n


if __name__ == "__main__":
   main(sys.argv[1:])


