#!/usr/bin/python

import os, sys, getopt, pxssh
sneak_err = 'run_and_escape.py -i <hosts> -j <job>'


def main(argv):
   hosts = ''
   jobs = ''
   try:
      opts, args = getopt.getopt(argv,"hi:j:",["hosts=","job="])
   except getopt.GetoptError:
      print sneak_err
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print sneak_err
         sys.exit()
      if opt in ("-i", "--hosts"):
         hosts = arg
      if opt in ("-j", "--job"):
         jobs = arg

   if ((hosts=='') or (jobs=='')):
       print sneak_err
       sys.exit(2)


   # find all the workers that are reachable and login-able and ask them to send a signal
   # default command that needs to be run on workers
   # number of jobs and workers is equal
   jobs = [line.strip() for line in open(jobs, 'r')]
   #jobs =[]
   #jobs.append("cd dir && nohup python test.py")
   #jobs.append("cd dir && nohup python test.py")
   data = [line.strip() for line in open(hosts, 'r')]
   hosts_data = [line.split() for line in data]
   i = 0
   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
           try:
               s = pxssh.pxssh()
               s.login (host[0], host[1], host[2])
               s.sendline(jobs[i] + " > " + str(i))
               i = i + 1
           except pxssh.ExceptionPxssh, e:
               print "Failed to login to host: " + host[0]

       else:
           print "Unreachable host: " + host[0]

if __name__ == "__main__":
   main(sys.argv[1:])
