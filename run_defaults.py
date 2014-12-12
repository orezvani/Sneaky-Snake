#!/usr/bin/python

import os, sys, getopt, pxssh
sneak_err = 'run_defaults.py -i <hosts> -j <job>'


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
   job = open(jobs,'r').readline().strip()
   data = [line.strip() for line in open(hosts, 'r')]
   hosts_data = [line.split() for line in data]
   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
           try:
                s = pxssh.pxssh()
                s.login (host[0], host[1], host[2])
                s.sendline(job)
                s.prompt()
                print host[0] + " " + s.before
                s.logout()
           except pxssh.ExceptionPxssh, e:
               print "Failed to login to host: " + host[0]

       else:
           print "Unreachable host: " + host[0]

if __name__ == "__main__":
   main(sys.argv[1:])
