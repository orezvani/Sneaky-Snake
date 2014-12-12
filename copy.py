#!/usr/bin/python

import os, sys, getopt, pexpect
sneak_err = 'copy.py -i <hosts> -k <project>'


def main(argv):
   hosts = ''
   jobs = ''
   dir = ''
   try:
      opts, args = getopt.getopt(argv,"hi:k:",["hosts=","dir="])
   except getopt.GetoptError:
      print sneak_err
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print sneak_err
         sys.exit()
      if opt in ("-i", "--hosts"):
         hosts = arg
      if opt in ("-k", "--dir"):
         dir = arg

   if ((hosts=='') or (dir=='')):
       print sneak_err
       sys.exit(2)


   # find all the workers that are reachable and login-able and ask them to send a signal
   # default command that needs to be run on workers
   data = [line.strip() for line in open(hosts, 'r')]
   hosts_data = [line.split() for line in data]
   for host in hosts_data:
       res = os.system("ping -c 1 " + host[0] + " > /dev/null 2>&1")
       if (res == 0):
           try:
               scp = pexpect.spawn("scp -r " + dir + " " + host[1] + "@" + host[0] + ":")
               scp.force_password = True
               if (scp.expect(["password:", pexpect.EOF])==0):
                   scp.sendline(host[2])
                   scp.expect(pexpect.EOF)
                   print host[0] + " " + scp.before
           except pexpect.ExceptionPexpect, e:
               print "Failed to login to host: " + host[0]

       else:
           print "Unreachable host: " + host[0]

if __name__ == "__main__":
   main(sys.argv[1:])
