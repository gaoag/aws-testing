#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import socket
#import re
import sys
import subprocess

def check_server(address, port):
    # Create a TCP socket
    s = socket.socket()
    print "Attempting to connect to %s on port %s" % (address, port)
    try:
        s.connect((address, port))
        print "Connected to %s on port %s" % (address, port)
        return "listening"
    except socket.error, e:
        print "Connection to %s on port %s failed: %s" % (address, port, e)
        return "not listening"

def main():
    cmd = "hostname -i"
    ip = subprocess.check_output(cmd , shell=True).strip()

    check_native = check_server(ip, 9042 )
    print 'check_server returned %s' % check_native
    check_thrift = check_server(ip, 9160 )
    print 'check_server returned %s' % check_thrift
    check_gossip = check_server(ip, 7001 )
    print 'check_server returned %s' % check_gossip

    if all(x == "listening" for x in (check_native, check_thrift, check_gossip)):
        print "All ports are up and listening"
        sys.exit(0)

    else:
        print "Status of the ports are Gossip: %s, Native: %s, Thrift: %s" % (check_gossip, check_native, check_thrift)

        sys.exit(1)


if __name__ == '__main__':
    main()
