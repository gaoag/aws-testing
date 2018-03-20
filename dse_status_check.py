#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import re
import sys
import subprocess
from time import sleep
import logging
import logging.handlers
import argparse

#LOG_FILENAME = "/var/log/dse_status_check.log"
#LOG_LEVEL = logging.DEBUG  # Could be e.g. "DEBUG" or "WARNING"

# Define and parse command line arguments
#parser = argparse.ArgumentParser(description="System health check script")
#parser.add_argument("-l", "--log", help="file to write log to (default '" + LOG_FILENAME + "')")


# Give the logger a unique name (good practice)
#logger = logging.getLogger(__name__)
# Set the log level to LOG_LEVEL
#logger.setLevel(LOG_LEVEL)
# Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
#handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="midnight", backupCount=3)
# Format each log message like this
#formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
# Attach the formatter to the handler
#handler.setFormatter(formatter)
# Attach the handler to the logger
#logger.addHandler(handler)

# Make a class we can use to capture stdout and sterr in the log
class MyLogger(object):
        def __init__(self, logger, level):
                """Needs a logger and a logger level."""
                self.logger = logger
                self.level = level

        def write(self, message):
                # Only log if there is a message (not just a new line)
                if message.rstrip() != "":
                        self.logger.log(self.level, message.rstrip())

# Replace stdout with logging to file at INFO level
#sys.stdout = MyLogger(logger, logging.INFO)
# Replace stderr with logging to file at ERROR level
#sys.stderr = MyLogger(logger, logging.ERROR)

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

    #for i in range(0,10):
    check_native = check_server(ip, 9042 )
    print 'check_server returned %s' % check_native
    check_thrift = check_server(ip, 9160 )
    print 'check_server returned %s' % check_thrift
    check_gossip = check_server(ip, 7001 )
    print 'check_server returned %s' % check_gossip
    #    sleep(10)
    if all(x == "listening" for x in (check_native, check_thrift, check_gossip)):
        print "All ports are up and listening"
        return False
        sys.exit(0)
    else:
        print "Status of the ports are Gossip: %s, Native: %s, Thrift: %s" % (check_gossip, check_native, check_thrift)
        return True

if __name__ == '__main__':
    main()
