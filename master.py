#!/usr/bin/env python2.7

import ebs_vol_script
import initdisk
import system_health_check
import keystore_script
import app_config_check
import finalize_ebs_tags
import dse_startup_script
import subprocess
import os
import consul
import boto3
import stat
import time
import logging
import logging.handlers
import argparse
import sys
import requests

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


def main():
    # Deafults
    LOG_FILENAME = "/var/log/master-script.log"
    LOG_LEVEL = logging.INFO  # Could be e.g. "DEBUG" or "WARNING"
    # Give the logger a unique name (good practice)
    logger = logging.getLogger(__name__)
    # Set the log level to LOG_LEVEL
    logger.setLevel(LOG_LEVEL)
    # Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
    handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="midnight", backupCount=3)
    # Format each log message like this
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    # Attach the formatter to the handler
    handler.setFormatter(formatter)
    # Attach the handler to the logger
    logger.addHandler(handler)
    # Replace stdout with logging to file at INFO level
    localLogger = MyLogger(logger, logging.INFO)
    localLoggerErr = MyLogger(logger, logging.ERROR)
    sys.stdout = localLogger
    # Replace stderr with logging to file at ERROR level
    sys.stderr = localLoggerErr
    try:
        print('attach/create ebs volumes in progress')
        ebs_vol_script.main("")
                
        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('mounting filesystems in progress')
        initdisk.main()

        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('health check of instances/volumes in progress')
        system_health_check.main()

        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('retrieving and storing certpath in progress')
        keystore_script.main()

        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('checking cassandra config in progress')
        app_config_check.main()

        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('saving ebs tags in dynamodb in progress')
        finalize_ebs_tags.main()

        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('starting dse in progress')
        dse_startup_script.main()

        sys.stdout = localLogger
        sys.stderr = localLoggerErr
        print('finished starting this instance!')
    except Exception as e:
        print(e)
        sys.exit(0)

if __name__ == "__main__":
    main()
