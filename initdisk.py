#!/usr/bin/python2.7
#
# chkconfig: 345 91 55
# description: Init Disk for Cassandra

from __future__ import print_function
import subprocess
import os
import stat
import time
import logging
import logging.handlers
import sys

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

def create_partition(device_name):
    if 'nvm' in device_name:
        create_partition_nvm(device_name)
    else:
        create_partition_ssd(device_name)


def create_partition_nvm(device_name):
    cmd = "file -sL " + device_name + "p1"
    p = subprocess.check_output(cmd, shell=True)
    if 'ext4' not in p:
        parttype_cmd = "parted " + device_name + " --script mklabel gpt"
        parttype_out = subprocess.check_output(parttype_cmd, shell=True)

        mkpart_cmd = "parted " + device_name + " --script mkpart primary 0% 100%"
        mkpart_out = subprocess.check_output(mkpart_cmd, shell=True)

        format_cmd = "mkfs -t ext4 " + device_name + "p1"
        p = subprocess.check_output(format_cmd, shell=True)
        return p


def create_partition_ssd(device_name):
    cmd = "file -sL " + device_name + "1"
    p = subprocess.check_output(cmd, shell=True)
    if 'ext4' not in p:
        parttype_cmd = "parted " + device_name + " --script mklabel gpt"
        parttype_out = subprocess.check_output(parttype_cmd, shell=True)

        mkpart_cmd = "parted " + device_name + " --script mkpart primary 0% 100%"
        mkpart_out = subprocess.check_output(mkpart_cmd, shell=True)

        while not os.path.exists(device_name + "1"):
            time.sleep(1)

        format_cmd = "mkfs -t ext4 " + device_name + "1"
        p = subprocess.check_output(format_cmd, shell=True)
        return p


def mount(device_name, directory):
    if 'nvm' in device_name:
        mount_nvm(device_name, directory)
    else:
        mount_ssd(device_name, directory)


def mount_nvm(device_name, directory):
    cmd = "mount"
    p = subprocess.check_output(cmd, shell=True)
    if device_name not in p:
        if directory == "/data/cassandra/data/000":
            mkdir_p(directory)
            cmd = "mount " + device_name + "p1 " + directory + " -o rw,noatime,nodiratime,discard,errors=remount-ro"
            p = subprocess.check_output(cmd, shell=True)
            return p
        else:
            mkdir_p(directory)
            cmd = "mount " + device_name + "p1 " + directory + " -o rw"
            p = subprocess.check_output(cmd, shell=True)
            return p


def mount_ssd(device_name, directory):
    cmd = "mount"
    p = subprocess.check_output(cmd, shell=True)
    if device_name not in p:
        if directory == "/data/cassandra/data/000":
            mkdir_p(directory)
            cmd = "mount " + device_name + "1 " + directory + " -o rw,noatime,nodiratime,discard,errors=remount-ro"
            p = subprocess.check_output(cmd, shell=True)
            return p
        else:
            mkdir_p(directory)
            cmd = "mount " + device_name + "1 " + directory + " -o rw"
            p = subprocess.check_output(cmd, shell=True)
            return p


def tune(device_name):
    if device_name[:2] == "sd":
        device_name = device_name.replace('sd', 'xvd')
        cmd = "echo 0 > /sys/class/block/" + device_name + "/queue/rotational && echo deadline > /sys/block/" + device_name + "/queue/scheduler && echo 8 > /sys/class/block/" + device_name + "/queue/read_ahead_kb"
        p = subprocess.check_output(cmd, shell=True)
        return p
    else:
        cmd = "echo 0 > /sys/class/block/" + device_name + "/queue/rotational && echo deadline > /sys/block/" + device_name + "/queue/scheduler && echo 8 > /sys/class/block/" + device_name + "/queue/read_ahead_kb"
        p = subprocess.check_output(cmd, shell=True)
        return p


def fix_ownership():
    cmd = "chown -R cassandra:cassandra /data/cassandra"
    p = subprocess.check_output(cmd, shell=True)
    return p


def disk_exists(path, target):
    try:
        disk_present = stat.S_ISBLK(os.stat(path).st_mode)
        if disk_present:
            print("Disk Present")
        p = subprocess.check_output("lsblk", shell=False)
        if target in p:
            return False
        return True

    except:
        return False


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError:
        pass

def main():

    # Defaults
    LOG_FILENAME = "/var/log/init-disk.log"
    LOG_LEVEL = logging.INFO  # Could be e.g. "DEBUG" or "WARNING"

    # Configure logging to log to a file, making a new file at midnight and keeping the last 3 day's data
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
    sys.stdout = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)

    device_map = [{'device': 'sdj', 'target': '/data/cassandra/data/000'},
                  {'device': 'sdk', 'target': '/data/cassandra/data/001'},
                  {'device': 'sdl', 'target': '/data/cassandra/data/002'},
                  {'device': 'sdm', 'target': '/data/cassandra/data/003'},
                  {'device': 'sdn', 'target': '/data/cassandra/data/004'},
                  {'device': 'sdo', 'target': '/data/cassandra/data/005'},
                  {'device': 'sdp', 'target': '/data/cassandra/data/006'},
                  {'device': 'sdq', 'target': '/data/cassandra/data/007'},
                  {'device': 'nvme0n1', 'target': '/data/cassandra/data/008'},
                  {'device': 'nvme1n1', 'target': '/data/cassandra/data/009'}]
    for disk in device_map:
        print("Checking " + disk['device'])
        if disk_exists("/dev/"+disk['device'], disk['target']):
            print("Creating partition on " + disk['device'])
            print(create_partition("/dev/"+disk['device']))
            print("Mounting " + disk['device'] + " to " + disk['target'])
            print(mount("/dev/"+disk['device'], disk['target']))
            print(tune(disk['device']))

    fix_ownership()

if __name__ == "__main__":
    main()
