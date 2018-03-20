#!/usr/bin/python2.7
#
# chkconfig: 345 88 56
# description: EBS attachment script

import os
import sys
import boto3
import consul
import time
import requests
import subprocess
import logging
import logging.handlers
import argparse
import sys

# Deafults
LOG_FILENAME = "/var/log/ebs-attach.log"
LOG_LEVEL = logging.INFO  # Could be e.g. "DEBUG" or "WARNING"

# Define and parse command line arguments
parser = argparse.ArgumentParser(description="My simple Python service")
parser.add_argument("-l", "--log", help="file to write log to (default '" + LOG_FILENAME + "')")


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
sys.stdout = MyLogger(logger, logging.INFO)
# Replace stderr with logging to file at ERROR level
sys.stderr = MyLogger(logger, logging.ERROR)

def create_volume(tagData, size, volType, iops, clusterAZ, name, client):
    tagArray=[]

    for key, value in tagData.iteritems():
        tagArray += [{'Key':key, 'Value':value}]
    tagArray += [{'Key':'index', 'Value':(tagData['mountPoint'][-3:])}]
    print('creating new volume with tags ' + str(tagArray))
    newVolumeInfo = client.create_volume(
        DryRun=False,
        Size = size,
        AvailabilityZone = clusterAZ,
        VolumeType = volType,
        Iops=iops,
        Encrypted=False,
        TagSpecifications=[
        {
            'ResourceType': 'volume',
            'Tags': tagArray

        }]
    )
    newVolumeID = newVolumeInfo['VolumeId']
    client.create_tags(Resources=[newVolumeID], Tags=[{'Key':'Name', 'Value':name+newVolumeID}])

    return newVolumeID

def find_unattached_volumes(client, instanceID, clusterAZ, instanceEnvironment, instanceApplication):
    #filter by available tags - need multiple filters because filter values do not "add" on each other
    volumesInAZ = client.describe_volumes(Filters=[{'Name': 'availability-zone', 'Values': [clusterAZ]},
                                                            {'Name': 'tag-key', 'Values':['mountPoint']},
                                                            {'Name': 'tag-key', 'Values':['instanceID']},
                                                            {'Name': 'tag-key', 'Values':['blockDevice']},
                                                            {'Name': 'tag-key', 'Values':['index']},
                                                            {'Name': 'tag-key', 'Values':['numInSet']},
                                                            {'Name': 'tag:env', 'Values':[instanceEnvironment]},
                                                            {'Name': 'tag:app', 'Values':[instanceApplication]}]) #{'Name': 'status', 'Values':['available']}

    startVolume = None


    for volume in volumesInAZ['Volumes']:
        volumeState = volume['State']
        if volume['Attachments']:
            attachmentState = volume['Attachments'][0]['State']
            if attachmentState == 'detached' and volumeState == 'available':
                startVolume = volume
                break
            elif attachmentState == 'detaching':
                print('waiting on detaching volume ' + volume['VolumeId'])
                while client.describe_volumes(VolumeIds=[volume['VolumeId']])['Volumes'][0]['Attachments'][0]['State'] != 'detached':
                    sleep(3)
                    startVolume = volume
                    break
        else:
            if volumeState == 'available':
                startVolume = volume
                break



    unattachedVolumes = []
    if startVolume:
        print('got a floating volume')
        print(startVolume)
        prevInstanceID = [n for n in startVolume['Tags'] if (n['Key'] == 'instanceID')][0]['Value']
        unattachedVolumes = [v for v in (client.describe_volumes(Filters=[{'Name': 'tag:instanceID', 'Values':[prevInstanceID]}])['Volumes'])]
    print('found ' + str(len(unattachedVolumes)) + 'unattached volumes')
    return unattachedVolumes

def attach_volume(client, volume_id, instanceID, blockDevice):
    if (client.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]['State']!='available'):
        volume_waiter = client.get_waiter('volume_available')
        volume_waiter.wait(VolumeIds=[volume_id])
    client.attach_volume(VolumeId=volume_id, InstanceId=instanceID, Device=blockDevice)
    print('attached ' + volume_id + ' to ' + instanceID)

def main(args):
    c = consul.Consul(host='localhost')
    p = subprocess.check_output("echo $SIEENV", shell=True).strip()
    
    #get blockDevice from consul
    path_blk = p + "/dse/cassandra/blk_dev_start"
    index, data = c.kv.get(path_blk)
    blockDevice =  str(data['Value'])
    #blockDevice = "sdj"
    #print blockDevice


    #get numInSet from consul
    path_vol = p + "/dse/cassandra/num_vol_set"
    index, data = c.kv.get(path_vol)
    numInSet = int(data['Value'])
    #numInSet = 2

    #sys.exit(0)
    ###NEED TO CREATE A PATH IN CONSUL FOR THIS DATA###
    size=1024 #get from consul
    volType='io1' #get from consul
    iops=5000 #get from consul

    #query metadata for info
    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content

    client = boto3.client('ec2', clusterAZ[:-1])
    instanceTags = client.describe_instances(InstanceIds=[instanceID])['Reservations'][0]['Instances'][0]['Tags']
    instanceEnvironment = [n for n in instanceTags if (n['Key'] == 'Environment')][0]['Value']
    instanceApplication = [n for n in instanceTags if (n['Key'] == 'Application')][0]['Value']
    instanceName = [n for n in instanceTags if (n['Key'] == 'Name')][0]['Value']


    unattachedVolumes = find_unattached_volumes(client, instanceID, clusterAZ, instanceEnvironment, instanceApplication)
    unattachedVolumesSortedByIndex = sorted(unattachedVolumes, key=lambda n: int([t for t in n['Tags'] if (t['Key'] == 'index')][0]['Value']))
    unattachedVolumeIDs = [v['VolumeId'] for v in unattachedVolumesSortedByIndex]

    #should rewrite logic to create create create, then attach-attach-attach - relevant only to the else statement below.
    #reasoning: prevent floating orphans if attachment fails
    for i in range(0, numInSet):
        mountPoint = '/data/cassandra/data/' + str(i).zfill(3)
        if (i < len(unattachedVolumeIDs)):
            volumeToAdd = unattachedVolumeIDs[i]
            attach_volume(client, volumeToAdd, instanceID, blockDevice)
            newTags=[#{'Key':'blockDevice', 'Value': blockDevice},
                        #{'Key':'mountPoint', 'Value': mountPoint},
                        {'Key':'instanceID', 'Value': instanceID},
                        #{'Key':'numInSet', 'Value': str(numInSet)},
                        #{'Key':'index', 'Value':str(i)},
                        {'Key':'Name', 'Value':instanceName+volumeToAdd}]
                        #{'Key':'env', 'Value':instanceEnvironment},
                        #{'Key':'app', 'Value':instanceApplication}]
            print('changing these tags of volume ' + volumeToAdd + ' to: ' + str(newTags))
            #client.create_tags(Resources=[volumeToAdd['VolumeId']],Tags=newTags)
            client.create_tags(Resources=[volumeToAdd],Tags=newTags)
        else:
            tagData = {'instanceID':instanceID, 'mountPoint':mountPoint, 'numInSet':str(numInSet), 'blockDevice':blockDevice, 'env':instanceEnvironment, 'app':instanceApplication}
            newVolumeID = create_volume(tagData, size, volType, iops, clusterAZ, instanceName, client)
            attach_volume(client, newVolumeID, instanceID, blockDevice)
        blockDevice = blockDevice[:-1] + chr(ord(blockDevice[-1:]) + 1)

if __name__ == "__main__":
    p = subprocess.check_output("lsblk", shell=False)
    if 'xvdj' and 'xvdk' in p:
        print "Disks are attached already."
        sys.exit(0)
    else:
        main('None')
