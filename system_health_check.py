#!/usr/bin/python2.7
#
# chkconfig: 345 91 55
#!/usr/bin/env python2.7

from __future__ import print_function
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

def send_cloudwatch_alarm(errorMessage, instanceID):
    errorMessage = str(errorMessage)
    print(errorMessage)
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    snsClient = boto3.client('sns', clusterAZ[:-1])
    snsARN = snsClient.create_topic(Name='Volume_Creation_Attachment_or_Mount_failure')
    snsClient.subscribe(TopicARN=snsARN, Protocol='email', Endpoint='alex.gao@sony.com')
    snsClient.publish(TopicARN=snsARN, Message=errorMessage+' for ' + instanceID, Subject='cassandra buildout error')
    #may not need the publish code? Check during testing if double email is sent.
    cwClient = boto3.client('cloudwatch', clusterAZ[:-1])
    cwClient.put_metric_alarm(AlarmName=(instanceID+' startup error'),
    AlarmDescription=errorMessage,
    AlarmActions=[snsARN],
    ComparisonOperator='GreaterThanThreshold',
    EvaluationPeriods=1,
    MetricName='instanceStatus',
    Namespace='instanceID/',
    Period=10,
    Statistic='Minimum',
    Threshold=1,
    ActionsEnabled=True,
    )

    cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 0}])
    #The idea is that the metric data above will cause the alarm to sound.

def vol_exists_mount_check(path, target,c,env,client,instanceID,volumesattached,instanceIDTag):
    try:
        disk_present = stat.S_ISBLK(os.stat("/dev/" + path).st_mode)
        if disk_present:
            print(path + " disk exist")
            #sys.exit(0)
            p = subprocess.check_output("lsblk", shell=False)
            if instanceIDTag is None:
                if target in p:
                    print(path + " volume is mounted on " + target)
                else:
                    print(path + " volume is not mounted on " + target)
                    send_cloudwatch_alarm(path + " volume is not mounted ", instanceID) #CW alarm code

                    for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                        client.detach_volume(VolumeId=volumeIDs[j])
                    for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                        client.delete_volume(VolumeId=v)
                    client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                    sys.exit(0)
            else:
                if target in p:
                    print(path + " volume is mounted on " + target)
                else:
                    print(path + " volume is not mounted on " + target)
                    send_cloudwatch_alarm(path + " volume is not mounted ", instanceID) #CW alarm code
                    sys.exit(0)
    except OSError:
        pass

def vol_size_check(c,env,client,instanceID,volumesattached,instanceIDTag):
    #try:

        vol_commitlog_size_consul_path = env + "/dse/cassandra/vol_commitlog_size"
        index, data = c.kv.get(vol_commitlog_size_consul_path)
        vol_size_commitlog_consul =  int(data['Value'])
        print(vol_size_commitlog_consul)
        vol_data_size_consul_path = env + "/dse/cassandra/vol_data_size"
        index, data = c.kv.get(vol_data_size_consul_path)
        vol_size_data_consul =  int(data['Value'])
        print(vol_size_data_consul)
        #vol_size_commitlog_consul = 8
        #vol_size_data_consul = 8
        #print(vol_size_consul)

        #print(volumesattached['Volumes'])
        if instanceIDTag is None:
            for volume in volumesattached['Volumes']:
                for tags in volume['Tags']:
                    if str(tags['Key']) == "blockDevice":
                        blockDevice = tags['Value']
                    if str(tags['Key']) == "mountPoint":
                        mountPoint = tags['Value']

                if mountPoint == "/data/cassandra/data/000":
                    if volume['Size'] != vol_size_commitlog_consul:
                        print(blockDevice + " volume size is not correct - commitlog")
                        send_cloudwatch_alarm(blockDevice + " volume size is not correct ", instanceID) #CW alarm code

                        for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                            client.detach_volume(VolumeId=volumeIDs[j])
                        for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                            client.delete_volume(VolumeId=v)
                        client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                        sys.exit(0)
                    else:
                        print(blockDevice + " volume size check is passed - commitlog")
                else:

                    if volume['Size'] != vol_size_data_consul:
                        print(blockDevice + " volume size is not correct - data")
                        send_cloudwatch_alarm(blockDevice + " volume size is not correct ", instanceID) #CW alarm code

                        for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                            client.detach_volume(VolumeId=volumeIDs[j])
                        for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                            client.delete_volume(VolumeId=v)
                        client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                        sys.exit(0)
                    else:
                        print(blockDevice + " volume size check is passed - data")
        else:
            for volume in volumesattached['Volumes']:
                for tags in volume['Tags']:
                    if str(tags['Key']) == "blockDevice":
                        blockDevice = tags['Value']
                    if str(tags['Key']) == "mountPoint":
                        mountPoint = tags['Value']

                if mountPoint == "/data/cassandra/data/000":
                    if volume['Size'] != vol_size_commitlog_consul:
                        print(blockDevice + " volume size is not correct - commitlog")
                        send_cloudwatch_alarm(blockDevice + " volume size is not correct ", instanceID) #Call CW alarm
                        sys.exit(0)
                    else:
                        print(blockDevice + " volume size check is passed - commitlog")
                else:

                    if volume['Size'] != vol_size_data_consul:
                        print(blockDevice + " volume size is not correct - data")
                        send_cloudwatch_alarm(blockDevice + " volume size is not correct ", instanceID) #Call CW alarm
                        sys.exit(0)
                    else:
                        print(blockDevice + " volume size check is passed - data")


    #except:
    #    return False

def vol_type_check(c,env,client,instanceID,volumesattached,instanceIDTag):
    try:

        vol_type_consul_path = env + "/dse/cassandra/vol_type"
        index, data = c.kv.get(vol_type_consul_path)
        vol_type_consul =  str(data['Value'])

        #vol_type_consul = "io1"
        #print(vol_type_consul)

        if instanceIDTag is None:
            for volume in volumesattached['Volumes']:
                for tags in volume['Tags']:
                    if str(tags['Key']) == "blockDevice":
                        blockDevice = tags['Value']
                if volume['VolumeType'] != vol_type_consul:
                    print(blockDevice + " volume type is not correct")
                    send_cloudwatch_alarm(blockDevice + " volume type is not correct ", instanceID) #Call CW alarm

                    for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                        client.detach_volume(VolumeId=volumeIDs[j])
                    for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                        client.delete_volume(VolumeId=v)
                    client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                    sys.exit(0)
                else:
                    print(blockDevice + " volume type check is passed")
        else:
            for volume in volumesattached['Volumes']:
                for tags in volume['Tags']:
                    if str(tags['Key']) == "blockDevice":
                        blockDevice = tags['Value']
                if volume['VolumeType'] != vol_type_consul:
                    print(blockDevice + " volume type is not correct")
                    send_cloudwatch_alarm(blockDevice + " volume type is not correct ", instanceID) #CW alarm code
                    sys.exit(0)
                else:
                    print(blockDevice + " volume type check is passed")

    except:
        return False

def vol_iops_check(c,env,client,instanceID,volumesattached,instanceIDTag):
    #try:

        vol_iops_consul_path = env + "/dse/cassandra/vol_iops"
        index, data = c.kv.get(vol_iops_consul_path)
        vol_iops_consul =  int(data['Value'])
        #vol_iops_consul = 100
        print(vol_iops_consul)

        if instanceIDTag is None:
            for volume in volumesattached['Volumes']:
                for tags in volume['Tags']:
                    if str(tags['Key']) == "blockDevice":
                        blockDevice = tags['Value']
                if volume['Iops'] != vol_iops_consul:
                    print(blockDevice + " volume IOPS is not correct")
                    send_cloudwatch_alarm(blockDevice + " volume IOPS is not correct ", instanceID) #CW alarm code

                    for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                        client.detach_volume(VolumeId=volumeIDs[j])
                    for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                        client.delete_volume(VolumeId=v)
                    client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                    #sys.exit(0)
                else:
                    print(blockDevice + " volume IOPS check is passed")
        else:
            for volume in volumesattached['Volumes']:
                for tags in volume['Tags']:
                    if str(tags['Key']) == "blockDevice":
                        blockDevice = tags['Value']
                if volume['Iops'] != vol_iops_consul:
                    print(blockDevice + " volume IOPS is not correct")
                    send_cloudwatch_alarm(blockDevice + " volume IOPS is not correct ", instanceID) #CW alarm code

                    for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                        client.detach_volume(VolumeId=volumeIDs[j])
                    for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                        client.delete_volume(VolumeId=v)
                    client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                else:
                    print(blockDevice + " volume IOPS check is passed")

    #except:
    #    return False

def ami_ena_check(c,env,client,instanceID,volumesattached,instanceIDTag):
    try:
        imageID = requests.get('http://169.254.169.254/latest/meta-data/ami-id').content
        image_attr = client.describe_images(ImageIds=[imageID])
        #print(image_attr)
        for images in image_attr['Images']:
            if images['EnaSupport']:
                print("ENA is enabled for ami-id " + imageID)
            else:
                print("ENA is not enabled for ami-id " + imageID)
                send_cloudwatch_alarm("ENA is not enabled for ami-id " + imageID, instanceID) #CW alarm code

                for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                    client.detach_volume(VolumeId=volumeIDs[j])
                for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                    client.delete_volume(VolumeId=v)
                client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

                sys.exit(0)
    except:
        return False

def instance_type_check(c,env,client,instanceID,volumesattached,instanceIDTag):
    try:
        instance_type_consul_path = env + "/dse/cassandra/instance_type"
        index, data = c.kv.get(instance_type_consul_path)
        vol_instance_type_consul =  str(data['Value'])
        #vol_instance_type_consul = "t2.large"

        instance_type = requests.get('http://169.254.169.254/latest/meta-data/instance-type').content
        print(instance_type)

        if vol_instance_type_consul == instance_type:
            print("Instance type spun up is correct")
        else:
            print("Instance type is not correct")
            send_cloudwatch_alarm("Instance type is not correct", instanceID) #CW alarm code

            for j in range(0, len(volumeIDs)): #first, detach the attached volumes
                client.detach_volume(VolumeId=volumeIDs[j])
            for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                client.delete_volume(VolumeId=v)
            client.terminate_instances(InstanceId=[instanceID], DryRun=True) # terminate the instance

            sys.exit(0)

    except:
        return False

def get_tags(resourceID, client):
    resourceTags = {}
    attempts=0
    while True:
        try:
            resourceTagsUnformatted = client.describe_tags(Filters=[{'Name': 'resource-id', 'Values':[resourceID]}])['Tags']
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'RequestLimitExceeded' and attempts < 10:
                sleep(random.random()*(min(300, 2*2**attempts)))
                attempts+=1
                continue
            else:
                send_cloudwatch_alarm(e)
                print(e)
                sys.exit(0)

    for tag in resourceTagsUnformatted:
        key = tag['Key']
        value = tag['Value']
        resourceTags[key] = value

    return resourceTags

def main():

    LOG_FILENAME = "/var/log/system-health-check.log"
    LOG_LEVEL = logging.DEBUG  # Could be e.g. "DEBUG" or "WARNING"


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

    c = consul.Consul(host='localhost')
    #env = subprocess.check_output("echo $SIEENV", shell=True).strip()
    #print(env)
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    client = boto3.client('ec2', clusterAZ[:-1])
    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    #sys.exit(0)
    #print(instanceID)
    volumesattached = client.describe_volumes(Filters=[{'Name': 'attachment.instance-id', 'Values': [instanceID]},{'Name': 'tag-key', 'Values':['index']}])
    env = get_tags(instanceID, client)['Environment']
    print(env)
    #print(volumesattached['Volumes'])
    #sys.exit(0)

    if len(volumesattached) > 0:
        volumeIDs = [v['VolumeId'] for v in volumesattached['Volumes']] # get volume IDs if you want to delete it in future

    instanceIDTag = ""
    for volume in volumesattached['Volumes']:
        if volume['Tags']:
            for tags in volume['Tags']:
                if str(tags['Key']) == "instanceID":
                    instanceIDTag = tags['Value']
                    print("InstanceID tag is " + instanceIDTag)
                    break
        break

    for disk in device_map:
        print("Checking existence and mounting of " + disk['device'])
        vol_exists_mount_check(disk['device'], disk['target'],c,env,client,instanceID,volumesattached,instanceIDTag)

    print("Checking size of the volumes")
    vol_size_check(c,env,client,instanceID,volumesattached,instanceIDTag)
    print("Checking type of the volumes")
    vol_type_check(c,env,client,instanceID,volumesattached,instanceIDTag)
    print("Checking IOPS of the volumes")
    vol_iops_check(c,env,client,instanceID,volumesattached,instanceIDTag)
    print("Checking ENA for the ami-id used")
    ami_ena_check(c,env,client,instanceID,volumesattached,instanceIDTag)
    print("Checking instance type")
    instance_type_check(c,env,client,instanceID,volumesattached,instanceIDTag)

if __name__ == "__main__":

    main()
