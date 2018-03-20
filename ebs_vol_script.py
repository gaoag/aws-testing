#!/usr/bin/python2.7
#
# chkconfig: 345 88 56
# description: EBS attachment script
from botocore.exceptions import ClientError
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
import random



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



def get_consul_data(consulPath, dataPaths, consulHost='localhost'):
    c = consul.Consul(host=consulHost)
    consulData = {}
    defaults = {'size':'8', 'volType':'io1', 'iops':'100', 'blockDevice':'/dev/sdj', 'numInSet':'3', 'commitLogSize':'100'}
    for dataName, dataPath in dataPaths.iteritems():
        ###for future, throw an error for these cases
        if dataName == '':
            dataName = dataPath
        elif dataPath == '':
            consulData[dataName] = str(defaults[dataName])
            continue
        print(consulPath + dataPath)
        index, data = c.kv.get(consulPath + dataPath)
        consulData[dataName] = str(data['Value'])
    return consulData
def get_instance_metadata():
    instanceMetadata = {}

    instanceMetadata['instanceID'] = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    instanceMetadata['clusterAZ'] = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    ###for future, add all the other existing metadata

    return instanceMetadata

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

def find_unattached_volumes(clusterAZ, instanceEnv, instanceApp, extraFilters, client):
    filters = [{'Name': 'availability-zone', 'Values': [clusterAZ]}, {'Name': 'tag:env', 'Values':[instanceEnv]}, {'Name': 'tag:app', 'Values':[instanceApp]}]
    for tagKey, tagValues in extraFilters.iteritems():
        if len(tagValues) == 0:
            filters.append({'Name': 'tag-key', 'Values':[tagKey]})
        else:
            filters.append({'Name': 'tag:' + tagKey, 'Values':[tagValues]})

    attempts = 0
    while True:
        try:
            yourVolumes = client.describe_volumes(Filters=filters)['Volumes']
            dbclient = boto3.client('dynamodb', clusterAZ[:-1])
            index, clusterName = (consul.Consul(host='localhost')).kv.get(instanceEnv + '/dse/cassandra/cluster_name')
            #clusterName = str(clusterName['Value'])
            tableName = instanceEnv+'_'+clusterName+'_EbsVolInfo'

            for v in yourVolumes:
                entry = dbclient.get_item(TableName=tableName, Key={'VolumeId': {'S':v['VolumeId']}}, ConsistentRead=True)
                if 'Item' in entry and 'Contaminated' in entry['Item'] and entry['Item']['Contaminated']['BOOL']:
                    yourVolumes.remove(v)
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'RequestLimitExceeded' and attempts < 10:
                sleep(random.random()*(min(300, 2*2**attempts)))
                attempts+=1
                continue
            else:
                print(e)
                send_cloudwatch_alarm(e)
                sys.exit(0)

    startVolume = None
    for volume in yourVolumes:
        volumeState = volume['State']
        if volume['Attachments']:
            attachmentState = volume['Attachments'][0]['State']
            if attachmentState == 'detached' and volumeState == 'available':
                startVolume = volume
                break
            elif attachmentState == 'detaching':
                print('waited on detaching volume ' + volume['VolumeId'])
                attempts = 0
                while True:
                    try:
                        while client.describe_volumes(VolumeIds=[volume['VolumeId']])['Volumes'][0]['Attachments'][0]['State'] != 'detached':
                            sleep(3)
                            startVolume = volume
                            break
                        break
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'RequestLimitExceeded' and attempts < 10:
                            sleep(random.random()*(min(300, 2*2**attempts)))
                            attempts += 1
                            continue
                        else:
                            print(e)
                            send_cloudwatch_alarm(e)
                            sys.exit(0)

        else:
            sys.exit('found a set that was tagged, but with a blank attachment state. Should never happen, since the call to attach is ALWAYS made before tagging. uncaught logic error somewhere.')

    unattachedVolumes = []
    if startVolume:
        print('got a floating volume')
        print(startVolume)
        prevInstanceID = get_tags(startVolume['VolumeId'], client)['instanceID']
        attempts = 0
        while True:
            try:
                unattachedVolumes = [v for v in (client.describe_volumes(Filters=[{'Name': 'tag:instanceID', 'Values':[prevInstanceID]}])['Volumes'])]
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'RequestLimitExceeded' and attempts < 10:
                    sleep(random.random()*(min(300, 2*2**attempts)))
                    attempts+=1
                    continue
                else:
                    print(e)
                    send_cloudwatch_alarm(e)
                    sys.exit(0)
    unattachedVolumesSortedByIndex = sorted(unattachedVolumes, key=lambda n: int([t for t in n['Tags'] if (t['Key'] == 'index')][0]['Value']))
    print('found ' + str(len(unattachedVolumes)) + 'unattached volumes')
    return unattachedVolumesSortedByIndex

def attach_volumes(volumeIDs, instanceID, blockDevices, fromNew, client):
    if (not len(volumeIDs) == len(blockDevices)):
        raise Exception('didnt pass in correct number of volumeIDs or blockDevices')
    for i in range(0, len(volumeIDs)):
        attempts=0
        while True:
            try:
                if (client.describe_volumes(VolumeIds=[volumeIDs[i]])['Volumes'][0]['State']!='available'):
                    volume_waiter = client.get_waiter('volume_available')
                    volume_waiter.wait(VolumeIds=[volumeIDs[i]]) #set a timeout
                client.attach_volume(VolumeId=volumeIDs[i], InstanceId=instanceID, Device=blockDevices[i])
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'RequestLimitExceeded' and attempts < 10:
                    sleep(random.random()*(min(300, 2*2**attempts)))
                    attempts+=1
                    continue
                else:
                    if fromNew:
                        for j in range(0, i): #first, detach the attached volumes
                            client.detach_volume(VolumeId=volumeIDs[j])
                        for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                            client.delete_volume(VolumeId=v)
                        send_cloudwatch_alarm('newly created volumes failed to attach', instanceID)
                        print(e)
                        client.terminate_instances(InstanceId=[instanceID])
                    else:
                        print(e)
                        for j in range(0, i): #first, detach the attached volumes
                            client.detach_volume(VolumeId=volumeIDs[j])
                        send_cloudwatch_alarm('pre-existing volumes we found failed to attach', instanceID)
                        sys.exit(0)
    #by now, all should have been attached. Loop through them again one more time.
    ###probably fraught with errors.
    for v in volumeIDs:
        volume = client.describe_volumes(VolumeIds=[v])['Volumes'][0]
        if 'Attachments' in volume:
            attachmentState = volume['Attachments'][0]['State']
            if attachmentState == 'attached':
                continue
            elif attachmentState == 'attaching':
                waiter = client.get_waiter('volume_in_use')
                try:
                    waiter.wait(VolumeIds=[v])
                    continue
                except Exception as e:
                    print(e)
                    if fromNew:
                        for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                            client.detach_volume(VolumeId=v)
                            try:
                                client.get_waiter('volume_available').wait(VolumeIds=[v])
                            except Exception as e:
                                print(e)
                                send_cloudwatch_alarm('new volumes failed to attach, couldnt even detach and delete them')
                                client.terminate_instances(InstanceId=[instanceID])
                            client.delete_volume(VolumeId=v)
                        send_cloudwatch_alarm('newly created volumes failed to attach - terminating instance')
                        client.terminate_instances(InstanceId=[instanceID])
                    else:
                        print(e)
                        for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                            client.detach_volume(VolumeId=v)
                        send_cloudwatch_alarm('pre-existing volumes we found failed to attach', instanceID)
                        sys.exit(0)
            elif attachmentState == 'detaching' or attachmentState == 'detached':
                if fromNew:
                    for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                        client.detach_volume(VolumeId=v)
                        try:
                            client.get_waiter('volume_available').wait(VolumeIds=[v])
                        except Exception as e:
                            print(e)
                            send_cloudwatch_alarm('new volumes failed to attach, couldnt even detach and delete them')
                            client.terminate_instances(InstanceId=[instanceID])
                        client.delete_volume(VolumeId=v)
                    send_cloudwatch_alarm('newly created volumes failed to attach - terminating instance')
                    client.terminate_instances(InstanceId=[instanceID])
                else:
                    print(e)
                    for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                        client.detach_volume(VolumeId=v)
                    send_cloudwatch_alarm('pre-existing volumes we found failed to attach', instanceID)
                    sys.exit(0)
        else:
            if fromNew:
                for v in volumeIDs:
                    client.detach_volume(VolumeId=v)
                    try:
                        client.get_waiter('volume_available').wait(VolumeIds=[v])
                    except Exception as e:
                        print(e)
                        send_cloudwatch_alarm('new volumes failed to attach, couldnt even detach and delete them')
                        client.terminate_instances(InstanceId=[instanceID])
                    client.delete_volume(VolumeId=v)
                send_cloudwatch_alarm('newly created volumes failed to attach - terminating instance')
                client.terminate_instances(InstanceId=[instanceID])
            else:
                print(e)
                for v in volumeIDs: #next, delete all the volumes scheduled to be attached
                    client.detach_volume(VolumeId=v)
                send_cloudwatch_alarm('pre-existing volumes we found failed to attach', instanceID)
                sys.exit(0)

        #check that all of them have an attachment state of this instanceID
def send_cloudwatch_alarm(errorMessage, instanceID=requests.get('http://169.254.169.254/latest/meta-data/instance-id').content):
    errorMessage = str(errorMessage)
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    snsClient = boto3.client('sns', clusterAZ[:-1])
    snsARN = snsClient.create_topic(Name='EBS_Attach_Script_Failure')
    snsClient.subscribe(TopicArn=snsARN['TopicArn'], Protocol='email', Endpoint='wahid.mohammed@sony.com')
    snsClient.publish(TopicArn=snsARN['TopicArn'], Message=errorMessage+' for ' + instanceID, Subject='cassandra buildout error')
    #may not need the publish code? Check during testing if double email is sent.
    cwClient = boto3.client('cloudwatch', clusterAZ[:-1])
    cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 0}])
    cwClient.put_metric_alarm(AlarmName=(instanceID+' startup error'),
    AlarmDescription=errorMessage,
    AlarmActions=[snsARN['TopicArn']],
    ComparisonOperator='LessThanThreshold',
    EvaluationPeriods=1,
    MetricName='instanceStatus',
    Namespace='instanceID',
    Period=60,
    Statistic='Minimum',
    Threshold=1,
    ActionsEnabled=True,
    )
    cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 0}])
    #The idea is that the metric data above will cause the alarm to sound.

    #ALTERNATIVE METHOD: DOCUMENTATION SAYS THIS IS ONLY FOR TESTING PURPOSES, BUT IT WORKS FOR US - DO THIS?
    #client.set_alarm_state(
    #AlarmName=instanceID+' startup error',
    #StateValue='ALARM',
    #StateReason=errorMessage
    #)


def create_volumes(num, volType, size, iops, clusterAZ, instanceID, commitLogSize, client):
    createdVols = []
    for i in range(0, num):
        attempts = 0
        while True:
            try:
                if i == 0:
                    createdVols.append(client.create_volume(
                        DryRun=False,
                        Size = commitLogSize,
                        AvailabilityZone = clusterAZ,
                        VolumeType = volType,
                        Iops=iops,
                        Encrypted=False
                    ))
                    break
                else:
                    createdVols.append(client.create_volume(
                        DryRun=False,
                        Size = size,
                        AvailabilityZone = clusterAZ,
                        VolumeType = volType,
                        Iops=iops,
                        Encrypted=False
                    ))
                    break
            except ClientError as e:
                if e.response['Error']['Code'] == 'RequestLimitExceeded' and attempts < 10:
                    sleep(random.random()*(min(300, 2*2**attempts)))
                    attempts+=1
                    continue
                else:
                    for v in createdVols: #next, delete all the volumes scheduled to be attached
                        client.delete_volume(VolumeId=v['VolumeId'])
                    send_cloudwatch_alarm('volume creation failed - resolve manually', instanceID)
                    print(e)
                    client.terminate_instances(InstanceId=[instanceID])
    return [v['VolumeId'] for v in createdVols]

def tag_volumes(volumeIDs, blockDevices, mountPoints, numInSet, instanceEnv, instanceApp, instanceName, instanceIP, client):
    for i in range (0, len(volumeIDs)):
        newTags=[{'Key':'blockDevice', 'Value': blockDevices[i]},
                    {'Key':'mountPoint', 'Value': mountPoints[i]},
                    {'Key':'numInSet', 'Value': str(numInSet)},
                    {'Key':'index', 'Value': str(i)},
                    {'Key':'Name', 'Value':instanceName+volumeIDs[i]},
                    {'Key':'env', 'Value':instanceEnv},
                    {'Key':'app', 'Value':instanceApp},
                    {'Key':'instanceIP', 'Value': instanceIP}]

        client.create_tags(Resources=[volumeIDs[i]], Tags=newTags)
def main(args):
    p = subprocess.check_output("lsblk", shell=True)
    if 'xvdj' and 'xvdk' in p:
        print "Disks are attached already."
        send_cloudwatch_alarm('someone tried to run ebs_vol_script when disks were already attached to instance')
        return None

    LOG_FILENAME = "/var/log/ebs-attach.log"
    LOG_LEVEL = logging.INFO  # Could be e.g. "DEBUG" or "WARNING"

    # Define and parse command line arguments

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
    instanceMetadata = get_instance_metadata()
    instanceID = instanceMetadata['instanceID']
    clusterAZ = instanceMetadata['clusterAZ']

    client = boto3.client('ec2', clusterAZ[:-1])
    instanceTags = get_tags(instanceID, client)
    instanceEnvironment = instanceTags['Environment']
    #instanceEnvironment = 'd1-np'
    instanceApplication = instanceTags['Application']
    instanceName = instanceTags['Name']
    instanceIP = client.describe_instances(InstanceIds=[instanceID])['Reservations'][0]['Instances'][0]['PrivateIpAddress']

    dataPaths = {'blockDevice': 'blk_dev_start', 'numInSet': 'num_vol_set', 'size':'vol_data_size', 'volType':'vol_type', 'iops':'vol_iops', 'commitLogSize':'vol_commitlog_size'}
    consulData = get_consul_data(instanceEnvironment + '/dse/cassandra/', dataPaths)
    numInSet = int(consulData['numInSet'])
    blockDevice = consulData['blockDevice']
    volType = consulData['volType']
    size = int(consulData['size'])
    iops = int(consulData['iops'])
    commitLogSize = int(consulData['commitLogSize'])
    extraFilters = {'mountPoint':[], 'blockDevice':[], 'numInSet':[], 'instanceID':[]}
    while True:
        try:
            unattachedVolumes = find_unattached_volumes(clusterAZ, instanceEnvironment, instanceApplication, extraFilters, client)
            if len(unattachedVolumes) == (numInSet):
                volumeIDsToAttach = [v['VolumeId'] for v in unattachedVolumes]
                address_of_dead_node = get_tags(volumeIDsToAttach[0], client)['instanceIP']
                cmd = "sed -i '$ a\JVM_OPTS=\"$JVM_OPTS -Dcassandra.replace_address=\"" + address_of_dead_node + "\"\"' /etc/dse/cassandra/cassandra-env.sh"
                p = subprocess.check_output(cmd, shell=True)
                fromNew=False
            else:
                if len(unattachedVolumes) > 0:
                    print('found leftover volumes from cluster; ignoring them because they are either from a partial set or earlier config.')
                    print('these volumes are:' + str(unattachedVolumes))
                volumeIDsToAttach = create_volumes(numInSet, volType, size, iops, clusterAZ, instanceID, commitLogSize, client)
                fromNew=True

            blockDevices = [blockDevice[:-1] + chr(ord(blockDevice[-1:])+i) for i in range(0, numInSet)]
            mountPoints = ['/data/cassandra/data/' + str(i).zfill(3) for i in range(0, numInSet)]
            attach_volumes(volumeIDsToAttach, instanceID, blockDevices, fromNew, client)
            while True:
                numOfAttachedVols = client.describe_volumes(Filters=[{'Name': 'attachment.status', 'Values': ['attached']}], VolumeIds=volumeIDsToAttach)['Volumes']
                if len(numOfAttachedVols) == len(volumeIDsToAttach):
                    break
                else:
                    time.sleep(5)
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'VolumeInUse':
                continue
            else:
                send_cloudwatch_alarm(e)
                print(e)
                sys.exit(e)
    print(volumeIDsToAttach)
    tag_volumes(volumeIDsToAttach, blockDevices, mountPoints, numInSet, instanceEnvironment, instanceApplication, instanceName, instanceIP, client)
    return 0

if __name__ == "__main__":
    main('')
