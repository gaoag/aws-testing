#!/usr/bin/env python2.7

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
import initdisk

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

def main(volumeIDsToReattach):
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    client = boto3.client('ec2', clusterAZ[:-1])
    dbclient = boto3.client('dynamodb', clusterAZ[:-1])
    tableName = line+'_'+clusterName+'_EbsVolInfo'
    for v in volumeIDsToReattach:
        try:
            blockDevice = get_tags(v, client)['blockDevice']
        except KeyError as e:
            print('detached volumes could not be reattached')
            print('blockDevice missing from its tags and from the EbsVolInfo table')

        try:
            client.attach_volume(VolumeIds=v, InstanceIds=instanceID, Device=blockDevice)
        except ClientError as e:
            return False

        try:
            attachmentState = client.describe_volumes(VolumeIds=[v])['Volumes'][0]['Attachments'][0]['State']
        except KeyError as e:
            return False

        if attachmentState == 'attached':
            continue
        elif attachmentState == 'attaching':
            waiter = client.get_waiter('volume_in_use')
            try:
                waiter.wait(VolumeIds=[v])
                continue
            except Exception as e:
                print(e)
                return False
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

    return True
if __name__ == '__main__':
    main('')
