#!/usr/bin/python2.7
#
# description: EBS recovery script
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
import ebs_data_cleanup
import reattach_failed_vol
import initdisk
import system_health_check
import keystore_script
import dse_status_check

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

def retrieve_instance_volumes(instanceID, tableName, dbclient):
    entry = dbclient.get_item(TableName=tableName, Key={'InstanceId': {'S': instanceID}})
    try:
        for v in entry['Item']['VolumeIds']['L']:
            for key, value in v.iteritems():
                volumeIDs += value
        return volumeIDs
    except KeyError as e:
        send_cloudwatch_alarm('finalize_ebs_tags script did not correctly dump volumeIDs into instanceVols table, restarting everything for ' + instanceID)
        #go into recovery mode (delete volumes, dump data, master script)

def vol_exists_mount_check(path, target):
    try:
        disk_present = stat.S_ISBLK(os.stat("/dev/" + path).st_mode)
        if disk_present:
            print(path + " disk exist")
            p = subprocess.check_output("lsblk", shell=False)
            if target in p:
                print(path + " volume is mounted on " + target)
                return True
            else:
                print(path + " volume is not mounted on " + target)
                return False
        else:
            return False
    except OSError:
        pass

def check_for_failed_volumes(instanceID, client, dbclient, line, clusterName):
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

    tableName = line+'_'+clusterName+'_InstanceVols'
    volsThatShouldExist = retrieve_instance_volumes(instanceID, tableName, dbclient)
    failedVolumes = []
    volDeviceMap = {}

    for v in volsThatShouldExist:
        try:
            device = dbclient.get_item(tableName=(line+'_'+clusterName+'_EbsVolInfo'), Key={'VolumeId': {'S':v}}, ConsistentRead=True))['Item']['blockDevice']['S']
            volDeviceMap[device] = v
        except KeyError as e:
            send_cloudwatch_alarm('finalize_ebs_tags script did not correctly dump volume tags into EbsVolInfo table, restarting everything for ' + instanceID)
    volsThatActuallyExist = []

    for disk in device_map:
        print("Checking existence and mounting of" + disk['device'])
        volExists = vol_exists_mount_check(disk['device'], disk['target'])
        if not volExists and disk['device'] in volDeviceMap:
            failedVolumes += volDeviceMap[disk['device']]

    return failedVolumes


def main():
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    client = boto3.client('ec2', clusterAZ[:-1])
    dbclient = boto3.client('dynamodb', clusterAZ[:-1])
    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    instanceTags = get_tags(instanceID, client)
    line = instanceTags['Environment']
    index, clusterName = (consul.Consul(host="localhost")).kv.get(line + '/dse/cassandra/cluster_name')
    clusterName = 'oauth'
    #clusterName = str(clusterName['Value'])
    while True:
        if dse_status_check.main():
            failedVolumes = check_for_failed_volumes(instanceID, client, dbclient, line, clusterName)
            if len(failedVolumes) == 0:
                send_cloudwatch_alarm('dse stopped, but no unmounted volumes for ' + instanceID)
                continue
            else:
                reattachmentSuccess = reattach_failed_vol.main(failedVolumes)
                if not reattachmentSuccess:
                    for v in failedVolumes:
                        try:
                            mountPoint = dbclient.get_item(TableName=line+'_'+clusterName+'_EbsVolInfo'), Key={'VolumeId': {'S':v}})['Item']['mountPoint']['S']
                            if '000' in mountPoint:
                                replace_commit_log.main()
                            else:
                                replace_all_volumes.main()
                        except:
                            send_cloudwatch_alarm('finalize_ebs_tags script did not correctly dump volume tags, restarting everything for ' + instanceID)

        else:
            #wait
            time.sleep(300)

if __name__ == '__main__':
    main()
