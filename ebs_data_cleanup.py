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
import yaml

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

def send_cloudwatch_alarm(errorMessage, instanceID=requests.get('http://169.254.169.254/latest/meta-data/instance-id').content):
    errorMessage = str(errorMessage)
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    snsClient = boto3.client('sns', clusterAZ[:-1])
    snsARN = snsClient.create_topic(Name='Data_Cleanup_on_EBS_Volume_loss_failed')
    snsClient.subscribe(TopicArn=snsARN['TopicArn'], Protocol='email', Endpoint='wahid.mohammed@sony.com')
    snsClient.publish(TopicArn=snsARN['TopicArn'], Message=errorMessage+' for ' + instanceID, Subject='ebs_vol_recovery_error')
    #may not need the publish code? Check during testing if double email is sent.
    cwClient = boto3.client('cloudwatch', clusterAZ[:-1])
    cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 0}])
    cwClient.put_metric_alarm(AlarmName=(instanceID+' ebs_vol_recovery_error'),
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
    #cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 0}])
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
                print(e)
                sys.exit(0)

    for tag in resourceTagsUnformatted:
        key = tag['Key']
        value = tag['Value']
        resourceTags[key] = value

    return resourceTags

def retrieve_instance_volumes(instanceID, tableName, dbclient):
    item = dbclient.get_item(TableName=tableName, Key={'InstanceId': {'S': instanceID}})
    volumeIDs = []
    if 'Item' in item and 'VolumeIds' in item['Item']:
        for v in item['Item']['VolumeIds']['L']:
            for key, value in v.iteritems():
                volumeIDs += value
        return volumeIDs
    else:
        send_cloudwatch_alarm('finalize_ebs_tags script did not correctly dump volumeIDs into instanceVols table')
        sys.exit(0)

def mark_contaminated(volumeIDs, tableName, dbclient):
    for v in volumeIDs:
        dbclient.update_item(TableName=tableName, Key={'VolumeId': {'S': v}},
            ExpressionAttributeNames = {'#C': 'Contaminated'}, ExpressionAttributeValues={':c': {'BOOL': 'true'}},
            UpdateExpression='SET #C = :c')

def remove_from_certMapping(volumeIDs, tableName1, tableName2, dbclient):
    keyPath = dbclient.get_item(TableName=tableName1, Key={'VolumeId': {'S': volumeIDs[0]}})['Item']['keystore']['S']
    currentVolumesWithKey = []
    item = dbclient.get_item(TableName = tableName2, Key={'CertInfo': {'S': keyPath}})
    if 'Item' in item and 'VolumeIds' in item['Item']:
        for v in item['Item']['VolumeIds']['L']:
            for key, value in v.iteritems():
                currentVolumesWithKey += value
    #remove only necessary volumes from the certMapping list
    for v in volumeIDs:
        if v in currentVolumesWithKey:
            currentVolumesWithKey.remove(v)

    dbclient.update_item(TableName=tableName2, Key={'CertInfo': {'S': keyPath}},
            ExpressionAttributeNames = {'#V': 'VolumeIds'}, ExpressionAttributeValues={':v': {'L': {'S': v for v in currentVolumesWithKey}}}},
            UpdateExpression='SET #V = :v') ###BIG QUESTION: HOW DOES SECONDARY INDEXING WORK?
def update_dse_status(instanceID, tableName, dbclient):
    dbclient.update_item(TableName=tableName, Key={'InstanceId': {'S': instanceID}},
            ExpressionAttributeNames = {'#E': 'Status'}, ExpressionAttributeValues = {':e': {'S': 'Down'}},
            UpdateExpression='SET #E = :e')
def main():
    '''
    BIG NOTE: MOdify main method to accept input from cron job - boolean for whether we replace all volumes or just commitlog
    '''

    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    client = boto3.client('ec2', clusterAZ[:-1])
    dbclient = boto3.client('dynamodb', clusterAZ[:-1])
    line = get_tags(instanceID, client)['Environment']
    index, clusterName = (consul.Consul(host="localhost")).kv.get(line + '/dse/cassandra/cluster_name')
    clusterName = 'oauth'
    #clusterName = str(clusterName['Value'])
    instanceVolsTable = line+'_'+clusterName+'_InstanceVols'
    volInfoTable = line+'_'+clusterName+'_EbsVolInfo'
    certMappingTable = line+'_'+clusterName+'_certMapping'
    dseStatusTable = line+'_'+clusterName+'_InstanceStatuses'

    volumeIDsToRemove = retrieve_instance_volumes(instanceID, instanceVolsTable, dbclient)
    remove_from_certMapping(volumeIDsToRemove, volInfoTable, certMappingTable, dbclient)
    mark_contaminated(volumeIDsToRemove, volInfoTable, dbclient)
    update_dse_status(instanceID, dseStatusTable, dbclient)



if __name__ == '__main__':
    main()
