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
import dse_status_check

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
    snsARN = snsClient.create_topic(Name='dse_start_status_error')
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
                time.sleep(random.random()*(min(300, 2*2**attempts)))
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


def start_cassandra(tableName, instanceID, dbclient):
    attempts = 0
    while True:
        try:
            status=(dbclient.get_item(TableName=tableName, Key={'DSE_Starter_Status': {'S':'Running'}}, ConsistentRead=True))['Item']['InstanceIds']['S'] #check this syntax again
            if status == 'Maintenance':
                print('DSE Startup is currently in maintenance mode; instance' + instanceID + ' is ready to start DSE manually')
                sys.exit(0)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                attempts+=1
                time.sleep(random.random()*(min(3000, 2*2**attempts)))
                continue
            else:
                print(e)
                send_cloudwatch_alarm(e)
                sys.exit(0)

    attempts = 0
    while True:
        try:
            dbclient.update_item(TableName=tableName, Key={'DSE_Starter_Status' : {'S': 'Running'}},
                ExpressionAttributeNames = {'#I': 'InstanceIds'}, ExpressionAttributeValues={':i': {'S': instanceID}, ':p': {'S': 'Unlocked'}},
                ReturnValues='NONE',
                UpdateExpression='SET #I = :i ',
                ConditionExpression='attribute_not_exists(InstanceIds) OR #I = :p')
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException' or e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                attempts+=1
                time.sleep(random.random()*(min(3000, 2*2**attempts)))
                continue
            else:
                print(e)
                send_cloudwatch_alarm(e)
                sys.exit(0)
                break
    print('sending start command')
    subprocess.call(['service', 'dse', 'start'])
    print('sent start command for instance ' + instanceID + ' which took ' + str(attempts) + ' attempts')
    subprocess.call(['service', 'datastax-agent', 'start'])
    print('sent datastax-agent start command')
    return True

def unlock_dse_table(tableName, instanceID, dbclient):
    attempts = 0
    while True:
        try:
            dbclient.update_item(TableName=tableName, Key={'DSE_Starter_Status' : {'S': 'Running'}},
                ExpressionAttributeNames = {'#I': 'InstanceIds'}, ExpressionAttributeValues={':i': {'S': instanceID}, ':p': {'S': 'Unlocked'}},
                ReturnValues='NONE',
                UpdateExpression='SET #I = :p ',
                ConditionExpression='#I = :i')
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print('you messed up dude. somehow, while you had locked the table with your instanceID, somebody else got in and changed it.')
                send_cloudwatch_alarm('table was locked, but written by another instance anyways')
                sys.exit(0)
            elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                attempts+=1
                sleep(random.random()*(min(3000, 2*2**attempts)))
                continue
            else:
                print(e)
                send_cloudwatch_alarm(e)
                sys.exit(0)
                break

def check_and_record_status(tableName, instanceID, dbclient):
    attempts = 0
    while True and attempts < 5:
        time.sleep(60)
        if (not dse_status_check.main()): ###WHILE Loop here to retry - shoot for 10 minutes of retry here
            status = 'Running'
            break
        else:
            status = 'Failed'
            attempts+=1

    attempts = 0
    while True:
        try:
            dbclient.update_item(TableName=tableName, Key={'InstanceId' : {'S': instanceID}},
                ExpressionAttributeNames = {'#S': 'Status'}, ExpressionAttributeValues={':s': {'S': status}},
                ReturnValues='NONE',
                UpdateExpression='SET #S = :s ')
            break
        except ClientError as e:
            if e.response['Error']['Code'] == e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                attempts+=1
                sleep(random.random()*(min(3000, 2*2**attempts)))
                continue
            else:
                print(e)
                send_cloudwatch_alarm(e)
                sys.exit(0)
                break

def main():
    LOG_FILENAME = "/var/log/dse-startup.log"
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
    #N O T E: THE TABLE CREATION MUST BE IN ANOTHER SCRIPT AND CENTRALLY STORED SOMEWHERE?
    # Replace stdout with logging to file at INFO level
    sys.stdout = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)
    if (not dse_status_check.main()):
        print('somehow, cassandra is already running')
        send_cloudwatch_alarm('dse_startup_script was called on an instance that was already started')
        sys.exit(0)

    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    client = boto3.client('ec2', clusterAZ[:-1])
    instanceTags = get_tags(instanceID, client)
    dbclient = boto3.client('dynamodb', clusterAZ[:-1])
    get_tags(instanceID, client)
    line = instanceTags['Environment']
    #line = 'd1-np'
    index, clusterName = (consul.Consul(host='localhost')).kv.get(line + '/dse/cassandra/cluster_name')
    #clusterName = str(clusterName['Value'])
    clusterName = 'oauth'
    startupStatusTable = line+'_'+clusterName+'_DSEStatus'
    instanceStatusTable = line+'_'+clusterName+'_InstanceStatuses'


    start_cassandra(startupStatusTable, instanceID, dbclient)

    check_and_record_status(instanceStatusTable, instanceID, dbclient)

    unlock_dse_table(startupStatusTable, instanceID, dbclient)

if __name__ == "__main__":


    main()
