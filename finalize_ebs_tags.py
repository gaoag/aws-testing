#!/usr/bin/env python2.7

from botocore.exceptions import ClientError #add cloudwatch alarms
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
def main():
    LOG_FILENAME = "/var/log/finalize-and-save-tags.log"
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
    #N O T E: THE TABLE CREATION MUST BE IN ANOTHER SCRIPT AND CENTRALLY STORED SOMEWHERE?
    # Replace stdout with logging to file at INFO level
    sys.stdofinut = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)

    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    client = boto3.client('ec2', clusterAZ[:-1])
    dbclient = boto3.client('dynamodb', clusterAZ[:-1])
    volumesOwnedByInstance = client.describe_volumes(Filters=[{'Name': 'attachment.instance-id', 'Values':[instanceID]}])['Volumes']
    line = get_tags(instanceID, client)['Environment']
    index, clusterName = (consul.Consul(host='localhost')).kv.get(line + '/dse/cassandra/cluster_name')
    clusterName = 'oauth'
    #clusterName = str(clusterName['Value'])
    tagsTableName = line+'_'+clusterName+'_EbsVolInfo'
    instanceVolsTableName = line+'_'+clusterName+'_InstanceVols'
    dbclient.update_item(TableName=instanceVolsTableName, Key={'InstanceId': {'S': instanceID}},
        ExpressionAttributeNames = {'#L': 'VolumeIds'}, ExpressionAttributeValues={':l': {'L': [{'S': v['VolumeId']} for v in volumesOwnedByInstance]}},
        UpdateExpression='SET #L = :l '))

    for v in volumesOwnedByInstance:
        client.create_tags(Resources=[v['VolumeId']], Tags=[{'Key':'instanceID', 'Value': instanceID}])
        instanceIDExists = False
        while not instanceIDExists:
            newTags = get_tags(v['VolumeId'], client)
            if 'instanceID' in newTags:
                instanceIDExists = True
            time.sleep(5)
        for key, value in newTags.iteritems():
            dbclient.update_item(TableName=tagsTableName, Key={'VolumeId' : {'S': v['VolumeId']}},
                ExpressionAttributeNames = {'#T': key}, ExpressionAttributeValues={':t': {'S': str(value)}},
                UpdateExpression='SET #T = :t ')
        dbclient.update_item(TableName=tagsTableName, Key={'VolumeId' : {'S': v['VolumeId']}},
            ExpressionAttributeNames = {'#T': 'Contaminated'}, ExpressionAttributeValues={':t': {'BOOL': 'false'}},
            UpdateExpression='SET #T = :t ')
if __name__ == "__main__":
    main()
