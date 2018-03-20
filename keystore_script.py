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

#N O T E: THE TABLE CREATION MUST BE IN ANOTHER SCRIPT AND CENTRALLY STORED SOMEWHERE?

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
    snsARN = snsClient.create_topic(Name='Keystore_Script_Failure')
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



def scan_for_available_cert(tableName, volumesOwnedByInstance, dbclient):
    counter = 1
    attempts = 0

    print(tableName)
    while True:
        try:
            entry = dbclient.get_item(TableName=tableName, Key={'SerialNumber': {'N':str(counter)}}, ConsistentRead=True)
            print(entry)
            if not 'Item' in entry:
                print('successful detection and reset of too-large counter')
                counter = 1
            dbclient.update_item(TableName=tableName, Key={'SerialNumber' : {'N': str(counter)}},
                ExpressionAttributeNames = {'#V': 'VolumeIds'}, ExpressionAttributeValues={':c':{'N':'0'},':v': {'L': [{'S': v['VolumeId']} for v in volumesOwnedByInstance]}},
                ReturnValues='NONE',
                UpdateExpression='SET #V = :v ',
                ConditionExpression='attribute_not_exists(VolumeIds) OR size(VolumeIds) = :c')
            print('successfully obtained free key after ' + str(attempts) + ' attempts')
            return counter
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                counter+=1
                continue
            elif e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                time.sleep(random.random()*(min(3000, 2*2**attempts)))
                attempts+=1
                continue
            else:
                print(e)
                sys.exit(0)

def retrieve_cert_path(serialNumber, tableName, dbclient): #LOOP
    certPath = (dbclient.get_item(TableName=tableName, Key={'SerialNumber': {'N':str(serialNumber)}}, ConsistentRead=True))['Item']['CertInfo']['S'] #check this syntax again
    return certPath

def save_and_decrypt_key(certPath, line): ###certPath is something like keystore.node_d_aws_623
    filePath = '/etc/dse/cassandra/security/' + certPath ###NOTE: PARAMETERIZE THE LINE IN THE PATH BELOW

    vaultReadCmd = "source /etc/profile.d/vault.sh; vault read -field=value nav/usw2_oauth_cass_" + line[:1] + "0/cass-inflight_ks/" + certPath + " | base64 -d > " + filePath
    subprocess.call(vaultReadCmd, shell=True)
    filePath2 = '/etc/dse/cassandra/security/truststore_oauth'
    vaultReadCmd2 = "source /etc/profile.d/vault.sh; vault read -field=value nav/usw2_oauth_cass_" + line[:1] + "0/cass-inflight_ks/" + 'truststore_oauth' + " | base64 -d > " + filePath2

    #sys.exit(0)
    subprocess.call(vaultReadCmd2, shell=True)

    subprocess.call('chown cassandra:cassandra -R /etc/dse/cassandra/security; chmod 700 -R /etc/dse/cassandra/security', shell=True)
    #vaultReadCmd = 'source /etc/profile; vault read -field=value nav/usw2_oauth_cass_' + line[:1] + '0/cass-inflight_ks/' + certPath
    #print(vaultReadCmd)
    #vaultReadCmd = vaultReadCmd.split()
    #with open(filePath,'w') as fout:
    #    subprocess.call(vaultReadCmd, stdout=fout)
    #    cat = subprocess.Popen(['cat', filePath], stdout=subprocess.PIPE)
    #    subprocess.call(['base64', '-d'], stdin = cat.stdout, stdout=fout)

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

def main():
    LOG_FILENAME = "/var/log/retrieve-store-key.log"
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
    sys.stdout = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)

    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    client = boto3.client('ec2', clusterAZ[:-1])
    instanceTags = get_tags(instanceID, client)
    line = instanceTags['Environment']
    index, clusterName = (consul.Consul(host="localhost")).kv.get(line + '/dse/cassandra/cluster_name')
    clusterName = 'oauth'
    #clusterName = str(clusterName['Value'])
    tableName = line+'_'+clusterName+'_certMapping'
    volumesOwnedByInstance = client.describe_volumes(Filters=[{'Name': 'attachment.instance-id', 'Values':[instanceID]}, {'Name': 'tag-key', 'Values':['numInSet']}])['Volumes']
    dbclient = boto3.client('dynamodb', clusterAZ[:-1])

    v = volumesOwnedByInstance[0]
    vTags = get_tags(v['VolumeId'], client)
    #need to check two volumes in case you accidentally pick up root volume
    if 'keystore' in vTags:
        certPath = vTags['keystore']
        print('keystore already in volume tag')
    else:
        nextAvailableCert = scan_for_available_cert(tableName, volumesOwnedByInstance, dbclient)
        certPath = retrieve_cert_path(nextAvailableCert, tableName, dbclient)
        print('retrieved certPath ' + certPath)
    subprocess.call(['/etc/init.d/setvault', 'start'])
    #subprocess.call(['source', '/etc/profile'])
    save_and_decrypt_key(certPath, line)
    truststorePath = 'truststore_oauth'

    subprocess.call('/etc/init.d/setkeypath.sh' + ' ' + certPath + ' ' + truststorePath, shell=True)
    #with open('/etc/dse/cassandra/cassandra.yaml', 'rw') as f:
    #    doc = yaml.load(f)
    #    keystore_config = doc["server_encryption_options"]
    #    print(keystore_config)
    #    for key in keystore_config.keys():
    #        if key == "keystore":
    #            keystore_config[key] = "/etc/dse/cassandra/security/" + certPath
    #        if key == "truststore":
    #            keystore_config[key] = "/etc/dse/cassandra/security/" + trustStorePath
    #    print(keystore_config)
    #    doc['server_encryption_options'] = keystore_config
    #    with open('/etc/dse/cassandra/cassandra.yaml', 'w') as f:
    #        yaml.safe_dump(doc, f)
        #doc["server_encryption_options"] = state
        #with open('/etc/dse/cassandra/cassandra.yaml', 'w') as f:
        #    yaml.dump(doc, f)

    newTags = [{'Key':'keystore', 'Value': certPath}, {'Key': 'truststore', 'Value': truststorePath}]
    client.create_tags(Resources=[v['VolumeId'] for v in volumesOwnedByInstance],Tags=newTags)

    return 0
if __name__ == "__main__":
    main()
