from botocore.exceptions import ClientError
import os
import sys
import boto3
import consul
import requests

def send_cloudwatch_alarm(errorMessage, instanceID=requests.get('http://169.254.169.254/latest/meta-data/instance-id').content):
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    snsClient = boto3.client('sns', clusterAZ[:-1])
    snsARN = snsClient.create_topic(Name='Keystore Script Failure')
    snsClient.subscribe(TopicARN=snsARN, Protocol='email', Endpoint='alex.gao@sony.com')
    snsClient.publish(TopicARN=snsARN, Message=errorMessage+' for ' + instanceID, Subject='cassandra buildout error')
    #may not need the publish code? Check during testing if double email is sent.
    cwClient = boto3.client('cloudwatch', clusterAZ[:-1])
    cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 1}])
    cwClient.put_metric_alarm(AlarmName=(instanceID+' startup error'),
    AlarmDescription=errorMessage,
    AlarmActions=[snsARN],
    ComparisonOperator='LessThanThreshold',
    EvaluationPeriods=1,
    MetricName='instanceStatus',
    Namespace='instanceID',
    Period=10,
    Statistic='Minimum',
    Threshold=1,
    ActionsEnabled=True,
    )

    cwClient.put_metric_data(Namespace='instanceID', MetricData=[{'MetricName': 'instanceStatus', 'Value': 0}])


send_cloudwatch_alarm('test message')
