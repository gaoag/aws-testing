#!/usr/bin/python2.7
#
# chkconfig: 345 91 55
#!/usr/bin/env python2.7

from __future__ import print_function
import subprocess
import os
import consul
import yaml
import boto3
import stat
import time
import logging
import logging.handlers
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
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    snsClient = boto3.client('sns', clusterAZ[:-1])
    snsARN = snsClient.create_topic(Name='C8-configuration-match-failure')
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

def cassandra_yaml_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag):
    #try:
        with open('/etc/dse/cassandra/cassandra.yaml', 'r') as f:
            doc = yaml.load(f)
            cluster_name_config = str(doc["cluster_name"])
            #print(cluster_name_config)
            num_tokens_config = str(doc["num_tokens"])
            #print(num_tokens_config)
            auto_bootstrap_config = str(doc["auto_bootstrap"]).lower()
            #print(auto_bootstrap_config)
            seeds_config = str(doc["seed_provider"][0]['parameters'][0]['seeds'])
            #print(seeds_config)
            saved_caches_dir_config = str(doc["saved_caches_directory"])
            #print(saved_caches_dir_config)
            commitlog_dir_config = str(doc["commitlog_directory"])
            #print(commitlog_dir_config)
            data_dir_config = str(doc["data_file_directories"])
            #print(data_dir_config)
            keystore_config = doc["server_encryption_options"]
            print(keystore_config)
            for key, value in keystore_config.iteritems() :
                if key == 'keystore':
                    keystore_config = value
                    print(keystore_config)
                if key == 'truststore':
                    truststore_config = value
                    print(truststore_config)

        cluster_consul_path = env + "/dse/cassandra/cluster_name"
        index, data = c.kv.get(cluster_consul_path)
        cluster_name_consul =  str(data['Value'])


        if cluster_name_config == cluster_name_consul:
            print("Cluster name is " + cluster_name_config + " correct")
        else:
            print("Cluster name is not correct")
            message = message + ["Cluster name is not correct"] #Call cloudwatch alarm

        num_tokens_consul_path = env + "/dse/cassandra/num_tokens"
        index, data = c.kv.get(num_tokens_consul_path)
        num_tokens_consul =  str(data['Value'])

        if num_tokens_config == num_tokens_consul:
            print("Number of tokens is " + str(num_tokens_config) + " and correct")
        else:
            print("Number of tokens is not correct")
            message = message + ["Number of tokens is not correct"] #Call cloudwatch alarm

        auto_bootstrap_consul_path = env + "/dse/cassandra/auto_bootstrap"
        index, data = c.kv.get(auto_bootstrap_consul_path)
        auto_bootstrap_consul =  str(data['Value'])

        if auto_bootstrap_config == auto_bootstrap_consul:
            print("autboot_strap value is " + auto_bootstrap_config + " and correct")
        else:
            print("autoboot_strap value is not correct")
            message = message + ["autoboot_strap value is not correct"] #Call cloudwatch alarm

        seeds_consul_path = env + "/dse/cassandra/seeds"
        index, data = c.kv.get(seeds_consul_path)
        seeds_consul =  str((data['Value']))

        if seeds_config  == seeds_consul:
            print("seeds value is " + seeds_config + " and correct")
        else:
            print("seeds  value is not correct")
            message = message + ["seeds value is not correct"] #Call cloudwatch alarm

        saved_caches_consul_path = env + "/dse/cassandra/saved_caches_dir"
        index, data = c.kv.get(saved_caches_consul_path)
        saved_caches_consul =  str(data['Value'])

        if saved_caches_dir_config == saved_caches_consul:
            print("saved_caches_directory value is " + saved_caches_dir_config + " and correct")
        else:
            print("saved_caches_directory value is not correct")
            message = message + ["saved_caches_directory value is not correct"] #Call cloudwatch alarm

        commitlog_dir_consul_path = env + "/dse/cassandra/commitlog_dir"
        index, data = c.kv.get(commitlog_dir_consul_path)
        commitlog_dir_consul =  str(data['Value'])


        if commitlog_dir_config == commitlog_dir_consul:
            print("commitlog directory value is " + commitlog_dir_config + " and correct")
        else:
            print("commitlog directory value is not correct")
            message = message + ["commitlog directory value is not correct"] #Call cloudwatch alarm

        data_dir_consul_path = env + "/dse/cassandra/data_dirs"
        index, data = c.kv.get(data_dir_consul_path)
        data_dir_consul =  str(data['Value'])
        data_dir_consul = subprocess.check_output("echo " + "\"" + data_dir_consul + "\"" + " | awk '{print $2}' ", shell=True).strip().split()

        if str(data_dir_config) == str(data_dir_consul):
            print("data directory value is " + data_dir_config + " and correct")
        else:
            print("data directory value is not correct")
            message = message + ["data directory value is not correct"] #Call cloudwatch alarm

        #keystore & trusstore file quality check
        command = "/etc/init.d/setvault start"
        process = subprocess.call(command, shell=True)

        #command = "source /etc/profile"
        #process = subprocess.call(command, shell=True)

        command = "source /etc/profile.d/vault.sh; vault read -field=value nav/usw2_oauth_cass_" + env[0] + "0/cass-inflight_ks/" + str(keystoreTag) + " | cksum | awk '{print $1}'"
        chksum_keystore_vault = subprocess.check_output(command, shell=True).strip()
        print("chksum_keystore_vault:" + chksum_keystore_vault)

        command = "source /etc/profile.d/vault.sh; vault read -field=value nav/usw2_oauth_cass_" + env[0] + "0/cass-inflight_ks/" + str(truststoreTag) + " | cksum | awk '{print $1}'"
        chksum_truststore_vault = subprocess.check_output(command, shell=True).strip()
        print("chksum_truststore_vault" + chksum_truststore_vault)

        command = "cat /etc/dse/cassandra/security/" + str(keystoreTag) + " | base64 | cksum | awk '{print $1}'"
        chksum_keystore_file = subprocess.check_output(command, shell=True).strip()
        print("chksum_keystore_file" + chksum_keystore_file)

        command = "cat /etc/dse/cassandra/security/" + str(truststoreTag) + " | base64 | cksum | awk '{print $1}'"
        chksum_truststore_file = subprocess.check_output(command, shell=True).strip()
        print("chksum_truststore_file" + chksum_truststore_file)

        if str(chksum_keystore_file) == str(chksum_keystore_vault):
            print("keystore file is correct")
        else:
            print("keystore file is not correct")
            message = message + ["keystore file is not correct"] #Call cloudwatch alarm

        if str(chksum_truststore_file) == str(chksum_truststore_vault):
            print("truststore file is correct")
        else:
            print("truststore file is not correct")
            message = message + ["truststore file is not correct"] #Call cloudwatch alarm

        if os.path.exists(keystore_config):
            print("keystore path in yaml matched with the file path")
        else:
            print("keystore path in yaml didn't match with the file path")
            message = message + ["keystore path in yaml didn't match with the file path"] #Call cloudwatch alarm

        if os.path.exists(truststore_config):
            print("truststore path in yaml matched with the file path")
        else:
            print("truststore path in yaml didn't match with the file path")
            message = message + ["truststore path in yaml didn't match with the file path"] #Call cloudwatch alarm



        return message
    #except:
    #    return False

def cassandra_env_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag):

    try:

        max_heap_consul_path = env + "/dse/cassandra/max_heap_size"
        index, data = c.kv.get(max_heap_consul_path)
        max_heap_consul =  str(data['Value'])
        print(max_heap_consul)

        max_heap_config = str(subprocess.check_output(["sed -n -e '/^[ \t]*MAX_HEAP_SIZE/ s/.*\= *//p' /etc/dse/cassandra/cassandra-env.sh | grep -v max_heap_size_in_mb"], shell=True).strip())
        print(max_heap_config)

        if max_heap_config == "\"" + max_heap_consul + "\"":
            print("MAX_HEAP_SIZE value is " + max_heap_config + " and correct")
        else:
            print("MAX_HEAP_SIZE value is not correct")
            message = message + ["MAX_HEAP_SIZE value is not correct"] #Call cloudwatch alarm

        return message

    except:
        return False

def cassandra_rackdc_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag):

    #try:

        dc_name_api = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document").json()['region']
        rack_name_api = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content

        print(rack_name_api)
        dc_name_config = str(subprocess.check_output(["sed -n -e '/^[ \t]*dc/ s/.*\= *//p' /etc/dse/cassandra/cassandra-rackdc.properties"], shell=True).strip())
        print(dc_name_config)
        rack_name_config = str(subprocess.check_output(["sed -n -e '/^[ \t]*rack/ s/.*\= *//p' /etc/dse/cassandra/cassandra-rackdc.properties"], shell=True).strip())
        print(rack_name_config)
        #sys.exit(0)
        #return 0

        if dc_name_config == dc_name_api:
            print("DC name is " + dc_name_config + " and correct")
        else:
            print("DC name is not correct")
            message = message + ["DC name is not correct"] #Call cloudwatch alarm

        if rack_name_config == rack_name_api:
            print("Rack name is " + rack_name_config + " and correct")
        else:
            print("Rack name is not correct")
            message = message + ["Rack name is not correct"] #Call cloudwatch alarm

        return message

    #except:
    #    return False

def adddress_yaml_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag):

    try:

        with open('/var/lib/datastax-agent/conf/address.yaml', 'r') as f:
            doc = yaml.load(f)
            stomp_interface_config = str(doc["stomp_interface"])
            cassandra_rpc_interface_config = str(doc["cassandra_rpc_interface"])

        stomp_interface_consul_path = env + "/dse/cassandra/stomp_interface"
        index, data = c.kv.get(stomp_interface_consul_path)
        stomp_interface_consul =  str(data['Value'])

        cassandra_rpc_interface_api = str(requests.get("http://169.254.169.254/latest/meta-data/local-ipv4").content)

        if stomp_interface_config == stomp_interface_consul:
            print("stomp_interface value is " + stomp_interface_config + " and correct")
        else:
            print("stomp_interface value is not correct")
            message = message + ["stomp_interface value is not correct"] #Call cloudwatch alarm

        if cassandra_rpc_interface_config == cassandra_rpc_interface_api:
            print("cassandra_rpc_interface value is " + cassandra_rpc_interface_config + " and correct")
        else:
            print("cassandra_rpc_interface value is not correct")
            message = message + ["cassandra_rpc_interface value is not correct"] #Call cloudwatch alarm

        return message

    except:
        return False

def main():

    LOG_FILENAME = "/var/log/app-config-check.log"
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

    c = consul.Consul(host='localhost')
    #env = subprocess.check_output("echo $SIEENV", shell=True).strip()
    clusterAZ = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone').content
    client = boto3.client('ec2', clusterAZ[:-1])
    instanceID = requests.get('http://169.254.169.254/latest/meta-data/instance-id').content
    env = get_tags(instanceID, client)['Environment']

    volumesattached = client.describe_volumes(Filters=[{'Name': 'attachment.instance-id', 'Values': [instanceID]},{'Name': 'tag-key', 'Values':['index']}])

    #print(volumesattached)
    #sys.exit(0)
    for volume in volumesattached['Volumes']:
        if volume['Tags']:
            for tags in volume['Tags']:
                if str(tags['Key']) == "keystore":
                    keystoreTag = tags['Value']
                    print("keystore tag is " + keystoreTag)
                if str(tags['Key']) == "truststore":
                    truststoreTag = tags['Value']
                    #print("truststore tag is " + truststoreTag)
        break
    message = []
    #print(message)
    print("Checking Cassandra yaml configuration")
    message = cassandra_yaml_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag)
    #print(message)
    print("Checking Cassandra env configuration")
    message = cassandra_env_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag)
    #print(message)
    print("Checking Cassandra rack-dc configuration")
    message = cassandra_rackdc_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag)
    #print(message)
    print("Checking datastax-agent address yaml configuration")
    message = adddress_yaml_check(message,env,c,instanceID,clusterAZ,client,volumesattached,keystoreTag,truststoreTag)
    print(message)
    if len(message) > 0:
        cloudwatch_message = ','.join(message)
        print(cloudwatch_message)
        send_cloudwatch_alarm(cloudwatch_message, instanceID)

if __name__ == "__main__":
    main()
