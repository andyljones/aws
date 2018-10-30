from io import BytesIO
import re
import time
import os
import json
import boto3
import subprocess
import shlex
import base64
import logging
from io import StringIO
import pandas as pd

IMAGE_ID = 'ami-4cd4fd3f'

AWS_IMAGE = 'ami-065ef4c5569ad0325'
INSTANCE_TYPE = 't3.xlarge'

log = logging.getLogger(__name__)


def config(key):
    config = {**json.load(open('config.json')), **json.load(open('credentials.json'))}
    assert set(config) == {'AWS_KEY', 'AWS_SECRET', 'REGION', 'AVAILABILITY_ZONE', 'KEYPAIR', 'SSH_GROUP', 'MUTUAL_ACCESS_GROUP'}
    return config[key]

_ec2 = None
def ec2():
    global _ec2
    if _ec2 is None:
        _ec2 = boto3.resource('ec2', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_KEY'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _ec2

_ssm = None
def ssm():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client('ssm', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_KEY'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _ssm

def cloud_config():
    return """#!/bin/bash""" 

def create_instance(name, n=1, image=None):
    image_id = AWS_IMAGE if image is None else image
    
    return ec2().create_instances(ImageId=image_id, 
                                MinCount=n, 
                                MaxCount=n, 
                                KeyName=config('KEYPAIR'),
                                UserData=cloud_config(),
                                SecurityGroups=[config('SSH_GROUP'), config('MUTUAL_ACCESS_GROUP')],
                                InstanceType=INSTANCE_TYPE,
                                Placement={'AvailabilityZone': config('AVAILABILITY_ZONE')})

def request_spot(name, config, bid, n=1, image=None):
    image_id = AWS_IMAGE if image is None else image
    
    requests = ec2().meta.client.request_spot_instances(
                    SpotPrice=str(bid),
                    InstanceCount=n,
                    LaunchSpecification=dict(
                         ImageId=image_id,
                         KeyName=config('KEYPAIR'),
                         UserData=base64.b64encode(cloud_config().encode()).decode(),
                         SecurityGroups=[config('SSH_GROUP'), config('MUTUAL_ACCESS_GROUP')],
                         InstanceType=INSTANCE_TYPE,
                         Placement={'AvailabilityZone': config('AVAILABILITY_ZONE')}
                    ))
    
    request_ids = [r['SpotInstanceRequestId'] for r in requests['SpotInstanceRequests']]
    while True:
        try:
            desc = ec2().meta.client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
            states = [d['Status']['Code'] for d in desc['SpotInstanceRequests']]
            print('States: {}'.format(', '.join(states)))
            if all([s == 'fulfilled' for s in states]):
                break
        except boto3.exceptions.botocore.client.ClientError as e:
            print('Exception while waiting for spot requests: {}'.format(e))
        time.sleep(5)
    
    instances = [ec2().Instance(d['InstanceId']) for d in desc['SpotInstanceRequests']]
    return instances
    
def reboot_and_create_image(instance, name='python-ec2'):
    return instance.create_image(Name=name, NoReboot=False)
    
def set_name(instance, name):
    ec2().create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': name}])

def as_dict(tags):
    return {t['Key']: t['Value'] for t in tags} if tags else {}
    
def list_instances():
    instances = {}
    for i in ec2().instances.all():
        instances.setdefault(as_dict(i.tags).get('Name', 'NONE'), []).append(i)
    return instances

def console_output(instance):
    print(instance.console_output().get('Output', 'No output'))

def command(instance, command):
    c = ssm().send_command(
            InstanceIds=[instance.instance_id], 
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': [command]})
    return c

def cloud_init_output(instance):
    return command(instance, 'cat /var/log/cloud-init.log')
    
def tunnel_alive():
    return subprocess.call(shlex.split("nc -z localhost 8786")) == 0

def collapse(s):
    return re.sub('\n\s+', ' ', s, flags=re.MULTILINE)

def tunnel(instance):
    TUNNEL = collapse(f"""
        ssh -N 
        -L 8785:localhost:8785
        -L 8786:localhost:8786
        -o StrictHostKeyChecking=no
        -o UserKnownHostsFile=/dev/null
        -i ~/.ssh/{config('KEYPAIR')}.pem
        ec2-user@{instance.public_ip}""")

    if tunnel_alive():
        print('Tunnel already created')
        return

    os.makedirs('logs', exist_ok=True)
    log = open('logs/tunnel.log', 'wb')
    p = subprocess.Popen(shlex.split(TUNNEL), stdout=log, stderr=subprocess.STDOUT)
    for _ in range(5):
        time.sleep(1)
        if tunnel_alive():
            print('Tunnel created')
            return

    p.kill()
    print('Failed to establish tunnel; check logs/tunnel.log for details')

def ssh(instance):
    print(collapse(f"""
        ssh 
        -i "~/.ssh/{config('KEYPAIR')}.pem" 
        -o StrictHostKeyChecking=no 
        -o UserKnownHostsFile=/dev/null 
        ec2-user@{instance.public_ip}"""))

def rsync(instance):
    """Need to install inotifytools with spaceman first"""
    RSYNC = collapse(f"""
        while true; do
        rsync -az --progress
        -e "ssh 
            -i "~/.ssh/{config('KEYPAIR')}.pem" 
            -o StrictHostKeyChecking=no 
            -o UserKnownHostsFile=/dev/null"
        --filter=':- .gitignore'
        --exclude .git
        .
        ec2-user@{instance.public_ip}:code;
        sleep 1;
        inotifywait -r -e modify,attrib,close_write,move,create,delete .;
        done""")
    
    os.makedirs('logs', exist_ok=True)
    log = open('logs/rsync.log', 'wb')
    p = subprocess.Popen(RSYNC, stdout=log, stderr=subprocess.STDOUT, shell=True)
