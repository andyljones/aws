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

log = logging.getLogger(__name__)

CLOUD_CONFIG = """
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && chmod u+x miniconda.sh 
./miniconda.sh -b -p $HOME/miniconda
echo 'export PATH="$PATH:$HOME/miniconda/bin"' >> ~/.bashrc
source ~/.bashrc
conda install jupyter --yes
nohup ipython kernel -f kernel.json >/dev/null 2>&1 &
"""

def config(key):
    config = {**json.load(open('config.json')), **json.load(open('credentials.json'))}
    return config[key]

_ec2 = None
def ec2():
    global _ec2
    if _ec2 is None:
        _ec2 = boto3.resource('ec2', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_ID'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _ec2

_ssm = None
def ssm():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client('ssm', region_name=config('REGION'), 
                              aws_access_key_id=config('AWS_ID'), 
                              aws_secret_access_key=config('AWS_SECRET'))
    
    return _ssm

def instance_spec(**kwargs):
    return {'ImageId': config('IMAGE'),
            'KeyName': config('KEYPAIR'),
            'SecurityGroups': [config('SSH_GROUP'), config('MUTUAL_ACCESS_GROUP')],
            'InstanceType': config('INSTANCE'),
            'Placement': {'AvailabilityZone': config('AVAILABILITY_ZONE')},
            'UserData': '#!/bin/bash\n' + kwargs.get('CONFIG', ''),
            **kwargs}

def create_instance(name, **kwargs):
    spec = instance_spec(**kwargs)
    return ec2().create_instances(MinCount=1, MaxCount=1, **spec)[0]

def request_spot(name, bid, **kwargs):
    spec = instance_spec(**kwargs)
    spec['UserData'] = base64.b64encode(spec['UserData'].encode()).decode()
    requests = ec2().meta.client.request_spot_instances(
                    InstanceCount=1,
                    SpotPrice=str(bid),
                    LaunchSpecification=spec)
    
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
    return instances[0]
    
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
    print(instance.console_output().get('Output', 'No output yet'))

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
    command = collapse(f"""
        ssh -N 
        -L 8785:localhost:8785
        -L 8786:localhost:8786
        -o StrictHostKeyChecking=no
        -o UserKnownHostsFile=/dev/null
        -i ~/.ssh/{config('KEYPAIR')}.pem
        ec2-user@{instance.public_ip_address}""")

    if tunnel_alive():
        print('Tunnel already created')
        return

    os.makedirs('logs', exist_ok=True)
    log = open('logs/tunnel.log', 'wb')
    p = subprocess.Popen(shlex.split(command), stdout=log, stderr=subprocess.STDOUT)
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
        ec2-user@{instance.public_ip_address}"""))

def scp(instance, path):
    command = collapse(f"""
        scp 
        -i "~/.ssh/{config('KEYPAIR')}.pem" 
        -o StrictHostKeyChecking=no 
        -o UserKnownHostsFile=/dev/null 
        ec2-user@{instance.public_ip_address}:/{path}
        .""")
    subprocess.check_call(shlex.split(command))
    pass


def rsync(instance):
    """Need to install inotifytools with spaceman first"""
    command = collapse(f"""
        while true; do
        rsync -az --progress
        -e "ssh 
            -i "~/.ssh/{config('KEYPAIR')}.pem" 
            -o StrictHostKeyChecking=no 
            -o UserKnownHostsFile=/dev/null"
        --filter=':- .gitignore'
        --exclude .git
        .
        ec2-user@{instance.public_ip_address}:code;
        sleep 1;
        inotifywait -r -e modify,attrib,close_write,move,create,delete .;
        done""")
    
    os.makedirs('logs', exist_ok=True)
    log = open('logs/rsync.log', 'wb')
    p = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, shell=True)

def example():
    import aws

    # Set up your credentials.json and config.json file first. 

    # Then request a spot instance!
    instance = aws.request_spot('python', .15)

    # Once you've got it, can check how boot is going with
    aws.console_output(instance) # uses the API, can take a while to catch up

    # Fetch the jupyter kernel file
    scp(instance, '/home/ec2-user/.local/share/jupyter/runtime/kernel.json')