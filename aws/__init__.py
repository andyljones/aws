from pathlib import Path
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
log.setLevel('DEBUG')

INITIAL_CONFIG = """
cd /home/ec2-user
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && chmod u+x miniconda.sh 
./miniconda.sh -b -p miniconda && chown -R ec2-user:ec2-user miniconda
rm miniconda.sh
echo 'export PATH="$PATH:$HOME/miniconda/bin"' >> .bashrc
mkdir code && chown -R ec2-user:ec2-user code
su ec2-user -l -c '
    conda install jupyter --yes
    cd ~/code
    nohup ipython kernel -f kernel.json >~/kernel.log 2>&1 &
'
"""

CONFIG = """
su ec2-user -l -c '
    cd ~/code
    nohup ipython kernel -f kernel.json >~/kernel.log 2>&1 &
'
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

def instance_spec(image=None, script=None, **kwargs):
    defaults = {'ImageId': config('IMAGE'),
                'KeyName': config('KEYPAIR'),
                'SecurityGroups': [config('SSH_GROUP'), config('MUTUAL_ACCESS_GROUP')],
                'InstanceType': config('INSTANCE'),
                'Placement': {'AvailabilityZone': config('AVAILABILITY_ZONE')},
                'UserData': '#!/bin/bash\n'}
    
    if image:
        defaults['ImageId'] = images()[image].id
    if script:
        defaults['UserData'] = defaults['UserData'] + script

    return defaults

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
            log.info('States: {}'.format(', '.join(states)))
            if all([s == 'fulfilled' for s in states]):
                break
        except boto3.exceptions.botocore.client.ClientError:
            log.info(f'Exception while waiting for spot requests')
            raise
        time.sleep(5)
    
    instances = [ec2().Instance(d['InstanceId']) for d in desc['SpotInstanceRequests']]
    return instances[0]
    
def create_image(instance, name='python-ec2'):
    return instance.create_image(Name=name, NoReboot=False)
    
def set_name(instance, name):
    ec2().create_tags(Resources=[instance.id], Tags=[{'Key': 'Name', 'Value': name}])

def as_dict(tags):
    return {t['Key']: t['Value'] for t in tags} if tags else {}
    
def instances():
    instances = {}
    for i in ec2().instances.all():
        instances.setdefault(as_dict(i.tags).get('Name', 'NONE'), []).append(i)
    return instances

def images():
    return {i.name: i for i in ec2().images.filter(Owners=['self']).all()}

def console_output(instance):
    print(instance.console_output().get('Output', 'No output yet'))

def collapse(s):
    return re.sub('\n\s+', ' ', s, flags=re.MULTILINE)

def ssh_options():
    return collapse(f"""
        -i "~/.ssh/{config('KEYPAIR')}.pem" 
        -o StrictHostKeyChecking=no 
        -o UserKnownHostsFile=/dev/null""")

def host(instance):
    return f"ec2-user@{instance.public_ip_address}"

def ssh(instance):
    print(collapse(f"""ssh {ssh_options()} {host(instance)}"""))

def command(instance, command):
    c = collapse(f"""ssh {ssh_options()} {host(instance)} {command}""")
    return subprocess.call(shlex.split(c))

def command_output(instance, command):
    c = collapse(f"""ssh {ssh_options()} {host(instance)} {command}""")
    return subprocess.check_output(shlex.split(c))

def cloud_init_output(instance):
    print(command_output(instance, 'tail -n100 /var/log/cloud-init-output.log').decode())

def await_boot(instance):
    while True:
        log.info('Awaiting boot')
        if command(instance, 'cat /var/lib/cloud/instance/boot-finished') == 0:
            log.info('Booted')
            return
        time.sleep(1)

def scp(instance, path):
    Path('./cache').mkdir(exist_ok=True, parents=True)
    command = collapse(f"""scp {ssh_options()} {host(instance)}:/{path} ./cache""")
    subprocess.check_call(shlex.split(command))

def rsync(instance):
    """Need to install fswatch with brew first"""
    log.info('Establishing rsync')
    subcommand = collapse(f"""
                rsync -az --progress
                -e "ssh {ssh_options()}"
                --filter=':- .gitignore'
                --exclude .git
                .
                {host(instance)}:code""")

    command = collapse(f"""
        {subcommand}
        fswatch -o  -l 1 . | while read f; do {subcommand}; done""")
    
    os.makedirs('logs', exist_ok=True)
    logs = open('logs/rsync.log', 'wb')
    p = subprocess.Popen(command, stdout=logs, stderr=subprocess.STDOUT, shell=True)
    return p

def kernel_config(instance):
    scp(instance, '/home/ec2-user/.local/share/jupyter/runtime/kernel.json')
    return json.loads(Path('cache/kernel.json').read_text())

def tunnel_alive(port):
    return subprocess.call(shlex.split(f"nc -z 127.0.0.1 {port}")) == 0

def tunnel(instance):
    log.info('Establishing tunnel')
    kernel = kernel_config(instance)
    ports = ' '.join(f'-L {v}:localhost:{v}' for k, v in kernel.items() if k.endswith('_port'))
    command = collapse(f"ssh -N {ports} {ssh_options()} {host(instance)}")

    if tunnel_alive(kernel['control_port']):
        log.info('Tunnel already created')
        return

    os.makedirs('logs', exist_ok=True)
    logs = open('logs/tunnel.log', 'wb')
    p = subprocess.Popen(shlex.split(command), stdout=logs, stderr=subprocess.STDOUT)
    for _ in range(20):
        time.sleep(1)
        if tunnel_alive(kernel['control_port']):
            log.info('Tunnel created')
            return p

    p.kill()
    raise IOError('Failed to establish tunnel; check logs/tunnel.log for details')

def remote_console():
    command = 'ipython qtconsole --existing ./cache/kernel.json'
    p = subprocess.Popen(shlex.split(command))

def example():
    import aws

    # Set up your credentials.json and config.json file first. 
    # There are templates in this repo; copy them into your working dir

    # Then request a spot instance!
    instance = aws.request_spot('python', .15, script=aws.INITIAL_CONFIG)
    aws.await_boot(instance) 

    # Once you've got it, can check how boot is going with
    aws.cloud_init_output(instance)
    ssh(instance) # prints the SSH command needed to connect

    # Set up a tunnel to the remote kernel, set up the rsync, then start a remote console
    aws.tunnel(instance)
    aws.rsync(instance)
    aws.remote_console()

    # Use the console and ! commands to install any packages you need. Then create an image with
    aws.create_image(instance, name='python-ec2')

    # Wait a minute or two until the image is finished
    while True:
        if all(v.state == 'available' for k, v in aws.images().items()):
            instance.terminate()
        aws.time.sleep(5)

    # Now test it out
    instance = aws.request_spot('python', .15, script=aws.CONFIG, image='python-ec2')
    aws.await_boot(instance)
    aws.tunnel(instance)
    aws.rsync(instance)
    aws.remote_console()