import os

user = 'AEUser'

server_list = [
    'clnode274.clemson.cloudlab.us',
    'clnode279.clemson.cloudlab.us',
    'clnode281.clemson.cloudlab.us',
    'clnode270.clemson.cloudlab.us',
    'clnode271.clemson.cloudlab.us',
    'clnode254.clemson.cloudlab.us',
    'clnode282.clemson.cloudlab.us',
    'clnode253.clemson.cloudlab.us',
    'clnode259.clemson.cloudlab.us',
    'clnode266.clemson.cloudlab.us',
]

# generate keys
for s in server_list:
    cmd = f'ssh -o StrictHostKeyChecking=no {user}@{s} "ssh-keygen -t rsa -b 2048 -N \'\' -f ~/.ssh/id_rsa"'
    os.system(cmd)

pub_key_list = []
for s in server_list:
    cmd = f'scp -o StrictHostKeyChecking=no {user}@{s}:.ssh/id_rsa.pub ./'
    os.system(cmd)
    with open('id_rsa.pub', 'r') as f:
        pub_key_list.append(f.readline())

cmd = f'scp -o StrictHostKeyChecking=no {user}@{server_list[0]}:.ssh/authorized_keys ./'
os.system(cmd)
with open('authorized_keys', 'a') as f:
    for key in pub_key_list:
        f.write(key)

for s in server_list:
    cmd = f'scp -o StrictHostKeyChecking=no ./authorized_keys {user}@{s}:.ssh/authorized_keys'
    os.system(cmd)
