import os
import memcache
import time
import sys

instance_controller_id = sys.argv[1]
memcached_ip = sys.argv[2]
my_server_ip = sys.argv[3]

num_servers = 64
server_port_st = 7000
num_cores = 64

server_ports = [server_port_st + i for i in range(num_servers)]
scale_ports = [server_port_st +
               i for i in range(num_servers // 2, num_servers)]

# clear old settings
for p in server_ports:
    if os.path.exists(f'./{p}'):
        if os.path.exists(f'./{p}/pid'):
            pid = int(open(f'./{p}/pid', 'r').read())
            os.system(f'sudo kill -9 {pid}')
        os.system(f'rm -rf ./{p}')
    os.mkdir(f'./{p}')

# construct configurations
config_templ = open('redis-large.conf.templ', 'r').read()
for p in server_ports:
    with open(f'./{p}/redis.conf', 'w') as f:
        f.write(config_templ.format(p, p, my_server_ip))

# start redis instances
os.system('ulimit -n unlimited')
for i, p in enumerate(server_ports):
    os.system(f'cd {p}; \
              taskset -c {i % num_cores} redis-server ./redis.conf; cd ..')

mc = memcache.Client([memcached_ip], debug=False)
assert (mc != None)
mc.set(f'redis-{instance_controller_id}-ready', instance_controller_id)
print("Finished creating instances, wait clean-up")

# wait for client loading signal and starts chekcing scale instance logs
val = mc.get('cluster-change')
while val == None:
    time.sleep(1)
    val = mc.get('cluster-change')

# check instance state
for p in scale_ports:
    success = False
    while not success:
        f = open(f'./{p}/log-{p}')
        log = f.readlines()
        for l in log:
            if 'Cluster state changed: ok' in l:
                success = True
                break
    mc.set(f'{my_server_ip}:{p}-success', 1)

# wait for finishing workload
val = mc.get('test-finish')
while val == None:
    time.sleep(1)
    val = mc.get('test-finish')

# clean-up redis instances
for p in server_ports:
    if not os.path.exists(f'./{p}'):
        continue
    if os.path.exists(f'./{p}/pid'):
        pid = int(open(f'./{p}/pid', 'r').read())
        os.system(f'sudo kill -9 {pid}')
    os.system(f'rm -rf ./{p}')
