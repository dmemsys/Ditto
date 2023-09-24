import os
import memcache
import time
import sys
import subprocess

if len(sys.argv) != 4:
    print("Usage {} <num_cores> <memcached_ip> <my_server_ip>", sys.argv[0])

num_required_instances = int(sys.argv[1].strip())
num_servers = num_required_instances if num_required_instances > 3 else 3

instance_controller_id = 0
memcached_ip = sys.argv[2]
my_server_ip = sys.argv[3]
server_port_st = 7000
num_cores = 64

server_ports = [server_port_st + i for i in range(num_servers)]
all_server_ports = [server_port_st + i for i in range(64)]

# clear old settings
for p in all_server_ports:
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
for i, p in enumerate(server_ports):
    os.system(f'cd {p}; \
              taskset -c {i % num_cores} redis-server ./redis.conf; cd ..')

mc = memcache.Client([memcached_ip], debug=False)
assert (mc != None)
mc.set(f'redis-{instance_controller_id}-ready', instance_controller_id)
print("Finished creating instances, wait clean-up")

# # reshard data if num instances < 3
# val = mc.get('redis-sync-reshard')
# while val == None:
#     val = mc.get('redis-sync-reshard')
# if num_required_instances < 3:
#     port_node_id = {}
#     node_id_port = {}
#     for i in server_ports:
#         proc = subprocess.Popen(f'redis-cli -h {my_server_ip} -p {i} \
#                                 cluster nodes | grep myself',
#                                 stdout=subprocess.PIPE, shell=True)
#         proc.wait()
#         output = proc.stdout.read().decode().strip()
#         node_id = output.split(' ')[0]
#         node_id_port[node_id] = i
#         port_node_id[i] = node_id
#         print(f'{i} {node_id}')

#     ip_slots = {}
#     for p in server_ports:
#         proc = subprocess.Popen(
#             f'redis-cli --cluster check {my_server_ip}:{p}\
#             | grep {my_server_ip}:{p}', stdout=subprocess.PIPE,
#             shell=True)
#         proc.wait()
#         l = proc.stdout.readline().decode().strip()
#         num_slots = int(l.split(' ')[6])
#         ip_slots[p] = num_slots
#         print(f'{p} {num_slots}')

#     if num_required_instances == 1:
#         os.system(f'redis-cli --cluster reshard {my_server_ip}:7001\
#                   --cluster-from {port_node_id[7001]}\
#                   --cluster-to {port_node_id[7000]}\
#                   --cluster-slots {ip_slots[7001]} --cluster-yes')
#         time.sleep(2)
#         os.system(f'redis-cli --cluster reshard {my_server_ip}:7002\
#                   --cluster-from {port_node_id[7002]}\
#                   --cluster-to {port_node_id[7000]}\
#                   --cluster-slots {ip_slots[7002]} --cluster-yes')
#         time.sleep(5)
#         os.system(
#             f'redis-cli --cluster del-node {my_server_ip}:7000 {port_node_id[7001]}')
#         time.sleep(5)
#         os.system(
#             f'redis-cli --cluster del-node {my_server_ip}:7000 {port_node_id[7002]}')
#     else:
#         assert (num_required_instances == 2)
#         num_slot_1 = ip_slots[7002] // 2
#         num_slot_2 = ip_slots[7002] - num_slot_1
#         os.system(f'redis-cli --cluster reshard {my_server_ip}:7002\
#                   --cluster-from {port_node_id[7002]}\
#                   --cluster-to {port_node_id[7001]}\
#                   --cluster-slots {num_slot_1} --cluster-yes')
#         time.sleep(2)
#         os.system(f'redis-cli --cluster reshard {my_server_ip}:7002\
#                   --cluster-from {port_node_id[7002]}\
#                   --cluster-to {port_node_id[7001]}\
#                   --cluster-slots {num_slot_2} --cluster-yes')
#         time.sleep(5)
#         os.system(
#             f'redis-cli --cluster del-node {my_server_ip}:7000 {port_node_id[7002]}')
# mc.set('redis-reshard-ready', 1)


# wait for finishing workload
val = mc.get('test-finish')
while val == None:
    time.sleep(1)
    val = mc.get('test-finish')

# clean-up redis instances
for p in all_server_ports:
    if not os.path.exists(f'./{p}'):
        continue
    if os.path.exists(f'./{p}/pid'):
        pid = int(open(f'./{p}/pid', 'r').read())
        os.system(f'sudo kill -9 {pid}')
    os.system(f'rm -rf ./{p}')
