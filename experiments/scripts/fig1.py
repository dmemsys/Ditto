import json
import memcache
import time
import os
import subprocess
import numpy as np

from cluster_setting import *
from utils.cmd_manager import CMDManager
from utils.utils import save_res, save_time
from utils.settings import get_cache_config_cmd, get_freq_cache_cmd, get_mn_cpu_cmd, get_make_cmd
from utils.plots import plot_fig1

# timer
g_st = time.time()

redis_instance_id = 1
redis_client_ids = [2, 3]

memcached_ip = cluster_ips[master_id]
num_instance_controllers = 1
instance_ips = [cluster_ips[redis_instance_id]]
num_clients = 512
rebalance_start_time = 180
num_instances = 64
run_time = 1200

work_dir = f"{EXP_HOME}/experiments/redis_test"
ULIMIT_CMD = "ulimit -n unlimited"

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([i for i in range(10)], RESET_CMD)

# set cache size configuration
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "ycsb", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)
# set freq_cache configuration
FC_CONFIG_CMD = get_freq_cache_cmd(config_dir, default_fc_size)
cmd_manager.execute_all(FC_CONFIG_CMD)
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 1)
cmd_manager.execute_all(MN_CPU_CMD)
# make
MAKE_CMD = get_make_cmd(build_dir, 'sample-adaptive', "ycsb", None)
cmd_manager.execute_all(MAKE_CMD)

# create instance list
initial_instances = [f'{instance_ips[i]}:{j+7000}'
                     for i in range(num_instance_controllers)
                     for j in range(num_instances // num_instance_controllers // 2)]
scale_instances = [f'{instance_ips[i]}:{j+7000}'
                   for i in range(num_instance_controllers)
                   for j in range(num_instances // num_instance_controllers // 2,
                                  num_instances // num_instance_controllers)]
all_instances = initial_instances + scale_instances
assert (len(initial_instances) == len(scale_instances))

scale2target = {scale_instances[i]: initial_instances[i]
                for i in range(len(scale_instances))}

# create memcached controller and clear all contents
mc = memcache.Client([memcached_ip])
assert (mc != None)
mc.flush_all()

# start redis instance
INSTANCE_CMD = f"{ULIMIT_CMD} && cd {work_dir} && ./run_instance_controller.sh {cluster_ips[redis_instance_id]}"
instance_prom = cmd_manager.execute_on_node(redis_instance_id, INSTANCE_CMD)
# wait for instance controllers to reply
for i in range(num_instance_controllers):
    ready_msg = f'redis-{i}-ready'
    val = mc.get(ready_msg)
    while val == None:
        val = mc.get(ready_msg)
    print(ready_msg)

# create a redis cluster with initial_instances
cmd = 'redis-cli --cluster create ' \
    + ' '.join(initial_instances) + ' --cluster-yes'
os.system(cmd)

# notify instance controllers to check master change
mc.set('cluster-change', 1)
# add empty masters to the cluster
for i in scale_instances:
    os.system(f'redis-cli --cluster add-node \
              {i} {initial_instances[0]}')
    val = mc.get(f'{i}-success')
    while val == None:
        val = mc.get(f'{i}-success')


# notify instance controllers to check master change
mc.set('cluster-change', 1)
# add empty masters to the cluster
for i in scale_instances:
    os.system(f'redis-cli --cluster add-node \
              {i} {initial_instances[0]}')
    val = mc.get(f'{i}-success')
    while val == None:
        val = mc.get(f'{i}-success')

# start redis clients
c_prom_list = []
for i in range(2):
    st_cid = i * 256 + 1
    CLIENT_CMD = f"{ULIMIT_CMD} && cd {work_dir} && ./run_redis_client.sh {st_cid} ycsbc tcp://{cluster_ips[redis_instance_id]}:7000 {run_time}"
    c_prom = cmd_manager.execute_on_node(redis_client_ids[i], CLIENT_CMD)
    c_prom_list.append(c_prom)

# sync ycsb load
print("Wait all clients ready.")
for i in range(1, num_clients + 1):
    ready_msg = f'client-{i}-ready-0'
    val = mc.get(ready_msg)
    while val == None:
        val = mc.get(ready_msg)
    print(ready_msg)
print("Notify Clients to load.")
mc.set('all-client-ready-0', 1)  # clients start loading

# wait all clients load ready and sync their to execute trans
for i in range(1, num_clients + 1):
    ready_msg = f'client-{i}-ready-1'
    val = mc.get(ready_msg)
    while val == None:
        val = mc.get(ready_msg)
    print(ready_msg)
mc.set('all-client-ready-1', 1)  # clients start executing trans

print("Notify all clients start trans")
# sleep a while before start rebalancing
time.sleep(rebalance_start_time)
print('Start rebalance')
st = time.time()
os.system(f'redis-cli --cluster rebalance \
          {initial_instances[0]} --cluster-use-empty-masters')
et = time.time()
print(f"Rebalance finishes in {et - st}s")
rebalance_period = et - st

# === starts shrinking resources
time.sleep(rebalance_start_time)
# get node_id to ip map and slot map
print("Getting node map!")
node_id_ip = {}
ip_node_id = {}
for i in all_instances:
    host = i.split(':')[0]
    port = i.split(':')[1]
    proc = subprocess.Popen(f'redis-cli -h {host} -p {port} \
                            cluster nodes | grep myself',
                            stdout=subprocess.PIPE, shell=True)
    proc.wait()
    output = proc.stdout.read().decode().strip()
    node_id = output.split(' ')[0]
    node_id_ip[node_id] = i
    ip_node_id[i] = node_id
    print(f'{i} {node_id}')

# get node_id_slots
print("Start resharding")
st = time.time()
while True:
    ip_slots = {}
    for i in all_instances:
        proc = subprocess.Popen(f'redis-cli --cluster check {i}\
                                | grep {i}', stdout=subprocess.PIPE,
                                shell=True)
        proc.wait()
        l = proc.stdout.readline().decode().strip()
        num_slots = int(l.split(' ')[6])
        ip_slots[i] = num_slots
        print(f'{i} {num_slots}')

    # check reshard finish
    reshard_finish = True
    for inst in scale_instances:
        if ip_slots[inst] != 0:
            reshard_finish = False
            break
    if reshard_finish:
        break

    # reshard scale nodes to initial nodes
    for inst in scale_instances:
        num_slots = ip_slots[inst]
        if num_slots == 0:
            continue
        target_inst = scale2target[inst]
        reshard_cmd = f'redis-cli --cluster reshard {inst} \
                  --cluster-from {ip_node_id[inst]} \
                  --cluster-to {ip_node_id[target_inst]} \
                  --cluster-slots {num_slots} --cluster-yes > /dev/null 2>&1'
        print(reshard_cmd)
        os.system(reshard_cmd)
        time.sleep(2)
    et1 = time.time()
    shrink_period1 = et1 - st
    print(f"Reshard finishes in {shrink_period1}")
    # remove machines from the cluster
    for inst in scale_instances:
        if ip_slots[inst] == 0:
            continue
        time.sleep(5)
        print(f"Remove {inst}")
        os.system(
            f'redis-cli --cluster del-node {instance_ips[0]}:7000 {ip_node_id[inst]}')
et = time.time()
shrink_period = et - st
print(f"Reshard finishes in {shrink_period}s")

# wait for client finish
res = {}
for i in range(1, num_clients + 1):
    key = f'client-{i}-result-0'
    val = mc.get(key)
    while val == None:
        val = mc.get(key)
    res[i] = json.loads(str(val.decode('utf-8')))
res['rebalance_start_time'] = rebalance_start_time
res['rebalance_end_time'] = rebalance_start_time + rebalance_period
res['shrink_start_time'] = res['rebalance_end_time'] + rebalance_start_time
res['shrink_end_time'] = res['shrink_start_time'] + shrink_period
res['shrink_period_1'] = shrink_period1

# save raw results
save_res('fig-raw', res)

# finishing experiment and exit!
mc.set('test-finish', 1)

for c_prom in c_prom_list:
    c_prom.join()
instance_prom.join()

# merge throughput
ticks = len(res[1]['ops_cont'])
agg = np.zeros(ticks)
for i in range(1, num_clients + 1):
    agg += np.array(res[i]['ops_cont'])
# merge latency map
lat_ticks = len(res[1]['lat_map_cont'])
combined_lat_map = []
for j in range(lat_ticks):
    cur_lat_map = {}
    for i in range(1, num_clients+1):
        for ent in res[i]['lat_map_cont'][j]:
            if ent[0] not in cur_lat_map:
                cur_lat_map[ent[0]] = 0
            cur_lat_map[ent[0]] += ent[1]
    combined_lat_map.append(cur_lat_map)
# get p50_cont and p99_cont
p50_cont = []
p99_cont = []
for m in combined_lat_map:
    tmp_lat = []
    for k, v in m.items():
        tmp_lat += [int(k)] * v
    tmp_lat.sort()
    p50_cont.append(tmp_lat[int(len(tmp_lat) * 0.5)])
    p99_cont.append(tmp_lat[int(len(tmp_lat) * 0.99)])

sft = [0] + list(agg)[:-1]
tpt = (agg - sft) / 0.5
combined_res = {
    'tpt': list(tpt),
    'p50_cont': p50_cont,
    'p99_cont': p99_cont,
    'rebalance_start_time': res['rebalance_start_time'],
    'rebalance_end_time': res['rebalance_end_time'],
    'shrink_start_time': res['shrink_start_time'],
    'shrink_end_time': res['shrink_end_time'],
    'shrink_period_1': res['shrink_period_1']
}

save_res('fig1', combined_res)

plot_fig1(combined_res)

g_et = time.time()
save_time('fig1', g_et - g_st)
