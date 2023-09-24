from itertools import product
import memcache
import time
import json
import os
import subprocess

from cluster_setting import *
from utils.utils import save_res, load_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig17

st = time.time()

work_dir = f'{EXP_HOME}/experiments/ycsb_test'

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([master_id], RESET_MASTER_CMD)
cmd_manager.execute_on_nodes(
    [i for i in range(len(cluster_ips)) if i != master_id], RESET_WORKER_CMD)

# set cache size configuration
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "ycsb", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)
# set freq_cache configuration
FC_CONFIG_CMD = get_freq_cache_cmd(config_dir, default_fc_size)
cmd_manager.execute_all(FC_CONFIG_CMD)

method_list = ['cliquemap-shard-lru', 'cliquemap-shard-lfu']
num_server_threads = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48]
workload_list = ['ycsbc', 'ycsba']

# All methods use the same build options
MAKE_CMD = get_make_cmd(build_dir, 'cliquemap-shard-lru', 'ycsb', None)
cmd_manager.execute_all(MAKE_CMD)

all_res = {}
for num_threads in num_server_threads:
    # set MN CPU
    MN_CPU_CMD = get_mn_cpu_cmd(config_dir, num_threads)
    cmd_manager.execute_all(MN_CPU_CMD)
    for method, wl in product(method_list, workload_list):
        print(
            f"Start executing {method} under {wl} with {num_threads} MN cores")
        controller_prom = cmd_manager.execute_on_node(
            master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 256 {wl}")
        mn_prom = cmd_manager.execute_on_node(
            mn_id, f"cd {work_dir} && ./run_server.sh {method}")

        time.sleep(5)
        c_prom_list = []
        for i in range(8):
            st_cid = NUM_CLIENT_PER_NODE * i + 1
            c_prom = cmd_manager.execute_on_node(
                client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} {wl} 32 256")
            c_prom_list.append(c_prom)

        # wait Client and MN finish
        for c_prom in c_prom_list:
            c_prom.join()
        mn_prom.join()

        raw_res = controller_prom.join()
        line = raw_res.tail("stdout", 1).strip()
        res = json.loads(line)
        if wl not in all_res:
            all_res[wl] = {}
        if method not in all_res[wl]:
            all_res[wl][method] = {}
        if num_threads not in all_res[wl][method]:
            all_res[wl][method][num_threads] = {}
        all_res[wl][method][num_threads] = res

# ================== run Redis =======================
memcached_ip = cluster_ips[master_id]
num_instance_controllers = 1
instance_ips = [cluster_ips[mn_id]]
client_ips = [cluster_ips[client_ids[0]], cluster_ips[client_ids[1]]]
num_clients = 512

redis_work_dir = f'{EXP_HOME}/experiments/redis_test'
ULIMIT_CMD = "ulimit -n unlimited"
for num_threads, wl in product(num_server_threads, workload_list):
    # start redis-cluster
    mc = memcache.Client([cluster_ips[master_id]])
    assert (mc != None)
    mc.flush_all()

    # start instances
    print(f"Start {num_threads} Redis instances")
    instance_prom = cmd_manager.execute_on_node(
        mn_id, f"{ULIMIT_CMD} && cd {redis_work_dir} && ./run_redis_cluster.sh {num_threads} {cluster_ips[mn_id]}")
    server_ports = [7000 + i for i in range(num_threads)]
    initial_instances = [f'{instance_ips[i]}:{j+7000}'
                         for i in range(num_instance_controllers)
                         for j in range(num_threads)]
    if num_threads < 3:
        server_ports = [7000 + i for i in range(3)]
        initial_instances = [f'{instance_ips[i]}:{j+7000}'
                             for i in range(num_instance_controllers)
                             for j in range(3)]

    # start instance controller wait for instance controllers to reply
    print("Wait Redis instance ready")
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

    # reshard cluster when thread_number < 3
    if num_threads < 3:
        port_node_id = {}
        node_id_port = {}
        for i in server_ports:
            proc = subprocess.Popen(f'redis-cli -h {cluster_ips[mn_id]} -p {i} \
                                    cluster nodes | grep myself',
                                    stdout=subprocess.PIPE, shell=True)
            proc.wait()
            output = proc.stdout.read().decode().strip()
            node_id = output.split(' ')[0]
            node_id_port[node_id] = i
            port_node_id[i] = node_id
            print(f'{i} {node_id}')

        ip_slots = {}
        for p in server_ports:
            proc = subprocess.Popen(
                f'redis-cli --cluster check {cluster_ips[mn_id]}:{p}\
                | grep {cluster_ips[mn_id]}:{p}', stdout=subprocess.PIPE,
                shell=True)
            proc.wait()
            l = proc.stdout.readline().decode().strip()
            num_slots = int(l.split(' ')[6])
            ip_slots[p] = num_slots
            print(f'{p} {num_slots}')

        if num_threads == 1:
            os.system(f'redis-cli --cluster reshard {cluster_ips[mn_id]}:7001\
                      --cluster-from {port_node_id[7001]}\
                      --cluster-to {port_node_id[7000]}\
                      --cluster-slots {ip_slots[7001]} --cluster-yes')
            time.sleep(2)
            os.system(f'redis-cli --cluster reshard {cluster_ips[mn_id]}:7002\
                      --cluster-from {port_node_id[7002]}\
                      --cluster-to {port_node_id[7000]}\
                      --cluster-slots {ip_slots[7002]} --cluster-yes')
            time.sleep(5)
            os.system(
                f'redis-cli --cluster del-node {cluster_ips[mn_id]}:7000 {port_node_id[7001]}')
            time.sleep(5)
            os.system(
                f'redis-cli --cluster del-node {cluster_ips[mn_id]}:7000 {port_node_id[7002]}')
        else:
            assert (num_threads == 2)
            num_slot_1 = ip_slots[7002] // 2
            num_slot_2 = ip_slots[7002] - num_slot_1
            os.system(f'redis-cli --cluster reshard {cluster_ips[mn_id]}:7002\
                      --cluster-from {port_node_id[7002]}\
                      --cluster-to {port_node_id[7001]}\
                      --cluster-slots {num_slot_1} --cluster-yes')
            time.sleep(2)
            os.system(f'redis-cli --cluster reshard {cluster_ips[mn_id]}:7002\
                      --cluster-from {port_node_id[7002]}\
                      --cluster-to {port_node_id[7001]}\
                      --cluster-slots {num_slot_2} --cluster-yes')
            time.sleep(5)
            os.system(
                f'redis-cli --cluster del-node {cluster_ips[mn_id]}:7000 {port_node_id[7002]}')

    # start clients
    time.sleep(10)
    c_prom_list = []
    for i in range(2):
        st_cid = 256 * i + 1
        c_prom = cmd_manager.execute_on_node(
            client_ids[i], f"{ULIMIT_CMD} && cd {redis_work_dir} && ./run_redis_client_tpt.sh {st_cid} {wl} tcp://{cluster_ips[mn_id]}:7000 20")
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

    # wait for client finish
    res = {}
    for i in range(1, num_clients + 1):
        key = f'client-{i}-result-0'
        val = mc.get(key)
        while val == None:
            val = mc.get(key)
        res[i] = json.loads(str(val.decode('utf-8')))

    for c_prom in c_prom_list:
        c_prom.join()

    # finishing experiment and exit!
    mc.set('test-finish', 1)
    instance_prom.join()

    # parse results
    combined_res = {}
    tpt = 0
    lat_map = {}
    for i in range(1, num_clients + 1):
        tpt += res[i]['ops_cont'][-1]
        for itm in res[i]['lat_map']:
            if itm[0] not in lat_map:
                lat_map[itm[0]] = 0
            lat_map[itm[0]] += itm[1]
    lat_list = []
    for item in lat_map.items():
        lat_list += [item[0]] * item[1]
    lat_list.sort()

    combined_res['tpt'] = tpt / 20
    combined_res['p99'] = lat_list[int(len(lat_list) * 0.99)]
    combined_res['p50'] = lat_list[int(len(lat_list) * 0.50)]

    if wl not in all_res:
        all_res[wl] = {}
    if 'redis' not in all_res[wl]:
        all_res[wl]['redis'] = {}
    if num_threads not in all_res[wl]['redis']:
        all_res[wl]['redis'][num_threads] = {}
    all_res[wl]['redis'][num_threads] = combined_res

save_res('fig17', all_res)

plot_res = load_res('fig17.json')
ada_res = load_res('fig14.json')
plot_fig17(plot_res, ada_res)

et = time.time()
save_time('fig17', et - st)
