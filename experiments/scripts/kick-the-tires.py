import time
import json
import sys

from utils.utils import save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd

from cluster_setting import *

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <num clients> <workload>")

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
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 1)
cmd_manager.execute_all(MN_CPU_CMD)

# start experiment
method_list = ['sample-adaptive']
client_num = int(sys.argv[1])
workload = sys.argv[2]

MAKE_CMD = get_make_cmd(build_dir, 'sample-adaptive', 'ycsb', None)
cmd_manager.execute_all(MAKE_CMD)

print(
    f'Start executing sample-adaptive with {client_num} clients under {workload}')
num_CN = client_num // NUM_CLIENT_PER_NODE + \
    (client_num % NUM_CLIENT_PER_NODE != 0)

# start controller and MN
controller_prom = cmd_manager.execute_on_node(
    master_id, f"cd {work_dir} && ./run_controller.sh sample-adaptive 1 {client_num} {workload}")
mn_prom = cmd_manager.execute_on_node(
    mn_id, f"cd {work_dir} && ./run_server.sh sample-adaptive")

# start CNs
time.sleep(5)
c_prom_list = []
for i in range(num_CN):
    st_cid = i * NUM_CLIENT_PER_NODE + 1
    if i == num_CN - 1 and client_num % NUM_CLIENT_PER_NODE != 0:
        c_prom = cmd_manager.execute_on_node(
            client_ids[i],
            f"cd {work_dir} && ./run_client_master.sh sample-adaptive {st_cid} {workload} {client_num % NUM_CLIENT_PER_NODE} {client_num}")
    else:
        c_prom = cmd_manager.execute_on_node(
            client_ids[i],
            f"cd {work_dir} && ./run_client_master.sh sample-adaptive {st_cid} {workload} {NUM_CLIENT_PER_NODE} {client_num}")
    c_prom_list.append(c_prom)

# wait Clients and MN
for c_prom in c_prom_list:
    c_prom.join()
mn_prom.join()

raw_res = controller_prom.join()
line = raw_res.tail("stdout", 1).strip()
res = json.loads(line)
print(res)

et = time.time()
save_time('kick-the-tires', et - st)
