import json
import time
import numpy as np

from cluster_setting import *
from utils.utils import save_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig19

st = time.time()

work_dir = f'{EXP_HOME}/experiments/workload_throughput'

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([master_id], RESET_MASTER_CMD)
cmd_manager.execute_on_nodes(
    [i for i in range(len(cluster_ips)) if i != master_id], RESET_WORKER_CMD)

# set freq_cache configuration
FC_CONFIG_CMD = get_freq_cache_cmd(config_dir, default_fc_size)
cmd_manager.execute_all(FC_CONFIG_CMD)
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 1)
cmd_manager.execute_all(MN_CPU_CMD)
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "changing", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)

method_list = ['sample-adaptive', 'sample-lru', 'sample-lfu',
               'cliquemap-precise-lru', 'cliquemap-precise-lfu']

MAKE_CMD = get_make_cmd(build_dir, 'sample-adaptive', "changing", None)
cmd_manager.execute_all(MAKE_CMD)

num_avg = 1

all_res = {}
for method in method_list:

    print(f"Start executing {method} with 64 clients under changing workload")

    tmp_res = {}
    for j in range(num_avg):
        controller_prom = cmd_manager.execute_on_node(
            master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 64 changing")
        mn_prom = cmd_manager.execute_on_node(
            mn_id, f"cd {work_dir} && ./run_server.sh {method}")

        time.sleep(5)
        c_prom_list = []
        for i in range(2):
            st_cid = i*32+1
            c_prom = cmd_manager.execute_on_node(
                client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} changing 32 64")
            c_prom_list.append(c_prom)

        # wait for Clients and MN
        for c_prom in c_prom_list:
            c_prom.join()
        mn_prom.join()

        raw_res = controller_prom.join()
        line = raw_res.tail('stdout', 1).strip()
        res = json.loads(line)
        for k in res:
            if k not in tmp_res:
                tmp_res[k] = []
            tmp_res[k].append(res[k])

    avg_res = {}
    for k in tmp_res:
        tmp_res[k].sort()
        avg_res[k] = np.average(
            tmp_res[k][1:-1]) if i > 3 else np.average(tmp_res[k])

    if method not in all_res:
        all_res[method] = {}
    all_res[method] = avg_res

# save res
save_res('fig19', all_res)

# draw figure
plot_fig19(all_res)

et = time.time()
save_time('fig19', et - st)
