from itertools import product
import json
import time

from cluster_setting import *
from utils.utils import save_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig15_16

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

method_list = ['sample-adaptive', 'sample-lru', 'sample-lfu',
               'cliquemap-precise-lru', 'cliquemap-precise-lfu']
workload_list = ['webmail-all', 'twitter020-10m',
                 'twitter049-10m', 'twitter042-10m', 'ibm044-10m']
cache_size_list = ['0.2', '0.1', '0.05', '0.01']

all_res = {}
for wl, cache_size in product(workload_list, cache_size_list):
    # All methods in this experiment use the same compile options
    MAKE_CMD = get_make_cmd(build_dir, "sample-adaptive", wl, cache_size)
    cmd_manager.execute_all(MAKE_CMD)
    CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, wl, cache_size)
    cmd_manager.execute_all(CACHE_CONFIG_CMD)
    for method in method_list:
        print(
            f"Start executing {method} with 64 clients under {wl} with {cache_size} cache size")
        # start Controller and MN
        controller_prom = cmd_manager.execute_on_node(
            master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 64 {wl}")
        mn_prom = cmd_manager.execute_on_node(
            mn_id, f"cd {work_dir} && ./run_server.sh {method}")

        # start Clients
        time.sleep(5)
        c_prom_list = []
        for i in range(2):
            st_cid = i * 32 + 1
            c_prom = cmd_manager.execute_on_node(
                client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} {wl} 32 64")
            c_prom_list.append(c_prom)

        # wait for Clients and MN
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
        if cache_size not in all_res[wl][method]:
            all_res[wl][method][cache_size] = {}
        all_res[wl][method][cache_size] = res


# save res
save_res('fig15_16', all_res)

plot_fig15_16(all_res)

et = time.time()
save_time('fig15_16', et - st)
