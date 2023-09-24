import time
import json
import numpy as np

from cluster_setting import *
from utils.utils import save_res, load_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig25

st = time.time()

work_dir = f'{EXP_HOME}/experiments/ycsb_test'

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([master_id], RESET_MASTER_CMD)
cmd_manager.execute_on_nodes(
    [i for i in range(len(cluster_ips)) if i != master_id], RESET_WORKER_CMD)

# build according to hash table size
MAKE_CMD = get_make_cmd(build_dir, "sample-adaptive", "ycsb", None)
cmd_manager.execute_all(MAKE_CMD)

# set cache size configuration
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "ycsb", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 1)
cmd_manager.execute_all(MN_CPU_CMD)


# start experiment
cache_size_list = [0, 1024, 10240, 102400, 1024*1024, 5 *
                   1024*1024, 10*1024*1024, 50*1024*1024, 100*1024*1024]
num_avg = 10

all_res = {}
for cache_size in cache_size_list:
    print(
        f"Start executing sample-adaptive with {cache_size} freq_cache under ycsbc")

    CONFIG_CMD = get_freq_cache_cmd(config_dir, cache_size)
    cmd_manager.execute_all(CONFIG_CMD)

    tmp_res = {}
    for j in range(num_avg):
        controller_prom = cmd_manager.execute_on_node(
            master_id, f"cd {work_dir} && ./run_controller.sh sample-adaptive 1 256 ycsbc")
        mn_prom = cmd_manager.execute_on_node(
            mn_id, f"cd {work_dir} && ./run_server.sh sample-adaptive")

        method = 'sample-adaptive'
        if cache_size == 0:
            method = 'sample-adaptive-async-nfc'
        time.sleep(5)
        c_prom_list = []
        for i in range(8):
            st_cid = i*32+1
            c_prom = cmd_manager.execute_on_node(
                client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} ycsbc 32 256")
            c_prom_list.append(c_prom)

        # wait Clients and MN
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
        avg_res[k] = np.average(tmp_res[k][1:-1])

    if cache_size not in all_res:
        all_res[cache_size] = {}
    all_res[cache_size] = avg_res

save_res('fig25', all_res)

plot_res = load_res('fig25.json')
plot_fig25(plot_res)

et = time.time()
save_time('fig25', et - st)
