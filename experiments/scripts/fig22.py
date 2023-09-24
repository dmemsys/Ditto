import time
import os

from cluster_setting import *
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_running_opt, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig22
from utils.utils import load_res, save_time

st = time.time()

work_dir = f'{EXP_HOME}/experiments/elasticity_hr_mem'

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

method_list = ['sample-adaptive', 'sample-lru', 'sample-lfu']

running_opt = get_running_opt()
running_opt['ela_mem_tpt'] = 'OFF'
running_opt['use_penalty'] = 'OFF'
MAKE_CMD = get_make_cmd(build_dir, "sample-adaptive",
                        "webmail-elastic", None, running_opt)
cmd_manager.execute_all(MAKE_CMD)
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "webmail-elastic", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)

for method in method_list:
    print(f"Start executing {method} on webmail-all with varying memory")
    controller_prom = cmd_manager.execute_on_node(
        master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 64 webmail-all")
    mn_prom = cmd_manager.execute_on_node(
        mn_id, f"cd {work_dir} && ./run_server.sh {method}")

    time.sleep(5)
    c_prom_list = []
    for i in range(2):
        st_cid = 32*i+1
        c_prom = cmd_manager.execute_on_node(
            client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} webmail-all 32 64")
        c_prom_list.append(c_prom)

    for c_prom in c_prom_list:
        c_prom.join()
    mn_prom.join()
    controller_prom.join()

    # get result file
    if not os.path.exists('./results'):
        os.mkdir('results')
    fname = f"webmail-all-{method}-s1-c64.json"
    print(f"Getting {work_dir}/{fname}")
    cmd_manager.get_file(master_id, f"{work_dir}/{fname}",
                         f"./results/fig22-{method}.json")

# # get result file
# if not os.path.exists('./results'):
#     os.mkdir('./results')
# for method in method_list:
#     fname = f"webmail-all-{method}-s1-c64.json"
#     cmd_manager.get_file(master_id, f"{work_dir}/{fname}",
#                          f"./results/fig22-{method}.json")

adaptive_res = load_res('fig22-sample-adaptive.json')
lru_res = load_res('fig22-sample-lru.json')
lfu_res = load_res('fig22-sample-lfu.json')
plot_fig22(adaptive_res, lru_res, lfu_res)

et = time.time()
save_time('fig22', et - st)
