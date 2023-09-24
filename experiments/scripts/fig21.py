import json
import time

from cluster_setting import *
from utils.utils import save_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig21

st = time.time()

work_dir = f'{EXP_HOME}/experiments/elasticity_hr_cpu'

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

# All methods use the same compile option
MAKE_CMD = get_make_cmd(build_dir, 'sample-adaptive', "webmail-all", "0.2")
cmd_manager.execute_all(MAKE_CMD)
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "webmail-all", "0.2")
cmd_manager.execute_all(CACHE_CONFIG_CMD)

all_res = {}
for method in method_list:
    print(
        f"Start executing {method} on webmail-all with varying number of clinets")
    controller_prom = cmd_manager.execute_on_node(
        master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 160 webmail-all")
    mn_prom = cmd_manager.execute_on_node(
        mn_id, f"cd {work_dir} && ./run_server.sh {method}")

    time.sleep(5)
    c_prom_list = []
    for i in range(5):
        st_cid = 32*i+1
        c_prom = cmd_manager.execute_on_node(
            client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} webmail-all 32 160")
        c_prom_list.append(c_prom)

    for c_prom in c_prom_list:
        c_prom.join()
    mn_prom.join()

    res = controller_prom.join()
    line = res.tail('stdout', 1).strip()
    res = json.loads(line)
    if method not in all_res:
        all_res[method] = res

# save res
save_res('fig21', all_res)

# draw figures
plot_fig21(all_res)

et = time.time()
save_time('fig21', et - st)
