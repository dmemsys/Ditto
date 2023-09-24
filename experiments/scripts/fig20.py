from itertools import product
import json
import time

from cluster_setting import *
from utils.utils import save_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd
from utils.plots import plot_fig20

st = time.time()

work_dir = f'{EXP_HOME}/experiments/hit_rate'

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
lru_num_list = [i for i in range(11)]

# All methods in the experiment have the same compile option
MAKE_CMD = get_make_cmd(build_dir, 'sample-adaptive', "mix", None)
cmd_manager.execute_all(MAKE_CMD)
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "mix", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)

all_res = {}
for method, num_lru in product(method_list, lru_num_list):
    num_lfu = 10 - num_lru
    print(
        f"Start executing {method} with {num_lru} lru clients and {num_lfu} lfu clients under mix workload")

    controller_prom = cmd_manager.execute_on_node(
        master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 10 mix")
    mn_prom = cmd_manager.execute_on_node(
        mn_id, f"cd {work_dir} && ./run_server.sh {method}")

    time.sleep(5)
    c_prom = cmd_manager.execute_on_node(
        client_ids[0], f"cd {work_dir} && ./run_client_mix_master.sh {method} 1 mix 10 10 {num_lru} {num_lfu}")

    c_prom.join()
    mn_prom.join()

    raw_res = controller_prom.join()
    line = raw_res.tail('stdout', 1).strip()
    res = json.loads(line)
    if method not in all_res:
        all_res[method] = {}
    ent_name = f'r{num_lru}-f{num_lfu}'
    if ent_name not in all_res[method]:
        all_res[method][ent_name] = {}
    all_res[method][ent_name] = res

# save res
save_res('fig20', all_res)

# draw figure
plot_fig20(all_res)


et = time.time()
save_time('fig20', et - st)
