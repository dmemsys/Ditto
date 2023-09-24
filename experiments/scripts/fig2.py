from itertools import product
import time
import json

from utils.utils import save_res, load_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_freq_cache_cmd, get_mn_cpu_cmd, get_running_opt
from utils.plots import plot_fig2

from cluster_setting import *

st = time.time()

work_dir = f'{EXP_HOME}/experiments/ycsb_test'
env_cmd = 'source ~/.zshrc'

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([i for i in range(10)], RESET_CMD)

# set freq_cache configuration
FC_CONFIG_CMD = get_freq_cache_cmd(config_dir, default_fc_size)
cmd_manager.execute_all(FC_CONFIG_CMD)
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 1)
cmd_manager.execute_all(MN_CPU_CMD)

method_list = ['non', 'precise-lru', 'shard-lru']
client_num_list = [1, 2, 4, 8, 16, 32, 64, 96, 128, 160]

running_opt = get_running_opt()
running_opt['ela_mem_tpt'] = 'OFF'
running_opt['use_penalty'] = 'OFF'

# config cache
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "ycsb", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)

all_res = {}
for method in method_list:
    MAKE_CMD = get_make_cmd(build_dir, method, "ycsb", None)
    cmd_manager.execute_all(MAKE_CMD)

    for num_c in client_num_list:
        print(f"Start executing {method} on ycsb with {num_c} clients")
        controller_prom = cmd_manager.execute_on_node(
            master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 {num_c} ycsbc")
        mn_prom = cmd_manager.execute_on_node(
            mn_id, f"cd {work_dir} && ./run_server.sh {method}")

        num_CN = num_c // NUM_CLIENT_PER_NODE + \
            (num_c % NUM_CLIENT_PER_NODE != 0)
        time.sleep(5)
        c_prom_list = []
        for i in range(num_CN):
            st_cid = i * NUM_CLIENT_PER_NODE + 1
            if i == num_CN - 1 and num_c % NUM_CLIENT_PER_NODE != 0:
                c_prom = cmd_manager.execute_on_node(
                    client_ids[i],
                    f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} ycsbc {num_c % NUM_CLIENT_PER_NODE} {num_c}")
            else:
                c_prom = cmd_manager.execute_on_node(
                    client_ids[i],
                    f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} ycsbc {NUM_CLIENT_PER_NODE} {num_c}")
            c_prom_list.append(c_prom)

        # wait Clients and MN
        for c_prom in c_prom_list:
            c_prom.join()
        mn_prom.join()

        raw_res = controller_prom.join()
        line = raw_res.tail("stdout", 1).strip()
        res = json.loads(line)
        if method not in all_res:
            all_res[method] = {}
        if num_c not in all_res[method]:
            all_res[method][num_c] = {}
        all_res[method][num_c] = res

# save res
save_res('fig2', all_res)


plot_res = load_res('fig2.json')
plot_fig2(plot_res)

et = time.time()
save_time('fig2', et - st)
