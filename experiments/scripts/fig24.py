import json
import time

from cluster_setting import *
from utils.utils import save_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_running_opt, get_mn_cpu_cmd
from utils.plots import plot_fig24

st = time.time()

work_dir = f'{EXP_HOME}/experiments/workload_throughput'

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([master_id], RESET_MASTER_CMD)
cmd_manager.execute_on_nodes(
    [i for i in range(len(cluster_ips)) if i != master_id], RESET_WORKER_CMD)

method_list = ['sample-adaptive', 'sample-adaptive-async-nfc',
               'sample-adaptive-sync-nfc', 'sample-adaptive-heavy', 'sample-adaptive-naive']

running_opt = get_running_opt()
running_opt['ela_mem_tpt'] = 'OFF'
running_opt['use_penalty'] = 'OFF'
running_opt['multi_policy'] = 'OFF'
MAKE_CMD = get_make_cmd(build_dir, "sample-adaptive",
                        "webmail-all", "0.1", running_opt)
cmd_manager.execute_all(MAKE_CMD)
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "webmail-all", "0.1")
cmd_manager.execute_all(CACHE_CONFIG_CMD)
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 2)
cmd_manager.execute_all(MN_CPU_CMD)

num_avg = 3

all_res = {}
for method in method_list:
    print(f"Start executing {method} on webmail-all with 0.1 cache size")
    tmp_res = {'tpt': 0}

    j = 0
    while j < num_avg:
        controller_prom = cmd_manager.execute_on_node(
            master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 64 webmail-all")
        mn_prom = cmd_manager.execute_on_node(
            mn_id, f"cd {work_dir} && ./run_server.sh {method}")

        # start clients
        time.sleep(5)
        c_prom_list = []
        for i in range(2):
            st_cid = 32*i+1
            c_prom = cmd_manager.execute_on_node(
                client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} webmail-all 32 64")
            c_prom_list.append(c_prom)

        # wait for Clients and MN
        try:
            for c_prom in c_prom_list:
                c_prom.join()
            mn_prom.join()
        except:
            cmd_manager.execute_on_nodes([i for i in range(10)], RESET_CMD)
            j -= 1
            continue
        j += 1

        raw_res = controller_prom.join()
        line = raw_res.tail('stdout', 1).strip()
        res = json.loads(line)
        tmp_res['tpt'] += res['tpt']
    tmp_res['tpt'] /= num_avg
    if method not in all_res:
        all_res[method] = {}
    all_res[method] = tmp_res

# save res
save_res("fig24", all_res)

plot_fig24(all_res)

et = time.time()
save_time("fig24", et - st)
