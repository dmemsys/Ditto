import json
import time

from cluster_setting import *
from utils.utils import save_res, save_time
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_make_cmd, get_running_opt
from utils.plots import plot_fig23

st = time.time()

work_dir = f'{EXP_HOME}/experiments/workload_throughput'

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([master_id], RESET_MASTER_CMD)
cmd_manager.execute_on_nodes(
    [i for i in range(len(cluster_ips)) if i != master_id], RESET_WORKER_CMD)

method_list = ['sample-lru', 'sample-lfu', 'sample-mru', 'sample-gds', 'sample-lirs', 'sample-fifo',
               'sample-size', 'sample-gdsf', 'sample-lrfu', 'sample-lruk', 'sample-lfuda', 'sample-hyperbolic']

running_opt = get_running_opt()
running_opt['ela_mem_tpt'] = 'OFF'
running_opt['use_penalty'] = 'OFF'
running_opt['multi_policy'] = 'ON'
MAKE_CMD = get_make_cmd(build_dir, "sample-lru",
                        "webmail-all", "0.1", running_opt)
cmd_manager.execute_all(MAKE_CMD)
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "webmail-all", "0.1")
cmd_manager.execute_all(CACHE_CONFIG_CMD)

all_res = {}
for method in method_list:
    print(f"Start executing {method} on webmail-all with 0.1 cache size")
    controller_prom = cmd_manager.execute_on_node(
        master_id, f"cd {work_dir} && ./run_controller.sh {method} 1 64 webmail-all")
    mn_prom = cmd_manager.execute_on_node(
        mn_id, f"cd {work_dir} && ./run_server.sh {method}")

    # start Clients
    time.sleep(5)
    c_prom_list = []
    for i in range(2):
        st_cid = 32*i+1
        c_prom = cmd_manager.execute_on_node(
            client_ids[i], f"cd {work_dir} && ./run_client_master.sh {method} {st_cid} webmail-all 32 64")
        c_prom_list.append(c_prom)

    # wait for Clients and MN
    for c_prom in c_prom_list:
        c_prom.join()
    mn_prom.join()

    raw_res = controller_prom.join()
    line = raw_res.tail('stdout', 1).strip()
    res = json.loads(line)
    if method not in all_res:
        all_res[method] = {}
    all_res[method] = res

# save res
save_res("fig23", all_res)

plot_fig23(all_res)


et = time.time()
save_time('fig23', et - st)
