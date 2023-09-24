import time

from cluster_setting import *
from utils.cmd_manager import CMDManager
from utils.utils import save_time

st = time.time()
work_dir = f'{EXP_HOME}/experiments/workloads'

cmd_manager = CMDManager(cluster_ips)

# download workload from internet on node-0
cmd_manager.execute_on_node(
    0, f"cd {work_dir} && ./download_all.sh", asynchronous=False)

# execute python http.server on node-0
master_prom = cmd_manager.execute_on_node(
    0, f"cd {work_dir} && python -m http.server")

# download workload from node-0 to other nodes
c_prom_list = []
for i in range(1, 10):
    c_prom = cmd_manager.execute_on_node(
        i, f"cd {work_dir} && ./download_all_from_peer.sh node-0:8000")
    c_prom_list.append(c_prom)

# wait for all clients
for c_prom in c_prom_list:
    c_prom.join()

# kill python http.server
cmd_manager.execute_on_node(0, "pkill -f http.server")

try:
    master_prom.join()
except:
    pass

et = time.time()
save_time('download_workload', et - st)
