import time
import os

from cluster_setting import *
from utils.cmd_manager import CMDManager
from utils.settings import get_cache_config_cmd, get_freq_cache_cmd, get_mn_cpu_cmd, get_running_opt, get_make_cmd
from utils.utils import load_res, save_time
from utils.plots import plot_fig13

st = time.time()

cmd_manager = CMDManager(cluster_ips)

# reset cluster
cmd_manager.execute_on_nodes([i for i in range(10)], RESET_CMD)

# set cache size configuration
CACHE_CONFIG_CMD = get_cache_config_cmd(config_dir, "ycsb", None)
cmd_manager.execute_all(CACHE_CONFIG_CMD)
# set freq_cache configuration
FC_CONFIG_CMD = get_freq_cache_cmd(config_dir, default_fc_size)
cmd_manager.execute_all(FC_CONFIG_CMD)
# set MN CPU
MN_CPU_CMD = get_mn_cpu_cmd(config_dir, 1)
cmd_manager.execute_all(MN_CPU_CMD)


def run_elastic_cpu():
    print("Start executing elastic cpu")
    work_dir = f'{EXP_HOME}/experiments/elasticity_tpt_cpu'

    # build ditto
    running_opt = get_running_opt()
    running_opt['use_fiber'] = 'ON'
    MAKE_CMD = get_make_cmd(build_dir, 'sample-adaptive',
                            'ycsb', None, running_opt)
    cmd_manager.execute_all(MAKE_CMD)

    controller_prom = cmd_manager.execute_on_node(
        master_id, f"cd {work_dir} && ./run_controller.sh sample-adaptive 1 64 ycsbc")
    mn_prom = cmd_manager.execute_on_node(
        mn_id, f"cd {work_dir} && ./run_server.sh sample-adaptive")

    time.sleep(5)
    c_prom_list = []
    for i in range(2):
        st_cid = NUM_CLIENT_PER_NODE * i + 1
        c_prom = cmd_manager.execute_on_node(
            client_ids[i], f"cd {work_dir} && ./run_client.sh sample-adaptive {st_cid} ycsbc")
        c_prom_list.append(c_prom)

    # wait for Clients and MN
    for c_prom in c_prom_list:
        c_prom.join()
    mn_prom.join()
    controller_prom.join()

    # get result file
    if not os.path.exists('./results'):
        os.mkdir('./results')
    fname = "ycsbc-sample-adaptive-s1-c64.json"
    cmd_manager.get_file(
        master_id, f"{work_dir}/{fname}", f"./results/fig13-sample-adaptive-ela-cpu.json")


def run_elastic_mem():
    print("Start executing elastic mem")
    work_dir = f"{EXP_HOME}/experiments/elasticity_tpt_mem"

    # build ditto
    running_opt = get_running_opt()
    running_opt['use_fiber'] = 'ON'
    running_opt['ela_mem_tpt'] = 'ON'
    MAKE_CMD = get_make_cmd(build_dir, "sample-adaptive",
                            "ycsb", None, running_opt)
    cmd_manager.execute_all(MAKE_CMD)

    controller_prom = cmd_manager.execute_on_node(
        master_id, f"cd {work_dir} && ./run_controller.sh sample-adaptive 1 32 ycsbc")
    mn_prom = cmd_manager.execute_on_node(
        mn_id, f"cd {work_dir} && ./run_server.sh sample-adaptive")
    time.sleep(5)
    c_prom = cmd_manager.execute_on_node(
        client_ids[0], f"cd {work_dir} && ./run_client.sh sample-adaptive 1 ycsbc")

    c_prom.join()
    mn_prom.join()
    controller_prom.join()

    # get result file
    if not os.path.exists('./results'):
        os.mkdir('./results')
    fname = "ycsbc-sample-adaptive-s1-c32.json"
    cmd_manager.get_file(
        master_id, f"{work_dir}/{fname}", f"./results/fig13-sample-adaptive-ela-mem.json")


def run_elastic_redis():
    if not os.path.exists('./results/fig1.json'):
        print("Running Redis elasticity test")
        os.system('python fig1.py')


run_elastic_redis()
run_elastic_cpu()
run_elastic_mem()

ela_cpu_res = load_res('fig13-sample-adaptive-ela-cpu.json')
ela_mem_res = load_res('fig13-sample-adaptive-ela-mem.json')
redis_res = load_res('fig1.json')
plot_fig13(redis_res, ela_cpu_res, ela_mem_res)

et = time.time()
save_time('fig13', et - st)
