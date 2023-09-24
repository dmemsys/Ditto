cluster_ips = [f'node-{i}' for i in range(10)]
master_id = 0
mn_id = 1
client_ids = [i for i in range(2, 10)]

default_fc_size = 10*1024*1024

NUM_CLIENT_PER_NODE = 32

EXP_HOME = 'Ditto'
build_dir = f'{EXP_HOME}/build'
config_dir = f'{EXP_HOME}/experiments'

RESET_CMD = 'pkill -f controller.py && pkill init'
RESET_WORKER_CMD = 'pkill init'
RESET_MASTER_CMD = 'pkill -f controller.py'
