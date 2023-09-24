hash_bucket_assoc_setting = 8

hash_bucket_num_setting = {
    "ycsb": 4192 * 4096,
    "webmail-all": {
        "0.5": 122112,
        "0.4": 97664,
        "0.3": 73216,
        "0.2": 48768,
        "0.1": 24064,
        "0.05": 12160,
        "0.01": 2432,
        "0.005": 1152,
        "0.001": 256
    },
    "twitter020-10m": {
        "0.2": 129536,
        "0.1": 64512,
        "0.05": 32256,
        "0.01": 6144
    },
    "twitter049-10m": {
        "0.2": 61440,
        "0.1": 30720,
        "0.05": 15360,
        "0.01": 3072
    },
    "twitter042-10m": {
        "0.2": 40960,
        "0.1": 20480,
        "0.05": 10240,
        "0.01": 2048
    },
    "ibm000-40m": {
        "0.1": 9856,
        "0.01": 896
    },
    "ibm005-40m": {
        "0.1": 4864,
        "0.01": 4864,
    },
    "ibm010-10m": {
        "0.1": 252288,
        "0.01": 25216
    },
    "ibm012-10m": {
        "0.1": 2432,
        "0.01": 2432
    },
    "ibm018-40m": {
        "0.1": 1380096,
        "0.01": 137984
    },
    "ibm024-10m": {
        "0.1": 128,
        "0.01": 128
    },
    "ibm026-10m": {
        "0.1": 16896,
        "0.01": 1664
    },
    "ibm029-10m": {
        "0.1": 168704,
        "0.01": 16768
    },
    "ibm031-10m": {
        "0.1": 9216,
        "0.01": 896
    },
    "ibm034-10m": {
        "0.1": 59008,
        "0.01": 5888
    },
    "ibm036-10m": {
        "0.1": 19840,
        "0.01": 1920
    },
    "ibm044-10m": {
        "0.2": 110208,
        "0.1": 55040,
        "0.05": 27520,
        "0.01": 5504
    },
    "ibm045-10m": {
        "0.1": 274560,
        "0.01": 27392
    },
    "ibm049-10m": {
        "0.1": 477312,
        "0.01": 47616
    },
    "ibm050-10m": {
        "0.1": 478464,
        "0.01": 47744
    },
    "ibm058-40m": {
        "0.1": 82304,
        "0.01": 8192
    },
    "ibm061-10m": {
        "0.1": 128,
        "0.01": 128
    },
    "ibm063-40m": {
        "0.1": 817024,
        "0.01": 81664
    },
    "ibm075-40m": {
        "0.1": 1504768,
        "0.01": 150400
    },
    "ibm083-40m": {
        "0.1": 1054080,
        "0.01": 105344
    },
    "ibm085-10m": {
        "0.1": 139008,
        "0.01": 13824
    },
    "ibm096-40m": {
        "0.1": 1449344,
        "0.01": 144896
    },
    "ibm097-40m": {
        "0.1": 256,
        "0.01": 256
    },
    "cphy01-50m": {
        "0.1": 1783552,
        "0.01": 178304
    },
    "cphy02-50m": {
        "0.1": 19072,
        "0.01": 1792
    },
    "cphy03-50m": {
        "0.1": 1061888,
        "0.01": 106112
    },
    "cphy04-50m": {
        "0.1": 707968,
        "0.01": 70784
    },
    "cphy05-50m": {
        "0.1": 1790336,
        "0.01": 178944
    },
    "cphy06-50m": {
        "0.1": 81664,
        "0.01": 8064
    },
    "cphy07-50m": {
        "0.1": 391936,
        "0.01": 39168
    },
    "cphy08-50m": {
        "0.1": 451968,
        "0.01": 45184
    },
    "cphy09-50m": {
        "0.1": 743296,
        "0.01": 74240
    },
    "cphy10-50m": {
        "0.1": 626432,
        "0.01": 62592
    },
    # "changing": 256,
    "changing": 260,
    "mix": 7168,
    "webmail-elastic": 122112
}

cache_config_cmd_map = {
    "ycsb": "python modify_config.py server_data_len=21474836480 segment_size=1048576 block_size=256",
    "webmail-all": {
        "0.5": "python modify_config.py server_data_len=125043712 segment_size=976896 block_size=256",
        "0.4": "python modify_config.py server_data_len=100008960 segment_size=781312 block_size=256",
        "0.3": "python modify_config.py server_data_len=74974208 segment_size=585728 block_size=256",
        "0.2": "python modify_config.py server_data_len=49939456 segment_size=390144 block_size=256",
        "0.1": "python modify_config.py server_data_len=25755648 segment_size=192512 block_size=256",
        "0.05": "python modify_config.py server_data_len=15089664 segment_size=97280 block_size=256",
        "0.01": "python modify_config.py server_data_len=6373376 segment_size=19456 block_size=256",
        "0.005": "python modify_config.py server_data_len=5226496 segment_size=9216 block_size=256",
        "0.001": "python modify_config.py server_data_len=4423680 segment_size=2048 block_size=256"
    },
    "twitter020-10m": {
        "0.2": "python modify_config.py server_data_len=132645888 segment_size=1036288 block_size=256",
        "0.1": "python modify_config.py server_data_len=66061312 segment_size=516096 block_size=256",
        "0.05": "python modify_config.py server_data_len=33095680 segment_size=258048 block_size=256",
        "0.01": "python modify_config.py server_data_len=9699328 segment_size=49152 block_size=256"
    },
    "twitter049-10m": {
        "0.2": "python modify_config.py server_data_len=62915584 segment_size=491520 block_size=256",
        "0.1": "python modify_config.py server_data_len=31719424 segment_size=245760 block_size=256",
        "0.05": "python modify_config.py server_data_len=17956864 segment_size=122880 block_size=256",
        "0.01": "python modify_config.py server_data_len=6946816 segment_size=24576 block_size=256"
    },
    "twitter042-10m": {
        "0.2": "python modify_config.py server_data_len=41944064 segment_size=327680 block_size=256",
        "0.1": "python modify_config.py server_data_len=22544384 segment_size=163840 block_size=256",
        "0.05": "python modify_config.py server_data_len=13369344 segment_size=81920 block_size=256",
        "0.01": "python modify_config.py server_data_len=6029312 segment_size=16384 block_size=256"
    },
    "ibm000-40m": {
        "0.1": "python modify_config.py server_data_len=13025280 segment_size=78848 block_size=256",
        "0.01": "python modify_config.py server_data_len=4997120 segment_size=7168 block_size=256"
    },
    "ibm005-40m": {
        "0.1": "python modify_config.py server_data_len=8552448 segment_size=38912 block_size=256",
        "0.01": "python modify_config.py server_data_len=8552448 segment_size=38912 block_size=256",
    },
    "ibm010-10m": {
        "0.1": "python modify_config.py server_data_len=258343936 segment_size=2018304 block_size=256",
        "0.01": "python modify_config.py server_data_len=26787840 segment_size=25216 block_size=256"
    },
    "ibm012-10m": {
        "0.1": "python modify_config.py server_data_len=6373376 segment_size=19456 block_size=256",
        "0.01": "python modify_config.py server_data_len=6373376 segment_size=19456 block_size=256"
    },
    "ibm018-40m": {
        "0.1": "python modify_config.py server_data_len=1413219328 segment_size=11040768 block_size=256",
        "0.01": "python modify_config.py server_data_len=141296640 segment_size=1103872 block_size=256"
    },
    "ibm024-10m": {
        "0.1": "python modify_config.py server_data_len=4308992 segment_size=1024 block_size=256",
        "0.01": "python modify_config.py server_data_len=4308992 segment_size=1024 block_size=256"
    },
    "ibm026-10m": {
        "0.1": "python modify_config.py server_data_len=19333120 segment_size=135168 block_size=256",
        "0.01": "python modify_config.py server_data_len=5685248 segment_size=13312 block_size=256"
    },
    "ibm029-10m": {
        "0.1": "python modify_config.py server_data_len=172753920 segment_size=1349632 block_size=256",
        "0.01": "python modify_config.py server_data_len=19218432 segment_size=134144 block_size=256"
    },
    "ibm031-10m": {
        "0.1": "python modify_config.py server_data_len=12451840 segment_size=73728 block_size=256",
        "0.01": "python modify_config.py server_data_len=4997120 segment_size=7168 block_size=256"
    },
    "ibm034-10m": {
        "0.1": "python modify_config.py server_data_len=60425216 segment_size=472064 block_size=256",
        "0.01": "python modify_config.py server_data_len=9469952 segment_size=47104 block_size=256"
    },
    "ibm036-10m": {
        "0.1": "python modify_config.py server_data_len=21970944 segment_size=158720 block_size=256",
        "0.01": "python modify_config.py server_data_len=5914624 segment_size=15360 block_size=256"
    },
    "ibm044-10m": {
        "0.2": "python modify_config.py server_data_len=112854016 segment_size=881664 block_size=256",
        "0.1": "python modify_config.py server_data_len=56361984 segment_size=440320 block_size=256",
        "0.05": "python modify_config.py server_data_len=28852224 segment_size=220160 block_size=256",
        "0.01": "python modify_config.py server_data_len=9125888 segment_size=44032 block_size=256"
    },
    "ibm045-10m": {
        "0.1": "python modify_config.py server_data_len=281150464 segment_size=2196480 block_size=256",
        "0.01": "python modify_config.py server_data_len=28737536 segment_size=219136 block_size=256"
    },
    "ibm049-10m": {
        "0.1": "python modify_config.py server_data_len=488768512 segment_size=3818496 block_size=256",
        "0.01": "python modify_config.py server_data_len=48759808 segment_size=380928 block_size=256"
    },
    "ibm050-10m": {
        "0.1": "python modify_config.py server_data_len=489948160 segment_size=3827712 block_size=256",
        "0.01": "python modify_config.py server_data_len=48890880 segment_size=381952 block_size=256"
    },
    "ibm058-40m": {
        "0.1": "python modify_config.py server_data_len=84280320 segment_size=658432 block_size=256",
        "0.01": "python modify_config.py server_data_len=11534336 segment_size=65536 block_size=256"
    },
    "ibm061-10m": {
        "0.1": "python modify_config.py server_data_len=4308992 segment_size=1024 block_size=256",
        "0.01": "python modify_config.py server_data_len=4308992 segment_size=1024 block_size=256"
    },
    "ibm063-40m": {
        "0.1": "python modify_config.py server_data_len=836633600 segment_size=6536192 block_size=256",
        "0.01": "python modify_config.py server_data_len=83624960 segment_size=653312 block_size=256"
    },
    "ibm075-40m": {
        "0.1": "python modify_config.py server_data_len=1540883456 segment_size=12038144 block_size=256",
        "0.01": "python modify_config.py server_data_len=154010624 segment_size=1203200 block_size=256"
    },
    "ibm083-40m": {
        "0.1": "python modify_config.py server_data_len=1079378944 segment_size=8432640 block_size=256",
        "0.01": "python modify_config.py server_data_len=107873280 segment_size=842752 block_size=256"
    },
    "ibm085-10m": {
        "0.1": "python modify_config.py server_data_len=142345216 segment_size=1112064 block_size=256",
        "0.01": "python modify_config.py server_data_len=16580608 segment_size=110592 block_size=256"
    },
    "ibm096-40m": {
        "0.1": "python modify_config.py server_data_len=1484129280 segment_size=11594752 block_size=256",
        "0.01": "python modify_config.py server_data_len=148374528 segment_size=1159168 block_size=256"
    },
    "ibm097-40m": {
        "0.1": "python modify_config.py server_data_len=4423680 segment_size=2048 block_size=256",
        "0.01": "python modify_config.py server_data_len=4423680 segment_size=2048 block_size=256"
    },
    "cphy01-50m": {
        "0.1": "python modify_config.py server_data_len=1826358272 segment_size=14268416 block_size=256",
        "0.01": "python modify_config.py server_data_len=182584320 segment_size=1426432 block_size=256"
    },
    "cphy02-50m": {
        "0.1": "python modify_config.py server_data_len=21282816 segment_size=152576 block_size=256",
        "0.01": "python modify_config.py server_data_len=5799936 segment_size=14336 block_size=256"
    },
    "cphy03-50m": {
        "0.1": "python modify_config.py server_data_len=1087374336 segment_size=8495104 block_size=256",
        "0.01": "python modify_config.py server_data_len=108659712 segment_size=848896 block_size=256"
    },
    "cphy04-50m": {
        "0.1": "python modify_config.py server_data_len=724960256 segment_size=5663744 block_size=256",
        "0.01": "python modify_config.py server_data_len=72483840 segment_size=566272 block_size=256"
    },
    "cphy05-50m": {
        "0.1": "python modify_config.py server_data_len=1833305088 segment_size=14322688 block_size=256",
        "0.01": "python modify_config.py server_data_len=183239680 segment_size=1431552 block_size=256"
    },
    "cphy06-50m": {
        "0.1": "python modify_config.py server_data_len=83624960 segment_size=653312 block_size=256",
        "0.01": "python modify_config.py server_data_len=11419648 segment_size=64512 block_size=256"
    },
    "cphy07-50m": {
        "0.1": "python modify_config.py server_data_len=401343488 segment_size=3135488 block_size=256",
        "0.01": "python modify_config.py server_data_len=40109056 segment_size=313344 block_size=256"
    },
    "cphy08-50m": {
        "0.1": "python modify_config.py server_data_len=462816256 segment_size=3615744 block_size=256",
        "0.01": "python modify_config.py server_data_len=46269440 segment_size=361472 block_size=256"
    },
    "cphy09-50m": {
        "0.1": "python modify_config.py server_data_len=761136128 segment_size=5946368 block_size=256",
        "0.01": "python modify_config.py server_data_len=76022784 segment_size=593920 block_size=256"
    },
    "cphy10-50m": {
        "0.1": "python modify_config.py server_data_len=641467392 segment_size=5011456 block_size=256",
        "0.01": "python modify_config.py server_data_len=64095232 segment_size=500736 block_size=256"
    },
    "changing": "python modify_config.py server_data_len=4521984 segment_size=2048 block_size=256",
    "mix": "python modify_config.py server_data_len=10616832 segment_size=229376 block_size=256",
    "webmail-elastic": "python modify_config.py server_data_len=125043712 segment_size=2048 block_size=256"
}

cmake_templates = {
    'use_shard_pqueue': '-Duse_shard_pqueue={}',
    'num_pqueue_shards': '-Dnum_pqueue_shards={}',
    'use_lock_backoff': '-Duse_lock_backoff={}',
    'ela_mem_tpt': '-Dela_mem_tpt={}',
    'use_fiber': '-Duse_fiber={}',
    'use_penalty': '-Duse_penalty={}',
    'multi_policy': '-Dmulti_policy={}',
    'hash_bucket_assoc': '-Dhash_bucket_assoc={}',
    'hash_bucket_num': '-Dhash_bucket_num={}'
}

running_default_opt = {
    'ela_mem_tpt': 'OFF',
    'use_fiber': 'OFF',
    'use_penalty': 'ON',
    'multi_policy': 'OFF',
}

hash_table_opt = {
    'hash_bucket_assoc': '8',
    'hash_bucket_num': '17170432'
}

default_method_opt_dict = {
    'use_shard_pqueue': 'OFF',
    'num_pqueue_shards': '32',
    'use_lock_backoff': 'ON',
}

shard_lru_opt_dict = {
    'use_shard_pqueue': 'ON',
    'num_pqueue_shards': '32',
    'use_lock_backoff': 'ON',
}

precise_lru_opt_dict = {
    'use_shard_pqueue': 'OFF',
    'num_pqueue_shards': '32',
    'use_lock_backoff': 'OFF',
}

cliquemap_shard_opt_dict = {
    'use_shard_pqueue': 'ON',
    'num_pqueue_shards': '128',
    'use_lock_backoff': 'ON',
}


def get_workload_defs(wl, size):
    if wl in ['ycsb', 'mix', 'webmail-elastic']:
        return f"-Dhash_bucket_assoc={hash_bucket_assoc_setting} -Dhash_bucket_num={hash_bucket_num_setting[wl]}"
    elif wl == 'changing':
        return f"-Dhash_bucket_assoc={16} -Dhash_bucket_num={hash_bucket_num_setting[wl]}"
    return f"-Dhash_bucket_assoc={hash_bucket_assoc_setting} -Dhash_bucket_num={hash_bucket_num_setting[wl][size]}"


def get_method_opts(method):
    if method in ['sample-adaptive', 'sample-lru', 'sample-lfu', 'non']:
        return default_method_opt_dict
    if method == 'shard-lru':
        return shard_lru_opt_dict
    if method in ['cliquemap-precise-lru', 'cliquemap-precise-lfu']:
        return default_method_opt_dict
    if method == 'precise-lru':
        return precise_lru_opt_dict
    if method in ['cliquemap-shard-lru', 'cliquemap-shard-lfu']:
        return cliquemap_shard_opt_dict


def get_workload_opts(wl, size):
    hash_bucket_assoc = 0
    hash_bucket_num = 0
    if wl in ['ycsb', 'mix']:
        hash_bucket_assoc = hash_bucket_assoc_setting
        hash_bucket_num = hash_bucket_num_setting[wl]
    elif wl == 'changing':
        hash_bucket_assoc = 17
        hash_bucket_num = hash_bucket_num_setting[wl]
    elif wl == 'webmail-elastic':
        hash_bucket_assoc = 8
        hash_bucket_num = hash_bucket_num_setting[wl]
    else:
        hash_bucket_assoc = hash_bucket_assoc_setting
        hash_bucket_num = hash_bucket_num_setting[wl][size]
    hash_table_opt = {
        'hash_bucket_assoc': f'{hash_bucket_assoc}',
        'hash_bucket_num': f'{hash_bucket_num}'
    }
    return hash_table_opt


def get_cmake_cmd(method, wl, size, running_opts):
    method_opts = get_method_opts(method)
    workload_opts = get_workload_opts(wl, size)
    running_opts = running_opts

    cmake_opt_list = []
    for opt in method_opts:
        cmake_opt_list.append(cmake_templates[opt].format(method_opts[opt]))
    for opt in workload_opts:
        cmake_opt_list.append(cmake_templates[opt].format(workload_opts[opt]))
    for opt in running_opts:
        cmake_opt_list.append(cmake_templates[opt].format(running_opts[opt]))
    cmake_opt_str = ' '.join(cmake_opt_list)
    return f"cmake .. {cmake_opt_str}"


def get_make_cmd(build_dir, method, wl, size, running_opts=running_default_opt):
    cmake_cmd = get_cmake_cmd(method, wl, size, running_opts)
    return f"cd {build_dir} && {cmake_cmd} && make clean && make -j 8"


def get_cache_config_cmd(config_dir, wl, size):
    if wl in ["ycsb", "changing", "mix", "webmail-elastic"]:
        return f"cd {config_dir} && {cache_config_cmd_map[wl]}"
    return f"cd {config_dir} && {cache_config_cmd_map[wl][size]}"


def get_running_opt():
    return running_default_opt.copy()


def get_freq_cache_cmd(config_dir, cache_size):
    if cache_size == 0:
        return f"cd {config_dir} && python modify_config.py use_freq_cache=false"
    else:
        return f"cd {config_dir} && python modify_config.py use_freq_cache=true freq_cache_size={cache_size}"


def get_mn_cpu_cmd(config_dir, num_cpu):
    assert (num_cpu > 0)
    return f"cd {config_dir} && python modify_config.py num_server_threads={num_cpu}"
