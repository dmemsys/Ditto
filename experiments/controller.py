import time
import memcache
import argparse
import json
import numpy as np

NUM_YCSB_LOADERS = 16
ELA_TPT_SCALE_CPU_TIME = 180
# ELA_TPT_SCALE_CPU_RUN_TIME = 500 # ycsba
# ELA_TPT_SCALE_CPU_RUN_TIME = 487 # ycsbc
ELA_TPT_SCALE_CPU_RUN_TIME = 488  # ycsbc
ELA_TPT_TOTAL_CPU_RUN_TIME = 1200
ELA_HR_SCALE_MEM_TIME = 40
# ELA_SCALE_TIME = 10
# ELA_SCALE_RUN_TIME = 10
# ELA_TOTAL_RUN_TIME = 30


class DMCMemcachedController:
    def __init__(self, memcached_ip, memcached_port,
                 num_servers, num_clients):
        self.memcached_ip = memcached_ip
        self.memcached_port = memcached_port
        self.num_servers = num_servers
        self.num_clients = num_clients
        self.num_sync = 0
        self.num_result = 0
        self.mc = memcache.Client(
            ['{}:{}'.format(memcached_ip, memcached_port)], debug=False)
        assert (self.mc != None)
        self.mc.flush_all()

    def sync_msg(self, msg):
        self.mc.set(msg, 1)

    def wait_msg(self, msg):
        val = self.mc.get(msg)
        while val == None:
            val = self.mc.get(msg)

    def sync_ready_clients_num(self, client_num):
        for i in range(client_num):
            cid = i + self.num_servers
            key = "client-{}-ready-{}".format(cid, self.num_sync)
            val = self.mc.get(key)
            while val == None:
                val = self.mc.get(key)
            print("Client {} ready: {}".format(cid, val))
        print("All client ready!")
        self.mc.set("all-client-ready-{}".format(self.num_sync), 1)
        self.num_sync += 1

    def sync_ready_clients(self):
        for i in range(self.num_clients):
            cid = i + self.num_servers
            key = "client-{}-ready-{}".format(cid, self.num_sync)
            val = self.mc.get(key)
            while val == None:
                val = self.mc.get(key)
            print("Client {} ready: {}".format(cid, val))
        print("All client ready!")
        self.mc.set("all-client-ready-{}".format(self.num_sync), 1)
        self.num_sync += 1

    def gather_client_results_fiber(self, client_num, num_fb_per_thread):
        client_result_dict = {}
        for i in range(client_num):
            cid = i + self.num_servers
            for fb in range(num_fb_per_thread):
                key = f'client-{cid}-fb-{fb}-result-{self.num_result}'
                val = self.mc.get(key)
                while val == None:
                    val = self.mc.get(key)
                client_result_dict[f'{cid}-{fb}'] = json.loads(
                    str(val.decode('UTF-8')))
                print(f'get {key}')
        self.num_result += 1
        return client_result_dict

    def gather_client_results_num(self, client_num):
        client_result_dict = {}
        for i in range(client_num):
            cid = i + self.num_servers
            key = "client-{}-result-{}".format(cid, self.num_result)
            val = self.mc.get(key)
            while val == None:
                val = self.mc.get(key)
            # print(json.loads(val))
            client_result_dict[cid] = json.loads(str(val.decode('UTF-8')))
        self.num_result += 1
        return client_result_dict

    def gather_client_results(self) -> dict:
        client_result_dict = {}
        for i in range(self.num_clients):
            cid = i + self.num_servers
            key = "client-{}-result-{}".format(cid, self.num_result)
            val = self.mc.get(key)
            while val == None:
                val = self.mc.get(key)
            # print(json.loads(val))
            client_result_dict[cid] = json.loads(str(val.decode('UTF-8')))
        self.num_result += 1
        return client_result_dict

    def clear_all_contents(self):
        self.mc.flush_all()

    def stop_server(self):
        self.mc.set("server-stop", 1)

    def get_server_stats(self):
        key = "server-result"
        val = self.mc.get(key)
        while val == None:
            val = self.mc.get(key)
        return json.loads(str(val.decode('UTF-8')))


def control_micro_bench(controller: DMCMemcachedController):
    res_dict = {}
    # sync set
    controller.sync_ready_clients()
    res_dict["set"] = controller.gather_client_results()

    # sync get
    controller.sync_ready_clients()
    res_dict["get"] = controller.gather_client_results()

    # sync upd
    controller.sync_ready_clients()
    res_dict["upd"] = controller.gather_client_results()
    return res_dict


def control_ycsb_bench_ela_mem_fiber(controller: DMCMemcachedController,
                                     num_clients, num_fb_per_thread):
    print("control ycsb ela memory")
    n_clients = max(num_clients, NUM_YCSB_LOADERS)
    res_dict = {}
    # sync load
    controller.sync_ready_clients_num(n_clients)
    res_dict['load'] = controller.gather_client_results_num(n_clients)

    # sync 32 clients to execute trans first
    controller.sync_ready_clients_num(n_clients)
    for i in range(1):
        time.sleep(ELA_TPT_SCALE_CPU_TIME)
        print(f'Scale memory {i}')
        controller.sync_msg(f'server-scale-memory-{i}')
        controller.wait_msg(f'server-scale-memory-ok-{i}')
        controller.sync_msg(f'client-scale-memory-{i}')
    print("Finish scaling")
    res_dict = controller.gather_client_results_fiber(num_clients,
                                                      num_fb_per_thread)

    # merge results
    # merge lat_map
    print(len(res_dict['1-0']['lat_map_cont']))
    assert (len(res_dict['1-0']['lat_map_cont'])
            == ELA_TPT_TOTAL_CPU_RUN_TIME / 8)
    combined_lat_map = []
    for j in range(len(res_dict['1-0']['lat_map_cont'])):
        cur_lat_map = {}
        for i in range(1, 33):
            for fb in range(num_fb_per_thread):
                cid = f'{i}-{fb}'
            for ent in res_dict[cid]['lat_map_cont'][j]:
                if ent[0] not in cur_lat_map:
                    cur_lat_map[ent[0]] = 0
                cur_lat_map[ent[0]] += ent[1]
        combined_lat_map.append(cur_lat_map)

    p50_cont = []
    p99_cont = []
    for m in combined_lat_map:
        tmp_lat = []
        for k, v in m.items():
            tmp_lat += [int(k)] * v
        tmp_lat.sort()
        p50_cont.append(tmp_lat[int(len(tmp_lat) * 0.5)])
        p99_cont.append(tmp_lat[int(len(tmp_lat) * 0.99)])

    # merge tpt
    assert (len(res_dict['1-0']['ops_cont']) == ELA_TPT_TOTAL_CPU_RUN_TIME * 2)
    agg = np.zeros(len(res_dict['1-0']['ops_cont']))
    for i in range(1, 33):
        for fb in range(num_fb_per_thread):
            cid = f'{i}-{fb}'
            agg += np.array(res_dict[cid]['ops_cont'])
    sft = np.array([0] + list(agg)[:-1])
    tpt = (agg - sft) / 0.5
    combined_dict = {
        'p99': list(p99_cont),
        'p50': list(p50_cont),
        'tpt': list(tpt),
        'agg': list(agg),
        'scale_time': ELA_TPT_SCALE_CPU_TIME,
        'shrink_time': ELA_TPT_SCALE_CPU_TIME + ELA_TPT_SCALE_CPU_RUN_TIME
    }
    return combined_dict


def control_ycsb_bench_ela_mem(controller: DMCMemcachedController, num_clients):
    print("control ycsb ela memory")
    n_clients = max(num_clients, NUM_YCSB_LOADERS)
    res_dict = {}
    # sync load
    controller.sync_ready_clients_num(n_clients)
    res_dict['load'] = controller.gather_client_results_num(n_clients)

    # sync 32 clients to execute trans first
    controller.sync_ready_clients_num(n_clients)
    for i in range(1):
        time.sleep(ELA_TPT_SCALE_CPU_TIME)
        print(f'Scale memory {i}')
        controller.sync_msg(f'server-scale-memory-{i}')
        controller.wait_msg(f'server-scale-memory-ok-{i}')
        controller.sync_msg(f'client-scale-memory-{i}')
    print("Finish scaling")
    res_dict = controller.gather_client_results()

    # merge results
    # merge lat_map
    print(len(res_dict[1]['lat_map_cont']))
    assert (len(res_dict[1]['lat_map_cont']) == ELA_TPT_TOTAL_CPU_RUN_TIME / 4)
    combined_lat_map = []
    for j in range(len(res_dict[1]['lat_map_cont'])):
        cur_lat_map = {}
        for i in range(1, 33):
            for ent in res_dict[i]['lat_map_cont'][j]:
                if ent[0] not in cur_lat_map:
                    cur_lat_map[ent[0]] = 0
                cur_lat_map[ent[0]] += ent[1]
        combined_lat_map.append(cur_lat_map)

    p50_cont = []
    p99_cont = []
    for m in combined_lat_map:
        tmp_lat = []
        for k, v in m.items():
            tmp_lat += [int(k)] * v
        tmp_lat.sort()
        p50_cont.append(tmp_lat[int(len(tmp_lat) * 0.5)])
        p99_cont.append(tmp_lat[int(len(tmp_lat) * 0.99)])

    # merge tpt
    assert (len(res_dict[1]['ops_cont']) == ELA_TPT_TOTAL_CPU_RUN_TIME * 2)
    agg = np.zeros(len(res_dict[1]['ops_cont']))
    for i in range(1, 33):
        agg += np.array(res_dict[i]['ops_cont'])
    sft = np.array([0] + list(agg)[:-1])
    tpt = (agg - sft) / 0.5
    combined_dict = {
        'p99': list(p99_cont),
        'p50': list(p50_cont),
        'tpt': list(tpt),
        'agg': list(agg),
        'scale_time': ELA_TPT_SCALE_CPU_TIME,
        'shrink_time': ELA_TPT_SCALE_CPU_TIME + ELA_TPT_SCALE_CPU_RUN_TIME
    }
    return combined_dict


def control_ycsb_bench_ela_cpu_fiber(controller: DMCMemcachedController,
                                     num_clients, num_fb_per_thread):
    n_clients = max(num_clients, NUM_YCSB_LOADERS)
    res_dict = {}
    # sync load
    controller.sync_ready_clients_num(n_clients)
    res_dict['load'] = controller.gather_client_results_num(n_clients)

    # sync 16 clients to execute trans first
    controller.sync_ready_clients_num(n_clients)
    # sync the other 16 clients to execute trans after 300s
    time.sleep(ELA_TPT_SCALE_CPU_TIME)
    print('Start another 32 clients')
    controller.sync_msg("scale-to-64")
    res_dict = controller.gather_client_results_fiber(num_clients,
                                                      num_fb_per_thread)

    # merge results
    # merge lat_map
    print(len(res_dict['1-0']['lat_map_cont']))
    assert (len(res_dict['1-0']['lat_map_cont'])
            == ELA_TPT_TOTAL_CPU_RUN_TIME // 8)
    combined_lat_map = []
    for j in range(len(res_dict['1-0']['lat_map_cont'])):
        cur_lat_map = {}
        for i in range(1, 33):
            for fb in range(num_fb_per_thread):
                cid = f'{i}-{fb}'
                for ent in res_dict[cid]['lat_map_cont'][j]:
                    if ent[0] not in cur_lat_map:
                        cur_lat_map[ent[0]] = 0
                    cur_lat_map[ent[0]] += ent[1]
        combined_lat_map.append(cur_lat_map)
    print(len(res_dict['33-0']['lat_map_cont']))
    assert (len(res_dict['33-0']['lat_map_cont'])
            == ELA_TPT_SCALE_CPU_RUN_TIME // 8)
    for j in range(len(res_dict['33-0']['lat_map_cont'])):
        cur_lat_map = {}
        for i in range(33, 65):
            for fb in range(num_fb_per_thread):
                cid = f'{i}-{fb}'
                for ent in res_dict[cid]['lat_map_cont'][j]:
                    if ent[0] not in cur_lat_map:
                        cur_lat_map[ent[0]] = 0
                    cur_lat_map[ent[0]] += ent[1]
        combined_lat_map[j + ELA_TPT_SCALE_CPU_TIME // 8].update(cur_lat_map)

    p50_cont = []
    p99_cont = []
    for m in combined_lat_map:
        tmp_lat = []
        for k, v in m.items():
            tmp_lat += [int(k)] * v
        tmp_lat.sort()
        p50_cont.append(tmp_lat[int(len(tmp_lat) * 0.5)])
        p99_cont.append(tmp_lat[int(len(tmp_lat) * 0.99)])

    # merge tpt
    assert (len(res_dict['1-0']['ops_cont']) == ELA_TPT_TOTAL_CPU_RUN_TIME * 2)
    agg = np.zeros(len(res_dict['1-0']['ops_cont']))
    for i in range(1, 33):
        for fb in range(num_fb_per_thread):
            agg += np.array(res_dict[f'{i}-{fb}']['ops_cont'])
    for i in range(33, 65):
        for fb in range(num_fb_per_thread):
            cid = f'{i}-{fb}'
            assert (len(res_dict[cid]['ops_cont'])
                    == ELA_TPT_SCALE_CPU_RUN_TIME * 2)
            tpt_lst = [0] * (ELA_TPT_SCALE_CPU_TIME * 2)\
                + res_dict[cid]['ops_cont']\
                + [res_dict[cid]['ops_cont'][-1]]\
                * ((ELA_TPT_TOTAL_CPU_RUN_TIME
                    - ELA_TPT_SCALE_CPU_RUN_TIME
                    - ELA_TPT_SCALE_CPU_TIME) * 2)
            agg += np.array(tpt_lst)
    sft = np.array([0] + list(agg)[:-1])
    tpt = (agg - sft) / 0.5
    combined_dict = {
        'p99': list(p99_cont),
        'p50': list(p50_cont),
        'tpt': list(tpt),
        'agg': list(agg),
        'scale_time': ELA_TPT_SCALE_CPU_TIME,
        'shrink_time': ELA_TPT_SCALE_CPU_TIME + ELA_TPT_SCALE_CPU_RUN_TIME
    }
    return combined_dict


def control_ycsb_bench_ela_cpu(controller: DMCMemcachedController, num_clients):
    n_clients = max(num_clients, NUM_YCSB_LOADERS)
    res_dict = {}
    # sync load
    controller.sync_ready_clients_num(n_clients)
    res_dict['load'] = controller.gather_client_results_num(n_clients)

    # sync 16 clients to execute trans first
    controller.sync_ready_clients_num(n_clients)
    # sync the other 16 clients to execute trans after 300s
    time.sleep(ELA_TPT_SCALE_CPU_TIME)
    print('Start another 32 clients')
    controller.sync_msg("scale-to-64")
    res_dict = controller.gather_client_results()

    # merge results
    # merge lat_map
    print(len(res_dict[1]['lat_map_cont']))
    assert (len(res_dict[1]['lat_map_cont']) == ELA_TPT_TOTAL_CPU_RUN_TIME / 4)
    combined_lat_map = []
    for j in range(len(res_dict[1]['lat_map_cont'])):
        cur_lat_map = {}
        for i in range(1, 33):
            for ent in res_dict[i]['lat_map_cont'][j]:
                if ent[0] not in cur_lat_map:
                    cur_lat_map[ent[0]] = 0
                cur_lat_map[ent[0]] += ent[1]
        combined_lat_map.append(cur_lat_map)
    print(len(res_dict[33]['lat_map_cont']))
    assert (len(res_dict[33]['lat_map_cont'])
            == ELA_TPT_SCALE_CPU_RUN_TIME / 4)
    for j in range(len(res_dict[33]['lat_map_cont'])):
        cur_lat_map = {}
        for i in range(33, 65):
            for ent in res_dict[i]['lat_map_cont'][j]:
                if ent[0] not in cur_lat_map:
                    cur_lat_map[ent[0]] = 0
                cur_lat_map[ent[0]] += ent[1]
        combined_lat_map[j + ELA_TPT_SCALE_CPU_TIME // 4].update(cur_lat_map)

    p50_cont = []
    p99_cont = []
    for m in combined_lat_map:
        tmp_lat = []
        for k, v in m.items():
            tmp_lat += [int(k)] * v
        tmp_lat.sort()
        p50_cont.append(tmp_lat[int(len(tmp_lat) * 0.5)])
        p99_cont.append(tmp_lat[int(len(tmp_lat) * 0.99)])

    # merge tpt
    assert (len(res_dict[1]['ops_cont']) == ELA_TPT_TOTAL_CPU_RUN_TIME * 2)
    agg = np.zeros(len(res_dict[1]['ops_cont']))
    for i in range(1, 33):
        agg += np.array(res_dict[i]['ops_cont'])
    for i in range(33, 65):
        assert (len(res_dict[i]['ops_cont']) == ELA_TPT_SCALE_CPU_RUN_TIME * 2)
        tpt_lst = [0] * (ELA_TPT_SCALE_CPU_TIME * 2)\
            + res_dict[i]['ops_cont']\
            + [res_dict[i]['ops_cont'][-1]]\
            * ((ELA_TPT_TOTAL_CPU_RUN_TIME
                - ELA_TPT_SCALE_CPU_RUN_TIME
                - ELA_TPT_SCALE_CPU_TIME) * 2)
        agg += np.array(tpt_lst)
    sft = np.array([0] + list(agg)[:-1])
    tpt = (agg - sft) / 0.5
    combined_dict = {
        'p99': list(p99_cont),
        'p50': list(p50_cont),
        'tpt': list(tpt),
        'agg': list(agg),
        'scale_time': ELA_TPT_SCALE_CPU_TIME,
        'shrink_time': ELA_TPT_SCALE_CPU_TIME + ELA_TPT_SCALE_CPU_RUN_TIME
    }
    return combined_dict


def control_ycsb_bench_fiber(controller: DMCMemcachedController, num_clients, num_fb_per_thread):
    n_clients = max(num_clients, NUM_YCSB_LOADERS)
    res_dict = {}
    # sync load
    controller.sync_ready_clients_num(n_clients)
    res_dict["load"] = controller.gather_client_results_num(n_clients)

    # sync trans
    controller.sync_ready_clients_num(n_clients)
    res_dict["trans"] = controller.gather_client_results_fiber(
        num_clients, num_fb_per_thread)

    # merge tpt and latency
    ops = 0
    merged_map = {}
    for i in range(num_clients):
        cid = i + 1
        for fb in range(num_fb_per_thread):
            ops += int(res_dict['trans'][f'{cid}-{fb}']['ops'])
            cur_map = res_dict['trans'][f'{cid}-{fb}']['lat_map']
            for ent in cur_map:
                if ent[0] not in merged_map:
                    merged_map[ent[0]] = 0
                merged_map[ent[0]] += ent[1]
    key_list = list(merged_map.keys())
    key_list.sort()
    lat_list = []
    for k in key_list:
        lat_list += [k] * merged_map[k]
    for i in range(1, len(lat_list)):
        assert (lat_list[i - 1] <= lat_list[i])
    print(f'tpt:  {ops/20}')
    print(f'p50:  {lat_list[int(len(lat_list) * 0.5)]}')
    print(f'p90:  {lat_list[int(len(lat_list) * 0.9)]}')
    print(f'p99:  {lat_list[int(len(lat_list) * 0.99)]}')
    print(f'p999: {lat_list[int(len(lat_list) * 0.999)]}')
    return res_dict


def control_ycsb_bench(controller: DMCMemcachedController, num_clients):
    n_clients = max(num_clients, NUM_YCSB_LOADERS)
    res_dict = {}
    # sync load
    controller.sync_ready_clients_num(n_clients)
    res_dict["load"] = controller.gather_client_results_num(n_clients)

    # sync trans
    controller.sync_ready_clients_num(n_clients)
    res_dict["trans"] = controller.gather_client_results()

    # merge tpt and latency
    ops = 0
    merged_map = {}
    for i in range(num_clients):
        cid = i + 1
        ops += int(res_dict['trans'][cid]['ops'])
        cur_map = res_dict['trans'][cid]['lat_map']
        for ent in cur_map:
            if ent[0] not in merged_map:
                merged_map[ent[0]] = 0
            merged_map[ent[0]] += ent[1]
    key_list = list(merged_map.keys())
    key_list.sort()
    lat_list = []
    for k in key_list:
        lat_list += [k] * merged_map[k]
    for i in range(1, len(lat_list)):
        assert (lat_list[i - 1] <= lat_list[i])
    json_res = {
        'tpt': ops/20,
        'p50': lat_list[int(len(lat_list) * 0.5)],
        'p90': lat_list[int(len(lat_list) * 0.9)],
        'p99': lat_list[int(len(lat_list) * 0.99)],
        'p999': lat_list[int(len(lat_list) * 0.999)],
    }
    print(json.dumps(json_res))
    # print(f'tpt:  {ops/20}')
    # print(f'p50:  {lat_list[int(len(lat_list) * 0.5)]}')
    # print(f'p90:  {lat_list[int(len(lat_list) * 0.9)]}')
    # print(f'p99:  {lat_list[int(len(lat_list) * 0.99)]}')
    # print(f'p999: {lat_list[int(len(lat_list) * 0.999)]}')
    return res_dict


def control_micro_singlekey_bench(controller: DMCMemcachedController):
    res_dict = {}

    # sync iterative set
    controller.sync_ready_clients()
    res_dict['set'] = controller.gather_client_results()

    # sync iterative get
    controller.sync_ready_clients()
    res_dict['get'] = controller.gather_client_results()
    return res_dict


def control_workload_bench_ela_cpu_mix(controller: DMCMemcachedController):
    # sync start
    for i in range(0, 11):
        controller.sync_ready_clients()
        controller.sync_ready_clients()

    # gather results
    res = controller.gather_client_results()

    # merge results
    print(f"len: {len(res[1]['n_ops'])}")
    n_ops_vec = np.zeros_like(res[1]['n_ops'])
    n_miss_vec = np.zeros_like(res[1]['n_misses'])
    for i in range(1, 11):
        n_ops_vec += np.array(res[i]['n_ops'])
        n_miss_vec += np.array(res[i]['n_misses'])
    combined_dict = {
        'ops': n_ops_vec.tolist(),
        'misses': n_miss_vec.tolist(),
        'hit_rate': (1 - (n_miss_vec / n_ops_vec)).tolist()
    }
    # print(combined_dict['hit_rate'])
    print(json.dumps(combined_dict))
    return combined_dict


def control_workload_bench_ela_cpu_webmail(controller: DMCMemcachedController):
    for i in range(16, 161, 16):
        controller.sync_ready_clients()
        controller.sync_ready_clients()
    res = controller.gather_client_results()

    # merge results
    print(f"len: {len(res[1]['n_ops'])}")
    n_ops_vec = np.zeros_like(res[1]['n_ops'])
    n_miss_vec = np.zeros_like(res[1]['n_misses'])
    for i in range(1, 161):
        n_ops_vec += np.array(res[i]['n_ops'])
        n_miss_vec += np.array(res[i]['n_misses'])
    combined_dict = {
        'ops': n_ops_vec.tolist(),
        'misses': n_miss_vec.tolist(),
        'hit_rate': (1 - (n_miss_vec / n_ops_vec)).tolist()
    }
    print(json.dumps(combined_dict))
    return combined_dict


def control_workload_bench_ela_cpu(workload, controller: DMCMemcachedController):
    if workload == 'mix':
        return control_workload_bench_ela_cpu_mix(controller)
    assert (workload == 'webmail-all')
    return control_workload_bench_ela_cpu_webmail(controller)


def control_workload_bench_ela_mem(controller: DMCMemcachedController):
    # sync warmup
    controller.sync_ready_clients()

    # sync start
    controller.sync_ready_clients()

    # sync scale memory
    for i in range(4):
        time.sleep(ELA_HR_SCALE_MEM_TIME)
        print(f"Start scale memory {i}")
        controller.sync_msg(f"server-scale-memory-{i}")
        controller.wait_msg(f"server-scale-memory-ok-{i}")
        controller.sync_msg(f"client-scale-memory-{i}")
    res_dict = controller.gather_client_results()

    # merge results
    agg_ops = np.zeros(len(res_dict[1]['n_ops_cont']))
    agg_miss = np.zeros(len(res_dict[1]['n_miss_cont']))
    for i in range(1, controller.num_clients + 1):
        agg_ops += np.array(res_dict[i]['n_ops_cont'])
        agg_miss += np.array(res_dict[i]['n_miss_cont'])
    sft_ops = np.array([0] + list(agg_ops)[:-1])
    sft_miss = np.array([0] + list(agg_miss)[:-1])
    tpt = (agg_ops - sft_ops) / 0.5
    hr = 1 - ((agg_miss - sft_miss) / (agg_ops - sft_ops))
    hr_agg = 1 - (agg_miss / agg_ops)
    combined_dict = {
        'tpt': list(tpt),
        'hr_fine': list(hr),
        'hr_coarse': list(hr_agg),
        'agg_ops': list(agg_ops),
        'agg_miss': list(agg_miss),
        'scale_start_time': ELA_HR_SCALE_MEM_TIME,
    }
    return combined_dict


def control_workload_bench(controller: DMCMemcachedController):
    # sync warmup
    controller.sync_ready_clients()

    # sync start
    controller.sync_ready_clients()
    res_dict = controller.gather_client_results()

    # merge hit rate and continuous hit rate
    agg_ops = np.zeros(len(res_dict[1]['n_ops_cont']))
    agg_miss = np.zeros(len(res_dict[1]['n_miss_cont']))
    agg_weight = np.zeros_like(
        np.array(res_dict[1]['adaptive_weights_cont']), dtype=np.float64)
    num_hist_match = []
    num_weight_adjust = []
    for i in range(1, controller.num_clients + 1):
        agg_ops += np.array(res_dict[i]['n_ops_cont'])
        agg_miss += np.array(res_dict[i]['n_miss_cont'])
        agg_weight += np.array(res_dict[i]['adaptive_weights_cont'])
        num_hist_match.append(res_dict[i]['n_hist_match'])
        num_weight_adjust.append(res_dict[i]['n_weights_adjust'])
    sft_ops = np.array([0] + list(agg_ops)[:-1])
    sft_miss = np.array([0] + list(agg_miss)[:-1])
    tpt = (agg_ops - sft_ops) / 0.5
    hr_fine = 1 - ((agg_miss - sft_miss) / (agg_ops - sft_ops))
    hr_coarse = 1 - (agg_miss / agg_ops)
    adaptive_weights = agg_weight / controller.num_clients
    combined_dict = {
        'tpt': list(tpt),
        'hr_overall': 1 - (agg_miss[-1] / agg_ops[-1]),
        'tpt_overall': agg_ops[-1] / 20,
        'hr_coarse': list(hr_coarse),
        'hr_fine': list(hr_fine),
        'adaptive_weights': adaptive_weights.tolist(),
        'num_hist_match': num_hist_match,
        'num_weight_adjust': num_weight_adjust
    }
    # print('hit_rate:', combined_dict['hr_overall'])
    # print('tpt:', combined_dict['tpt_overall'])
    json_res = {
        'tpt': combined_dict['tpt_overall'],
        'hr': combined_dict['hr_overall']
    }
    print(json.dumps(json_res))
    return combined_dict


def control_hit_rate(controller: DMCMemcachedController):
    # sycn warmup
    controller.sync_ready_clients()

    # sync start
    controller.sync_ready_clients()
    res_dict = controller.gather_client_results()

    # merge hit rate and continuous hit rate
    agg_ops = np.zeros(len(res_dict[1]['n_ops_cont']))
    agg_miss = np.zeros(len(res_dict[1]['n_miss_cont']))
    agg_weight = np.zeros_like(
        np.array(res_dict[1]['adaptive_weights_cont']), dtype=np.float64)
    num_expert_evict = np.zeros_like(
        np.array(res_dict[1]['n_expert_evict_cont']))
    num_weight_adjust_cont = np.zeros_like(
        np.array(res_dict[1]['n_weights_adjust_cont']))
    num_expert_reward_cont = np.zeros(10)
    try:
        num_expert_reward_cont = np.zeros_like(
            np.array(res_dict[1]['n_expert_reward_cont']))
    except:
        pass
    num_hist_match = []
    total_ops = 0
    total_miss = 0
    for i in range(1, controller.num_clients + 1):
        total_ops += res_dict[i]['n_ops_cont'][-1]
        total_miss += res_dict[i]['n_miss_cont'][-1]
        # agg_ops += np.array(res_dict[i]['n_ops_cont'])
        # agg_miss += np.array(res_dict[i]['n_miss_cont'])
        # agg_weight += np.array(res_dict[i]['adaptive_weights_cont'])
        # num_weight_adjust_cont += np.array(res_dict[i]['n_weights_adjust_cont'])
        # num_expert_evict += np.array(res_dict[i]['n_expert_evict_cont'])
        # num_hist_match.append(res_dict[i]['n_hist_match'])
        # try:
        #     num_expert_reward_cont += np.array(res_dict[i]['n_expert_reward_cont'])
        # except:
        #     pass
    # sft_ops = np.array([0] + list(agg_ops)[:-1])
    # sft_miss = np.array([0] + list(agg_miss)[:-1])
    # tpt = (agg_ops - sft_ops) / 0.05
    # hr_fine = 1 - ((agg_miss - sft_miss) / (agg_ops - sft_ops))
    # hr_coarse = 1 - (agg_miss / agg_ops)
    # adaptive_weights = agg_weight / controller.num_clients
    combined_dict = {
        # 'tpt': list(tpt),
        'hr_overall': 1 - total_miss / total_ops,
        # 'hr_overall': 1 - (agg_miss[-1] / agg_ops[-1]),
        # 'hr_coarse': list(hr_coarse),
        # 'hr_fine': list(hr_fine),
        # 'adaptive_weights': adaptive_weights.tolist(),
        # 'num_hist_match': num_hist_match,
        # 'num_weight_adjust_cont': num_weight_adjust_cont.tolist(),
        # 'num_expert_evict_cont': num_expert_evict.tolist(),
        # 'n_expert_reward_cont': num_expert_reward_cont.tolist()
    }
    # print('hit_rate:', combined_dict['hr_overall'])
    json_res = {
        'hr': combined_dict['hr_overall']
    }
    print(json.dumps(json_res))
    return combined_dict


def control_evict_micro(controller: DMCMemcachedController):
    # sync load cache to full
    controller.sync_ready_clients()

    # sync to start
    controller.sync_ready_clients()
    res_dict = controller.gather_client_results()
    return res_dict


def stop_and_get_server_stats(controller: DMCMemcachedController):
    controller.stop_server()
    res_dict = controller.get_server_stats()
    return res_dict


def is_real_workload(workload_name: str):
    return ("twitter" in workload_name) or ("wiki" in workload_name) \
        or ("changing" in workload_name) or ("webmail" in workload_name) \
        or ("ibm" in workload_name) or ("cphy" in workload_name) \
        or ("mix" in workload_name)


micro_wl_list = ['micro', 'micro-singlekey', 'evict-micro']
changing_wl_list = ['changing', 'changing-small']
twitter_wl_id = [20, 42, 49]
twitter_wl_list = ['twitter{:03d}-10m'.format(i) for i in twitter_wl_id]
ycsb_wl_id = ['a', 'b', 'c', 'd']
ycsb_wl_list = ['ycsb{}'.format(i) for i in ycsb_wl_id]
webmail_wl_list = ['webmail', 'webmail-all', 'webmail-11']
mix_wl = ['mix']
ibm_wl_list = ['ibm000-40m', 'ibm010-10m', 'ibm018-40m', 'ibm026-10m', 'ibm031-10m',
               'ibm036-10m', 'ibm045-10m', 'ibm050-10m', 'ibm061-10m', 'ibm075-40m',
               'ibm085-10m', 'ibm097-40m', 'ibm005-40m', 'ibm012-10m', 'ibm024-10m',
               'ibm029-10m', 'ibm034-10m', 'ibm044-10m', 'ibm049-10m', 'ibm058-40m',
               'ibm063-40m', 'ibm083-40m', 'ibm096-40m']
cphy_wl_list = ['cphy01-50m', 'cphy02-50m', 'cphy03-50m', 'cphy04-50m', 'cphy05-50m',
                'cphy06-50m', 'cphy07-50m', 'cphy08-50m', 'cphy09-50m', 'cphy10-50m']
_real_wl_list = changing_wl_list + twitter_wl_list\
    + webmail_wl_list + ibm_wl_list + cphy_wl_list
hit_rate_wl_list = ['hit-rate-' + wl for wl in _real_wl_list]
hit_rate_wl_list += ['hit-rate-mix']

workload_list = micro_wl_list + changing_wl_list \
    + twitter_wl_list + ycsb_wl_list + webmail_wl_list + hit_rate_wl_list \
    + ibm_wl_list + cphy_wl_list + mix_wl

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DMC Controller')
    parser.add_argument('workload', help='The workload (experiment) type (micro | micro-singlekey | twitterX)',
                        choices=workload_list)
    parser.add_argument('-s', '--num_servers',
                        type=int, default=1)
    parser.add_argument('-c', '--num_clients',
                        type=int, default=8)
    parser.add_argument('-m', '--memcached_ip',
                        type=str, default="127.0.0.1")
    parser.add_argument('-p', '--memcached_port',
                        type=int, default=11211)
    parser.add_argument('-o', '--out_fname',
                        type=str, default="{}".format(time.time()))
    parser.add_argument('-E', '--elastic',
                        choices=['cpu', 'mem'])
    parser.add_argument('-f', '--fiber', type=int, default=0)

    args = parser.parse_args()

    controller = DMCMemcachedController(args.memcached_ip, args.memcached_port,
                                        args.num_servers, args.num_clients)

    if args.workload == "micro":
        res = control_micro_bench(controller)
    elif args.workload.startswith("hit-rate-"):
        res = control_hit_rate(controller)
    elif args.workload == "micro-singlekey":
        res = control_micro_singlekey_bench(controller)
    elif is_real_workload(args.workload):
        if args.elastic == 'cpu':
            res = control_workload_bench_ela_cpu(args.workload, controller)
        elif args.elastic == 'mem':
            res = control_workload_bench_ela_mem(controller)
        else:
            res = control_workload_bench(controller)
    elif args.workload == "evict-micro":
        res = control_evict_micro(controller)
    elif args.workload[:4] == "ycsb":
        if args.elastic == 'cpu':
            if args.fiber:
                res = control_ycsb_bench_ela_cpu_fiber(
                    controller, args.num_clients, args.fiber)
            else:
                res = control_ycsb_bench_ela_cpu(controller, args.num_clients)
        elif args.elastic == 'mem':
            if args.fiber:
                res = control_ycsb_bench_ela_mem_fiber(
                    controller, args.num_clients, args.fiber)
            else:
                res = control_ycsb_bench_ela_mem(controller, args.num_clients)
        else:
            if args.fiber:
                res = control_ycsb_bench_fiber(
                    controller, args.num_clients, args.fiber)
            else:
                res = control_ycsb_bench(controller, args.num_clients)

    # stop the server and get server results
    server_res = stop_and_get_server_stats(controller)
    res['server'] = server_res

    with open("{}-{}-s{}-c{}.json".format(args.workload, args.out_fname, args.num_servers, args.num_clients), 'w') as f:
        json.dump(res, f)
    controller.clear_all_contents()
