#include "run_client.h"
#include "memcached.h"
#include "third_party/json.hpp"
#include "workload.h"

#include <assert.h>
#include <sys/time.h>
#include <unistd.h>

using json = nlohmann::json;

// #define ELA_SCALE_RUN_TIME 500 // ycsba
// #define ELA_SCALE_RUN_TIME 487 // ycsbc
#define ELA_SCALE_RUN_TIME 488  // ycsbc-new

static bool is_real_workload(const char* workload_name) {
  return memcmp(workload_name, "twitter", strlen("twitter")) == 0 ||
         memcmp(workload_name, "wiki", strlen("wiki")) == 0 ||
         memcmp(workload_name, "changing", strlen("changing")) == 0 ||
         memcmp(workload_name, "webmail", strlen("webmail")) == 0 ||
         memcmp(workload_name, "ibm", strlen("ibm")) == 0 ||
         memcmp(workload_name, "cphy", strlen("cphy")) == 0 ||
         memcmp(workload_name, "mix", strlen("mix")) == 0;
}

static void get_workload_kv(DMCWorkload* wl,
                            uint32_t idx,
                            __OUT uint64_t* key_addr,
                            __OUT uint64_t* val_addr,
                            __OUT uint32_t* key_size,
                            __OUT uint32_t* val_size,
                            __OUT uint8_t* op) {
  idx = idx % wl->num_ops;
  *key_addr = ((uint64_t)wl->key_buf + idx * 128);
  *val_addr = ((uint64_t)wl->val_buf + idx * 120);
  *key_size = wl->key_size_list[idx];
  *val_size = wl->val_size_list[idx];
  *op = wl->op_list[idx];
}

void* client_evict_micro_main(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  // prepare recording data structures
  char key_buf[128];
  char val_buf[128];
  char tmp_buf[128];
  uint32_t tmp_len = 0;

  std::vector<uint64_t> ops_vec;
  std::vector<uint64_t> num_evict_vec;
  std::vector<uint64_t> succ_evict_vec;
  std::vector<uint64_t> num_evict_bucket_vec;
  std::vector<uint64_t> succ_evict_bucket_vec;
  std::map<uint32_t, uint32_t> lat_map;
  int cnt = 0;
  int tick = 0;
  uint32_t num_ticks = args->run_tims_s * TICK_PER_SEC;

  // sync to make the cache full
  con_client.memcached_sync_ready(args->cid);
  for (cnt = 0;; cnt++) {
    sprintf(key_buf, "%d-key-%d", args->cid, cnt);
    sprintf(val_buf, "%d-val_%d", args->cid, cnt);
    ret = client->kv_set(key_buf, strlen(key_buf) + 1, val_buf,
                         strlen(val_buf) + 1);
    if (client->num_evict_ != 0)
      break;
  }

  // sync to start evicting
  struct timeval st;
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);
  gettimeofday(&st, NULL);
  while (tick < num_ticks) {
    int item_id = cnt;
    sprintf(key_buf, "%d-key-%d", args->cid, item_id);
    sprintf(val_buf, "%d-val-%d", args->cid, cnt);
    struct timeval tst, tet;
    gettimeofday(&tst, NULL);
    ret = client->kv_set(key_buf, strlen(key_buf) + 1, val_buf,
                         strlen(val_buf) + 1);
    gettimeofday(&tet, NULL);
    assert(ret == DMC_SUCCESS);
    lat_map[diff_ts_us(&tet, &tst)]++;
    cnt++;
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        TICK_US * tick) {
      ops_vec.push_back(cnt);
      num_evict_bucket_vec.push_back(client->num_bucket_evict_);
      succ_evict_bucket_vec.push_back(client->num_success_bucket_evict_);
      num_evict_vec.push_back(client->num_evict_);
      succ_evict_vec.push_back(client->num_success_evict_);
      tick++;
    }
  }
  json res;
  res["n_ops_cont"] = json(ops_vec);
  res["n_evict_cont"] = json(num_evict_vec);
  res["n_succ_evict_cont"] = json(succ_evict_vec);
  res["n_bucket_evict_cont"] = json(num_evict_bucket_vec);
  res["n_succ_bucket_evict_cont"] = json(succ_evict_bucket_vec);
  res["n_retry"] = client->num_set_retry_;
  res["lat_map"] = json(lat_map);
  res["n_evict_read_bucket"] = client->num_evict_read_bucket_;
  res["n_rdma_read"] = client->num_rdma_read_;
  res["n_rdma_write"] = client->num_rdma_write_;
  res["n_rdma_cas"] = client->num_rdma_cas_;
  res["n_rdma_faa"] = client->num_rdma_faa_;
  res["n_rdma_send"] = client->num_rdma_send_;
  res["n_rdma_recv"] = client->num_rdma_recv_;
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  return NULL;
}

void* client_micro_main(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  char key_buf[128];
  char val_buf[128];
  char tmp_buf[128];
  uint32_t tmp_len = 0;
  int cnt = 0;
  std::map<uint32_t, uint32_t> lat_map;
  bool is_single_key = (strcmp(args->workload_name, "micro-singlekey") == 0);

  if (is_single_key) {
    sprintf(key_buf, "%d-key-%d", args->cid, 233);
    sprintf(val_buf, "%d-val-%d", args->cid, 233);
    ret = client->kv_set(key_buf, strlen(key_buf) + 1, val_buf,
                         strlen(val_buf) + 1);
    assert(ret == DMC_SUCCESS);
    ret = client->kv_get(key_buf, strlen(key_buf) + 1, tmp_buf, &tmp_len);
    assert(ret == 0);
    assert(strcmp(tmp_buf, val_buf) == 0);
  }

  // set
  lat_map.clear();
  printd(L_DEBUG, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);
  printd(L_DEBUG, "client %d start set", args->cid);
  struct timeval st, tst, tet;
  gettimeofday(&st, NULL);
  for (cnt = 0; cnt < args->num_load_ops; cnt++) {
    if (!is_single_key) {
      sprintf(key_buf, "%d-key-%d", args->cid, cnt);
      sprintf(val_buf, "%d-val-%d", args->cid, cnt);
    }
    gettimeofday(&tst, NULL);
    ret = client->kv_set(key_buf, strlen(key_buf) + 1, val_buf,
                         strlen(val_buf) + 1);
    gettimeofday(&tet, NULL);
    assert(ret == DMC_SUCCESS);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (args->validate) {
      ret = client->kv_get(key_buf, strlen(key_buf) + 1, tmp_buf, &tmp_len);
      assert(ret == 0);
      assert(strcmp(tmp_buf, val_buf) == 0);
    }
    if (diff_ts_us(&tet, &st) > 10000000)
      break;
  }
  // prepare and publish get result
  json res;
  res["n_ops"] = cnt;
  res["time_us"] = diff_ts_us(&tet, &st);
  res["n_evict"] = client->num_evict_;
  res["n_retry"] = client->num_set_retry_;
  res["lat_map"] = json(lat_map);
  std::string res_str = res.dump();
  con_client.memcached_put_result((void*)res_str.c_str(),
                                  strlen(res_str.c_str()), args->cid);
  int num_ins = cnt;

  // get
  int n_get_success = 0;
  int n_get_failure = 0;
  lat_map.clear();
  con_client.memcached_sync_ready(args->cid);
  printd(L_DEBUG, "client %d start get", args->cid);
  gettimeofday(&st, NULL);
  for (cnt = 0; cnt < num_ins; cnt++) {
    if (!is_single_key) {
      sprintf(key_buf, "%d-key-%d", args->cid, cnt);
      sprintf(val_buf, "%d-val-%d", args->cid, cnt);
    }
    gettimeofday(&tst, NULL);
    ret = client->kv_get(key_buf, strlen(key_buf) + 1, tmp_buf, &tmp_len);
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (args->validate && ret == 0) {
      assert(tmp_len == strlen(val_buf) + 1);
      if (memcmp(tmp_buf, val_buf, tmp_len) != 0) {
        printf("get: %s != expected: %s\n", tmp_buf, val_buf);
      }
    }
    if (ret == 0)
      n_get_success++;
    else
      n_get_failure++;
    if (diff_ts_us(&tet, &st) > 10000000)
      break;
  }
  // prepare get results
  res.clear();
  res["n_ops"] = cnt;
  res["time_us"] = diff_ts_us(&tet, &st);
  res["n_evict"] = client->num_evict_;
  res["n_success"] = n_get_success;
  res["n_failure"] = n_get_failure;
  res["lat_map"] = json(lat_map);
  res_str = res.dump();
  con_client.memcached_put_result((void*)res_str.c_str(),
                                  strlen(res_str.c_str()), args->cid);

  if (is_single_key) {
    // single key micro benchmark should return here
    return NULL;
  }

  // set (update)
  lat_map.clear();
  con_client.memcached_sync_ready(args->cid);
  printd(L_DEBUG, "client %d start upd", args->cid);
  gettimeofday(&st, NULL);
  for (cnt = 0; cnt < num_ins; cnt++) {
    sprintf(key_buf, "%d-key-%d", args->cid, cnt);
    sprintf(val_buf, "%d-val-%d-n", args->cid, cnt);
    gettimeofday(&tst, NULL);
    ret = client->kv_set(key_buf, strlen(key_buf) + 1, val_buf,
                         strlen(val_buf) + 1);
    gettimeofday(&tet, NULL);
    assert(ret == DMC_SUCCESS);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (args->validate) {
      ret = client->kv_get(key_buf, strlen(key_buf) + 1, tmp_buf, &tmp_len);
      assert(ret == 0);
      assert(strcmp(tmp_buf, val_buf) == 0);
    }
    if (diff_ts_us(&tet, &st) > 10000000)
      break;
  }
  // prepare upd results
  res.clear();
  res["n_ops"] = cnt;
  res["time_us"] = diff_ts_us(&tet, &st);
  res["n_evict"] = client->num_evict_;
  res["n_retry"] = client->num_set_retry_;
  res["lat_map"] = json(lat_map);
  res_str = res.dump();
  con_client.memcached_put_result((void*)res_str.c_str(),
                                  strlen(res_str.c_str()), args->cid);
  printf("Client %d finish\n", args->cid);

  return NULL;
}

void* client_hit_rate_real(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  // load workload
  DMCWorkload wl;
  ret = load_workload_hit_rate(args->workload_name, -1, args->cid,
                               args->all_client_num, args->n_lru_clients,
                               args->n_lfu_clients, &wl);
  assert(ret == 0);

  // ready to run workload
  char tmp_buf[128] = {0};
  uint32_t tmp_len = 0;

  // sync to start warmup
  struct timeval st, tst, tet;
  printd(L_INFO, "Client %d wait for warmup", args->cid);
  con_client.memcached_sync_ready(args->cid);
  uint32_t warmup_seq = 0;
  gettimeofday(&st, NULL);
  while (true) {
    int idx = warmup_seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        assert(ret == DMC_SUCCESS);
      }
    } else {
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      assert(ret == DMC_SUCCESS);
    }
    warmup_seq++;
    gettimeofday(&tet, NULL);
    if (diff_ts_us(&tet, &st) > 10LL * 1000000)
      break;
  }
  printf("client %d warmup %d ops\n", args->cid, warmup_seq);

  // sync to start testing
  printd(L_INFO, "Client %d wait for start", args->cid);
  con_client.memcached_sync_ready(args->cid);
  uint32_t n_op = 0;
  uint32_t n_miss = 0;
  std::map<uint32_t, uint32_t> lat_map;
  gettimeofday(&st, NULL);
  while (true) {
    uint32_t idx = n_op % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        n_miss++;
        usleep(500);
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        assert(ret == DMC_SUCCESS);
      }
      gettimeofday(&tet, NULL);
    } else {
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      gettimeofday(&tet, NULL);
      assert(ret == DMC_SUCCESS);
    }
    lat_map[diff_ts_us(&tet, &tst)]++;
    n_op++;
    if (diff_ts_us(&tet, &st) > args->run_tims_s * 1000000)
      break;
  }
  // sync results
  json res;
  res["n_ops"] = n_op;
  res["n_miss"] = n_miss;
  res["time_us"] = diff_ts_us(&tet, &st);
  res["lat_map"] = json(lat_map);
  res["n_rdma_send"] = client->num_rdma_send_;
  res["n_rdma_recv"] = client->num_rdma_recv_;
  res["n_rdma_read"] = client->num_rdma_read_;
  res["n_rdma_write"] = client->num_rdma_write_;
  res["n_rdma_cas"] = client->num_rdma_cas_;
  res["n_rdma_faa"] = client->num_rdma_faa_;
  res["n_weights_sync"] = client->num_adaptive_weight_sync_;
  res["n_read_hist_head"] = client->num_read_hist_head_;
  res["n_weights_ajdust"] = client->num_adaptive_adjust_weights_;
  res["n_hist_match"] = client->num_hist_match_;
  res["n_hist_overwrite"] = client->num_hist_overwite_;
  res["n_hist_access"] = client->num_hist_access_;
  std::vector<std::vector<float>> adaptive_weight_vec;
  client->get_adaptive_weight_vec(adaptive_weight_vec);
  json jadaptive_weight_vec(adaptive_weight_vec);
  res["adaptive_weights"] = json(adaptive_weight_vec);
  std::string res_str = res.dump();
  con_client.memcached_put_result((void*)res_str.c_str(),
                                  strlen(res_str.c_str()), args->cid);

  printf("Client %d miss rate: %f\n", args->cid, (float)n_miss / n_op);
  return NULL;
}

void* client_hit_rate(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  // load workload
  DMCWorkload wl;
  ret = load_workload_hit_rate(args->workload_name, -1, args->cid,
                               args->all_client_num, args->n_lru_clients,
                               args->n_lfu_clients, &wl);
  assert(ret == 0);

  // ready to run workload
  char tmp_buf[128] = {0};
  uint32_t tmp_len = 0;

  // sync to start warmup
  printd(L_INFO, "Client %d wait for warmup", args->cid);
  con_client.memcached_sync_ready(args->cid);
  uint32_t cnt = 0;
  uint32_t warmup_size = (uint32_t)(wl.num_ops * 0.1);
  for (; args->use_warmup && cnt < warmup_size; cnt++) {
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, cnt, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        // assert(ret == DMC_SUCCESS);
      }
    } else {
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      // assert(ret == DMC_SUCCESS);
    }
  }

  // sync to start testing
  printd(L_INFO, "Client %d wait for start", args->cid);
  // usleep(args->cid);
  con_client.memcached_sync_ready(args->cid);
  struct timeval st, tst, tet;
  std::map<uint32_t, uint32_t> lat_map;
  std::vector<uint32_t> ops_cont;
  std::vector<uint32_t> miss_cont;
  std::vector<std::vector<float>> expert_weights_cont;
  std::vector<float> tmp_weights;
  std::vector<std::vector<uint32_t>> expert_evict_cont;
  std::vector<uint32_t> tmp_expert_evict;
  std::vector<uint32_t> num_weight_adjust_cont;
  std::vector<std::vector<uint32_t>> expert_reward_cont;
  std::vector<uint32_t> tmp_expert_reward;
  uint32_t n_op = 0;
  uint32_t n_miss = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  uint32_t n_get_miss = 0;
  uint32_t n_set_miss = 0;
  uint32_t record_interval = (uint32_t)(wl.num_ops * 0.9 / 100);
  gettimeofday(&st, NULL);
  for (; cnt < wl.num_ops; cnt++) {
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, cnt, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      n_get++;
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        n_get_miss++;
        n_miss++;
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        // assert(ret == DMC_SUCCESS);
      }
      gettimeofday(&tet, NULL);
    } else {
      n_set++;
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      // assert(ret == DMC_SUCCESS);
      gettimeofday(&tet, NULL);
      n_set_miss += (ret < 0);
      n_miss += (ret < 0);
    }
    n_op++;
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (n_op % record_interval == 0) {
      ops_cont.push_back(n_op);
      miss_cont.push_back(n_miss);
      client->get_adaptive_weights(tmp_weights);
      expert_weights_cont.push_back(tmp_weights);
      client->get_expert_evict_cnt(tmp_expert_evict);
      expert_evict_cont.push_back(tmp_expert_evict);
      num_weight_adjust_cont.push_back(client->num_adaptive_adjust_weights_);
#ifdef USE_REWARDS
      client->get_expert_reward_cnt(tmp_expert_reward);
      expert_reward_cont.push_back(tmp_expert_reward);
#endif
    }
  }
  // sync results
  assert(n_miss == n_get_miss + n_set_miss);
  json res;
  res["n_ops"] = n_op;
  res["n_miss"] = n_miss;
  res["time_us"] = diff_ts_us(&tet, &st);
  res["lat_map"] = json(lat_map);
  res["adaptive_weights_cont"] = json(expert_weights_cont);
  res["n_ops_cont"] = json(ops_cont);
  res["n_miss_cont"] = json(miss_cont);
  res["n_hist_match"] = client->num_hist_match_;
  res["n_weights_adjust_cont"] = json(num_weight_adjust_cont);
  res["n_expert_evict_cont"] = json(expert_evict_cont);
#ifdef USE_REWARDS
  res["n_hit_reward"] = client->num_hit_rewards_;
  res["n_expert_reward_cont"] = json(expert_reward_cont);
#endif
  std::string res_str = res.dump();
  con_client.memcached_put_result((void*)res_str.c_str(),
                                  strlen(res_str.c_str()), args->cid);

  printf("Client %d miss rate: %f\n", args->cid, (float)n_miss / n_op);
  printf(
      "n_get: %d, n_set: %d\nn_get_miss: %d, n_set_miss: %d, n_set_miss_c: "
      "%d\n",
      n_get, n_set, n_get_miss, n_set_miss, client->n_set_miss_);
  printf("n_miss: %d, n_eviction: %d\n", n_miss, client->num_evict_);
  return NULL;
}

void* client_ycsb_ela_mem(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload load_wl, trans_wl;
  ret = load_workload_ycsb(args->workload_name, -1, args->cid,
                           args->all_client_num, client->get_evict_type(),
                           &load_wl, &trans_wl);
  assert(ret == 0);

  // ready to run workload
  struct timeval st, tst, tet;
  char tmp_buf[128];
  uint32_t tmp_len;
  std::map<uint64_t, uint32_t> lat_map;

  uint8_t eviction_type = client->get_evict_type();

  // sync to load ycsb dataset
  lat_map.clear();
  printd(L_INFO, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);
  gettimeofday(&st, NULL);
  for (int i = 0; i < load_wl.num_ops; i++) {
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&load_wl, i, &key_addr, &val_addr, &key_size, &val_size,
                    &op);
    gettimeofday(&tst, NULL);
    if (eviction_type == EVICT_PRECISE)
      ret = client->kv_p_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
    else
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (args->validate) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
      assert(tmp_len == val_size);
      assert(*(uint32_t*)tmp_buf == *(uint32_t*)val_addr);
    }
    if (client->num_evict_ != 0 || client->num_bucket_evict_ != 0) {
      printf("Evicted during load!");
      abort();
    }
  }
  json load_res;
  load_res["n_ops"] = load_wl.num_ops;
  load_res["n_retry"] = client->num_set_retry_;
  load_res["time_us"] = diff_ts_us(&tet, &st);
  load_res["lat_map"] = json(lat_map);
  std::string load_res_str = load_res.dump();
  con_client.memcached_put_result((void*)load_res_str.c_str(),
                                  strlen(load_res_str.c_str()), args->cid);

  // sync to do trans
  std::vector<std::map<uint64_t, uint32_t>> lat_map_cont;
  lat_map.clear();
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);
  printf("Client %d started trans\n", args->cid);

  uint32_t seq = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  uint32_t num_ticks =
      args->cid <= 32 ? args->run_tims_s * 2 : ELA_SCALE_RUN_TIME * 2;
  uint64_t tick_us = 500000;
  uint32_t cur_tick = 0;
  std::vector<uint32_t> ops_list;
  gettimeofday(&st, NULL);
  while (cur_tick < num_ticks) {
    uint32_t idx = seq % trans_wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&trans_wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                    &op);
    gettimeofday(&tst, NULL);
    if (op == GET) {
      n_get++;
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
    } else {
      n_set++;
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    }
    seq++;
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (diff_ts_us(&tet, &st) > cur_tick * tick_us) {
      ops_list.push_back(seq);
      if (cur_tick % 8 == 0) {
        lat_map_cont.push_back(lat_map);
        lat_map.clear();
      }
      cur_tick++;
    }
  }
  printf("Client %d finish\n", args->cid);
  json trans_res;
  trans_res["ops_cont"] = json(ops_list);
  trans_res["num_ticks"] = num_ticks;
  trans_res["lat_map_cont"] = json(lat_map_cont);
  std::string str = trans_res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client sync: %ld\n", client->num_cliquemap_sync_);
  printf("Item size: %ld\n", str.size());
  return NULL;
}

void* client_ycsb_ela_cpu(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload load_wl, trans_wl;
  ret = load_workload_ycsb(args->workload_name, -1, args->cid,
                           args->all_client_num, client->get_evict_type(),
                           &load_wl, &trans_wl);
  assert(ret == 0);

  // ready to run workload
  struct timeval st, tst, tet;
  char tmp_buf[128];
  uint32_t tmp_len;
  std::map<uint64_t, uint32_t> lat_map;

  uint8_t eviction_type = client->get_evict_type();

  // sync to load ycsb dataset
  lat_map.clear();
  printd(L_INFO, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);
  gettimeofday(&st, NULL);
  for (int i = 0; i < load_wl.num_ops; i++) {
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&load_wl, i, &key_addr, &val_addr, &key_size, &val_size,
                    &op);
    gettimeofday(&tst, NULL);
    if (eviction_type == EVICT_PRECISE)
      ret = client->kv_p_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
    else
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (args->validate) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
      assert(tmp_len == val_size);
      assert(*(uint32_t*)tmp_buf == *(uint32_t*)val_addr);
    }
    if (client->num_evict_ != 0 || client->num_bucket_evict_ != 0) {
      printf("Evicted during load!");
      abort();
    }
  }
  json load_res;
  load_res["n_ops"] = load_wl.num_ops;
  load_res["n_retry"] = client->num_set_retry_;
  load_res["time_us"] = diff_ts_us(&tet, &st);
  load_res["lat_map"] = json(lat_map);
  std::string load_res_str = load_res.dump();
  con_client.memcached_put_result((void*)load_res_str.c_str(),
                                  strlen(load_res_str.c_str()), args->cid);

  // sync to do trans
  std::vector<std::map<uint64_t, uint32_t>> lat_map_cont;
  lat_map.clear();
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);
  if (args->cid > 32)
    con_client.memcached_wait("scale-to-64");
  printf("Client %d started trans\n", args->cid);
  if (args->is_load_only)
    return NULL;  // return loader thread

  uint32_t seq = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  uint32_t num_ticks =
      args->cid <= 32 ? args->run_tims_s * 2 : ELA_SCALE_RUN_TIME * 2;
  uint64_t tick_us = 500000;
  uint32_t cur_tick = 0;
  std::vector<uint32_t> ops_list;
  gettimeofday(&st, NULL);
  while (cur_tick < num_ticks) {
    uint32_t idx = seq % trans_wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&trans_wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                    &op);
    gettimeofday(&tst, NULL);
    if (op == GET) {
      n_get++;
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
    } else {
      n_set++;
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    }
    seq++;
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (diff_ts_us(&tet, &st) > cur_tick * tick_us) {
      ops_list.push_back(seq);
      if (cur_tick % 8 == 0) {
        lat_map_cont.push_back(lat_map);
        lat_map.clear();
      }
      cur_tick++;
    }
  }
  printf("Client %d finish\n", args->cid);
  json trans_res;
  trans_res["ops_cont"] = json(ops_list);
  trans_res["num_ticks"] = num_ticks;
  trans_res["lat_map_cont"] = json(lat_map_cont);
  std::string str = trans_res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client sync: %ld\n", client->num_cliquemap_sync_);
  printf("Item size: %ld\n", str.size());
  return NULL;
}

#ifdef USE_FIBER
typedef struct _ClientFiberArgs {
  int tid;
  int cid;
  int fb_id;
  uint32_t num_all_clients;
  uint32_t run_time_s;
  DMCWorkload* load_wl;
  DMCWorkload* trans_wl;
  uint32_t trans_st_idx;
  uint32_t num_trans_ops;
  DMCConfig* config;
  char controller_ip[256];

  // for sync trans
  boost::fibers::barrier* start_trans_barrier;

  // for nofity memory scale
  DMCClient** client_list;
  int num_l_clients;
} ClientFiberArgs;

void client_ycsb_ela_mem_fiber_worker(ClientFiberArgs* args) {
  DMCConfig cur_config;
  memcpy(&cur_config, args->config, sizeof(DMCConfig));
  cur_config.server_id = args->num_all_clients * args->fb_id + args->cid;
  DMCClient* client = new DMCClient(&cur_config);
  args->client_list[args->fb_id * args->num_l_clients + args->tid] = client;
  DMCMemcachedClient con_client(args->controller_ip);
  int ret = 0;

  // ready to run workload
  struct timeval st, tst, tet;
  char tmp_buf[128];
  uint32_t tmp_len;
  std::map<uint32_t, uint32_t> lat_map;

  uint8_t eviction_type = client->get_evict_type();

  // sync to load ycsb dataset
  if (args->fb_id == 0) {
    printf("Client %d-%d waiting sync\n", args->cid, args->fb_id);
    con_client.memcached_sync_ready(args->config->server_id);
    for (int i = 0; i < args->load_wl->num_ops; i++) {
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(args->load_wl, i, &key_addr, &val_addr, &key_size,
                      &val_size, &op);
      gettimeofday(&tst, NULL);
      if (eviction_type == EVICT_PRECISE)
        ret = client->kv_p_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
      else
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      gettimeofday(&tet, NULL);
      lat_map[diff_ts_us(&tet, &tst)]++;
      if (client->num_evict_ != 0 || client->num_bucket_evict_ != 0) {
        printf("Evicted during load!");
        abort();
      }
    }
    json load_res;
    load_res["n_ops"] = args->load_wl->num_ops;
    con_client.memcached_put_result((void*)load_res.dump().c_str(),
                                    strlen(load_res.dump().c_str()), args->cid);
  }

  // sync to do trans
  lat_map.clear();
  std::vector<std::map<uint32_t, uint32_t>> lat_map_cont;
  printf("client %d-%d wait sync\n", args->cid, args->fb_id);
  client->clear_counters();
  if (args->fb_id == 0) {
    con_client.memcached_sync_ready(args->cid);
    args->start_trans_barrier->wait();
  } else {
    con_client.increase_result_cntr();
    args->start_trans_barrier->wait();
  }
  printf("client %d-%d start trans\n", args->cid, args->fb_id);

  uint32_t seq = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  uint32_t num_ticks =
      args->cid <= 32 ? args->run_time_s * 2 : ELA_SCALE_RUN_TIME * 2;
  uint64_t tick_us = 500000;
  uint32_t cur_tick = 0;
  std::vector<uint32_t> ops_list;
  gettimeofday(&st, NULL);
  while (cur_tick < num_ticks) {
    uint32_t idx = args->trans_st_idx + (seq % args->num_trans_ops);
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(args->trans_wl, idx, &key_addr, &val_addr, &key_size,
                    &val_size, &op);
    gettimeofday(&tst, NULL);
    if (op == GET) {
      n_get++;
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
    } else {
      n_set++;
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    }
    seq++;
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (diff_ts_us(&tet, &st) > cur_tick * tick_us) {
      ops_list.push_back(seq);
      if (cur_tick % 16 == 0) {
        lat_map_cont.push_back(lat_map);
        lat_map.clear();
      }
      cur_tick++;
    }
  }
  printf("Client %d finish\n", args->cid);
  json trans_res;
  trans_res["ops_cont"] = json(ops_list);
  trans_res["num_ticks"] = num_ticks;
  trans_res["lat_map_cont"] = json(lat_map_cont);
  std::string str = trans_res.dump();
  con_client.memcached_put_result_fiber((void*)str.c_str(), strlen(str.c_str()),
                                        args->cid, args->fb_id);
  printf("Item size: %ld\n", str.size());
  printf("Client %d-%d finish\n", args->cid, args->fb_id);
}

void client_ycsb_ela_cpu_fiber_worker(ClientFiberArgs* args) {
  DMCConfig cur_config;
  memcpy(&cur_config, args->config, sizeof(DMCConfig));
  cur_config.server_id = args->num_all_clients * args->fb_id + args->cid;
  DMCClient* client = new DMCClient(&cur_config);
  DMCMemcachedClient con_client(args->controller_ip);
  int ret = 0;

  // ready to run workload
  struct timeval st, tst, tet;
  char tmp_buf[128];
  uint32_t tmp_len;
  std::map<uint32_t, uint32_t> lat_map;

  uint8_t eviction_type = client->get_evict_type();

  // sync to load ycsb dataset
  if (args->fb_id == 0) {
    printf("Client %d-%d waiting sync\n", args->cid, args->fb_id);
    con_client.memcached_sync_ready(args->config->server_id);
    for (int i = 0; i < args->load_wl->num_ops; i++) {
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(args->load_wl, i, &key_addr, &val_addr, &key_size,
                      &val_size, &op);
      gettimeofday(&tst, NULL);
      if (eviction_type == EVICT_PRECISE)
        ret = client->kv_p_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
      else
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      gettimeofday(&tet, NULL);
      lat_map[diff_ts_us(&tet, &tst)]++;
      if (client->num_evict_ != 0 || client->num_bucket_evict_ != 0) {
        printf("Evicted during load!");
        abort();
      }
    }
    json load_res;
    load_res["n_ops"] = args->load_wl->num_ops;
    con_client.memcached_put_result((void*)load_res.dump().c_str(),
                                    strlen(load_res.dump().c_str()), args->cid);
  }

  // sync to do trans
  lat_map.clear();
  std::vector<std::map<uint32_t, uint32_t>> lat_map_cont;
  printf("client %d-%d wait sync\n", args->cid, args->fb_id);
  client->clear_counters();
  if (args->fb_id == 0) {
    con_client.memcached_sync_ready(args->cid);
    if (args->cid > 32)
      con_client.memcached_wait("scale-to-64");
    args->start_trans_barrier->wait();
  } else {
    con_client.increase_result_cntr();
    args->start_trans_barrier->wait();
  }
  printf("client %d-%d start trans\n", args->cid, args->fb_id);

  uint32_t seq = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  uint32_t num_ticks =
      args->cid <= 32 ? args->run_time_s * 2 : ELA_SCALE_RUN_TIME * 2;
  uint64_t tick_us = 500000;
  uint32_t cur_tick = 0;
  std::vector<uint32_t> ops_list;
  gettimeofday(&st, NULL);
  while (cur_tick < num_ticks) {
    uint32_t idx = args->trans_st_idx + (seq % args->num_trans_ops);
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(args->trans_wl, idx, &key_addr, &val_addr, &key_size,
                    &val_size, &op);
    gettimeofday(&tst, NULL);
    if (op == GET) {
      n_get++;
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
    } else {
      n_set++;
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    }
    seq++;
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (diff_ts_us(&tet, &st) > cur_tick * tick_us) {
      ops_list.push_back(seq);
      if (cur_tick % 16 == 0) {
        lat_map_cont.push_back(lat_map);
        lat_map.clear();
      }
      cur_tick++;
    }
  }
  printf("Client %d finish\n", args->cid);
  json trans_res;
  trans_res["ops_cont"] = json(ops_list);
  trans_res["num_ticks"] = num_ticks;
  trans_res["lat_map_cont"] = json(lat_map_cont);
  std::string str = trans_res.dump();
  con_client.memcached_put_result_fiber((void*)str.c_str(), strlen(str.c_str()),
                                        args->cid, args->fb_id);
  printf("Item size: %ld\n", str.size());
  printf("Client %d-%d finish\n", args->cid, args->fb_id);
}

void client_ycsb_fiber_worker(ClientFiberArgs* args) {
  DMCConfig cur_config;
  memcpy(&cur_config, args->config, sizeof(DMCConfig));
  cur_config.server_id = args->num_all_clients * args->fb_id + args->cid;
  DMCClient* client = new DMCClient(&cur_config);
  DMCMemcachedClient con_client(args->controller_ip);
  int ret = 0;

  // ready to run workload
  struct timeval st, tst, tet;
  char tmp_buf[128];
  uint32_t tmp_len;
  std::map<uint32_t, uint32_t> lat_map;

  uint8_t eviction_type = client->get_evict_type();

  // sync to load ycsb dataset
  if (args->fb_id == 0) {
    printf("Client %d-%d waiting sync\n", args->cid, args->fb_id);
    con_client.memcached_sync_ready(args->config->server_id);
    for (int i = 0; i < args->load_wl->num_ops; i++) {
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(args->load_wl, i, &key_addr, &val_addr, &key_size,
                      &val_size, &op);
      gettimeofday(&tst, NULL);
      if (eviction_type == EVICT_PRECISE)
        ret = client->kv_p_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
      else
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      gettimeofday(&tet, NULL);
      lat_map[diff_ts_us(&tet, &tst)]++;
      if (client->num_evict_ != 0 || client->num_bucket_evict_ != 0) {
        printf("Evicted during load!");
        abort();
      }
    }
    json load_res;
    load_res["n_ops"] = args->load_wl->num_ops;
    con_client.memcached_put_result((void*)load_res.dump().c_str(),
                                    strlen(load_res.dump().c_str()), args->cid);
  }

  // sync to do trans
  lat_map.clear();
  printf("client %d-%d wait sync\n", args->cid, args->fb_id);
  client->clear_counters();
  if (args->fb_id == 0) {
    con_client.memcached_sync_ready(args->cid);
    args->start_trans_barrier->wait();
  } else {
    con_client.increase_result_cntr();
    args->start_trans_barrier->wait();
  }
  printf("client %d-%d start trans\n", args->cid, args->fb_id);

  gettimeofday(&st, NULL);
  uint32_t seq = 0;
  while (true) {
    uint32_t idx = args->trans_st_idx + (seq % args->num_trans_ops);
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(args->trans_wl, idx, &key_addr, &val_addr, &key_size,
                    &val_size, &op);
    if (op == GET) {
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      gettimeofday(&tet, NULL);
    } else {
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      gettimeofday(&tet, NULL);
    }
    lat_map[diff_ts_us(&tet, &tst)]++;
    seq++;
    if (diff_ts_us(&tet, &st) > args->run_time_s * 1000000) {
      break;
    }
  }
  printf("Client %d-%d finish\n", args->cid, args->fb_id);
  json trans_res;
  trans_res["ops"] = seq;
  trans_res["lat_map"] = json(lat_map);
  std::string str = trans_res.dump();
  con_client.memcached_put_result_fiber((void*)str.c_str(), strlen(str.c_str()),
                                        args->cid, args->fb_id);
}

void* client_ycsb_ela_mem_fiber_main(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload load_wl, trans_wl;
  ret = load_workload_ycsb(args->workload_name, -1, args->cid,
                           args->all_client_num, args->config->eviction_type,
                           &load_wl, &trans_wl);
  assert(ret == 0);

  // start fiber
  ClientFiberArgs fb_args_list[NUM_FB_PER_THREAD];
  boost::fibers::fiber fb_list[NUM_FB_PER_THREAD];
  boost::fibers::barrier* start_trans_barrier = new boost::fibers::barrier(2);
  for (int i = 0; i < NUM_FB_PER_THREAD; i++) {
    uint32_t num_ops = trans_wl.num_ops / NUM_FB_PER_THREAD;
    uint32_t st_op_idx = num_ops * i;
    fb_args_list[i].tid = args->tid;
    fb_args_list[i].client_list = args->client_list;
    fb_args_list[i].num_l_clients = args->num_l_clients;
    fb_args_list[i].num_all_clients = args->all_client_num;
    fb_args_list[i].cid = args->cid;
    fb_args_list[i].fb_id = i;
    fb_args_list[i].load_wl = &load_wl;
    fb_args_list[i].trans_wl = &trans_wl;
    fb_args_list[i].num_trans_ops = num_ops;
    fb_args_list[i].trans_st_idx = st_op_idx;
    fb_args_list[i].config = args->config;
    fb_args_list[i].run_time_s = args->run_tims_s;
    fb_args_list[i].start_trans_barrier = start_trans_barrier;
    strcpy(fb_args_list[i].controller_ip, args->controller_ip);

    boost::fibers::fiber fb(client_ycsb_ela_mem_fiber_worker, &fb_args_list[i]);
    fb_list[i] = std::move(fb);
  }

  for (int i = 0; i < NUM_FB_PER_THREAD; i++) {
    fb_list[i].join();
  }
}

void* client_ycsb_ela_cpu_fiber_main(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload load_wl, trans_wl;
  ret = load_workload_ycsb(args->workload_name, -1, args->cid,
                           args->all_client_num, args->config->eviction_type,
                           &load_wl, &trans_wl);
  assert(ret == 0);

  // start fiber
  ClientFiberArgs fb_args_list[NUM_FB_PER_THREAD];
  boost::fibers::fiber fb_list[NUM_FB_PER_THREAD];
  boost::fibers::barrier* start_trans_barrier = new boost::fibers::barrier(2);
  for (int i = 0; i < NUM_FB_PER_THREAD; i++) {
    uint32_t num_ops = trans_wl.num_ops / NUM_FB_PER_THREAD;
    uint32_t st_op_idx = num_ops * i;
    fb_args_list[i].num_all_clients = args->all_client_num;
    fb_args_list[i].cid = args->cid;
    fb_args_list[i].fb_id = i;
    fb_args_list[i].load_wl = &load_wl;
    fb_args_list[i].trans_wl = &trans_wl;
    fb_args_list[i].num_trans_ops = num_ops;
    fb_args_list[i].trans_st_idx = st_op_idx;
    fb_args_list[i].config = args->config;
    fb_args_list[i].run_time_s = args->run_tims_s;
    fb_args_list[i].start_trans_barrier = start_trans_barrier;
    strcpy(fb_args_list[i].controller_ip, args->controller_ip);

    boost::fibers::fiber fb(client_ycsb_ela_cpu_fiber_worker, &fb_args_list[i]);
    fb_list[i] = std::move(fb);
  }

  for (int i = 0; i < NUM_FB_PER_THREAD; i++) {
    fb_list[i].join();
  }
}

void* client_ycsb_fiber_main(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload load_wl, trans_wl;
  ret = load_workload_ycsb(args->workload_name, -1, args->cid,
                           args->all_client_num, args->config->eviction_type,
                           &load_wl, &trans_wl);
  assert(ret == 0);

  // start fiber
  ClientFiberArgs fb_args_list[NUM_FB_PER_THREAD];
  boost::fibers::fiber fb_list[NUM_FB_PER_THREAD];
  boost::fibers::barrier* start_trans_barrier = new boost::fibers::barrier(2);
  for (int i = 0; i < NUM_FB_PER_THREAD; i++) {
    uint32_t num_ops = trans_wl.num_ops / NUM_FB_PER_THREAD;
    uint32_t st_op_idx = num_ops * i;
    fb_args_list[i].num_all_clients = args->all_client_num;
    fb_args_list[i].cid = args->cid;
    fb_args_list[i].fb_id = i;
    fb_args_list[i].load_wl = &load_wl;
    fb_args_list[i].trans_wl = &trans_wl;
    fb_args_list[i].num_trans_ops = num_ops;
    fb_args_list[i].trans_st_idx = st_op_idx;
    fb_args_list[i].config = args->config;
    fb_args_list[i].run_time_s = args->run_tims_s;
    fb_args_list[i].start_trans_barrier = start_trans_barrier;
    strcpy(fb_args_list[i].controller_ip, args->controller_ip);

    boost::fibers::fiber fb(client_ycsb_fiber_worker, &fb_args_list[i]);
    fb_list[i] = std::move(fb);
  }

  for (int i = 0; i < NUM_FB_PER_THREAD; i++) {
    fb_list[i].join();
  }
}
#endif

void* client_ycsb(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload load_wl, trans_wl;
  ret = load_workload_ycsb(args->workload_name, -1, args->cid,
                           args->all_client_num, client->get_evict_type(),
                           &load_wl, &trans_wl);
  assert(ret == 0);

  // ready to run workload
  struct timeval st, tst, tet;
  char tmp_buf[128];
  uint32_t tmp_len;
  std::map<uint32_t, uint32_t> lat_map;

  uint8_t eviction_type = client->get_evict_type();

  // sync to load ycsb dataset
  lat_map.clear();
  printd(L_INFO, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);
  gettimeofday(&st, NULL);
  for (int i = 0; i < load_wl.num_ops; i++) {
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&load_wl, i, &key_addr, &val_addr, &key_size, &val_size,
                    &op);
    gettimeofday(&tst, NULL);
    if (eviction_type == EVICT_PRECISE)
      ret = client->kv_p_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
    else
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    gettimeofday(&tet, NULL);
    lat_map[diff_ts_us(&tet, &tst)]++;
    if (args->validate) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      assert(ret == 0);
      assert(tmp_len == val_size);
      assert(*(uint32_t*)tmp_buf == *(uint32_t*)val_addr);
    }
    if (client->num_evict_ != 0 || client->num_bucket_evict_ != 0) {
      printf("Evicted during load!");
      abort();
    }
  }
  json load_res;
  load_res["n_ops"] = load_wl.num_ops;
  load_res["n_retry"] = client->num_set_retry_;
  load_res["time_us"] = diff_ts_us(&tet, &st);
  load_res["lat_map"] = json(lat_map);
  std::string load_res_str = load_res.dump();
  con_client.memcached_put_result((void*)load_res_str.c_str(),
                                  strlen(load_res_str.c_str()), args->cid);

  // sync to do trans
  lat_map.clear();
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);
  if (args->is_load_only)
    return NULL;  // return loader thread

  gettimeofday(&st, NULL);
  uint32_t seq = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  while (true) {
    uint32_t idx = seq % trans_wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&trans_wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                    &op);
    // if (strcmp((char *)key_addr, "8457711521909979413") == 0) printd(L_INFO,
    // "op: %d %s", op, (char *)key_addr);
    if (op == GET) {
      n_get++;
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      gettimeofday(&tet, NULL);
      // assert(ret == 0); // disable this on ycsbd
    } else {
      n_set++;
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      gettimeofday(&tet, NULL);
    }
    lat_map[diff_ts_us(&tet, &tst)]++;
    seq++;
    if (diff_ts_us(&tet, &st) > args->run_tims_s * 1000000) {
      break;
    }
  }
  printf("Client %d finish\n", args->cid);
  json trans_res;
  trans_res["ops"] = seq;
  trans_res["n_get"] = n_get;
  trans_res["n_set"] = n_set;
  trans_res["n_retry"] = client->num_set_retry_;
  trans_res["n_evict"] = client->num_evict_;
  trans_res["lat_map"] = json(lat_map);
  std::string str = trans_res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client sync: %ld\n", client->num_cliquemap_sync_);
  return NULL;
}

void* client_mix_ela_cpu(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);
  DMCWorkload lru_wl, lfu_wl;
  ret = load_mix_all(&lru_wl, &lfu_wl);
  assert(ret == 0);

  client->clear_counters();

  char tmp_buf[128];
  uint32_t tmp_len;
  std::vector<uint32_t> n_miss_vec;
  std::vector<uint32_t> ops_vec;
  uint32_t lru_idx = 0;
  uint32_t lfu_idx = 0;
  for (int i = 0; i <= 10; i++) {
    con_client.memcached_sync_ready(args->cid);
    printf("Client %d start %d\n", args->cid, i);
    int num_lru = 10 - i;
    int num_lfu = i;
    uint32_t* cur_idx;
    DMCWorkload* cur_wl;
    if (args->cid <= num_lru) {
      cur_wl = &lru_wl;
      cur_idx = &lru_idx;

    } else {
      cur_wl = &lfu_wl;
      cur_idx = &lfu_idx;
    }
    uint32_t cur_cid = args->cid - 1;
    uint32_t cur_num = num_lru;
    if (args->cid > num_lru) {
      cur_cid -= num_lru;
      cur_num = num_lfu;
    }
    uint32_t ops = 0;
    uint32_t n_miss = 0;
    uint32_t num_execute_ops = 100000;
    if (num_lru == 0 || num_lfu == 0) {
      num_execute_ops = cur_wl->num_ops / cur_num;
    } else {
      num_execute_ops =
          std::min(lru_wl.num_ops / num_lru, lfu_wl.num_ops / num_lfu);
    }
    // num_execute_ops *= 2;
    // num_execute_ops = ((num_execute_ops / 1000) + 1) * 1000;
    // uint32_t num_warmup_ops = 100000;
    num_execute_ops = 6534;
    uint32_t num_warmup_ops = int(num_execute_ops * 0.1);
    for (int i = 0; i < num_warmup_ops; i++) {
      uint32_t idx = (i * cur_num + cur_cid + *cur_idx) % cur_wl->num_ops;
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(cur_wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                      &op);
      if (op == GET) {
        ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
        if (ret != 0) {
          ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
        }
      } else {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
    }
    num_execute_ops = num_execute_ops - num_warmup_ops;
    con_client.memcached_sync_ready(args->cid);
    printf("Client %d execute %d ops\n", args->cid, num_execute_ops);
    struct timeval st, et;
    gettimeofday(&st, NULL);
    while (ops < num_execute_ops) {
      uint32_t idx = ((ops + num_warmup_ops) * cur_num + cur_cid + *cur_idx) %
                     cur_wl->num_ops;
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(cur_wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                      &op);
      if (op == GET) {
        ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
        if (ret != 0) {
          n_miss++;
          ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
        }
      } else {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        n_miss += (ret < 0);
      }
      ops++;
      // if (ops % 10000 == 0) {
      //     ops_vec.push_back(ops);
      //     n_miss_vec.push_back(n_miss);
      // }
    }
    *cur_idx += 6534;
    ops_vec.push_back(ops);
    n_miss_vec.push_back(n_miss);
  }

  json res;
  res["n_ops"] = json(ops_vec);
  res["n_misses"] = json(n_miss_vec);
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  return NULL;
}

void* client_webmail_ela_cpu(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  char tmp_buf[128];
  uint32_t tmp_len;
  DMCWorkload wl;
  std::vector<uint32_t> n_miss_vec;
  std::vector<uint32_t> ops_vec;
  for (int i = 16; i <= 160; i += 16) {
    if (i != 16) {
      free_workload(&wl);
    }
    ret = load_workload("webmail-all", -1, args->cid, i, &wl);
    con_client.memcached_sync_ready(args->cid);
    printf("Client %d start %d\n", args->cid, i);
    if (args->cid > i) {
      con_client.memcached_sync_ready(args->cid);
      ops_vec.push_back(0);
      n_miss_vec.push_back(0);
      continue;
    }
    uint32_t cur_cid = args->cid - 1;
    uint32_t ops = 0;
    uint32_t warmup_ops = 0;
    uint32_t n_miss = 0;
    struct timeval st, et;
    gettimeofday(&st, NULL);
    // warmup 10 seconds
    while (true) {
      uint32_t idx = warmup_ops % wl.num_ops;
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                      &op);
      if (op == GET) {
        ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
        if (ret != 0) {
          ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
        }
      } else {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
      warmup_ops++;
      gettimeofday(&et, NULL);
      if (diff_ts_us(&et, &st) > 10LL * 1000000) {
        break;
      }
    }
    client->clear_counters();
    con_client.memcached_sync_ready(args->cid);
    // execute 20 seconds
    gettimeofday(&st, NULL);
    while (true) {
      uint32_t idx = ops % wl.num_ops;
      uint64_t key_addr, val_addr;
      uint32_t key_size, val_size;
      uint8_t op;
      get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size,
                      &op);
      if (op == GET) {
        ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
        if (ret != 0) {
          n_miss++;
#ifdef USE_PENALTY
          usleep(500);
#endif
          ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                               val_size);
        }
      } else {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        n_miss += (ret < 0);
      }
      ops++;
      gettimeofday(&et, NULL);
      if (diff_ts_us(&et, &st) > 20LL * 1000000) {
        break;
      }
    }
    ops_vec.push_back(ops);
    n_miss_vec.push_back(n_miss);
  }

  json res;
  res["n_ops"] = json(ops_vec);
  res["n_misses"] = json(n_miss_vec);
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  return NULL;
}

void* client_workload_real_ela_cpu(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;

  if (strcmp(args->workload_name, "mix") == 0) {
    return client_mix_ela_cpu(_args);
  } else {
    assert(memcmp(args->workload_name, "webmail", strlen("webmail")) == 0);
    return client_webmail_ela_cpu(_args);
  }
}

void* client_workload_real_ela_mem(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload wl;
  ret = load_workload(args->workload_name, -1, args->cid, args->all_client_num,
                      &wl);
  assert(ret == 0);

  char tmp_buf[128];
  uint32_t tmp_len;

  printd(L_DEBUG, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);

  // warmup for 10 seconds
  struct timeval st, tst, tet;
  gettimeofday(&st, NULL);
  uint32_t warmup_seq = 0;
  while (true) {
    uint32_t idx = warmup_seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
    } else {
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    }
    warmup_seq++;
    gettimeofday(&tet, NULL);
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        10LL * 1000000) {
      break;
    }
  }
  printf("client %d warmup %d ops\n", args->cid, warmup_seq);

  // sync for testing
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);

  uint32_t seq = 0;
  uint32_t n_miss = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  int tick = 0;
  std::vector<uint32_t> ops_vec;
  std::vector<uint32_t> n_miss_vec;
  uint32_t num_ticks = args->run_tims_s * 2;  // 500ms a tick
  uint32_t tick_us = 500000;                  // 500ms
  gettimeofday(&st, NULL);
  while (tick < num_ticks) {
    uint32_t idx = seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      n_get++;
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        n_miss++;
#ifdef USE_PENALTY
        usleep(500);
#endif
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
    } else {
      n_set++;
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      n_miss += (ret < 0);
    }
    seq++;
    gettimeofday(&tet, NULL);
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        tick_us * tick) {
      ops_vec.push_back(seq);
      n_miss_vec.push_back(n_miss);
      tick++;
    }
  }

  json res;
  res["n_ops_cont"] = json(ops_vec);
  res["n_miss_cont"] = json(n_miss_vec);
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client %d finished %d ops with %f miss ratio\n", args->cid, seq,
         (float)n_miss / seq);
  return NULL;
}

void* client_workload_real_n(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload wl;
  ret = load_workload(args->workload_name, -1, args->cid, args->all_client_num,
                      &wl);
  assert(ret == 0);

  char tmp_buf[128];
  uint32_t tmp_len;

  printd(L_DEBUG, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);

  // warmup for 0.1 trace
  struct timeval st, tst, tet;
  gettimeofday(&st, NULL);
  uint32_t warmup_seq = 0;
  // uint32_t warmup_size = (uint32_t)(wl.num_ops * 0.1);
  uint32_t warmup_size = 0;
  while (warmup_seq < warmup_size) {
    uint32_t idx = warmup_seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        assert(ret == DMC_SUCCESS);
      }
    } else {
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      assert(ret == DMC_SUCCESS);
    }
    warmup_seq++;
  }
  printf("client %d warmup %d ops\n", args->cid, warmup_seq);

  // sync for testing
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);

  uint32_t seq = 0;
  uint32_t n_miss = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  int tick = 0;
  std::vector<uint32_t> ops_vec;
  std::vector<uint32_t> n_miss_vec;
  std::vector<uint64_t> num_evict_vec;
  std::vector<uint64_t> succ_evict_vec;
  std::vector<uint64_t> num_evict_bucket_vec;
  std::vector<uint64_t> succ_evict_bucket_vec;
  std::vector<float> tmp_weights;
  std::vector<std::vector<float>> adaptive_weight_vec;
  std::map<uint32_t, uint32_t> lat_map;
  uint32_t num_ticks =
      args->run_tims_s * TICK_PER_SEC;  // 50ms a tick record a value
  gettimeofday(&st, NULL);
  while (tick < num_ticks) {
    uint32_t idx = seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      n_get++;
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        n_miss++;
#ifdef USE_PENALTY
        usleep(500);
#endif
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
        assert(ret == DMC_SUCCESS);
      }
      gettimeofday(&tet, NULL);
    } else {
      n_set++;
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      gettimeofday(&tet, NULL);
      assert(ret == DMC_SUCCESS);
      n_miss += (ret < 0);
    }
    lat_map[diff_ts_us(&tet, &tst)]++;
    seq++;
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        TICK_US * tick) {
      ops_vec.push_back(seq);
      num_evict_bucket_vec.push_back(client->num_bucket_evict_);
      succ_evict_bucket_vec.push_back(client->num_success_bucket_evict_);
      num_evict_vec.push_back(client->num_evict_);
      succ_evict_vec.push_back(client->num_success_evict_);
      n_miss_vec.push_back(n_miss);
      client->get_adaptive_weights(tmp_weights);
      adaptive_weight_vec.push_back(tmp_weights);
      tick++;
    }
  }

  json res;
  res["n_ops_cont"] = json(ops_vec);
  res["n_evict_cont"] = json(num_evict_vec);
  res["n_succ_evict_cont"] = json(succ_evict_vec);
  res["n_evict_bucket_cont"] = json(num_evict_bucket_vec);
  res["n_succ_evict_bucket_cont"] = json(succ_evict_bucket_vec);
  res["n_miss_cont"] = json(n_miss_vec);
  res["adaptive_weights_cont"] = json(adaptive_weight_vec);
  res["n_hist_match"] = client->num_hist_match_;
  res["n_get"] = n_get;
  res["n_set"] = n_set;
  res["n_retry"] = client->num_set_retry_;
  res["n_rdma_send"] = client->num_rdma_send_;
  res["n_rdma_recv"] = client->num_rdma_recv_;
  res["n_rdma_read"] = client->num_rdma_read_;
  res["n_rdma_write"] = client->num_rdma_write_;
  res["n_rdma_cas"] = client->num_rdma_cas_;
  res["n_rdma_faa"] = client->num_rdma_faa_;
  res["n_weights_sync"] = client->num_adaptive_weight_sync_;
  res["n_weights_adjust"] = client->num_adaptive_adjust_weights_;
  res["n_read_hist_head"] = client->num_read_hist_head_;
  res["n_hist_overwrite"] = client->num_hist_overwite_;
  res["n_hist_access"] = client->num_hist_access_;
  res["n_ada_evict_inconsistent"] = client->num_ada_evict_inconsistent_;
  res["n_ada_bucket_evict_history"] = client->num_bucket_evict_history_;
  res["n_expert_evict"] = json(client->expert_evict_cnt_);
  res["lat_map"] = json(lat_map);
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client %d finished %d ops with %f miss ratio\n", args->cid, seq,
         (float)n_miss / seq);
  return NULL;
}

void* client_workload_change(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload wl;
  ret = load_workload(args->workload_name, -1, args->cid, args->all_client_num,
                      &wl);
  assert(ret == 0);

  char tmp_buf[128];
  uint32_t tmp_len;

  printd(L_DEBUG, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);

  // warmup for 10 seconds
  struct timeval st, tst, tet;
  gettimeofday(&st, NULL);
  uint32_t warmup_seq = 0;

  // sync for testing
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);

  uint32_t seq = 0;
  uint32_t n_miss = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  int tick = 0;
  std::vector<uint32_t> ops_vec;
  std::vector<uint32_t> n_miss_vec;
  std::vector<uint64_t> num_evict_vec;
  std::vector<uint64_t> succ_evict_vec;
  std::vector<uint64_t> num_evict_bucket_vec;
  std::vector<uint64_t> succ_evict_bucket_vec;
  std::vector<float> tmp_weights;
  std::vector<std::vector<float>> adaptive_weight_vec;
  std::map<uint32_t, uint32_t> lat_map;
  uint32_t num_ticks =
      args->run_tims_s * TICK_PER_SEC;  // 500ms a tick record a value
  gettimeofday(&st, NULL);
  while (tick < num_ticks) {
    uint32_t idx = seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      n_get++;
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        n_miss++;
#ifdef USE_PENALTY
        usleep(500);
#endif
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
      gettimeofday(&tet, NULL);
    } else {
      n_set++;
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      gettimeofday(&tet, NULL);
      n_miss += (ret < 0);
    }
    lat_map[diff_ts_us(&tet, &tst)]++;
    seq++;
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        TICK_US * tick) {
      ops_vec.push_back(seq);
      num_evict_bucket_vec.push_back(client->num_bucket_evict_);
      succ_evict_bucket_vec.push_back(client->num_success_bucket_evict_);
      num_evict_vec.push_back(client->num_evict_);
      succ_evict_vec.push_back(client->num_success_evict_);
      n_miss_vec.push_back(n_miss);
      client->get_adaptive_weights(tmp_weights);
      adaptive_weight_vec.push_back(tmp_weights);
      tick++;
    }
  }

  json res;
  res["n_ops_cont"] = json(ops_vec);
  res["n_evict_cont"] = json(num_evict_vec);
  res["n_succ_evict_cont"] = json(succ_evict_vec);
  res["n_evict_bucket_cont"] = json(num_evict_bucket_vec);
  res["n_succ_evict_bucket_cont"] = json(succ_evict_bucket_vec);
  res["n_miss_cont"] = json(n_miss_vec);
  res["adaptive_weights_cont"] = json(adaptive_weight_vec);
  res["n_hist_match"] = client->num_hist_match_;
  res["n_get"] = n_get;
  res["n_set"] = n_set;
  res["n_retry"] = client->num_set_retry_;
  res["n_rdma_send"] = client->num_rdma_send_;
  res["n_rdma_recv"] = client->num_rdma_recv_;
  res["n_rdma_read"] = client->num_rdma_read_;
  res["n_rdma_write"] = client->num_rdma_write_;
  res["n_rdma_cas"] = client->num_rdma_cas_;
  res["n_rdma_faa"] = client->num_rdma_faa_;
  res["n_weights_sync"] = client->num_adaptive_weight_sync_;
  res["n_weights_adjust"] = client->num_adaptive_adjust_weights_;
  res["n_read_hist_head"] = client->num_read_hist_head_;
  res["n_hist_overwrite"] = client->num_hist_overwite_;
  res["n_hist_access"] = client->num_hist_access_;
  res["n_ada_evict_inconsistent"] = client->num_ada_evict_inconsistent_;
  res["n_ada_bucket_evict_history"] = client->num_bucket_evict_history_;
  res["n_expert_evict"] = json(client->expert_evict_cnt_);
  res["lat_map"] = json(lat_map);
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client %d finished %d ops with %f miss ratio\n", args->cid, seq,
         (float)n_miss / seq);
  return NULL;
}
void* client_workload_real(void* _args) {
  ClientArgs* args = (ClientArgs*)_args;
  DMCClient* client = args->client;
  DMCMemcachedClient con_client(args->controller_ip);

  int ret = stick_this_thread_to_core(args->core);
  if (ret)
    printd(L_INFO, "Failed to bind client %d to core %d", args->cid,
           args->core);
  else
    printd(L_INFO, "Running client %d on core %d", args->cid, args->core);

  DMCWorkload wl;
  ret = load_workload(args->workload_name, -1, args->cid, args->all_client_num,
                      &wl);
  assert(ret == 0);

  char tmp_buf[128];
  uint32_t tmp_len;

  printd(L_DEBUG, "client %d waiting sync", args->cid);
  con_client.memcached_sync_ready(args->cid);

  // warmup for 10 seconds
  struct timeval st, tst, tet;
  gettimeofday(&st, NULL);
  uint32_t warmup_seq = 0;
  while (true) {
    uint32_t idx = warmup_seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
    } else {
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
    }
    warmup_seq++;
    gettimeofday(&tet, NULL);
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        10LL * 1000000) {
      break;
    }
  }
  printf("client %d warmup %d ops\n", args->cid, warmup_seq);

  // sync for testing
  printd(L_INFO, "client %d waiting sync", args->cid);
  client->clear_counters();
  con_client.memcached_sync_ready(args->cid);

  uint32_t seq = 0;
  uint32_t n_miss = 0;
  uint32_t n_get = 0;
  uint32_t n_set = 0;
  int tick = 0;
  std::vector<uint32_t> ops_vec;
  std::vector<uint32_t> n_miss_vec;
  std::vector<uint64_t> num_evict_vec;
  std::vector<uint64_t> succ_evict_vec;
  std::vector<uint64_t> num_evict_bucket_vec;
  std::vector<uint64_t> succ_evict_bucket_vec;
  std::vector<float> tmp_weights;
  std::vector<std::vector<float>> adaptive_weight_vec;
  std::map<uint32_t, uint32_t> lat_map;
  uint32_t num_ticks =
      args->run_tims_s * TICK_PER_SEC;  // 500ms a tick record a value
  gettimeofday(&st, NULL);
  while (tick < num_ticks) {
    uint32_t idx = seq % wl.num_ops;
    uint64_t key_addr, val_addr;
    uint32_t key_size, val_size;
    uint8_t op;
    get_workload_kv(&wl, idx, &key_addr, &val_addr, &key_size, &val_size, &op);
    if (op == GET) {
      n_get++;
      gettimeofday(&tst, NULL);
      ret = client->kv_get((void*)key_addr, key_size, tmp_buf, &tmp_len);
      if (ret != 0) {
        n_miss++;
#ifdef USE_PENALTY
        usleep(500);
#endif
        ret = client->kv_set((void*)key_addr, key_size, (void*)val_addr,
                             val_size);
      }
      gettimeofday(&tet, NULL);
    } else {
      n_set++;
      gettimeofday(&tst, NULL);
      ret =
          client->kv_set((void*)key_addr, key_size, (void*)val_addr, val_size);
      gettimeofday(&tet, NULL);
      n_miss += (ret < 0);
    }
    lat_map[diff_ts_us(&tet, &tst)]++;
    seq++;
    if ((tet.tv_sec - st.tv_sec) * 1000000 + (tet.tv_usec - st.tv_usec) >
        TICK_US * tick) {
      ops_vec.push_back(seq);
      num_evict_bucket_vec.push_back(client->num_bucket_evict_);
      succ_evict_bucket_vec.push_back(client->num_success_bucket_evict_);
      num_evict_vec.push_back(client->num_evict_);
      succ_evict_vec.push_back(client->num_success_evict_);
      n_miss_vec.push_back(n_miss);
      client->get_adaptive_weights(tmp_weights);
      adaptive_weight_vec.push_back(tmp_weights);
      tick++;
    }
  }

  json res;
  res["n_ops_cont"] = json(ops_vec);
  res["n_evict_cont"] = json(num_evict_vec);
  res["n_succ_evict_cont"] = json(succ_evict_vec);
  res["n_evict_bucket_cont"] = json(num_evict_bucket_vec);
  res["n_succ_evict_bucket_cont"] = json(succ_evict_bucket_vec);
  res["n_miss_cont"] = json(n_miss_vec);
  res["adaptive_weights_cont"] = json(adaptive_weight_vec);
  res["n_hist_match"] = client->num_hist_match_;
  res["n_get"] = n_get;
  res["n_set"] = n_set;
  res["n_retry"] = client->num_set_retry_;
  res["n_rdma_send"] = client->num_rdma_send_;
  res["n_rdma_recv"] = client->num_rdma_recv_;
  res["n_rdma_read"] = client->num_rdma_read_;
  res["n_rdma_write"] = client->num_rdma_write_;
  res["n_rdma_cas"] = client->num_rdma_cas_;
  res["n_rdma_faa"] = client->num_rdma_faa_;
  res["n_weights_sync"] = client->num_adaptive_weight_sync_;
  res["n_weights_adjust"] = client->num_adaptive_adjust_weights_;
  res["n_read_hist_head"] = client->num_read_hist_head_;
  res["n_hist_overwrite"] = client->num_hist_overwite_;
  res["n_hist_access"] = client->num_hist_access_;
  res["n_ada_evict_inconsistent"] = client->num_ada_evict_inconsistent_;
  res["n_ada_bucket_evict_history"] = client->num_bucket_evict_history_;
  res["n_expert_evict"] = json(client->expert_evict_cnt_);
  res["lat_map"] = json(lat_map);
  std::string str = res.dump();
  con_client.memcached_put_result((void*)str.c_str(), strlen(str.c_str()),
                                  args->cid);
  printf("Client %d finished %d ops with %f miss ratio\n", args->cid, seq,
         (float)n_miss / seq);
  return NULL;
}
typedef struct _CheckerArgs {
  char memcached_ip[32];
  char workload_name[128];
  DMCClient** client_list;
  int num_clients;
} CheckerArgs;

void* ela_mem_checker(void* _args) {
  CheckerArgs* args = (CheckerArgs*)_args;
  DMCMemcachedClient con_client(args->memcached_ip);
  int num_sync = 0;
  if (memcmp(args->workload_name, "ycsb", strlen("ycsb")) == 0) {
    num_sync = 1;
  } else {
    assert(is_real_workload(args->workload_name) == true);
    num_sync = 4;
  }

  printf("start checking (%d)\n", num_sync);
  for (int i = 0; i < num_sync; i++) {
    char sync_msg[256];
    sprintf(sync_msg, "client-scale-memory-%d", i);
    printf("Check %s\n", sync_msg);
    con_client.memcached_wait(sync_msg);
    printf("scale %s\n", sync_msg);
#ifdef USE_FIBER
    for (int c = 0; c < args->num_clients * NUM_FB_PER_THREAD; c++) {
#else
    for (int c = 0; c < args->num_clients; c++) {
#endif
      args->client_list[c]->scale_memory();
    }
  }
  return NULL;
}

void run_client(const InitArgs* args) {
  int ret = 0;
  int num_clients = args->num_client_threads;
  if (memcmp(args->workload_name, "ycsb", strlen("ycsb")) == 0) {
    num_clients = YCSB_LOAD_NUM > args->num_client_threads
                      ? YCSB_LOAD_NUM
                      : args->num_client_threads;
  }
  DMCConfig client_conf_list[num_clients];
#ifdef USE_FIBER
  DMCClient* client_list[num_clients * NUM_FB_PER_THREAD];
#else
  DMCClient* client_list[num_clients];
#endif
  pthread_t client_tid_list[num_clients];
  ClientArgs client_main_arg_list[num_clients];

  pthread_t checker_tid;
  CheckerArgs checker_args;
  if (args->elastic == ELA_MEM) {
    strcpy(checker_args.memcached_ip, args->memcached_ip);
    strcpy(checker_args.workload_name, args->workload_name);
    checker_args.client_list = client_list;
    checker_args.num_clients = num_clients;
    pthread_create(&checker_tid, NULL, ela_mem_checker, (void*)&checker_args);
  }

  for (int i = 0; i < num_clients; i++) {
    ret = load_config(args->config_fname, &client_conf_list[i]);
    assert(ret == 0);
    int core = client_conf_list[i].core_id + i;

    client_conf_list[i].server_id = args->server_id + i;
#ifndef USE_FIBER
    client_list[i] = new DMCClient(&client_conf_list[i]);
#endif

    memset(&client_main_arg_list[i], 0, sizeof(ClientArgs));
#ifdef USE_FIBER
    client_main_arg_list[i].config = &client_conf_list[i];
    client_main_arg_list[i].tid = i;
    client_main_arg_list[i].num_l_clients = num_clients;
    client_main_arg_list[i].client_list = client_list;
#else
    client_main_arg_list[i].client = client_list[i];
#endif
    client_main_arg_list[i].core = core;
    client_main_arg_list[i].cid = client_conf_list[i].server_id;
    client_main_arg_list[i].num_load_ops = args->num_load_ops;
    client_main_arg_list[i].validate = args->validate;
    client_main_arg_list[i].all_client_num = args->all_client_num;
    client_main_arg_list[i].run_tims_s = args->run_time;
    client_main_arg_list[i].n_lru_clients = args->n_lru_client;
    client_main_arg_list[i].n_lfu_clients = args->n_lfu_client;
    client_main_arg_list[i].use_warmup = args->use_warmup;
    client_main_arg_list[i].is_load_only = (i >= args->num_client_threads);
    strcpy(client_main_arg_list[i].workload_name, args->workload_name);
    strcpy(client_main_arg_list[i].controller_ip, args->memcached_ip);

    if (memcmp(args->workload_name, "micro", strlen("micro")) == 0) {
      pthread_create(&client_tid_list[i], NULL, client_micro_main,
                     &client_main_arg_list[i]);
    } else if (is_real_workload(args->workload_name)) {
      if (args->elastic == ELA_CPU)
        pthread_create(&client_tid_list[i], NULL, client_workload_real_ela_cpu,
                       &client_main_arg_list[i]);
      else if (args->elastic == ELA_MEM)
        pthread_create(&client_tid_list[i], NULL, client_workload_real_ela_mem,
                       &client_main_arg_list[i]);
      else {
        pthread_create(&client_tid_list[i], NULL, client_workload_real,
                       &client_main_arg_list[i]);
      }
    } else if (memcmp(args->workload_name, "evict_micro",
                      strlen("evict_micrio")) == 0) {
      pthread_create(&client_tid_list[i], NULL, client_evict_micro_main,
                     &client_main_arg_list[i]);
    } else if (memcmp(args->workload_name, "ycsb", strlen("ycsb")) == 0) {
      if (args->elastic == ELA_CPU)
#ifdef USE_FIBER
        pthread_create(&client_tid_list[i], NULL,
                       client_ycsb_ela_cpu_fiber_main,
                       &client_main_arg_list[i]);
#else
        pthread_create(&client_tid_list[i], NULL, client_ycsb_ela_cpu,
                       &client_main_arg_list[i]);
#endif
      else if (args->elastic == ELA_MEM)
#ifdef USE_FIBER
        pthread_create(&client_tid_list[i], NULL,
                       client_ycsb_ela_mem_fiber_main,
                       &client_main_arg_list[i]);
#else
        pthread_create(&client_tid_list[i], NULL, client_ycsb_ela_mem,
                       &client_main_arg_list[i]);
#endif
      else
#ifdef USE_FIBER
        pthread_create(&client_tid_list[i], NULL, client_ycsb_fiber_main,
                       &client_main_arg_list[i]);
#else
        pthread_create(&client_tid_list[i], NULL, client_ycsb,
                       &client_main_arg_list[i]);
#endif
    } else if (memcmp(args->workload_name, "hit-rate-", strlen("hit-rate-")) ==
               0) {
      pthread_create(&client_tid_list[i], NULL, client_hit_rate,
                     &client_main_arg_list[i]);
    }
  }

  for (int i = 0; i < args->num_client_threads; i++) {
    pthread_join(client_tid_list[i], NULL);
  }
  pthread_join(checker_tid, NULL);
}