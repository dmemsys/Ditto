#include "test_client.h"
#include <algorithm>
#include <cstdlib>

void ClientTest::SetUp() {
  setup_server_conf();
  setup_client_conf();
  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);
}

void ClientTest::TearDown() {
  stop_server();
  delete client_;
  delete server_;
}

void ClientTest::start_server() {
  server_main_args_.server = server_;
  server_main_args_.core_id = 0;
  pthread_create(&server_tid_, NULL, server_main, (void*)&server_main_args_);
}

void ClientTest::stop_server() {
  server_->stop();
  pthread_join(server_tid_, NULL);
}

void ClientTest::reconfig_cliquemap_test(bool use_dumb_hash) {
  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_CLIQUEMAP;
  client_conf_.eviction_type = EVICT_CLIQUEMAP;
  if (use_dumb_hash) {
    server_conf_.hash_type = HASH_DUMB;
    client_conf_.hash_type = HASH_DUMB;
  }

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);
}

void ClientTest::reconfig_plru_test(bool use_dumb_hash) {
  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_PRECISE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.eviction_type = EVICT_PRECISE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;
  if (use_dumb_hash) {
    server_conf_.hash_type = HASH_DUMB;
    client_conf_.hash_type = HASH_DUMB;
  }

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);
}

void ClientTest::dump_counters() {
  printd(L_INFO, "Counter informations:");
  printd(L_INFO, "num_rdma_write: %ld", client_->num_rdma_write_);
  printd(L_INFO, "num_rdma_read:  %ld", client_->num_rdma_read_);
  printd(L_INFO, "num_rdma_cas:   %ld", client_->num_rdma_cas_);
  printd(L_INFO, "num_rdma_faa:   %ld", client_->num_rdma_faa_);
  printd(L_INFO, "num_evict:      %ld", client_->num_evict_);
  printd(L_INFO, "=====================");
}

void ClientTest::kv_fuzz() {
  int ret = 0;
  std::map<std::string, std::string> gt_table;
  for (int i = 0; i < 100000; i++) {
    uint32_t k_rand = rand();
    uint32_t v_rand = rand();
    uint32_t op_rand = rand() % 2;
    sprintf(key_buf_, "Key-%d", k_rand);
    sprintf(val_buf_, "Val-%d", v_rand);
    if (op_rand == 0) {
      // get
      uint32_t val_size;
      std::map<std::string, std::string>::iterator it =
          gt_table.find(std::string(key_buf_));
      ret =
          client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &val_size);
      if (it == gt_table.end()) {
        ASSERT_TRUE(ret == -1);
      } else {
        ASSERT_TRUE(strcmp(it->second.c_str(), tmp_buf_) == 0);
      }
    } else {
      // set
      gt_table[std::string(key_buf_)] = std::string(val_buf_);
      ret = client_->kv_set(key_buf_, strlen(key_buf_) + 1, val_buf_,
                            strlen(val_buf_) + 1);
      ASSERT_TRUE(ret == 0);

      uint32_t val_size;
      ret =
          client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &val_size);
      ASSERT_TRUE(ret == 0);
      ASSERT_TRUE(strcmp(val_buf_, tmp_buf_) == 0);
    }
  }
}

void ClientTest::kv_evict_get_set(int num_iter) {
  int ret = 0;
  for (int i = 0; i < num_iter; i++) {
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    if ((i % 100) == 0) {
      printd(L_INFO, "set %s", key_buf_);
    }
    ret = client_->kv_set(key_buf_, strlen(key_buf_) + 1, val_buf_,
                          strlen(val_buf_) + 1);
    ASSERT_TRUE(ret == 0);

    uint32_t tmp_len = 0;
    if ((i % 100) == 0) {
      printd(L_INFO, "get %s", key_buf_);
    }
    ret = client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &tmp_len_);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(val_buf_, tmp_buf_) == 0);
  }
}

void ClientTest::reconfig_evict_memory(int evict_type) {
  // 16 * 1024 * 1024 bucket size
  server_conf_.block_size = 2048;
  server_conf_.segment_size = 1024 * 1024;
  server_conf_.server_data_len =
      HASH_SPACE_SIZE + 1024 * 1024 +
      std::max(get_list_size(HASH_BUCKET_ASSOC_NUM * HASH_NUM_BUCKETS),
               4 * 1024 * 1024U);
  client_conf_.segment_size = server_conf_.segment_size;
  client_conf_.block_size = server_conf_.block_size;
  client_conf_.server_data_len = server_conf_.server_data_len;
}

TEST_F(ClientTest, initialization) {
  ASSERT_TRUE(true);
}

TEST_F(ClientTest, sample_lru_kv_exhaust_segment) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.block_size = 1024;
  client_conf_.block_size = 1024;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  // 7000 * 1024 > 64 MB
  for (int i = 0; i < 70000; i++) {
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    ret = client_->kv_set(key_buf_, strlen(key_buf_) + 1, val_buf_,
                          strlen(val_buf_) + 1);
    ASSERT_TRUE(ret == 0);
  }

  for (int i = 0; i < 70000; i++) {
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    uint32_t val_size;
    ret = client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &val_size);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(tmp_buf_, val_buf_) == 0);
  }
  dump_counters();
}

TEST_F(ClientTest, sample_lru_kv_fuzz) {
  srand((uint32_t)time(NULL));
  int ret;

  kv_fuzz();
  dump_counters();
}

TEST_F(ClientTest, sample_lru_kv_evict) {
  int ret;
  printd(L_INFO, "server: %d %d", server_conf_.eviction_priority,
         server_conf_.eviction_type);
  printd(L_INFO, "client: %d %d", client_conf_.eviction_priority,
         client_conf_.eviction_type);

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE);

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, cliquemap_lru_kv_fuzz) {
  srand((uint32_t)time(NULL));
  int ret;

  reconfig_cliquemap_test(false);
  kv_fuzz();
}

TEST_F(ClientTest, cliquemap_lru_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_CLIQUEMAP);
  server_conf_.eviction_type = EVICT_CLIQUEMAP;
  client_conf_.eviction_type = EVICT_CLIQUEMAP;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
}

TEST_F(ClientTest, precise_lru_kv_dumb_set_get) {
  int ret;

  reconfig_plru_test(true);

  for (int i = 0; i < 100; i++) {
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    ret = client_->kv_set(key_buf_, strlen(key_buf_) + 1, val_buf_,
                          strlen(val_buf_) + 1);
    ASSERT_TRUE(ret == 0);

    uint32_t val_size;
    ret = client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &val_size);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(tmp_buf_, val_buf_) == 0);
  }
}

TEST_F(ClientTest, precise_lru_kv_fuzz) {
  srand((uint32_t)time(NULL));
  int ret;

  reconfig_plru_test(false);
  kv_fuzz();
}

TEST_F(ClientTest, precise_lru_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_PRECISE);
  server_conf_.eviction_type = EVICT_PRECISE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.eviction_type = EVICT_PRECISE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
}

TEST_F(ClientTest, precise_lfu_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_PRECISE);
  server_conf_.eviction_type = EVICT_PRECISE;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.eviction_type = EVICT_PRECISE;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
}

TEST_F(ClientTest, naive_sample_lru_kv_fuzz) {
  int ret = 0;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
}

TEST_F(ClientTest, naive_sample_lru_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE_NAIVE);
  server_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, naive_sample_lfu_kv_fuzz) {
  int ret = 0;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
}

TEST_F(ClientTest, naive_sample_lfu_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE_NAIVE);
  server_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.eviction_type = EVICT_SAMPLE_NAIVE;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, adaptive_heavy_kv_fuzz) {
  int ret = 0;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_HEAVY;
  server_conf_.num_experts = 2;
  server_conf_.experts[0] = EVICT_PRIO_LRU;
  server_conf_.experts[1] = EVICT_PRIO_LFU;
  server_conf_.learning_rate = 0.1;
  server_conf_.history_size = 1024;
  server_conf_.use_async_weight = false;
  client_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_HEAVY;
  client_conf_.num_experts = 2;
  client_conf_.experts[0] = EVICT_PRIO_LRU;
  client_conf_.experts[1] = EVICT_PRIO_LFU;
  client_conf_.learning_rate = 0.1;
  client_conf_.history_size = 1024;
  client_conf_.use_async_weight = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
}

TEST_F(ClientTest, adaptive_heavy_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE_NAIVE);
  server_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_HEAVY;
  server_conf_.num_experts = 2;
  server_conf_.experts[0] = EVICT_PRIO_LRU;
  server_conf_.experts[1] = EVICT_PRIO_LFU;
  server_conf_.learning_rate = 0.1;
  server_conf_.history_size = 1024;
  server_conf_.use_async_weight = false;
  client_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_HEAVY;
  client_conf_.num_experts = 2;
  client_conf_.experts[0] = EVICT_PRIO_LRU;
  client_conf_.experts[1] = EVICT_PRIO_LFU;
  client_conf_.learning_rate = 0.1;
  client_conf_.history_size = 1024;
  client_conf_.use_async_weight = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, adaptive_naive_kv_fuzz) {
  int ret = 0;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_NAIVE;
  server_conf_.num_experts = 2;
  server_conf_.experts[0] = EVICT_PRIO_LRU;
  server_conf_.experts[1] = EVICT_PRIO_LFU;
  server_conf_.learning_rate = 0.1;
  server_conf_.history_size = 1024;
  server_conf_.use_async_weight = false;
  client_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_NAIVE;
  client_conf_.num_experts = 2;
  client_conf_.experts[0] = EVICT_PRIO_LRU;
  client_conf_.experts[1] = EVICT_PRIO_LFU;
  client_conf_.learning_rate = 0.1;
  client_conf_.history_size = 1024;
  client_conf_.use_async_weight = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
}

TEST_F(ClientTest, adaptive_naive_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE_NAIVE);
  server_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_NAIVE;
  server_conf_.num_experts = 2;
  server_conf_.experts[0] = EVICT_PRIO_LRU;
  server_conf_.experts[1] = EVICT_PRIO_LFU;
  server_conf_.learning_rate = 0.1;
  server_conf_.history_size = 1024;
  server_conf_.use_async_weight = false;
  client_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE_NAIVE;
  client_conf_.num_experts = 2;
  client_conf_.experts[0] = EVICT_PRIO_LRU;
  client_conf_.experts[1] = EVICT_PRIO_LFU;
  client_conf_.learning_rate = 0.1;
  client_conf_.history_size = 1024;
  client_conf_.use_async_weight = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, adaptive_kv_fuzz) {
  int ret = 0;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE;
  server_conf_.num_experts = 2;
  server_conf_.experts[0] = EVICT_PRIO_LRU;
  server_conf_.experts[1] = EVICT_PRIO_LFU;
  server_conf_.learning_rate = 0.1;
  server_conf_.history_size = 1024;
  server_conf_.use_async_weight = true;
  client_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE;
  client_conf_.num_experts = 2;
  client_conf_.experts[0] = EVICT_PRIO_LRU;
  client_conf_.experts[1] = EVICT_PRIO_LFU;
  client_conf_.learning_rate = 0.1;
  client_conf_.history_size = 1024;
  client_conf_.use_async_weight = true;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
}

TEST_F(ClientTest, adaptive_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE_NAIVE);
  server_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE;
  server_conf_.num_experts = 2;
  server_conf_.experts[0] = EVICT_PRIO_LRU;
  server_conf_.experts[1] = EVICT_PRIO_LFU;
  server_conf_.learning_rate = 0.1;
  server_conf_.history_size = 1024;
  server_conf_.use_async_weight = true;
  client_conf_.eviction_type = EVICT_SAMPLE_ADAPTIVE;
  client_conf_.num_experts = 2;
  client_conf_.experts[0] = EVICT_PRIO_LRU;
  client_conf_.experts[1] = EVICT_PRIO_LFU;
  client_conf_.learning_rate = 0.1;
  client_conf_.history_size = 1024;
  client_conf_.use_async_weight = true;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, sample_lru_test_combining) {
  int ret;
  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  server_conf_.use_freq_cache = true;
  server_conf_.freq_cache_size = 10485760;
  client_conf_.eviction_type = EVICT_SAMPLE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.use_freq_cache = true;
  client_conf_.freq_cache_size = 10485760;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
  dump_counters();
  printd(L_INFO, "==================");

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  server_conf_.use_freq_cache = false;
  client_conf_.eviction_type = EVICT_SAMPLE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.use_freq_cache = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
  dump_counters();
}

TEST_F(ClientTest, sample_lfu_kv_fuzz) {
  int ret;
  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_SAMPLE;
  client_conf_.eviction_type = EVICT_SAMPLE;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_fuzz();
}

TEST_F(ClientTest, sample_lfu_kv_evict) {
  int ret;
  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_SAMPLE);
  server_conf_.eviction_type = EVICT_SAMPLE;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.eviction_type = EVICT_SAMPLE;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, cliquemap_lfu_kv_evict) {
  int ret;
  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_CLIQUEMAP);
  server_conf_.eviction_type = EVICT_CLIQUEMAP;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  server_conf_.is_server_precise = false;
  client_conf_.eviction_type = EVICT_CLIQUEMAP;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.is_server_precise = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, cliquemap_precise_lru_kv_fuzz) {
  srand((uint32_t)time(NULL));
  int ret;

  reconfig_cliquemap_test(false);
  server_conf_.is_server_precise = true;
  client_conf_.is_server_precise = true;
  kv_fuzz();
}

TEST_F(ClientTest, cliquemap_precise_lfu_kv_evict) {
  int ret;
  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_CLIQUEMAP);
  server_conf_.eviction_type = EVICT_CLIQUEMAP;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  server_conf_.is_server_precise = true;
  client_conf_.eviction_type = EVICT_CLIQUEMAP;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.is_server_precise = true;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, cliquemap_precise_lru_kv_evict) {
  int ret;

  delete client_;
  stop_server();
  delete server_;

  reconfig_evict_memory(EVICT_CLIQUEMAP);
  server_conf_.eviction_type = EVICT_CLIQUEMAP;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  server_conf_.is_server_precise = true;
  client_conf_.eviction_type = EVICT_CLIQUEMAP;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.is_server_precise = true;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  kv_evict_get_set(1000);
  dump_counters();
}

TEST_F(ClientTest, precise_2s_load_lru_kv_fuzz) {
  srand((uint32_t)time(NULL));
  int ret;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_PRECISE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.eviction_type = EVICT_PRECISE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  std::map<std::string, std::string> gt_table;
  for (int i = 0; i < 10; i++) {
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    ret = client_->kv_p_set(key_buf_, strlen(key_buf_) + 1, val_buf_,
                            strlen(val_buf_) + 1);
    ASSERT_TRUE(ret == 0);
  }

  for (int i = 0; i < 10; i++) {
    uint32_t val_size;
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    ret = client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &val_size);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(tmp_buf_, val_buf_) == 0);
    ASSERT_TRUE(val_size == strlen(val_buf_) + 1);
  }
}

TEST_F(ClientTest, precise_2s_load_lfu_kv_fuzz) {
  srand((uint32_t)time(NULL));
  int ret;

  delete client_;
  stop_server();
  delete server_;

  server_conf_.eviction_type = EVICT_PRECISE;
  server_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.eviction_type = EVICT_PRECISE;
  client_conf_.eviction_priority = EVICT_PRIO_LFU;
  client_conf_.use_freq_cache = false;

  server_ = new Server(&server_conf_);
  start_server();
  client_ = new DMCClient(&client_conf_);

  std::map<std::string, std::string> gt_table;
  for (int i = 0; i < 10; i++) {
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    ret = client_->kv_p_set(key_buf_, strlen(key_buf_) + 1, val_buf_,
                            strlen(val_buf_) + 1);
    ASSERT_TRUE(ret == 0);
  }

  for (int i = 0; i < 10; i++) {
    uint32_t val_size;
    sprintf(key_buf_, "Key-%d", i);
    sprintf(val_buf_, "Val-%d", i);
    ret = client_->kv_get(key_buf_, strlen(key_buf_) + 1, tmp_buf_, &val_size);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(tmp_buf_, val_buf_) == 0);
    ASSERT_TRUE(val_size == strlen(val_buf_) + 1);
  }
}