#include <gtest/gtest.h>
#include <sys/time.h>

#include "dmc_table.h"
#include "dmc_utils.h"

#include <unordered_map>

// TEST(test_kv_utils, config) {
//     const char * config_file_name = "./test_conf.json";
//     DMCConfig conf;
//     int ret = load_config(config_file_name, &conf);
//     ASSERT_TRUE(ret == 0);

//     ASSERT_TRUE(conf.role == SERVER);
//     ASSERT_TRUE(conf.conn_type == IB);
//     ASSERT_TRUE(conf.server_id == 0);
//     ASSERT_TRUE(conf.udp_port == 2333);
//     ASSERT_TRUE(conf.memory_num == 2);
//     ASSERT_EQ(strcmp(conf.memory_ip_list[0], "10.10.10.1"), 0);
//     ASSERT_EQ(strcmp(conf.memory_ip_list[1], "10.10.10.2"), 0);

//     ASSERT_TRUE(conf.hash_type == HASH_DEFAULT);

//     ASSERT_TRUE(conf.eviction_type == EVICT_SAMPLE);
//     ASSERT_TRUE(conf.num_samples == 5);

//     ASSERT_TRUE(conf.ib_dev_id == 0);
//     ASSERT_TRUE(conf.ib_port_id == 1);
//     ASSERT_TRUE(conf.ib_gid_idx == -1);

//     ASSERT_TRUE(conf.server_base_addr == 0x10000000);
//     ASSERT_TRUE(conf.server_data_len == 2147483648);
//     ASSERT_TRUE(conf.segment_size == 64 * 1024 * 1024);
//     ASSERT_TRUE(conf.block_size == 256);

//     ASSERT_TRUE(conf.client_local_size == 64 * 1024 * 1024);

//     ASSERT_TRUE(conf.core_id == 0);
//     ASSERT_TRUE(conf.num_cores == 1);

//     ASSERT_TRUE(conf.testing == true);
// }

TEST(test_kv_utils, test_table) {
  printd(L_INFO, "slot size:  %ld B", sizeof(Slot));
  printd(L_INFO, "table size: %ld MB", sizeof(Table) / 1024 / 1024);
  printd(L_INFO, "slot_atomic_off:       %ld", SLOT_ATOMIC_OFF);
  printd(L_INFO, "slot_meta_off:         %ld", SLOT_META_OFF);
  printd(L_INFO, "slotm_info_hash_off:   %ld", SLOTM_INFO_HASH_OFF);
  printd(L_INFO, "slotm_info_acc_ts_off: %ld", SLOTM_INFO_ACC_TS_OFF);
  printd(L_INFO, "slotm_info_ins_ts_off: %ld", SLOTM_INFO_INS_TS_OFF);
  printd(L_INFO, "slotm_info_freq_off:   %ld", SLOTM_INFO_FREQ_OFF);
}

TEST(test_kv_utils, test_ts) {
  struct timeval cur;
  gettimeofday(&cur, NULL);
  uint64_t ts = (uint64_t)(cur.tv_sec) * 1e6 + cur.tv_usec;
  printf("0x%lx\n", ts);
  printf("0x%lx\n", ts >> 4);
  ASSERT_TRUE(((ts >> 4) >> 48) == 0);
}

TEST(test_kv_utils, test_combined_ts) {
  struct timeval cur;
  gettimeofday(&cur, NULL);
  uint64_t ts = ((uint64_t)(cur.tv_sec) * 1000000 + cur.tv_usec) >> 4;
  uint8_t buf[8];
  uint8_t n_buf[8];
  *(uint16_t*)&buf[7] = 3;
  *(uint16_t*)&n_buf[7] = 4;
  memcpy(buf, &ts, 6);
  memcpy(n_buf, &ts, 6);
  printf("ts: 0x%lx\n", ts);
  printf("buf: 0x%lx nbuf: 0x%lx\n", *(uint64_t*)buf, *(uint64_t*)n_buf);
  ASSERT_TRUE(*(uint64_t*)buf < *(uint64_t*)n_buf);
}

TEST(hash_test, test_table_conflict) {
  srand(time(NULL));
  std::unordered_map<uint64_t, int> bucket_cnt;
  // DMCHash * hash = new DefaultHash();
  uint64_t num_buckets = 256 * 1024;
  int assoc_num = 16;
  uint64_t num_inserted = 0;
  for (int i = 1; i <= 64; i++) {
    for (int j = 0; j < 100000; j++) {
      char key_buf[128];
      sprintf(key_buf, "%d-key-%d", i, j);
      // uint64_t hash_val = hash->hash_func1(key_buf, strlen(key_buf) + 1);
      uint64_t hash_val = rand();
      uint64_t bucket_id = hash_val % num_buckets;
      if (bucket_cnt[bucket_id] < 16) {
        num_inserted++;
      } else {
        break;
      }
      bucket_cnt[bucket_id]++;
    }
  }
  printf("%f\n", (double)num_inserted / (256 * 1024 * 16));
}