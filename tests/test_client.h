#ifndef _DMC_TEST_CLIENT_H_
#define _DMC_TEST_CLIENT_H_

#include <gtest/gtest.h>
#include "client.h"
#include "dmc_test.h"
#include "server.h"

class ClientTest : public DMCTest {
 protected:
  void SetUp() override;
  void TearDown() override;

 private:
  pthread_t server_tid_;
  ServerMainArgs server_main_args_;

 public:
  Server* server_;
  DMCClient* client_;

  // for kv operations
  char key_buf_[128];
  char val_buf_[128];
  char tmp_buf_[128];
  uint32_t tmp_len_;

  void start_server();
  void stop_server();
  void dump_counters();

  void reconfig_cliquemap_test(bool use_dumb_hash);
  void reconfig_hlru_test(bool use_dumb_hash);
  void reconfig_hlfu_test(bool use_dumb_hash);
  void reconfig_plru_test(bool use_dumb_hash);

  void reconfig_evict_memory(int evict_type);

  void kv_fuzz();
  void kv_evict_get_set(int num_iter);
};

#endif