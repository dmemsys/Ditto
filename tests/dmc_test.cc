#include "dmc_test.h"

void DMCTest::setup_server_conf() {
  server_conf_.role = SERVER;
  server_conf_.conn_type = IB;
  server_conf_.server_id = 0;
  server_conf_.udp_port = 2333;
  server_conf_.memory_num = 1;
  strcpy(server_conf_.memory_ip_list[0], "127.0.0.1");

  server_conf_.ib_dev_id = 0;
  server_conf_.ib_port_id = 1;
  server_conf_.ib_gid_idx = 0;

  server_conf_.hash_type = HASH_DEFAULT;

  server_conf_.eviction_type = EVICT_SAMPLE;
  server_conf_.eviction_priority = EVICT_PRIO_LRU;
  server_conf_.num_samples = 5;

  server_conf_.server_base_addr = 0x10000000;
  server_conf_.server_data_len = 4ll * GB;
  server_conf_.segment_size = 64ll * MB;
  server_conf_.block_size = 256;

  server_conf_.client_local_size = 64ll * MB;
  server_conf_.testing = true;
  server_conf_.num_server_threads = 1;
}

void DMCTest::setup_client_conf() {
  client_conf_.role = CLIENT;
  client_conf_.conn_type = IB;
  client_conf_.server_id = 1;
  client_conf_.udp_port = 2333;
  client_conf_.memory_num = 1;
  strcpy(client_conf_.memory_ip_list[0], "127.0.0.1");

  client_conf_.ib_dev_id = 0;
  client_conf_.ib_port_id = 1;
  client_conf_.ib_gid_idx = 0;

  client_conf_.hash_type = HASH_DEFAULT;

  client_conf_.eviction_type = EVICT_SAMPLE;
  client_conf_.eviction_priority = EVICT_PRIO_LRU;
  client_conf_.num_samples = 5;

  client_conf_.server_base_addr = 0x10000000;
  client_conf_.server_data_len = 4ll * GB;
  client_conf_.segment_size = 64ll * MB;
  client_conf_.block_size = 256;

  client_conf_.client_local_size = 1ll * MB;
  client_conf_.testing = true;
}
