#ifndef _DMC_EXP_INIT_H_
#define _DMC_EXP_INIT_H_

#include <stdint.h>

enum ElasticType { ELA_NON = 0, ELA_CPU, ELA_MEM };

typedef struct _InitArgs {
  uint8_t role;
  uint32_t server_id;  // server_id / the first client id
  char config_fname[128];
  uint32_t all_client_num;

  // client config
  int num_client_threads;   // number of clients to start
  int num_load_ops;         // number of operations to execute
  int run_time;             // time to iteratively execute
  char workload_name[128];  // workload filename
  bool validate;            // enable validation

  // mix workload
  int n_lru_client;
  int n_lfu_client;

  // warmup
  bool use_warmup;

  // memcached controller
  char memcached_ip[128];

  // elasticity testing
  uint8_t elastic;
} InitArgs;

#endif