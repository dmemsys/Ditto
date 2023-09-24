#ifndef _DMC_EXP_RUN_CLIENT_H_
#define _DMC_EXP_RUN_CLIENT_H_

#include "client.h"
#include "init.h"

#include <pthread.h>

#define TICK_US (500000)
#define TICK_PER_SEC (1000000 / 500000)

typedef struct _ClientArgs {
#ifdef USE_FIBER
  DMCConfig* config;
#endif
  DMCClient* client;
  uint32_t core;
  uint32_t cid;
  uint32_t all_client_num;

  // workload args
  char workload_name[128];
  uint32_t num_load_ops;
  bool validate;

  // return values
  uint32_t num_success_ops;
  uint32_t num_failed_ops;

  // micro benchmark lat
  int lat_list_len;

  // macro benchmark
  uint32_t num_ops;
  uint32_t run_tims_s;

  // miss rate test
  bool use_warmup;

  // mix miss
  uint32_t n_lru_clients;
  uint32_t n_lfu_clients;

  // controller ip
  char controller_ip[64];

  // ycsb load client
  bool is_load_only;

#ifdef USE_FIBER
  int tid;
  int num_l_clients;
  DMCClient** client_list;
#endif
} ClientArgs;

void* client_micro_main(void* client_main_args);
void* client_main(void* client_main_args);

void run_client(const InitArgs* args);

#endif