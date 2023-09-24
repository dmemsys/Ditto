#ifndef _DMC_WORKLOAD_H_
#define _DMC_WORKLOAD_H_

#include <dmc_utils.h>
#include <stdint.h>

typedef struct _DMCWorkload {
  void* key_buf;
  void* val_buf;
  uint32_t* key_size_list;
  uint32_t* val_size_list;
  uint8_t* op_list;
  uint32_t num_ops;
} DMCWorkload;

int load_workload(char* workload_name,
                  int num_load_ops,
                  uint32_t server_id,
                  uint32_t all_client_num,
                  __OUT DMCWorkload* wl);

int load_workload_ycsb(char* workload_name,
                       int num_load_ops,
                       uint32_t server_id,
                       uint32_t all_client_num,
                       uint8_t evict_type,
                       __OUT DMCWorkload* load_wl,
                       __OUT DMCWorkload* trans_wl);

int load_workload_hit_rate(char* workload_name,
                           int num_load_ops,
                           uint32_t server_id,
                           uint32_t all_client_num,
                           uint32_t n_lru_client,
                           uint32_t n_lfu_client,
                           __OUT DMCWorkload* wl);

int load_mix_all(__OUT DMCWorkload* lru_wl, __OUT DMCWorkload* lfu_wl);

void free_workload(DMCWorkload* wl);

#endif