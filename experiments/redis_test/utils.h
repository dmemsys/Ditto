#ifndef _UTILS_H_
#define _UTILS_H_

#include <stdint.h>

#define __OUT

enum KVOP { SET, GET };

typedef struct _Workload {
  void* key_buf;
  void* val_buf;
  uint32_t* key_size_list;
  uint32_t* val_size_list;
  uint8_t* op_list;
  uint32_t num_ops;
} Workload;

int load_workload_ycsb(char* workload_name,
                       int num_load_ops,
                       uint32_t server_id,
                       uint32_t all_client_num,
                       __OUT Workload* load_wl,
                       __OUT Workload* trans_wl);

typedef struct _ClientArgs {
  uint32_t cid;
  uint32_t all_client_num;

  uint32_t core;

  char controller_ip[256];
  char wl_name[256];
  char redis_ip[256];
  uint32_t run_times_s;
} ClientArgs;

static inline uint64_t diff_ts_us(const struct timeval* et,
                                  const struct timeval* st) {
  return (et->tv_sec - st->tv_sec) * 1000000 + (et->tv_usec - st->tv_usec);
}

#endif