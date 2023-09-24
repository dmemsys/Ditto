#ifndef _DMC_UTILS_H_
#define _DMC_UTILS_H_

#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>

#define __OUT
#define MSG_BUF_SIZE (2 * 1024ULL)
#define MSG_BUF_NUM (1024)
#define MAX_NUM_EXPERTS (5)
#define ADAPTIVE_NUM_LOCAL_REWARD (100)
#define LOCAL_FREQ_THRESH (10)
#define MAX_NUM_FAA (10)
#define HIST_MASK (0xFFFFFFFFFFFFULL)
#define ADAPTIVE_TMP_SLOT (0xFFFFFFFFFFFFFFFFULL)

#define ROUNDUP(x, n) ((x + (n - 1)) & (~(n - 1)))

// #define ALLOC_SEGMENT_UDP
#define YCSB_LOAD_NUM 16
// #define USE_PENALTY
// #define MULTI_POLICY
#define CLIQUEMAP_SYNC_INTERVAL_US (2000)  // 5ms
#define NUM_CORES (72)
// #define ELA_MEM_TPT

// #define USE_REWARDS
// #define DEFER_EVICT_HIST

// use sharded pqueue
// #define USE_SHARD_PQUEUE
#ifdef USE_SHARD_PQUEUE
#define NUM_PQUEUE_SHARDS (32)  // for shard-lru
// #define NUM_PQUEUE_SHARDS (128) // for clieuqmap
#endif
// #define USE_LOCK_BACKOFF

// #define USE_FIBER
#ifdef USE_FIBER
#include <boost/fiber/all.hpp>
#define NUM_FB_PER_THREAD (2)
#endif

#define DEFER_ALLOC

// const int reserved_segment_list[] = {
//     30464, 30240, 29920, 27400, 24512, 18336, 12224, 6112, 0
// };

const int reserved_segment_list[] = {24512, 18336, 12224, 6112, 0};

enum Role {
  SERVER,
  CLIENT,
};

enum ConnType {
  IB,
  ROCE,
};

enum UDPMsgType {
  UDPMSG_REQ_CONNECT,
  UDPMSG_REP_CONNECT,
  UDPMSG_REQ_ALLOC,
  UDPMSG_REP_ALLOC,
  UDPMSG_REQ_TS,
  UDPMSG_REP_TS,
};

enum IBMsgType {
  IBMSG_REQ_SET,
  IBMSG_REP_SET,
  IBMSG_REQ_GET,
  IBMSG_REP_GET,
  IBMSG_REQ_PRIORITY,
  IBMSG_REP_PRIORITY,
  IBMSG_REQ_MERGE,
  IBMSG_REP_MERGE,
  IBMSG_REQ_ALLOC,
  IBMSG_REP_ALLOC,
  IBMSG_REQ_PSET,  // for load precise
  IBMSG_REP_PSET
};

enum HashType {
  HASH_DEFAULT,
  HASH_DUMB,
};

enum EvictionType {
  EVICT_NON,  // random
  EVICT_PRECISE,
  EVICT_CLIQUEMAP,
  EVICT_SAMPLE,
  EVICT_SAMPLE_NAIVE,
  EVICT_SAMPLE_ADAPTIVE,
  EVICT_SAMPLE_ADAPTIVE_NAIVE,
  EVICT_SAMPLE_ADAPTIVE_HEAVY,
};

enum EvictionPrio {
  EVICT_PRIO_NON,  // random
  EVICT_PRIO_LRU,
  EVICT_PRIO_LFU,
  EVICT_PRIO_GDSF,
  EVICT_PRIO_GDS,
  EVICT_PRIO_LIRS,
  EVICT_PRIO_LRFU,
  EVICT_PRIO_FIFO,
  EVICT_PRIO_LFUDA,
  EVICT_PRIO_LRUK,
  EVICT_PRIO_SIZE,
  EVICT_PRIO_MRU,
  EVICT_PRIO_HYPERBOLIC
};

typedef struct _DMCConfig {
  // global server info
  uint8_t role;
  uint8_t conn_type;
  uint32_t server_id;
  uint16_t udp_port;
  uint32_t memory_num;
  char memory_ip_list[16][16];

  // ib device info
  uint32_t ib_dev_id;
  uint32_t ib_port_id;
  int32_t ib_gid_idx;

  // hash info
  uint8_t hash_type;

  // eviction info
  uint8_t eviction_type;
  uint8_t eviction_priority;
  uint32_t num_samples;

  // memory node
  uint64_t server_base_addr;
  uint64_t server_data_len;
  uint64_t segment_size;
  uint64_t block_size;

  // client local
  uint32_t client_local_size;

  uint32_t core_id;
  uint32_t num_cores;

  // for adaptive caching
  int num_experts;
  int experts[MAX_NUM_EXPERTS];
  float learning_rate;
  uint32_t history_size;
  bool use_async_weight;

  // freq cache
  bool use_freq_cache;
  uint32_t freq_cache_size;

  // precise cliquemap
  bool is_server_precise;

  // testing
  bool testing;

  // elastic memory on server
  bool elastic_mem;

  // more cliquemap threads
  uint32_t num_server_threads;
} DMCConfig;

typedef struct _MrInfo {
  uint64_t addr;
  uint32_t rkey;
} MrInfo;

typedef struct _QPInfo {
  uint32_t qp_num;
  uint16_t lid;
  uint8_t port_num;
  uint8_t gid[16];
  uint8_t gid_idx;
} QPInfo;

typedef struct _ConnInfo {
  // qp info
  QPInfo qp_info;
  // initial mr info
  MrInfo mr_info;
} ConnInfo;

typedef struct _UDPMsg {
  uint16_t type;
  uint16_t id;
  union {
    ConnInfo conn_info;
    MrInfo mr_info;
    uint64_t sys_start_ts;
  } body;
} UDPMsg;

static inline uint64_t new_ts() {
  struct timeval cur;
  gettimeofday(&cur, NULL);
  return cur.tv_sec * 1000000 + cur.tv_usec;
}

static inline uint64_t diff_ts_us(const struct timeval* et,
                                  const struct timeval* st) {
  return (et->tv_sec - st->tv_sec) * 1000000 + (et->tv_usec - st->tv_usec);
}

static inline bool is_evict_adaptive(uint8_t eviction_type) {
  return eviction_type == EVICT_SAMPLE_ADAPTIVE ||
         eviction_type == EVICT_SAMPLE_ADAPTIVE_NAIVE ||
         eviction_type == EVICT_SAMPLE_ADAPTIVE_HEAVY;
}

int load_config(const char* fname, __OUT DMCConfig* config);
// for udpmsg
void serialize_qp_info(__OUT QPInfo* qp_info);
void deserialize_qp_info(__OUT QPInfo* qp_info);
void serialize_mr_info(__OUT MrInfo* mr_info);
void deserialize_mr_info(__OUT MrInfo* mr_info);
void serialize_conn_info(__OUT ConnInfo* conn_info);
void deserialize_conn_info(__OUT ConnInfo* conn_info);
void serialize_udpmsg(__OUT UDPMsg* msg);
void deserialize_udpmsg(__OUT UDPMsg* msg);

int stick_this_thread_to_core(int core_id);

void set_sys_start_ts(uint64_t ts);
uint32_t new_ts32();

#endif