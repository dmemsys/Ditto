#ifndef _DMC_SERVER_H_
#define _DMC_SERVER_H_

#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <list>
#include <map>
#include <set>
#include <vector>

#include "client_mm.h"
#include "cms.h"
#include "dmc_table.h"
#include "dmc_utils.h"
#include "fifo_history.h"
#include "lw_history.h"
#include "nm.h"
#include "priority.h"
#include "rlist.h"
#include "server_mm.h"
#include "third_party/atomicops.h"
#include "third_party/readerwriterqueue.h"
#include "third_party/spinlock.h"

class Server;

typedef struct _ServerMainArgs {
  Server* server;
  int core_id;
  int worker_id;
} ServerMainArgs;

class Server {
  uint32_t server_id_;
  uint32_t block_size_;

  uint8_t eviction_type_;
  uint8_t eviction_prio_;
  uint32_t num_samples_;
  bool is_evict_precise_;

  // use lock to protect the prio_slot_addr_map_;
  // sharded prio_slot_addr_map_lock_;
#ifdef USE_SHARD_PQUEUE
  spinlock shard_prio_slot_addr_map_lock_[NUM_PQUEUE_SHARDS];
  std::map<double, std::set<uint64_t>>
      shard_prio_slot_addr_map_[NUM_PQUEUE_SHARDS];
#else
  spinlock prio_slot_addr_map_lock_;
  std::map<double, std::set<uint64_t>> prio_slot_addr_map_;
#endif

  uint8_t need_stop_;

  UDPNetworkManager* nm_;
  spinlock mm_lock_;
  ServerMM* mm_;

  // global ts
  uint64_t sys_start_ts_;

  // for testing
  ClientMM** c_mm_;
  DMCHash* hash_;
  Priority* priority_;

  // for active server
  void* send_msg_buffer_;
  void* recv_msg_buffer_;
  struct ibv_mr* send_msg_buffer_mr_;
  struct ibv_mr* recv_msg_buffer_mr_;
  struct ibv_recv_wr rr_list_[MSG_BUF_NUM];
  struct ibv_send_wr sr_list_[MSG_BUF_NUM];
  struct ibv_sge rr_sge_list_[MSG_BUF_NUM];
  struct ibv_sge sr_sge_list_[MSG_BUF_NUM];
  int send_counter_;  // for periodically generate a send wc

  uint32_t num_workers_;
  pthread_t* worker_tid_;
  ServerMainArgs* worker_args_;

  // for evict precise
  RList* prio_list_;

  // for adaptive eviction
  int num_experts_;
  uint32_t history_size_;
  float* expert_weights_;
  LWHistory* lw_history_;
  FIFOHistory* naive_history_;

  bool testing_;
  bool elastic_mem_;

 public:
  // counters
  uint64_t num_evict_dequeue_;
  uint64_t num_evict_;
  std::vector<std::vector<float>> weight_vec_;

 private:
  int init_hash_table();
  int server_on_connect(const UDPMsg* request,
                        struct sockaddr_in* src_addr,
                        socklen_t src_addr_len);
  int server_on_alloc(const UDPMsg* request,
                      struct sockaddr_in* src_addr,
                      socklen_t src_addr_len);
  int server_on_get_ts(const UDPMsg* request,
                       struct sockaddr_in* src_addr,
                       socklen_t src_addr_len);
  int server_on_recv_msg_priority(const struct ibv_wc* wc);
  int server_on_recv_msg_set(uint32_t worker_id, const struct ibv_wc* wc);
  int server_on_recv_msg_get(const struct ibv_wc* wc);
  int server_on_recv_msg_merge(const struct ibv_wc* wc);
  int server_on_recv_msg_alloc(const struct ibv_wc* wc);
  int server_on_recv_msg_pset(
      uint32_t worker_id,
      const struct ibv_wc* wc);  // for load precise faster

  void init_counters();

  // for kv operations
  Slot* kv_search_slot(void* key, uint32_t key_size);
  int local_alloc_segment(uint32_t worker_id);
  int evict_local_cmm(uint32_t worker_id);

  // for evict
  uint64_t evict();
  uint64_t evict_precise();
  uint64_t evict_sample();

  int server_reply_to_sid(const struct ibv_wc* wc,
                          uint16_t sid,
                          void* ret_val,
                          uint32_t val_size,
                          uint8_t ibmsg_type);

  // for update priority
  void update_priority(Slot* slot);
  void update_priority_precise(Slot* slot);
  void update_priority_sample(Slot* slot);

  // for merge priority
  void merge_priority(Slot* slot, uint64_t ts);
  void merge_priority_precise(Slot* slot, uint64_t ts);
  void merge_priority_sample(Slot* slot, uint64_t ts);

  // update access information
  void update_acc_info(Slot* slot, uint32_t info_update_mask);
  void merge_acc_info(Slot* slot, uint32_t info_update_mask, uint64_t ts);

  // for precise
  void adjust_prio_slot_map(Slot* slot,
                            double old_priority,
                            double new_priority);

  // for load precise
  uint32_t get_slot_id(Slot* slot);
  void p_update_priority(Slot* slot);
  int p_set(uint32_t worker_id,
            void* key,
            uint32_t key_size,
            void* val,
            uint32_t val_size);

 public:
  Server(const DMCConfig* conf);
  ~Server();

  void* thread_main();  // listen for connection and allocation requests
  void* worker_main(uint32_t worker_id);  // polling for 2-sided rdma rpcs

  void stop();

  // for testing
  int get(void* key,
          uint32_t key_size,
          __OUT void* val,
          __OUT uint32_t* val_size);
  int set(uint32_t worker_id,
          void* key,
          uint32_t key_size,
          void* val,
          uint32_t val_size);

  void get_adaptive_weights(std::vector<float>& adaptive_weights);
  void get_adaptive_weight_vec(std::vector<std::vector<float>>& weight_vec);

  inline void scale_memory(int reserved_idx) {
    assert(elastic_mem_ == true);
    mm_->scale_memory(reserved_idx);
  }
};

void* server_main(void* server_main_args);

#endif