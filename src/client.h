#ifndef _DMC_CLIENT_H_
#define _DMC_CLIENT_H_

#include <list>
#include <map>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include <infiniband/verbs.h>

#include "client_mm.h"
#include "cms.h"
#include "dmc_table.h"
#include "dmc_utils.h"
#include "fifo_history.h"
#include "freq_cache.h"
#include "lw_history.h"
#include "nm.h"
#include "priority.h"
#include "rlist.h"

#define MAX_NUM_SAMPLES 64

enum KVOP { SET, GET };

enum DMC_RET_CODE { DMC_SUCCESS = 0, DMC_SET_RETURN, DMC_SET_RETRY };

enum CACHE_OP {
  CACHE_ERROROP,
  CACHE_LOCALHOT,
  CACHE_X2HOT,
  CACHE_HOT2COLD,
  CACHE_COLD2COLD,
};

typedef struct _KVOpsCtx {
  uint8_t op;
  void* key;
  void* val;
  uint32_t key_size;
  uint32_t val_size;
  uint32_t kv_size;

  // hash value
  uint64_t key_hash;
  uint8_t fp;

  // for reading index
  uint64_t bucket_laddr;
  uint64_t init_bucket_raddr;
  uint16_t init_bucket_sid;

  // for writing data
  RemoteBlock kv_remote_block;
  uint64_t write_buf_laddr;
  uint32_t write_buf_size;

  // for arbitrary operations
  uint64_t op_laddr;
  uint32_t op_size;

  // for reading data
  uint64_t read_buf_laddr;
  uint32_t num_fp_match;
  uint8_t remote_data_slot_id[HASH_BUCKET_ASSOC_NUM];
  uint32_t num_free_slot;
  uint8_t remote_free_slot_id[HASH_BUCKET_ASSOC_NUM];
  uint32_t num_amortized_evict;
  uint8_t amortized_evict_id[HASH_BUCKET_ASSOC_NUM];
  bool key_found;

  // for modify index
  uint64_t target_slot_laddr;
  uint64_t target_slot_raddr;
  uint16_t target_slot_sid;
  uint64_t target_block_laddr;
  uint64_t target_block_raddr;
  Slot new_slot;

  // for adaptive
  bool hist_match;
  uint64_t hist_match_head;
  uint8_t hist_expert_bmap;
  bool has_read_hist_head;
  uint64_t read_hist_head;
  bool has_faa_hist_head;
  uint64_t faa_hist_head;

  // for naive adaptive
  HistEntry histEntry;

  // for indicating return value
  uint8_t ret;
} KVOpsCtx;

class DMCClient {
  uint16_t my_sid_;
  uint16_t num_servers_;
  uint64_t server_base_addr_;

  uint32_t block_size_;

  uint32_t local_buf_size_;
  void* local_buf_;
  struct ibv_mr* local_buf_mr_;
  void* transient_buf_;
  struct ibv_mr* transient_buf_mr_;

  UDPNetworkManager* nm_;
  ClientMM* mm_;
  DMCHash* hash_;

  Priority* priority_;
  uint8_t eviction_type_;
  uint8_t priority_type_;
  uint32_t num_samples_;

  std::map<uint16_t, uint32_t> server_rkey_map_;

  bool server_oom_;

  // local counter write combining counter
  bool use_freq_cache_;
  uint64_t freq_cache_size_;
  FreqCache* freq_cache_;

  // local timestamp buffer & recv reply buffer for cliquemap
  void* ts_buf_;
  struct ibv_mr* ts_buf_mr_;
  uint32_t ts_buf_off_;
  void* ts_rep_buf_;
  struct ibv_mr* ts_rep_buf_mr_;

  // timestamp
  uint64_t sys_start_ts_;

  // priority list
  RList* prio_list_;
  void* list_op_buf_;
  struct ibv_mr* list_op_buf_mr_;

  // adaptive sample
  //   adaptive experts
  int num_experts_;
  Priority* experts_[MAX_NUM_EXPERTS];

  //   adaptive history
  uint32_t history_size_;
  LWHistory* lw_history_;
  //   naive adaptive history
  void* history_op_buf_;
  struct ibv_mr* history_op_buf_mr_;
  FIFOHistory* naive_history_;

  //   adaptive weights
  uint64_t weights_raddr_;
  float learning_rate_;
  float l_weights_[MAX_NUM_EXPERTS];
  void* weights_sync_buf_;
  uint32_t weights_sync_size;
  float* r_weights_;
  struct ibv_mr* weights_sync_buf_mr_;
  bool use_async_weight_;

  //   adaptive rewards
  float base_reward_;
  uint32_t reward_sync_size_;
  void* reward_sync_buf_;
  float* reward_buf_;
  struct ibv_mr* reward_sync_buf_mr_;
  int local_reward_cntr_;

  // profilers
  float evict_1s_lat_;

  // cliquemap sync
  struct timeval last_sync_time_;

  // for debug
  FILE* log_f_;

 public:
  // counters
  uint64_t num_evict_;
  uint64_t num_success_evict_;
  uint64_t num_bucket_evict_;
  uint64_t num_success_bucket_evict_;
  uint64_t num_evict_read_bucket_;
  uint64_t num_set_retry_;
  uint64_t num_rdma_send_;
  uint64_t num_rdma_recv_;
  uint64_t num_rdma_read_;
  uint64_t num_rdma_write_;
  uint64_t num_rdma_cas_;
  uint64_t num_rdma_faa_;
  uint64_t num_udp_req_;
  uint64_t num_read_hist_head_;
  uint64_t num_hist_access_;
  uint64_t num_hist_overwite_;
  uint64_t num_ada_evict_inconsistent_;
  uint64_t num_ada_access_inconsistent_;
  uint64_t num_hist_match_;
  uint64_t num_adaptive_weight_sync_;
  uint64_t num_adaptive_adjust_weights_;
  uint64_t num_bucket_evict_history_;
  uint64_t num_cliquemap_sync_;
#ifdef USE_REWARDS
  uint64_t num_hit_rewards_;
  std::vector<uint32_t> expert_reward_cnt_;
#endif
  std::map<uint32_t, uint32_t> evict_bucket_cnt_;
  std::vector<std::vector<float>> weight_vec_;
  std::vector<uint32_t> expert_evict_cnt_;

 private:
  int alloc_segment(KVOpsCtx* ctx);
  int connect_all_rc_qp();
  int init_eviction(const DMCConfig* conf);
  uint64_t get_sys_start_ts();

  // common op
  void create_op_ctx(__OUT KVOpsCtx* ctx,
                     void* key,
                     uint32_t key_size,
                     void* val,
                     uint32_t val_size,
                     uint8_t op_type);
  void get_init_bucket_raddr(uint64_t hash,
                             __OUT uint64_t* r_addr,
                             __OUT uint16_t* server);
  void get_slot_raddr(const KVOpsCtx* ctx,
                      uint64_t slot_laddr,
                      __OUT uint64_t* r_addr,
                      __OUT uint16_t* server);

  void match_fp_and_find_empty(KVOpsCtx* ctx);
  void read_and_find_kv(
      KVOpsCtx* ctx);  // iteratively read kv using rdma read and compare key
  void find_empty(KVOpsCtx* ctx);  // set target_slot to be an empty slot

  // kv_set
  void kv_set_alloc_rblock(KVOpsCtx* ctx);
  void kv_set_read_index_write_kv(KVOpsCtx* ctx);
  void kv_set_update_index(KVOpsCtx* ctx);
  void kv_set_delete_duplicate(KVOpsCtx* ctx);

  // kv_get
  void kv_get_read_index(KVOpsCtx* ctx);
  void kv_get_copy_value(KVOpsCtx* ctx,
                         __OUT void* val,
                         __OUT uint32_t* val_len);

  // remote list
  void remote_list_lock(KVOpsCtx* ctx);    // used in evict precise
  void remote_list_unlock(KVOpsCtx* ctx);  // used in evict precise

  // evict
  int evict(KVOpsCtx* ctx);
  int evict_sample(KVOpsCtx* ctx);
  int evict_sample_naive(KVOpsCtx* ctx);
  int evict_sample_adaptive(KVOpsCtx* ctx);
  int evict_sample_adaptive_naive(KVOpsCtx* ctx);
  int evict_sample_adaptive_heavy(KVOpsCtx* ctx);
  int evict_precise(KVOpsCtx* ctx);

  void update_priority(KVOpsCtx* ctx);
  void update_priority_cliquemap(KVOpsCtx* ctx);  // eviction for cliquemap
  void update_priority_sample(KVOpsCtx* ctx);
  void update_priority_sample_naive(KVOpsCtx* ctx);
  void update_priority_sample_adaptive(KVOpsCtx* ctx);
  void update_priority_precise(KVOpsCtx* ctx);
  void update_priority_common(KVOpsCtx* ctx, uint32_t info_upd_mask);

  // expand bucket on bucket eviction failure
  int evict_bucket(KVOpsCtx* ctx);
  int evict_bucket_sample(KVOpsCtx* ctx);
  int evict_bucket_sample_naive(KVOpsCtx* ctx);
  int evict_bucket_sample_adaptive(KVOpsCtx* ctx);
  int evict_bucket_sample_adaptive_naive(KVOpsCtx* ctx);
  int evict_bucket_sample_adaptive_heavy(KVOpsCtx* ctx);
  int evict_bucket_precise(KVOpsCtx* ctx);

  void check_priority(KVOpsCtx* ctx);

  // generate new header
  void gen_info_meta(KVOpsCtx* ctx,
                     uint32_t info_update_mask,
                     __OUT SlotMeta* meta);
  void gen_info_update_mask(const SlotMeta* meta,
                            __OUT uint32_t* info_update_mask);

  // slot transformation
  uint32_t get_slot_id(uint64_t slot_raddr);  // get slot id from slot raddr
  uint64_t get_slot_raddr(uint32_t slot_id);  // get slot raddr from slot id

  uint64_t get_hist_head_raddr();

  // get str key
  std::string get_str_key(KVOpsCtx* ctx);

  // adaptive function
  void adaptive_update_weights(KVOpsCtx* ctx);
  void adaptive_update_weights_sync(KVOpsCtx* ctx);
  void adaptive_update_weights_async(KVOpsCtx* ctx);
  void adaptive_sync_weights();
  void adaptive_read_weights();
  int adaptive_get_best_candidate(const std::vector<int>& candidates,
                                  __OUT uint8_t* expert_bmap);
  uint64_t adaptive_get_best_candidate(const std::vector<uint64_t>& candidates,
                                       __OUT uint8_t* expert_bmap);
  int adaptive_get_best_expert(__OUT uint8_t* expert_bmap);
  void adaptive_read_weights(KVOpsCtx* ctx);
  void adaptive_get_expert_rank(
      std::vector<std::pair<double, int>>& expert_rank);
  void count_expert_evict(uint8_t expert_bmap);

  // 1-sided get
  int kv_get_1s(void* key,
                uint32_t key_size,
                __OUT void* val,
                __OUT uint32_t* val_size);

  int kv_set_1s(void* key, uint32_t key_size, void* val, uint32_t val_size);
  int kv_set_2s(void* key, uint32_t key_size, void* val, uint32_t val_size);

  void log_op(const char* op, void* key, uint32_t key_size, bool miss);

#ifdef USE_REWARDS
  void hit_adjust_weights(KVOpsCtx* ctx);
#endif

 public:
  uint32_t n_set_miss_;
  void clear_counters();
  DMCClient(const DMCConfig* conf);
  ~DMCClient();
  int kv_get(void* key,
             uint32_t key_size,
             __OUT void* val,
             __OUT uint32_t* val_size);
  int kv_get_2s(void* key,
                uint32_t key_size,
                __OUT void* val,
                __OUT uint32_t* val_size);
  int kv_p_set(void* key, uint32_t key_size, void* val, uint32_t val_size);
  int kv_set(void* key, uint32_t key_size, void* val, uint32_t val_size);

  void get_adaptive_weights(std::vector<float>& adaptive_weights);
  void get_adaptive_weight_vec(std::vector<std::vector<float>>& weight_vec);
  void get_expert_evict_cnt(std::vector<uint32_t>& evict_cnt);
#ifdef USE_REWARDS
  void get_expert_reward_cnt(std::vector<uint32_t>& reward_cnt);
#endif

  inline uint8_t get_evict_type() { return eviction_type_; }

  inline void scale_memory() { server_oom_ = false; }
};

#endif