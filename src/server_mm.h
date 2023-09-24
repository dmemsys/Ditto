#ifndef _DMC_SERVER_MM_H_
#define _DMC_SERVER_MM_H_

#include <assert.h>
#include <infiniband/verbs.h>
#include <stdint.h>

#include <map>
#include <queue>

#include "dmc_utils.h"

typedef struct _SegmentInfo {
  uint64_t addr;
  uint16_t allocated_to;
} SegmentInfo;

class ServerMM {
 private:
  uint64_t base_addr_;
  uint64_t base_len_;
  uint64_t index_area_addr_;
  uint64_t index_area_len_;
  uint64_t stateful_area_addr_;
  uint64_t stateful_area_len_;
  uint64_t free_space_addr_;
  uint64_t free_space_len_;

  uint32_t segment_size_;
  uint32_t num_segments_;
  std::queue<SegmentInfo> free_segment_list_;
  std::map<uint64_t, SegmentInfo> used_segment_map_;

  struct ibv_mr* mr_;
  void* data_;

  bool elastic_mem_;
  uint32_t num_reserved_segments_;

 public:
  ServerMM(const DMCConfig* conf, struct ibv_pd* pd);
  ~ServerMM();

  int get_mr_info(__OUT MrInfo* mr_info);
  inline uint64_t get_base_addr() { return base_addr_; }
  inline uint64_t get_stateful_area_addr() { return stateful_area_addr_; }
  inline uint32_t get_stateful_area_len() { return stateful_area_len_; }
  inline float get_usage() {
    return 1 - ((float)free_segment_list_.size() / num_segments_);
  }
  inline uint64_t get_free_addr() { return free_space_addr_; }
  inline void scale_memory(int reserved_idx) {
#ifdef ELA_MEM_TPT
    assert(num_reserved_segments_ > 0);
    printf("Server decreases reserved memory from %d to %d\n",
           num_reserved_segments_, 0);
    num_reserved_segments_ = 0;
#else
    assert(num_reserved_segments_ > 0);
    printf("Server decreases reserved memory from %d to %d\n",
           num_reserved_segments_, reserved_segment_list[reserved_idx]);
    num_reserved_segments_ = reserved_segment_list[reserved_idx];
#endif
  }

  int alloc_segment(uint16_t sid, __OUT SegmentInfo* seg_info);
  int free_segment(uint64_t seg_addr);

  // for testing
  bool check_num_segments();

  inline uint32_t get_rkey() { return mr_->rkey; }
};

#endif