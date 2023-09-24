#include "server_mm.h"
#include "debug.h"
#include "dmc_table.h"
#include "dmc_utils.h"
#include "rlist.h"

#include <assert.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>

#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#define MAP_HUGE_1GB (30 << MAP_HUGE_SHIFT)

ServerMM::ServerMM(const DMCConfig* conf, struct ibv_pd* pd) {
  segment_size_ = conf->segment_size;
  base_addr_ = conf->server_base_addr;
  base_len_ = conf->server_data_len;
  elastic_mem_ = conf->elastic_mem;

  // allocate data area
  int port_flag = PROT_READ | PROT_WRITE;
  int mm_flag =
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB | MAP_HUGE_2MB;
  data_ = mmap((void*)base_addr_, base_len_, port_flag, mm_flag, -1, 0);
  if ((uint64_t)data_ != base_addr_) {
    printd(L_ERROR, "mapping %ld to 0x%lx failed", base_len_, base_addr_);
    assert(0);
  }

  uint32_t list_size = get_list_size(HASH_NUM_BUCKETS * HASH_BUCKET_ASSOC_NUM);
  index_area_addr_ = base_addr_;
  index_area_len_ = HASH_SPACE_SIZE;
  printd(L_INFO, "hash_size: %ld, %ld\n", index_area_len_,
         ROUNDUP(sizeof(Table), 1024));
  stateful_area_addr_ = base_addr_ + index_area_len_;
  stateful_area_len_ =
      STATEFUL_SAPCE_SIZE > list_size ? STATEFUL_SAPCE_SIZE : list_size;
  free_space_addr_ = stateful_area_addr_ + stateful_area_len_;
  free_space_len_ = base_len_ - index_area_len_ - stateful_area_len_;
  printf("cache size: %ld\n", free_space_len_ / conf->block_size);

  printd(L_INFO,
         "ServerMM initialized with parameters:\n"
         "\tsegment_size: %d, base_addr: 0x%lx, base_len: %ld \n"
         "\tindex_area_addr: 0x%lx, index_area_len: %ld\n"
         "\tstateful_area_addr: 0x%lx, stateful_area_len: %ld\n"
         "\tfree_space_addr: 0x%lx, free_space_len: %ld",
         segment_size_, base_addr_, base_len_, index_area_addr_,
         index_area_len_, stateful_area_addr_, stateful_area_len_,
         free_space_addr_, free_space_len_);

  // register memory region
  int access_flag = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  mr_ = ibv_reg_mr(pd, data_, base_len_, access_flag);
  assert(mr_ != NULL);
  printd(L_INFO, "Registered MR: %p rkey %x\n", mr_->addr, mr_->rkey);

  // init free_segment_list
  free_segment_list_ = std::queue<SegmentInfo>();
  used_segment_map_.clear();
  num_segments_ = free_space_len_ / segment_size_;
  for (uint64_t i = 0; i < num_segments_; i++) {
    uint64_t segment_addr = free_space_addr_ + i * segment_size_;
    SegmentInfo info;
    info.addr = segment_addr;
    info.allocated_to = -1;
    free_segment_list_.push(info);
  }

  if (elastic_mem_) {
#ifdef ELA_MEM_TPT
    num_reserved_segments_ = num_segments_ / 2;
#else
    num_reserved_segments_ = reserved_segment_list[0];
#endif
    printf("num_reserved_segments: %d\n", num_reserved_segments_);
  } else {
    num_reserved_segments_ = 0;
  }

  printd(L_INFO, "%ld segments in total", free_segment_list_.size());
}

ServerMM::~ServerMM() {
  ibv_dereg_mr(mr_);
  munmap(data_, base_len_);
}

int ServerMM::get_mr_info(__OUT MrInfo* mr_info) {
  mr_info->addr = (uint64_t)mr_->addr;
  mr_info->rkey = mr_->rkey;
  return 0;
}

int ServerMM::alloc_segment(uint16_t sid, __OUT SegmentInfo* _seg_info) {
  if (free_segment_list_.size() == num_reserved_segments_) {
    memset(_seg_info, 0, sizeof(SegmentInfo));
    return -1;
  }
  SegmentInfo seg_info = free_segment_list_.front();
  seg_info.allocated_to = sid;
  free_segment_list_.pop();
  used_segment_map_[seg_info.addr] = seg_info;
  memcpy(_seg_info, &seg_info, sizeof(SegmentInfo));
  printd(L_INFO, "free segments: %ld", free_segment_list_.size());
  return 0;
}

int ServerMM::free_segment(uint64_t seg_addr) {
  std::map<uint64_t, SegmentInfo>::iterator it =
      used_segment_map_.find(seg_addr);
  if (it == used_segment_map_.end()) {
    printd(L_ERROR, "Freeing invalid segment %lu", seg_addr);
    return -1;
  }
  SegmentInfo seg_info = it->second;
  seg_info.allocated_to = -1;
  free_segment_list_.push(seg_info);
  used_segment_map_.erase(it);
  return 0;
}

bool ServerMM::check_num_segments() {
  uint64_t num_free = free_segment_list_.size();
  uint64_t num_used = used_segment_map_.size();
  return (num_free + num_used) == num_segments_;
}