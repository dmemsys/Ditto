#ifndef _DMC_LW_HISTORY_H_
#define _DMC_LW_HISTORY_H_

#include "dmc_table.h"
#include "dmc_utils.h"
#include "nm.h"

#include <stdint.h>

class LWHistory {
 private:
  uint32_t hist_size_;
  uint64_t hist_head_raddr_;

  uint32_t occupy_size_;

 public:
  LWHistory(uint32_t hist_size, uint64_t hist_base_raddr, uint8_t type) {
    hist_size_ = hist_size;
    hist_head_raddr_ = hist_base_raddr;
    occupy_size_ = sizeof(uint64_t);
    if (type == SERVER)
      memset((void*)hist_base_raddr, 0, occupy_size_);
  }

  inline uint32_t size() { return occupy_size_; }
  inline uint64_t hist_cntr_raddr() { return hist_head_raddr_; }
  inline bool has_overwritten(uint64_t cur_head, uint64_t stored_head) {
    cur_head &= HIST_MASK;
    stored_head &= HIST_MASK;
    if (cur_head >= stored_head)
      return (cur_head - stored_head) >= hist_size_;
    return (cur_head + (1ULL << 48) - stored_head) >= hist_size_;
  }
  inline bool is_in_history(const Slot* slot) {
    return slot->atomic.kv_len == 0xF;
  }
};

#endif