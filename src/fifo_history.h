#ifndef _DMC_FIFO_HISTORY_H_
#define _DMC_FIFO_HISTORY_H_

#include "dmc_table.h"
#include "dmc_utils.h"
#include "nm.h"

#include <stdint.h>

typedef struct __attribute__((__packed__)) _HistEntry {
  uint64_t slot_ptr;
  uint64_t key_hash;
  uint64_t freq;
  uint64_t head;
  uint8_t expert_bmap;
} HistEntry;

class FIFOHistory {
 private:
  uint32_t hist_size_;

  uint64_t index_base_raddr_;
  uint64_t hist_base_raddr_;
  uint64_t hist_cntr_raddr_;
  uint32_t rkey_;

  uint64_t op_buf_laddr_;
  uint32_t op_buf_size_;
  uint32_t lkey_;

  uint32_t occupy_size_;

 public:
  FIFOHistory(uint32_t hist_size,
              uint64_t index_base_raddr,
              uint64_t hist_base_raddr,
              uint32_t rkey,
              uint64_t op_buf_laddr,
              uint32_t op_buf_size,
              uint32_t lkey,
              uint8_t type);
  void insert(UDPNetworkManager* nm,
              uint64_t target_slot_raddr,
              uint64_t key_hash,
              uint8_t expert_bmap,
              const SlotMeta* meta);
  bool is_in_history(uint64_t kv_raddr);
  int try_evict(UDPNetworkManager* nm, uint64_t slot_raddr, const Slot* slot);
  void free(UDPNetworkManager* nm, uint64_t hist_raddr);

  inline uint32_t size() { return occupy_size_; }
  inline uint64_t hist_cntr_raddr() { return hist_cntr_raddr_; }
};

#endif