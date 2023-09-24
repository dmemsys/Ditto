#ifndef _DMC_PRIORITY_H_
#define _DMC_PRIORITY_H_

#include "debug.h"
#include "dmc_table.h"
#include "dmc_utils.h"

#include <math.h>
#include <stdint.h>

#define UPD_TS (1)
#define UPD_FREQ (1 << 1)
#define UPD_LAT (1 << 2)
#define UPD_COST (1 << 3)
#define UPD_CNTR (1 << 4)

typedef struct __attribute__((__packed__)) _ObjHeader {
  uint32_t key_size;
  uint32_t val_size;
  SlotMeta meta;
} ObjHeader;

#define OBJ_META_OFF (offsetof(ObjHeader, meta))

class Priority {
 public:
  virtual uint32_t info_update_mask(const SlotMeta* meta) = 0;
  virtual double parse_priority(const SlotMeta* meta, uint8_t size) = 0;
  virtual void evict_callback(double evict_prio) {}
  virtual double get_counter_val(const SlotMeta* meta, uint8_t size) {
    return 0;
  }
};

class DumbPriority : public Priority {
  uint32_t info_update_mask(const SlotMeta* meta) { return 0; }
  double parse_priority(const SlotMeta* meta, uint8_t size) { return random(); }
};

class LRUPriority : public Priority {
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.acc_ts;
  }
};

class LFUPriority : public Priority {
 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.freq;
  }
};

class GDSFPriority : public Priority {
 private:
  double L_;

 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return L_ + meta->acc_info.freq / size;
  }
  void evict_callback(double evict_prio) { L_ = evict_prio; }
};

class GDSPriority : public Priority {
 private:
  double L_;

 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return L_ + 1 / size;
  }
  void evict_callback(double evict_prio) { L_ = evict_prio; }
};

class LIRSPriority : public Priority {
 public:
  uint32_t info_update_mask(const SlotMeta* meta) {
    return UPD_TS | UPD_FREQ | UPD_CNTR;
  }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.counter;
  }
  double get_counter_val(const SlotMeta* meta, uint8_t size) {
    return new_ts() - meta->acc_info.acc_ts;
  }
};

class LRFUPriority : public Priority {
 private:
  float lambda_ = 0.5;
  double f(uint64_t interval) { return pow(0.5, this->lambda_ * interval); }

 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_CNTR; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.counter;
  }
  double get_counter_val(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.counter * f(new_ts() - meta->acc_info.acc_ts) + f(0);
  }
};

class FIFOPriority : public Priority {
 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.ins_ts;
  }
};

class LFUDAPriority : public Priority {  // TODO: check this
 private:
  double L_;

 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return meta->acc_info.freq + L_;
  }
  void evict_callback(double evict_prio) { L_ = evict_prio; }
};

class LRUKPriority : public Priority {
 private:
  int K_ = 2;

 public:
  uint32_t info_update_mask(const SlotMeta* meta) {
    if (((meta->acc_info.freq + 1) % K_) == 0) {
      return UPD_TS | UPD_FREQ;
    }
    return UPD_CNTR | UPD_FREQ;
  }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    if (meta->acc_info.freq < K_) {
      return -1;
    }
    if (meta->acc_info.freq % K_ == 0) {
      return meta->acc_info.acc_ts;
    }
    return meta->acc_info.counter;
  }
  double get_counter(const SlotMeta* meta, uint8_t size) { return new_ts(); }
};

class SIZEPriority : public Priority {
 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return (double)size * 100000000000 + meta->acc_info.acc_ts;
  }
};

class MRUPriority : public Priority {
 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_TS | UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    return -(double)meta->acc_info.acc_ts;
  }
};

class HyperbolicPriority : public Priority {
 public:
  uint32_t info_update_mask(const SlotMeta* meta) { return UPD_FREQ; }
  double parse_priority(const SlotMeta* meta, uint8_t size) {
    uint64_t cur_ts = new_ts();
    uint64_t ins_ts = meta->acc_info.ins_ts;
    return (double)meta->acc_info.freq / (cur_ts - ins_ts);
  }
};

static inline Priority* dmc_new_priority(uint8_t eviction_prio) {
  printd(L_INFO, "%d", eviction_prio);
  switch (eviction_prio) {
    case EVICT_PRIO_LRU:
      return new LRUPriority();
    case EVICT_PRIO_LFU:
      return new LFUPriority();
    case EVICT_PRIO_GDSF:
      return new GDSFPriority();
    case EVICT_PRIO_GDS:
      return new GDSPriority();
    case EVICT_PRIO_LIRS:
      return new LIRSPriority();
    case EVICT_PRIO_LRFU:
      return new LRFUPriority();
    case EVICT_PRIO_FIFO:
      return new FIFOPriority();
    case EVICT_PRIO_LFUDA:
      return new LFUDAPriority();
    case EVICT_PRIO_LRUK:
      return new LRUKPriority();
    case EVICT_PRIO_SIZE:
      return new SIZEPriority();
    case EVICT_PRIO_MRU:
      return new MRUPriority();
    case EVICT_PRIO_HYPERBOLIC:
      return new HyperbolicPriority();
    case EVICT_PRIO_NON:
      return new DumbPriority();
    default:
      printd(L_ERROR, "Unknown eviction type %d", eviction_prio);
      return NULL;
  }
  return NULL;
}

#endif