#ifndef _DMC_TABLE_H_
#define _DMC_TABLE_H_

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include "debug.h"
#include "dmc_utils.h"

#define HASH_SPACE_SIZE (ROUNDUP(sizeof(Table), 1024))
#define STATEFUL_SAPCE_SIZE (4 * 1024 * 1024)

#define MAX_CHAIN_LEN (8)
typedef union __attribute__((__packed__)) _SlotMeta {
  struct {
    uint64_t key_hash;
    uint64_t ins_ts;
    uint64_t acc_ts;
    uint64_t counter;
    uint64_t freq;
  } acc_info;
} SlotMeta;

typedef struct __attribute__((__packed__)) _Slot {
  struct {
    uint8_t fp;
    uint8_t kv_len;
    uint8_t pointer[6];
  } atomic;

  SlotMeta meta;
} Slot;

#define SLOT_ATOMIC_OFF (offsetof(Slot, atomic))
#define SLOT_META_OFF (offsetof(Slot, meta))

#define SLOTM_INFO_HASH_OFF (offsetof(SlotMeta, acc_info.key_hash))
#define SLOTM_INFO_ACC_TS_OFF (offsetof(SlotMeta, acc_info.acc_ts))
#define SLOTM_INFO_INS_TS_OFF (offsetof(SlotMeta, acc_info.ins_ts))
#define SLOTM_INFO_CNTR_OFF (offsetof(SlotMeta, acc_info.counter))
#define SLOTM_INFO_FREQ_OFF (offsetof(SlotMeta, acc_info.freq))

typedef Slot Bucket[HASH_BUCKET_ASSOC_NUM];
typedef Bucket Table[HASH_NUM_BUCKETS];

class DMCHash {
 public:
  virtual uint64_t hash_func1(const void* data, uint64_t length) = 0;
  virtual uint64_t hash_func2(const void* data, uint64_t length) = 0;
};

class DefaultHash : public DMCHash {
 public:
  uint64_t hash_func1(const void* data, uint64_t length);
  uint64_t hash_func2(const void* data, uint64_t length);
};

class DumbHash : public DMCHash {
  // a hash function that always return the same value for testing
 public:
  uint64_t hash_func1(const void* data, uint64_t length) { return 233; }
  uint64_t hash_func2(const void* data, uint64_t length) { return 666; }
};

static inline uint64_t HashIndexConvert48To64Bits(const uint8_t* addr) {
  uint64_t ret = 0;
  return ret | ((uint64_t)addr[0] << 40) | ((uint64_t)addr[1] << 32) |
         ((uint64_t)addr[2] << 24) | ((uint64_t)addr[3] << 16) |
         ((uint64_t)addr[4] << 8) | ((uint64_t)addr[5]);
}

static inline void HashIndexConvert64To48Bits(uint64_t addr,
                                              __OUT uint8_t* o_addr) {
  o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
  o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
  o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
  o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
  o_addr[4] = (uint8_t)((addr >> 8) & 0xFF);
  o_addr[5] = (uint8_t)(addr & 0xFF);
}

static inline bool is_key_match(const void* key1,
                                uint32_t key_len1,
                                const void* key2,
                                uint32_t key_len2) {
  if (key_len1 != key_len2) {
    return false;
  }
  return memcmp(key1, key2, key_len1) == 0;
}

static inline uint8_t HashIndexComputeFp(uint64_t hash) {
  uint8_t fp = 0;
  hash >>= 48;
  fp ^= hash;
  hash >>= 8;
  fp ^= hash;
  return fp;
}

static inline DMCHash* dmc_new_hash(uint8_t hash_type) {
  switch (hash_type) {
    case HASH_DEFAULT:
      return new DefaultHash;
    case HASH_DUMB:
      return new DumbHash;
    default:
      printd(L_ERROR, "Unsupported hash type (%d)!", hash_type);
      return NULL;
  }
}

#endif
