#ifndef _DMC_LIST_H_
#define _DMC_LIST_H_

#include "nm.h"

#include <float.h>
#include <stdint.h>

typedef struct __attribute__((__packed__)) _RListEntry {
  double priority;
  uint32_t prev;
  uint32_t next;
} RListEntry;

class RList {
 private:
  uint64_t base_raddr_;
  uint32_t num_slots_;
  uint32_t occupy_size_;
#ifdef USE_SHARD_PQUEUE
  uint64_t lock_raddrs_[NUM_PQUEUE_SHARDS];
  uint64_t head_raddrs_[NUM_PQUEUE_SHARDS];
  uint64_t tail_raddrs_[NUM_PQUEUE_SHARDS];
  uint32_t head_slot_ids_[NUM_PQUEUE_SHARDS];
  uint32_t tail_slot_ids_[NUM_PQUEUE_SHARDS];
#else
  uint64_t lock_raddr_;
  uint64_t head_raddr_;
  uint64_t tail_raddr_;
  uint32_t head_slot_id_;
  uint32_t tail_slot_id_;
#endif

  uint32_t rkey_;

  void* op_buf_;
  uint32_t op_buf_size_;
  uint32_t lkey_;

#ifdef USE_SHARD_PQUEUE
  void list_lock(UDPNetworkManager* nm, uint32_t list_id);
  void list_unlock(UDPNetworkManager* nm, uint32_t list_id);
#else
  void list_lock(UDPNetworkManager* nm);
  void list_unlock(UDPNetworkManager* nm);
#endif
  void delete_entry(UDPNetworkManager* nm,
                    uint64_t target_raddr,
                    const RListEntry* target);
  void delete_clear_entry(UDPNetworkManager* nm,
                          uint64_t target_raddr,
                          const RListEntry* target);
#ifdef USE_SHARD_PQUEUE
  void find_insert_place(UDPNetworkManager* nm,
                         uint32_t list_id,
                         double priority,
                         __OUT RListEntry* entry,
                         __OUT uint64_t* entry_raddr);
#else
  void find_insert_place(UDPNetworkManager* nm,
                         double priority,
                         __OUT RListEntry* entry,
                         __OUT uint64_t* entry_raddr);
#endif
  // insert the target entry and modify its priority before the victim entry
  void insert_before(UDPNetworkManager* nm,
                     const RListEntry* target,
                     uint64_t target_raddr,
                     const RListEntry* victim,
                     uint64_t victim_raddr,
                     double new_prio);

  uint64_t getEntryRaddr(uint32_t slotID);
  uint32_t getEntryID(uint64_t raddr);

 public:
  RList(uint32_t num_slots,
        uint64_t base_addr,
        uint32_t rkey,
        void* op_buf,
        uint32_t op_buf_size,
        uint32_t lkey,
        uint8_t type);
  void update(UDPNetworkManager* nm,
              uint32_t slot_id,
              double prio,
              uint64_t slot_raddr,
              uint64_t expected_slot_val);
  int delete_slot(UDPNetworkManager* nm, uint32_t slot_id, uint64_t slot_raddr);

  // get the slot offset in the bucket with the minimium priority
  int get_bucket_min(UDPNetworkManager* nm,
                     uint32_t bucket_init_id,
                     uint32_t assoc_num);
  // get the minimum slot_id in the list
  uint32_t get_list_min(UDPNetworkManager* nm);
  uint32_t size();

  void local_update(uint32_t slot_id, double prio);

  void printList(UDPNetworkManager* nm);
};

static inline uint32_t get_list_size(uint32_t num_slots) {
  uint64_t head_raddr = sizeof(RListEntry) * num_slots;
#ifdef USE_SHARD_PQUEUE
  uint64_t tail_raddr = head_raddr + sizeof(RListEntry) * NUM_PQUEUE_SHARDS;
  uint64_t lock_raddr = tail_raddr + sizeof(RListEntry) * NUM_PQUEUE_SHARDS;
  uint64_t end_raddr = lock_raddr + sizeof(uint64_t) * NUM_PQUEUE_SHARDS;
#else
  uint64_t tail_raddr = head_raddr + sizeof(RListEntry);
  uint64_t lock_raddr = tail_raddr + sizeof(RListEntry);
  uint64_t end_raddr = lock_raddr + sizeof(uint64_t);
#endif
  return ROUNDUP((uint32_t)end_raddr, 1024);
}

#endif