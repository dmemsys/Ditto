#include "rlist.h"
#include "dmc_table.h"
#include "dmc_utils.h"

#include "debug.h"
#include "ib.h"

#include <assert.h>
#include <unistd.h>

RList::RList(uint32_t num_slots,
             uint64_t base_addr,
             uint32_t rkey,
             void* op_buf,
             uint32_t op_buf_size,
             uint32_t lkey,
             uint8_t type) {
  base_raddr_ = base_addr;
  num_slots_ = num_slots;
#ifdef USE_SHARD_PQUEUE
  // | ---slot entries--- | ---head entries--- | ---tail entries--- |
  // ---locks--- |
  for (int s = 0; s < NUM_PQUEUE_SHARDS; s++) {
    head_raddrs_[s] = base_addr + sizeof(RListEntry) * (num_slots_ + s);
    head_slot_ids_[s] = num_slots_ + s;
    assert(getEntryID(head_raddrs_[s]) == head_slot_ids_[s]);
    assert(getEntryRaddr(head_slot_ids_[s]) == head_raddrs_[s]);
  }
  for (int s = 0; s < NUM_PQUEUE_SHARDS; s++) {
    tail_raddrs_[s] =
        base_addr + sizeof(RListEntry) * (num_slots + NUM_PQUEUE_SHARDS + s);
    tail_slot_ids_[s] = num_slots + NUM_PQUEUE_SHARDS + s;
    assert(getEntryID(tail_raddrs_[s]) == tail_slot_ids_[s]);
    assert(getEntryRaddr(tail_slot_ids_[s]) == tail_raddrs_[s]);
  }
  for (int s = 0; s < NUM_PQUEUE_SHARDS; s++)
    lock_raddrs_[s] = base_addr +
                      sizeof(RListEntry) * (num_slots + NUM_PQUEUE_SHARDS * 2) +
                      sizeof(uint64_t) * s;
  occupy_size_ = ROUNDUP(
      lock_raddrs_[NUM_PQUEUE_SHARDS - 1] + sizeof(uint64_t) - base_addr, 1024);
#else
  head_raddr_ = base_addr + sizeof(RListEntry) * num_slots_;
  tail_raddr_ = head_raddr_ + sizeof(RListEntry);
  lock_raddr_ = tail_raddr_ + sizeof(RListEntry);
  head_slot_id_ = num_slots_;
  tail_slot_id_ = num_slots_ + 1;
  assert(getEntryID(head_raddr_) == head_slot_id_);
  assert(getEntryRaddr(head_slot_id_) == head_raddr_);
  assert(getEntryID(tail_raddr_) == tail_slot_id_);
  assert(getEntryRaddr(tail_slot_id_) == tail_raddr_);
  occupy_size_ = ROUNDUP(lock_raddr_ + sizeof(uint64_t) - base_addr, 1024);
  printd(L_DEBUG, "head %d tail %d", head_slot_id_, tail_slot_id_);
#endif

  rkey_ = rkey;
  op_buf_ = op_buf;
  op_buf_size_ = op_buf_size;
  lkey_ = lkey;

  if (type == SERVER) {
#ifdef USE_SHARD_PQUEUE
    memset((void*)base_addr, 0, sizeof(occupy_size_));
    for (int s = 0; s < NUM_PQUEUE_SHARDS; s++) {
      (*(RListEntry*)head_raddrs_[s]).next = tail_slot_ids_[s];
      (*(RListEntry*)head_raddrs_[s]).prev = tail_slot_ids_[s];
      (*(RListEntry*)head_raddrs_[s]).priority = 1e20;
      (*(RListEntry*)tail_raddrs_[s]).next = head_slot_ids_[s];
      (*(RListEntry*)tail_raddrs_[s]).prev = head_slot_ids_[s];
      (*(RListEntry*)tail_raddrs_[s]).priority = -1;
    }
#else
    memset((void*)base_addr, 0, sizeof(occupy_size_));
    (*(RListEntry*)head_raddr_).next = tail_slot_id_;
    (*(RListEntry*)head_raddr_).prev = tail_slot_id_;
    (*(RListEntry*)head_raddr_).priority = 1e20;
    (*(RListEntry*)tail_raddr_).next = head_slot_id_;
    (*(RListEntry*)tail_raddr_).prev = head_slot_id_;
    (*(RListEntry*)tail_raddr_).priority = -1;
#endif
  }
}

#ifdef USE_SHARD_PQUEUE
void RList::list_lock(UDPNetworkManager* nm, uint32_t list_id) {
  int ret = 0;
  *(uint64_t*)op_buf_ = 1;
  do {
    ret = nm->rdma_cas_sid_sync(0, lock_raddrs_[list_id], rkey_,
                                (uint64_t)op_buf_, lkey_, 0, 1);
    assert(ret == 0);
#ifdef USE_LOCK_BACKOFF
    if (*(uint64_t*)op_buf_ != 0)
      usleep(5);
#endif
  } while (*(uint64_t*)op_buf_ != 0);
}

void RList::list_unlock(UDPNetworkManager* nm, uint32_t list_id) {
  *(uint64_t*)op_buf_ = 0;
  int ret =
      nm->rdma_inl_write_sid_async(0, lock_raddrs_[list_id], rkey_,
                                   (uint64_t)op_buf_, lkey_, sizeof(uint64_t));
  assert(ret == 0);
}
#else
void RList::list_lock(UDPNetworkManager* nm) {
  int ret = 0;
  *(uint64_t*)op_buf_ = 1;
  do {
    ret = nm->rdma_cas_sid_sync(0, lock_raddr_, rkey_, (uint64_t)op_buf_, lkey_,
                                0, 1);
    assert(ret == 0);
  } while (*(uint64_t*)op_buf_ != 0);
}

void RList::list_unlock(UDPNetworkManager* nm) {
  *(uint64_t*)op_buf_ = 0;
  int ret = nm->rdma_inl_write_sid_async(
      0, lock_raddr_, rkey_, (uint64_t)op_buf_, lkey_, sizeof(uint64_t));
  assert(ret == 0);
}
#endif

void RList::delete_entry(UDPNetworkManager* nm,
                         uint64_t target_raddr,
                         const RListEntry* target) {
  struct ibv_send_wr write_wr[2];
  struct ibv_sge write_sge[2];
  memset(write_wr, 0, sizeof(struct ibv_send_wr) * 2);
  memset(write_sge, 0, sizeof(struct ibv_sge) * 2);

  ib_create_sge((uint64_t)&target->next, 0, sizeof(uint32_t), &write_sge[0]);
  ib_create_sge((uint64_t)&target->prev, 0, sizeof(uint32_t), &write_sge[1]);

  // modify the previous entry
  write_wr[0].wr_id = 88182;
  write_wr[0].next = &write_wr[1];
  write_wr[0].sg_list = &write_sge[0];
  write_wr[0].num_sge = 1;
  write_wr[0].opcode = IBV_WR_RDMA_WRITE;
  write_wr[0].send_flags = IBV_SEND_INLINE;
  write_wr[0].wr.rdma.remote_addr =
      getEntryRaddr(target->prev) + offsetof(RListEntry, next);
  write_wr[0].wr.rdma.rkey = rkey_;

  // modify the next entry
  write_wr[1].wr_id = 88183;
  write_wr[1].next = NULL;
  write_wr[1].sg_list = &write_sge[1];
  write_wr[1].num_sge = 1;
  write_wr[1].opcode = IBV_WR_RDMA_WRITE;
  write_wr[1].send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
  write_wr[1].wr.rdma.remote_addr =
      getEntryRaddr(target->next) + offsetof(RListEntry, prev);
  write_wr[1].wr.rdma.rkey = rkey_;

  int ret = nm->rdma_post_send_sid_sync(write_wr, 0);
  assert(ret == 0);
}

void RList::delete_clear_entry(UDPNetworkManager* nm,
                               uint64_t target_raddr,
                               const RListEntry* target) {
  struct ibv_send_wr write_wr[3];
  struct ibv_sge write_sge[3];
  memset(write_wr, 0, sizeof(struct ibv_send_wr) * 3);
  memset(write_sge, 0, sizeof(struct ibv_sge) * 3);

  RListEntry emptyEntry;
  memset(&emptyEntry, 0, sizeof(RListEntry));
  ib_create_sge((uint64_t)&emptyEntry, 0, sizeof(RListEntry), &write_sge[0]);
  ib_create_sge((uint64_t)&target->next, 0, sizeof(uint32_t), &write_sge[1]);
  ib_create_sge((uint64_t)&target->prev, 0, sizeof(uint32_t), &write_sge[2]);

  write_wr[0].wr_id = 1351;
  write_wr[0].next = &write_wr[1];
  write_wr[0].sg_list = &write_sge[0];
  write_wr[0].num_sge = 1;
  write_wr[0].opcode = IBV_WR_RDMA_WRITE;
  write_wr[0].send_flags = IBV_SEND_INLINE;
  write_wr[0].wr.rdma.remote_addr = target_raddr;
  write_wr[0].wr.rdma.rkey = rkey_;

  write_wr[1].wr_id = 1352;
  write_wr[1].next = &write_wr[2];
  write_wr[1].sg_list = &write_sge[1];
  write_wr[1].num_sge = 1;
  write_wr[1].opcode = IBV_WR_RDMA_WRITE;
  write_wr[1].send_flags = IBV_SEND_INLINE;
  write_wr[1].wr.rdma.remote_addr =
      getEntryRaddr(target->prev) + offsetof(RListEntry, next);
  write_wr[1].wr.rdma.rkey = rkey_;

  write_wr[2].wr_id = 1353;
  write_wr[2].next = NULL;
  write_wr[2].sg_list = &write_sge[2];
  write_wr[2].num_sge = 1;
  write_wr[2].opcode = IBV_WR_RDMA_WRITE;
  write_wr[2].send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
  write_wr[2].wr.rdma.remote_addr =
      getEntryRaddr(target->next) + offsetof(RListEntry, prev);
  write_wr[2].wr.rdma.rkey = rkey_;

  int ret = nm->rdma_post_send_sid_sync(write_wr, 0);
  assert(ret == 0);
}

#ifdef USE_SHARD_PQUEUE
void RList::find_insert_place(UDPNetworkManager* nm,
                              uint32_t list_id,
                              double priority,
#else
void RList::find_insert_place(UDPNetworkManager* nm,
                              double priority,
#endif
                              __OUT RListEntry* entry,
                              __OUT uint64_t* entry_raddr) {
  int ret = 0;
#ifdef USE_SHARD_PQUEUE
  uint32_t next_entry_id = head_slot_ids_[list_id];
  uint64_t next_entry_raddr = head_raddrs_[list_id];
#else
  uint32_t next_entry_id = head_slot_id_;
  uint64_t next_entry_raddr = head_raddr_;
#endif
  uint64_t cur_entry_raddr = 0;
  RListEntry* lentry = (RListEntry*)(op_buf_);

  std::map<uint64_t, bool> loop_detect_;
  loop_detect_.clear();

  do {
    cur_entry_raddr = next_entry_raddr;
    if (loop_detect_[cur_entry_raddr] == true) {
      printd(L_ERROR, "LOOP!");
      abort();
    }
    loop_detect_[cur_entry_raddr] = true;
    ret = nm->rdma_read_sid_sync(0, next_entry_raddr, rkey_, (uint64_t)lentry,
                                 lkey_, sizeof(RListEntry));
    assert(ret == 0);
    next_entry_id = lentry->next;
    next_entry_raddr = getEntryRaddr(next_entry_id);
#ifdef USE_SHARD_PQUEUE
  } while (next_entry_id != head_slot_ids_[list_id] &&
           lentry->priority > priority);
#else
  } while (next_entry_id != head_slot_id_ && lentry->priority > priority);
#endif

  memcpy(entry, lentry, sizeof(RListEntry));
  *(entry_raddr) = cur_entry_raddr;
}

void RList::insert_before(UDPNetworkManager* nm,
                          const RListEntry* target,
                          uint64_t target_raddr,
                          const RListEntry* victim,
                          uint64_t victim_raddr,
                          double new_prio) {
  int ret = 0;
  struct ibv_send_wr write_wr[3];
  struct ibv_sge write_sge[3];
  memset(write_wr, 0, sizeof(struct ibv_send_wr) * 3);
  memset(write_sge, 0, sizeof(struct ibv_sge) * 3);

  RListEntry new_target;
  new_target.next = getEntryID(victim_raddr);
  new_target.prev = victim->prev;
  new_target.priority = new_prio;
  uint32_t target_id = getEntryID(target_raddr);
  ib_create_sge((uint64_t)&new_target, 0, sizeof(RListEntry), &write_sge[0]);
  ib_create_sge((uint64_t)&target_id, 0, sizeof(uint32_t), &write_sge[1]);
  ib_create_sge((uint64_t)&target_id, 0, sizeof(uint32_t), &write_sge[2]);
  printd(L_DEBUG, "new entry %d (%lf, %d, %d)", target_id, new_target.priority,
         new_target.prev, new_target.next);

  // write entire entry
  write_wr[0].wr_id = 12131;
  write_wr[0].next = &write_wr[1];
  write_wr[0].sg_list = &write_sge[0];
  write_wr[0].num_sge = 1;
  write_wr[0].opcode = IBV_WR_RDMA_WRITE;
  write_wr[0].send_flags = IBV_SEND_INLINE;
  write_wr[0].wr.rdma.remote_addr = target_raddr;
  write_wr[0].wr.rdma.rkey = rkey_;

  // modify victim prev pointer
  write_wr[1].wr_id = 12132;
  write_wr[1].next = &write_wr[2];
  write_wr[1].sg_list = &write_sge[1];
  write_wr[1].num_sge = 1;
  write_wr[1].opcode = IBV_WR_RDMA_WRITE;
  write_wr[1].send_flags = IBV_SEND_INLINE;
  write_wr[1].wr.rdma.remote_addr = victim_raddr + offsetof(RListEntry, prev);
  write_wr[1].wr.rdma.rkey = rkey_;

  // modify prev next pointer
  write_wr[2].wr_id = 12133;
  write_wr[2].next = NULL;
  write_wr[2].sg_list = &write_sge[2];
  write_wr[2].num_sge = 1;
  write_wr[2].opcode = IBV_WR_RDMA_WRITE;
  write_wr[2].send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
  write_wr[2].wr.rdma.remote_addr =
      getEntryRaddr(victim->prev) + offsetof(RListEntry, next);
  write_wr[2].wr.rdma.rkey = rkey_;

  ret = nm->rdma_post_send_sid_sync(write_wr, 0);
  assert(ret == 0);
}

void RList::local_update(uint32_t slot_id, double prio) {
  // 1. read slot entry
  RListEntry* target =
      (RListEntry*)(base_raddr_ + slot_id * sizeof(RListEntry));
  if (target->next != 0 || target->prev != 0) {
    printf("Should not be occupied slot!\n");
    abort();
  }
#ifdef USE_SHARD_PQUEUE
  uint32_t list_id = slot_id % NUM_PQUEUE_SHARDS;
  RListEntry* head = (RListEntry*)head_raddrs_[list_id];
  RListEntry* first = (RListEntry*)getEntryRaddr(head->next);
  target->prev = head_slot_ids_[list_id];
#else
  RListEntry* head =
      (RListEntry*)(base_raddr_ + head_slot_id_ * sizeof(RListEntry));
  RListEntry* first =
      (RListEntry*)(base_raddr_ + head->next * sizeof(RListEntry));
  target->prev = head_slot_id_;
#endif
  target->next = head->next;
  target->priority = prio;
  first->prev = slot_id;
  head->next = slot_id;
}

void RList::update(UDPNetworkManager* nm,
                   uint32_t slot_id,
                   double prio,
                   uint64_t slot_raddr,
                   uint64_t expected_slot_val) {
  printd(L_DEBUG, "update begin");
  int ret = 0;
  // 1. lock remote list
#ifdef USE_SHARD_PQUEUE
  uint32_t list_id = slot_id % NUM_PQUEUE_SHARDS;
  list_lock(nm, list_id);
#else
  list_lock(nm);
#endif

  // check slot
  nm->rdma_read_sid_sync(0, slot_raddr, rkey_, (uint64_t)op_buf_, lkey_,
                         sizeof(uint64_t));
  if (*(uint64_t*)op_buf_ != expected_slot_val) {
    // no need to update
#ifdef USE_SHARD_PQUEUE
    list_unlock(nm, list_id);
#else
    list_unlock(nm);
#endif
    return;
  }

  // 2. read slot entry
  RListEntry target;
  uint64_t target_raddr = getEntryRaddr(slot_id);
  ret = nm->rdma_read_sid_sync(0, target_raddr, rkey_, (uint64_t)op_buf_, lkey_,
                               sizeof(RListEntry));
  assert(ret == 0);
  memcpy(&target, op_buf_, sizeof(RListEntry));
  printd(L_DEBUG, "read target %d (%lf, %d, %d)", getEntryID(target_raddr),
         target.priority, target.prev, target.next);

  // 3. delete slot entry
  if (target.next != target.prev) {
    delete_entry(nm, target_raddr, &target);
  }

  // 4. find insert place
  RListEntry victim;
  uint64_t victim_raddr;
#ifdef USE_SHARD_PQUEUE
  find_insert_place(nm, list_id, prio, &victim, &victim_raddr);
#else
  find_insert_place(nm, prio, &victim, &victim_raddr);
#endif
  printd(L_DEBUG, "insert before %d (%lf, %d, %d)", getEntryID(victim_raddr),
         victim.priority, victim.prev, victim.next);

  // 5. insert entry
  insert_before(nm, &target, target_raddr, &victim, victim_raddr, prio);

  // 6. unlock list
#ifdef USE_SHARD_PQUEUE
  list_unlock(nm, list_id);
#else
  list_unlock(nm);
#endif
}

int RList::delete_slot(UDPNetworkManager* nm,
                       uint32_t slot_id,
                       uint64_t slot_raddr) {
  int ret = 0;
#ifdef USE_SHARD_PQUEUE
  uint32_t list_id = slot_id % NUM_PQUEUE_SHARDS;
  assert(slot_id != head_slot_ids_[list_id]);
  assert(slot_id != tail_slot_ids_[list_id]);
#else
  assert(slot_id != head_slot_id_);
  assert(slot_id != tail_slot_id_);
#endif

  // 1. lock list
#ifdef USE_SHARD_PQUEUE
  list_lock(nm, list_id);
#else
  list_lock(nm);
#endif

  // check slot
  nm->rdma_read_sid_sync(0, slot_raddr, rkey_, (uint64_t)op_buf_, lkey_,
                         sizeof(uint64_t));
  if (*(uint64_t*)op_buf_ != 0) {
    // no need to update
#ifdef USE_SHARD_PQUEUE
    list_unlock(nm, list_id);
#else
    list_unlock(nm);
#endif
    return 0;
  }

  // 2. read target entry
  RListEntry target;
  uint64_t target_raddr = getEntryRaddr(slot_id);
  nm->rdma_read_sid_sync(0, target_raddr, rkey_, (uint64_t)op_buf_, lkey_,
                         sizeof(RListEntry));
  memcpy(&target, op_buf_, sizeof(RListEntry));

  if (target.next == target.prev) {
    ret = -1;
  } else {
    delete_clear_entry(nm, target_raddr, &target);
  }
#ifdef USE_SHARD_PQUEUE
  list_unlock(nm, list_id);
#else
  list_unlock(nm);
#endif
  return ret;
}

uint32_t RList::get_list_min(UDPNetworkManager* nm) {
  int ret = 0;
#ifdef USE_SHARD_PQUEUE
  while (true) {
    uint32_t list_id = rand() % NUM_PQUEUE_SHARDS;
    // 1. lock list
    list_lock(nm, list_id);

    // 2. read the tail
    RListEntry tail;
    uint64_t tail_raddr = getEntryRaddr(tail_slot_ids_[list_id]);
    ret = nm->rdma_read_sid_sync(0, tail_raddr, rkey_, (uint64_t)op_buf_, lkey_,
                                 sizeof(RListEntry));
    assert(ret == 0);
    memcpy(&tail, op_buf_, sizeof(RListEntry));

    if (tail.prev != head_slot_ids_[list_id]) {
      // success
      list_unlock(nm, list_id);
      return tail.prev;
    }
    // fail for this shard of list, retry another shard
    list_unlock(nm, list_id);
  }
#else
  // 1. lock list
  list_lock(nm);

  // 2. read the tail
  RListEntry tail;
  uint64_t tail_raddr = getEntryRaddr(tail_slot_id_);
  ret = nm->rdma_read_sid_sync(0, tail_raddr, rkey_, (uint64_t)op_buf_, lkey_,
                               sizeof(RListEntry));
  assert(ret == 0);
  memcpy(&tail, op_buf_, sizeof(RListEntry));
  assert(tail.prev != head_slot_id_);

  // 3. return last node id
  list_unlock(nm);
  return tail.prev;
#endif
}

int RList::get_bucket_min(UDPNetworkManager* nm,
                          uint32_t bucket_init_id,
                          uint32_t assoc_num) {
  int ret = 0;

  // 1. read the bucket
  RListEntry target_bucket[assoc_num];
  uint64_t target_bucket_raddr = getEntryRaddr(bucket_init_id);
  ret = nm->rdma_read_sid_sync(0, target_bucket_raddr, rkey_, (uint64_t)op_buf_,
                               lkey_, sizeof(RListEntry) * assoc_num);
  assert(ret == 0);
  memcpy(target_bucket, op_buf_, sizeof(RListEntry) * assoc_num);

  // 3. find the entry with minimum priority
  int minIdx = 0;
  double minPrio = target_bucket[0].priority;
  for (int i = 0; i < assoc_num; i++) {
    if (target_bucket[i].next == target_bucket[i].prev) {
      // there is an empty bucket
      return -i;
    }
    if (target_bucket[i].priority < minPrio) {
      minIdx = i;
      minPrio = target_bucket[i].priority;
    }
  }

  return minIdx;
}

void RList::printList(UDPNetworkManager* nm) {
  int ret = 0;
#ifdef USE_SHARD_PQUEUE
  for (int s = 0; s < NUM_PQUEUE_SHARDS; s++) {
    printf("===== pqueue shard: %d =====\n", s);
    uint32_t next_id = head_slot_ids_[s];
    uint64_t next_raddr = head_raddrs_[s];
    RListEntry* lentry = (RListEntry*)(op_buf_);

    do {
      ret = nm->rdma_read_sid_sync(0, next_raddr, rkey_, (uint64_t)lentry,
                                   lkey_, sizeof(RListEntry));
      assert(ret == 0);
      printf("entry: %d (%lf, %d, %d)\n", next_id, lentry->priority,
             lentry->prev, lentry->next);
      next_id = lentry->next;
      next_raddr = getEntryRaddr(next_id);
    } while (next_id != head_slot_ids_[s]);
  }
#else
  uint32_t next_id = head_slot_id_;
  uint64_t next_raddr = head_raddr_;
  RListEntry* lentry = (RListEntry*)(op_buf_);

  do {
    ret = nm->rdma_read_sid_sync(0, next_raddr, rkey_, (uint64_t)lentry, lkey_,
                                 sizeof(RListEntry));
    assert(ret == 0);
    printd(L_INFO, "entry: %d (%lf, %d, %d)", next_id, lentry->priority,
           lentry->prev, lentry->next);
    next_id = lentry->next;
    next_raddr = getEntryRaddr(next_id);
  } while (next_id != head_slot_id_);
#endif
}

uint64_t RList::getEntryRaddr(uint32_t slotID) {
  return base_raddr_ + slotID * sizeof(RListEntry);
}

uint32_t RList::getEntryID(uint64_t raddr) {
  return (raddr - base_raddr_) / sizeof(RListEntry);
}

uint32_t RList::size() {
  return occupy_size_;
}