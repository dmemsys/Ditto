#include "fifo_history.h"
#include "ib.h"

FIFOHistory::FIFOHistory(uint32_t hist_size,
                         uint64_t index_base_raddr,
                         uint64_t hist_base_raddr,
                         uint32_t rkey,
                         uint64_t op_buf_laddr,
                         uint32_t op_buf_size,
                         uint32_t lkey,
                         uint8_t type) {
  hist_size_ = hist_size;
  index_base_raddr_ = index_base_raddr;
  hist_base_raddr_ = hist_base_raddr;
  hist_cntr_raddr_ = hist_base_raddr + sizeof(HistEntry) * hist_size_;
  occupy_size_ = sizeof(HistEntry) * hist_size_ + sizeof(uint64_t);
  rkey_ = rkey;
  op_buf_laddr_ = op_buf_laddr;
  op_buf_size_ = op_buf_size;
  lkey_ = lkey;

  if (type == SERVER)
    memset((void*)hist_base_raddr, 0, occupy_size_);
}

void FIFOHistory::insert(UDPNetworkManager* nm,
                         uint64_t target_slot_raddr,
                         uint64_t key_hash,
                         uint8_t expert_bmap,
                         const SlotMeta* meta) {
  int ret = 0;
  uint64_t cntr_laddr = op_buf_laddr_;
  HistEntry* old_histEntry = (HistEntry*)(op_buf_laddr_ + sizeof(uint64_t));
  uint64_t swap_back_laddr_1 =
      op_buf_laddr_ + sizeof(uint64_t) + sizeof(HistEntry);
  uint64_t swap_back_laddr_2 =
      op_buf_laddr_ + sizeof(uint64_t) + sizeof(HistEntry) + sizeof(uint64_t);
  // 1. faa remote counter
  ret =
      nm->rdma_faa_size_sync(0, hist_cntr_raddr_, rkey_, cntr_laddr, lkey_, 1);
  uint64_t cntr = *(uint64_t*)cntr_laddr;
  assert(ret == 0);

  // 2. read entry
  uint64_t histEntry_raddr =
      (cntr % hist_size_) * sizeof(HistEntry) + hist_base_raddr_;
  if (cntr >= hist_size_) {
    nm->rdma_read_sid_sync(0, histEntry_raddr, rkey_, (uint64_t)old_histEntry,
                           lkey_, sizeof(HistEntry));
  }

  // 3. write new entry and cas old slot and cas new slot
  HistEntry new_histEntry;
  memset(&new_histEntry, 0, sizeof(HistEntry));
  new_histEntry.freq = meta->acc_info.freq;
  new_histEntry.key_hash = key_hash;
  new_histEntry.slot_ptr = target_slot_raddr;
  new_histEntry.head = cntr;
  new_histEntry.expert_bmap = expert_bmap;
  struct ibv_send_wr wr[3];
  struct ibv_sge sge[3];
  memset(&wr, 0, sizeof(struct ibv_send_wr) * 3);
  memset(&sge, 0, sizeof(struct ibv_sge) * 3);

  // write new entry
  ib_create_sge((uint64_t)&new_histEntry, lkey_, sizeof(HistEntry), &sge[0]);
  wr[0].wr_id = 3103;
  wr[0].next = &wr[1];
  wr[0].sg_list = &sge[0];
  wr[0].num_sge = 1;
  wr[0].opcode = IBV_WR_RDMA_WRITE;
  wr[0].send_flags = IBV_SEND_INLINE;
  wr[0].wr.rdma.remote_addr = histEntry_raddr;
  wr[0].wr.rdma.rkey = rkey_;

  // CAS target_slot
  Slot new_slot;
  memset(&new_slot, 0, sizeof(Slot));
  new_slot.atomic.fp = HashIndexComputeFp(key_hash);
  new_slot.atomic.kv_len = 0;
  HashIndexConvert64To48Bits(histEntry_raddr, new_slot.atomic.pointer);
  ib_create_sge(swap_back_laddr_1, lkey_, sizeof(uint64_t), &sge[1]);
  wr[1].wr_id = 3104;
  wr[1].next = NULL;
  wr[1].sg_list = &sge[1];
  wr[1].num_sge = 1;
  wr[1].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  wr[1].send_flags = IBV_SEND_FENCE;
  wr[1].wr.atomic.compare_add = ADAPTIVE_TMP_SLOT;
  wr[1].wr.atomic.swap = *(uint64_t*)&new_slot;
  wr[1].wr.atomic.remote_addr = target_slot_raddr;
  wr[1].wr.atomic.rkey = rkey_;

  // the old history may be evicted and its pointer may be zero
  if (cntr >= hist_size_ && old_histEntry->slot_ptr != 0) {
    wr[1].next = &wr[2];
    wr[1].send_flags = IBV_SEND_FENCE;
    Slot old_slot;
    memset(&old_slot, 0, sizeof(Slot));
    old_slot.atomic.fp = HashIndexComputeFp(old_histEntry->key_hash);
    old_slot.atomic.kv_len = 0;
    HashIndexConvert64To48Bits(histEntry_raddr, old_slot.atomic.pointer);
    ib_create_sge(swap_back_laddr_2, lkey_, sizeof(uint64_t), &sge[2]);
    wr[2].wr_id = 3105;
    wr[2].next = NULL;
    wr[2].sg_list = &sge[2];
    wr[2].num_sge = 1;
    wr[2].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr[2].send_flags = 0;
    wr[2].wr.atomic.compare_add = *(uint64_t*)&old_slot;
    wr[2].wr.atomic.swap = 0;
    wr[2].wr.atomic.remote_addr = old_histEntry->slot_ptr;
    wr[2].wr.atomic.rkey = rkey_;
  }

  ret = nm->rdma_post_send_sid_async(wr, 0);
  assert(ret == 0);
}

int FIFOHistory::try_evict(UDPNetworkManager* nm,
                           uint64_t slot_raddr,
                           const Slot* slot) {
  printd(L_DEBUG, "try evict!");
  int ret = 0;
  nm->rdma_cas_sid_sync(0, slot_raddr, rkey_, op_buf_laddr_, lkey_,
                        *(uint64_t*)slot, 0);
  if (*(uint64_t*)op_buf_laddr_ != *(uint64_t*)slot) {
    return -1;
  }

  struct ibv_send_wr write_wr[2];
  struct ibv_sge write_sge[2];
  memset(write_wr, 0, sizeof(struct ibv_send_wr) * 2);
  memset(write_sge, 0, sizeof(struct ibv_sge) * 2);
  HistEntry* new_lhistEntry = (HistEntry*)op_buf_laddr_;
  SlotMeta* new_lslotMeta = (SlotMeta*)(op_buf_laddr_ + sizeof(HistEntry));
  memset(new_lhistEntry, 0, sizeof(HistEntry));
  memset(new_lslotMeta, 0, sizeof(SlotMeta));
  ib_create_sge((uint64_t)new_lhistEntry, lkey_, sizeof(HistEntry),
                &write_sge[0]);
  ib_create_sge((uint64_t)new_lslotMeta, lkey_, sizeof(SlotMeta),
                &write_sge[1]);
  write_wr[0].wr_id = 19971;
  write_wr[0].next = &write_wr[1];
  write_wr[0].sg_list = &write_sge[0];
  write_wr[0].num_sge = 1;
  write_wr[0].opcode = IBV_WR_RDMA_WRITE;
  write_wr[0].send_flags = IBV_SEND_INLINE;
  write_wr[0].wr.rdma.remote_addr =
      HashIndexConvert48To64Bits(slot->atomic.pointer);
  write_wr[0].wr.rdma.rkey = rkey_;

  write_wr[1].wr_id = 19972;
  write_wr[1].next = NULL;
  write_wr[1].sg_list = &write_sge[1];
  write_wr[1].num_sge = 1;
  write_wr[1].opcode = IBV_WR_RDMA_WRITE;
  write_wr[1].send_flags = IBV_SEND_INLINE;
  write_wr[1].wr.rdma.remote_addr = slot_raddr + SLOT_META_OFF;
  write_wr[1].wr.rdma.rkey = rkey_;

  ret = nm->rdma_post_send_sid_async(write_wr, 0);
  return 0;
}

bool FIFOHistory::is_in_history(uint64_t kv_raddr) {
  uint64_t offset = kv_raddr - hist_base_raddr_;
  return offset >= 0 && offset < occupy_size_;
}

void FIFOHistory::free(UDPNetworkManager* nm, uint64_t hist_raddr) {
  HistEntry* empty_histEntry = (HistEntry*)op_buf_laddr_;
  memset(empty_histEntry, 0, sizeof(HistEntry));
  int ret = nm->rdma_inl_write_sid_async(0, hist_raddr, rkey_,
                                         (uint64_t)empty_histEntry, lkey_,
                                         sizeof(HistEntry));
  assert(ret == 0);
}