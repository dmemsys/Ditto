#include "server.h"

#include <assert.h>
#include <stdio.h>
#include <sys/time.h>

#include "debug.h"
#include "dmc_table.h"
#include "dmc_utils.h"
#include "ib.h"
#include "priority.h"

#include <algorithm>
#include <vector>

void* server_main(void* server_main_args) {
  ServerMainArgs* args = (ServerMainArgs*)server_main_args;
  Server* server_instance = args->server;

  int ret = stick_this_thread_to_core(args->core_id);
  assert(ret == 0);
  printd(L_INFO, "Server is running on core: %d\n", args->core_id);

  return server_instance->thread_main();
}

void* server_worker_thread(void* server_main_args) {
  ServerMainArgs* args = (ServerMainArgs*)server_main_args;
  Server* server_instance = args->server;

  int ret = stick_this_thread_to_core(args->core_id);
  assert(ret == 0);
  printf("Server worker %d thread is running on core: %d\n", args->worker_id,
         args->core_id);

  return server_instance->worker_main(args->worker_id);
}

Server::Server(const DMCConfig* conf) {
  server_id_ = conf->server_id;
  block_size_ = conf->block_size;
  testing_ = conf->testing;
  eviction_type_ = conf->eviction_type;
  eviction_prio_ = conf->eviction_priority;
  num_samples_ = conf->num_samples;
  elastic_mem_ = conf->elastic_mem;
  num_workers_ = conf->num_server_threads;

  need_stop_ = 0;
  is_evict_precise_ = false;

  srand(server_id_);
  init_counters();

  nm_ = new UDPNetworkManager(conf);

  struct ibv_pd* pd = nm_->get_ib_pd();

  // register local memory
  mm_ = new ServerMM(conf, pd);
  spin_unlock(&mm_lock_);

  // initialize hash table
  int ret = init_hash_table();
  assert(ret == 0);

  sys_start_ts_ = new_ts();
  printd(L_INFO, "start ts: %lx", sys_start_ts_);

  // initialize testing and active server mm & hash instance
  if (eviction_type_ == EVICT_CLIQUEMAP || eviction_type_ == EVICT_PRECISE ||
      testing_) {
    c_mm_ = new ClientMM*[num_workers_];
    for (int i = 0; i < num_workers_; i++) {
      c_mm_[i] = new ClientUniformMM(conf);
    }
    hash_ = dmc_new_hash(conf->hash_type);
    assert(c_mm_ != NULL && hash_ != NULL);
    priority_ = dmc_new_priority(conf->eviction_priority);
  }

  // initialize data structures for active server
  // if (eviction_type_ == EVICT_CLIQUEMAP || eviction_type_ ==
  // EVICT_SAMPLE_ADAPTIVE) {
  if (true) {
    // allocate message buffer
    send_msg_buffer_ = malloc(MSG_BUF_SIZE * MSG_BUF_NUM);
    recv_msg_buffer_ = malloc(MSG_BUF_SIZE * MSG_BUF_NUM);
    assert(send_msg_buffer_ != NULL);
    assert(recv_msg_buffer_ != NULL);
    struct ibv_pd* pd = nm_->get_ib_pd();
    send_msg_buffer_mr_ =
        ibv_reg_mr(pd, send_msg_buffer_, MSG_BUF_SIZE * MSG_BUF_NUM,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    recv_msg_buffer_mr_ =
        ibv_reg_mr(pd, recv_msg_buffer_, MSG_BUF_SIZE * MSG_BUF_NUM,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    assert(send_msg_buffer_mr_ != NULL && recv_msg_buffer_mr_ != NULL);
    memset(sr_list_, 0, sizeof(struct ibv_send_wr) * MSG_BUF_NUM);

    // prepare multiple work requests
    for (uint64_t i = 0; i < MSG_BUF_NUM; i++) {
      // initialize sr_list_
      sr_list_[i].wr_id = i + 1000;
      sr_list_[i].next = NULL;
      sr_list_[i].sg_list = &sr_sge_list_[i];
      sr_list_[i].num_sge = 1;
      sr_list_[i].opcode = IBV_WR_SEND;
      sr_list_[i].send_flags = IBV_SEND_SIGNALED;
      sr_sge_list_[i].addr = (uint64_t)send_msg_buffer_ + i * MSG_BUF_SIZE;
      sr_sge_list_[i].length = MSG_BUF_SIZE;
      sr_sge_list_[i].lkey = send_msg_buffer_mr_->lkey;

      // initialize rr_list_
      rr_list_[i].wr_id = i;
      rr_list_[i].next = NULL;
      rr_list_[i].sg_list = &rr_sge_list_[i];
      rr_list_[i].num_sge = 1;
      rr_sge_list_[i].addr = (uint64_t)recv_msg_buffer_ + i * MSG_BUF_SIZE;
      rr_sge_list_[i].length = MSG_BUF_SIZE;
      rr_sge_list_[i].lkey = recv_msg_buffer_mr_->lkey;
    }

    // start worker thread
    worker_tid_ = (pthread_t*)malloc(sizeof(pthread_t) * num_workers_);
    worker_args_ =
        (ServerMainArgs*)malloc(sizeof(ServerMainArgs) * num_workers_);
    for (int w = 0; w < num_workers_; w++) {
      printf("start worker %d\n", w);
      worker_args_[w].worker_id = w;
      worker_args_[w].core_id =
          (w % NUM_CORES) * 2;  // bind to the same numa node
      worker_args_[w].server = this;
      ret = pthread_create(&worker_tid_[w], NULL, server_worker_thread,
                           (void*)&worker_args_[w]);
      assert(ret == 0);
    }

    // check if server should be precise
    if (conf->is_server_precise) {
      is_evict_precise_ = true;
#ifdef USE_SHARD_PQUEUE
      for (int i = 0; i < NUM_PQUEUE_SHARDS; i++) {
        shard_prio_slot_addr_map_[i].clear();
        spin_unlock(&shard_prio_slot_addr_map_lock_[i]);
      }
#else
      prio_slot_addr_map_.clear();
      spin_unlock(&prio_slot_addr_map_lock_);
#endif
    }
  }

  if (eviction_type_ == EVICT_PRECISE) {
    priority_ = dmc_new_priority(conf->eviction_priority);
    uint64_t base_addr = mm_->get_base_addr() + HASH_SPACE_SIZE;
    assert(base_addr == mm_->get_stateful_area_addr());
    prio_list_ = new RList(HASH_BUCKET_ASSOC_NUM * HASH_NUM_BUCKETS, base_addr,
                           0, NULL, 0, 0, SERVER);
    printd(L_INFO, "%d = %d", prio_list_->size(),
           get_list_size(HASH_NUM_BUCKETS * HASH_BUCKET_ASSOC_NUM));
    if (prio_list_->size() > STATEFUL_SAPCE_SIZE) {
      printd(L_INFO, "%lx == %lx", base_addr + prio_list_->size(),
             mm_->get_free_addr());
      assert(base_addr + prio_list_->size() == mm_->get_free_addr());
    } else {
      assert(base_addr + STATEFUL_SAPCE_SIZE == mm_->get_free_addr());
    }
  }

  if (is_evict_adaptive(eviction_type_)) {
    uint64_t base_addr = mm_->get_base_addr();
    uint64_t stateful_base_addr = base_addr + HASH_SPACE_SIZE;
    num_experts_ = conf->num_experts;
    expert_weights_ = (float*)stateful_base_addr;
    for (int i = 0; i < num_experts_; i++) {
      expert_weights_[i] = 1.0 / num_experts_;
    }

    // initialte history ring buffer
    uint32_t stateful_size = mm_->get_stateful_area_len();
    history_size_ = conf->history_size;
    uint64_t history_base_addr =
        stateful_base_addr + sizeof(float) * num_experts_;
    printd(L_INFO, "history_base_addr: %lx", history_base_addr);
    if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE) {
      lw_history_ = new LWHistory(history_size_, history_base_addr, SERVER);
      assert(lw_history_->size() + sizeof(float) * num_experts_ <=
             stateful_size);
    } else {
      naive_history_ = new FIFOHistory(history_size_, base_addr,
                                       history_base_addr, 0, 0, 0, 0, SERVER);
      assert(naive_history_->size() + sizeof(float) * num_experts_ <=
             stateful_size);
    }
  }
}

Server::~Server() {
  delete mm_;
  delete nm_;
  if (eviction_type_ == EVICT_CLIQUEMAP) {
    ibv_dereg_mr(send_msg_buffer_mr_);
    ibv_dereg_mr(recv_msg_buffer_mr_);
    free(send_msg_buffer_);
    free(recv_msg_buffer_);
  }
}

int Server::init_hash_table() {
  if (sizeof(Table) > HASH_SPACE_SIZE) {
    printd(L_ERROR,
           "Table size (%ld) is greater than allocated hash space (%ld)",
           sizeof(Table), HASH_SPACE_SIZE);
    return -1;
  }

  uint64_t base_addr = mm_->get_base_addr();
  printd(L_INFO, "used %.2f percent of hash sapce",
         (float)sizeof(Table) / HASH_SPACE_SIZE);
  memset((void*)base_addr, 0, sizeof(Table));
  return 0;
}

int Server::server_on_connect(const UDPMsg* request,
                              struct sockaddr_in* src_addr,
                              socklen_t src_addr_len) {
  int rc = 0;
  UDPMsg reply;
  memset(&reply, 0, sizeof(UDPMsg));

  reply.id = server_id_;
  reply.type = UDPMSG_REP_CONNECT;
  rc = nm_->nm_on_connect_new_qp(request, &reply.body.conn_info.qp_info);
  assert(rc == 0);

  spin_lock(&mm_lock_);
  rc = mm_->get_mr_info(&reply.body.conn_info.mr_info);
  spin_unlock(&mm_lock_);
  assert(rc == 0);

  serialize_udpmsg(&reply);

  rc = nm_->send_udp_msg(&reply, src_addr, src_addr_len);
  assert(rc == 0);

  deserialize_udpmsg(&reply);
  rc = nm_->nm_on_connect_connect_qp(request->id, &reply.body.conn_info.qp_info,
                                     &request->body.conn_info.qp_info);
  assert(rc == 0);

  // post recv if the server is active
  if (true) {
    assert(request->id < 512);
    rc = nm_->rdma_post_recv_sid_async(&rr_list_[request->id], request->id);
  }
  return 0;
}

int Server::server_on_alloc(const UDPMsg* request,
                            struct sockaddr_in* src_addr,
                            socklen_t src_addr_len) {
  SegmentInfo info;
  int ret = 0;
  ret = mm_->alloc_segment(request->id, &info);
  if (ret == -1) {
    printd(L_INFO, "Server run out-of-memory");
  }
  printd(L_DEBUG, "Allocated addr: 0x%lx to %d", info.addr, info.allocated_to);

  UDPMsg reply;
  memset(&reply, 0, sizeof(UDPMsg));
  reply.type = UDPMSG_REP_ALLOC;
  reply.id = server_id_;
  reply.body.mr_info.addr = info.addr;
  reply.body.mr_info.rkey = mm_->get_rkey();
  serialize_udpmsg(&reply);

  ret = nm_->send_udp_msg(&reply, src_addr, src_addr_len);
  assert(ret == 0);
  return 0;
}

int Server::server_on_get_ts(const UDPMsg* request,
                             struct sockaddr_in* src_addr,
                             socklen_t src_addr_len) {
  UDPMsg reply;
  memset(&reply, 0, sizeof(UDPMsg));
  reply.type = UDPMSG_REP_TS;
  reply.id = server_id_;
  reply.body.sys_start_ts = sys_start_ts_;
  serialize_udpmsg(&reply);
  int ret = nm_->send_udp_msg(&reply, src_addr, src_addr_len);
  assert(ret == 0);
  return 0;
}

int Server::local_alloc_segment(uint32_t worker_id) {
  int ret;
  SegmentInfo si;
  spin_lock(&mm_lock_);
  ret = mm_->alloc_segment(0, &si);
  spin_unlock(&mm_lock_);
  if (ret) {
    return ret;
  }
  assert(ret == 0);
  c_mm_[worker_id]->add_segment(si.addr, 0, 0);
  return ret;
}

Slot* Server::kv_search_slot(void* key, uint32_t key_size) {
  uint64_t key_hash = hash_->hash_func1(key, key_size);
  uint8_t fp = HashIndexComputeFp(key_hash);

  uint64_t bucket_id = key_hash % HASH_NUM_BUCKETS;
  Slot* slot = (Slot*)(mm_->get_base_addr() + bucket_id * sizeof(Bucket));
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    printd(L_DEBUG, "Inspecting slot 0x%lx %lx", (uint64_t)&slot[i],
           *(uint64_t*)&slot[i]);
    if (slot[i].atomic.kv_len == 0) {
      continue;
    }
    if (slot[i].atomic.fp != fp) {
      continue;
    }
    uint64_t kv_addr = HashIndexConvert48To64Bits(slot[i].atomic.pointer);
    printd(L_DEBUG, "kv_addr: %lx", kv_addr);
    ObjHeader* header = (ObjHeader*)kv_addr;
    uint32_t key_len = header->key_size;
    uint32_t val_len = header->val_size;
    if (is_key_match(key, key_size, (void*)(kv_addr + sizeof(ObjHeader)),
                     key_len)) {
      // found
      printd(L_DEBUG, "server found locally!");
      return &slot[i];
    }
  }
  return NULL;
}

uint64_t Server::evict() {
  if (is_evict_precise_)
    return evict_precise();
  return evict_sample();
}

uint64_t Server::evict_precise() {
cliquemap_evict_retry:
  // find the slot with the lowest priority
#ifdef USE_SHARD_PQUEUE
  uint32_t shard_id = rand() % NUM_PQUEUE_SHARDS;
  spin_lock(&shard_prio_slot_addr_map_lock_[shard_id]);
  auto it = shard_prio_slot_addr_map_[shard_id].begin();
  for (; it->second.size() == 0 &&
         it != shard_prio_slot_addr_map_[shard_id].end();) {
    it = shard_prio_slot_addr_map_[shard_id].erase(it);
  }

  printd(L_DEBUG, "Evict prio: %lf", it->first);
  Slot* target_slot_ptr = (Slot*)(*it->second.begin());
  Slot target_slot;
  memcpy(&target_slot, target_slot_ptr, sizeof(Slot));
  uint64_t target_free_addr =
      HashIndexConvert48To64Bits(target_slot.atomic.pointer);
  int ret = __sync_bool_compare_and_swap((uint64_t*)target_slot_ptr,
                                         *(uint64_t*)&target_slot, 0);
  if (ret != true) {
    // printf("Evict conflict!\n");
    spin_unlock(&shard_prio_slot_addr_map_lock_[shard_id]);
    goto cliquemap_evict_retry;
  }
  it->second.erase(it->second.begin());
  memset(&target_slot_ptr->meta, 0, sizeof(SlotMeta));
  spin_unlock(&shard_prio_slot_addr_map_lock_[shard_id]);
  return target_free_addr;
#else
  spin_lock(&prio_slot_addr_map_lock_);
  auto it = prio_slot_addr_map_.begin();
  for (; it->second.size() == 0 && it != prio_slot_addr_map_.end();) {
    it = prio_slot_addr_map_.erase(it);
  }

  printd(L_DEBUG, "Evict prio: %lf", it->first);
  Slot* target_slot_ptr = (Slot*)(*it->second.begin());
  Slot target_slot;
  memcpy(&target_slot, target_slot_ptr, sizeof(Slot));
  uint64_t target_free_addr =
      HashIndexConvert48To64Bits(target_slot.atomic.pointer);
  int ret = __sync_bool_compare_and_swap((uint64_t*)target_slot_ptr,
                                         *(uint64_t*)&target_slot, 0);
  if (ret != true) {
    // printf("Evict conflict!\n");
    spin_unlock(&prio_slot_addr_map_lock_);
    goto cliquemap_evict_retry;
  }
  it->second.erase(it->second.begin());
  memset(&target_slot_ptr->meta, 0, sizeof(SlotMeta));
  spin_unlock(&prio_slot_addr_map_lock_);
  return target_free_addr;
#endif
}

uint64_t Server::evict_sample() {
  printd(L_DEBUG, "Evict!");
  int ret = 0;
  uint64_t table_base_addr = mm_->get_base_addr();

  int num_sampled_keys = 0;
  Slot* sampled_slot_ptr[num_samples_];
  Slot sampled_slots[num_samples_];
  std::map<double, int> sorted_idx;
  while (num_sampled_keys < num_samples_) {
    uint64_t rand_hash = random();
    uint64_t bucket_id = rand_hash % HASH_NUM_BUCKETS;
    Slot* slot = (Slot*)(table_base_addr + bucket_id * sizeof(Bucket));
    for (int i = 0;
         i < HASH_BUCKET_ASSOC_NUM && num_sampled_keys < num_samples_; i++) {
      if (HashIndexConvert48To64Bits(slot[i].atomic.pointer) == 0) {
        continue;
      }
      double prio =
          priority_->parse_priority(&slot[i].meta, slot[i].atomic.kv_len);
      sorted_idx[prio] = num_sampled_keys;
      sampled_slot_ptr[num_sampled_keys] = &slot[i];
      memcpy(&sampled_slots[num_sampled_keys], &slot[i], sizeof(Slot));
      num_sampled_keys++;
    }
  }

  // traverse the sorted idx
  auto it = sorted_idx.begin();
  for (; it != sorted_idx.end(); it++) {
    printd(L_DEBUG, "priority: %f", it->first);
    Slot* target_slot_ptr = sampled_slot_ptr[it->second];
    uint64_t target_free_addr =
        HashIndexConvert48To64Bits(target_slot_ptr->atomic.pointer);
    ret = __sync_bool_compare_and_swap(
        (uint64_t*)target_slot_ptr, *(uint64_t*)&sampled_slots[it->second], 0);
    if (ret == true) {
      memset(&target_slot_ptr->meta, 0, sizeof(SlotMeta));
      return target_free_addr;
    }
  }

  return 0;
}

void Server::init_counters() {
  num_evict_dequeue_ = 0;
  num_evict_ = 0;
}

int Server::evict_local_cmm(uint32_t worker_id) {
  printd(L_DEBUG, "Evict!");
  // printf("Server evict!\n");

  uint64_t free_addr = 0;
  while (free_addr == 0)
    free_addr = evict();

  c_mm_[worker_id]->free(free_addr, mm_->get_rkey(), block_size_, 0);
  return 0;
}

int Server::get(void* key,
                uint32_t key_size,
                __OUT void* val,
                __OUT uint32_t* val_size) {
  Slot* match_slot = kv_search_slot(key, key_size);
  if (match_slot != NULL) {
    uint64_t kv_addr = HashIndexConvert48To64Bits(match_slot->atomic.pointer);
    ObjHeader* header = (ObjHeader*)kv_addr;
    uint32_t key_len = header->key_size;
    uint32_t val_len = header->val_size;
    *val_size = val_len;
    memcpy(val, (void*)(kv_addr + sizeof(ObjHeader) + key_len), val_len);
    return 0;
  }
  return -1;
}

void Server::p_update_priority(Slot* slot) {
  uint32_t info_update_mask = priority_->info_update_mask(&slot->meta);

  // 1. update slot meta
  update_priority_sample(slot);

  // 2. update list
  double new_priority =
      priority_->parse_priority(&slot->meta, slot->atomic.kv_len);
  uint32_t slot_id = get_slot_id(slot);
  prio_list_->local_update(slot_id, new_priority);
}

int Server::p_set(uint32_t worker_id,
                  void* key,
                  uint32_t key_size,
                  void* val,
                  uint32_t val_size) {
  // see if need allocate new memory
  uint32_t kv_size = key_size + val_size + sizeof(ObjHeader);
  RemoteBlock rb;
  int ret = c_mm_[worker_id]->alloc(kv_size, &rb);
  if (ret == -1) {
    if (local_alloc_segment(worker_id) == -1) {
      printf("Should not be evicting things!\n");
      abort();
    }
    ret = c_mm_[worker_id]->alloc(kv_size, &rb);
  }
  assert(ret == 0);
  ObjHeader* header = (ObjHeader*)rb.addr;
  header->key_size = key_size;
  header->val_size = val_size;
  memcpy((void*)(rb.addr + sizeof(ObjHeader)), key, key_size);
  memcpy((void*)(rb.addr + sizeof(ObjHeader) + key_size), val, val_size);

  // calculate hash
  uint64_t key_hash = hash_->hash_func1(key, key_size);
  uint8_t fp = HashIndexComputeFp(key_hash);

  uint64_t bucket_id = key_hash % HASH_NUM_BUCKETS;
  Slot* slot = (Slot*)(mm_->get_base_addr() + bucket_id * sizeof(Bucket));

  // try to find the match key and an empty slot
  bool key_found = false;
  Slot* key_match_slot = NULL;
  std::vector<int> empty_slot_idx_list;
  std::vector<std::pair<double, int>> slotIdx_prio_list;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    printd(L_DEBUG, "Inspecting slot 0x%lx %lx", (uint64_t)&slot[i],
           *(uint64_t*)&slot[i]);
    double cur_prio =
        priority_->parse_priority(&slot[i].meta, slot[i].atomic.kv_len);
    std::pair<double, int> p(cur_prio, i);
    uint64_t kv_addr = HashIndexConvert48To64Bits(slot[i].atomic.pointer);
    if (slot[i].atomic.kv_len == 0) {
      empty_slot_idx_list.push_back(i);
      continue;
    }
    if (slot[i].meta.acc_info.key_hash != key_hash) {
      slotIdx_prio_list.push_back(p);
      continue;
    }
    printd(L_DEBUG, "kv_addr: %lx", kv_addr);
    ObjHeader* header = (ObjHeader*)kv_addr;
    uint32_t key_len = header->key_size;
    uint32_t val_len = header->val_size;
    if (is_key_match(key, key_size, (void*)(kv_addr + sizeof(ObjHeader)),
                     key_len)) {
      // found
      printd(L_DEBUG, "server found locally!");
      key_match_slot = &slot[i];
      key_found = true;
      break;
    }
  }

  Slot new_slot;
  new_slot.atomic.fp = fp;
  new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(rb.addr, new_slot.atomic.pointer);

  Slot* match_slot = NULL;
  if (key_found) {
    uint64_t old_block_addr =
        HashIndexConvert48To64Bits(key_match_slot->atomic.pointer);
    assert(old_block_addr != 0);

    // atomic cas
    ret = __sync_bool_compare_and_swap((uint64_t*)key_match_slot,
                                       *(uint64_t*)key_match_slot,
                                       *(uint64_t*)&new_slot);
    assert(ret == true);

    // free the rb
    c_mm_[worker_id]->free(old_block_addr, mm_->get_rkey(), block_size_,
                           server_id_);

    match_slot = key_match_slot;
  } else if (empty_slot_idx_list.size() != 0) {
    uint32_t rand_id = rand() % empty_slot_idx_list.size();
    Slot* target_slot = &slot[empty_slot_idx_list[rand_id]];

    // modify slot content
    ret = __sync_bool_compare_and_swap(
        (uint64_t*)target_slot, *(uint64_t*)target_slot, *(uint64_t*)&new_slot);
    assert(ret == true);

    match_slot = target_slot;
    memset(&match_slot->meta, 0, sizeof(SlotMeta));
    match_slot->meta.acc_info.key_hash = key_hash;
  } else {
    printf("Should not be evicting buckets\n");
    abort();
  }

  // update priority
  p_update_priority(match_slot);

  return 0;
}

int Server::set(uint32_t worker_id,
                void* key,
                uint32_t key_size,
                void* val,
                uint32_t val_size) {
  // see if need allocate new memory
  uint32_t kv_size = key_size + val_size + sizeof(ObjHeader);
  RemoteBlock rb;
  int ret = c_mm_[worker_id]->alloc(kv_size, &rb);
  if (ret == -1) {
    ret = local_alloc_segment(worker_id);
    while (ret == -1) {
      ret = evict_local_cmm(worker_id);
    }
    ret = c_mm_[worker_id]->alloc(kv_size, &rb);
  }
  assert(ret == 0);
  ObjHeader* header = (ObjHeader*)rb.addr;
  header->key_size = key_size;
  header->val_size = val_size;
  memcpy((void*)(rb.addr + sizeof(ObjHeader)), key, key_size);
  memcpy((void*)(rb.addr + sizeof(ObjHeader) + key_size), val, val_size);

  // calculate hash
  uint64_t key_hash = hash_->hash_func1(key, key_size);
  uint64_t bucket_id = key_hash % HASH_NUM_BUCKETS;
  uint8_t fp = HashIndexComputeFp(key_hash);
  Slot* slot_ptr = (Slot*)(mm_->get_base_addr() + bucket_id * sizeof(Bucket));

cliquemap_set_retry:

  // try to find the match key and an empty slot
  bool key_found = false;
  Slot* key_match_slot_ptr = NULL;
  Slot key_match_slot;
  memset(&key_match_slot, 0, sizeof(Slot));
  std::vector<int> empty_slot_idx_list;
  std::vector<std::pair<double, int>> slotIdx_prio_list;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    Slot tmp_slot;
    memcpy(&tmp_slot, &slot_ptr[i], sizeof(Slot));
    printd(L_DEBUG, "Inspecting slot 0x%lx %lx", (uint64_t)&tmp_slot,
           *(uint64_t*)&tmp_slot);
    double cur_prio =
        priority_->parse_priority(&tmp_slot.meta, tmp_slot.atomic.kv_len);
    std::pair<double, int> p(cur_prio, i);
    uint64_t kv_addr = HashIndexConvert48To64Bits(tmp_slot.atomic.pointer);
    if (tmp_slot.atomic.kv_len == 0) {
      assert(kv_addr == 0);
      empty_slot_idx_list.push_back(i);
      continue;
    }
    if (tmp_slot.atomic.fp != fp) {
      slotIdx_prio_list.push_back(p);
      continue;
    }
    printd(L_DEBUG, "kv_addr: %lx", kv_addr);
    ObjHeader* header = (ObjHeader*)kv_addr;
    uint32_t key_len = header->key_size;
    uint32_t val_len = header->val_size;
    if (is_key_match(key, key_size, (void*)(kv_addr + sizeof(ObjHeader)),
                     key_len)) {
      // found
      printd(L_DEBUG, "server found locally!");
      key_match_slot_ptr = &slot_ptr[i];
      memcpy(&key_match_slot, &tmp_slot, sizeof(Slot));
      key_found = true;
      break;
    }
    slotIdx_prio_list.push_back(p);
  }

  Slot new_slot;
  new_slot.atomic.fp = fp;
  new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(rb.addr, new_slot.atomic.pointer);

  Slot* match_slot = NULL;
  bool set_miss = true;
  if (key_found) {
    uint64_t old_block_addr =
        HashIndexConvert48To64Bits(key_match_slot.atomic.pointer);
    assert(old_block_addr != 0);

    // atomic cas
    ret = __sync_bool_compare_and_swap((uint64_t*)key_match_slot_ptr,
                                       *(uint64_t*)&key_match_slot,
                                       *(uint64_t*)&new_slot);
    if (ret != true) {
      // printf("Update - Set conflict!\n");
      goto cliquemap_set_retry;
    }

    // free the rb
    c_mm_[worker_id]->free(old_block_addr, mm_->get_rkey(), block_size_,
                           server_id_);

    match_slot = key_match_slot_ptr;
    set_miss = false;
  } else if (empty_slot_idx_list.size() != 0) {
    uint32_t rand_id = rand() % empty_slot_idx_list.size();
    Slot* target_slot_addr = &slot_ptr[empty_slot_idx_list[rand_id]];
    Slot empty_slot;
    memset(&empty_slot, 0, sizeof(Slot));

    // modify slot content
    // printf("CAS: @%lx(%lx) %lx -> %lx\n",
    // target_slot_addr, *(uint64_t *)target_slot_addr,
    // *(uint64_t *)&empty_slot, *(uint64_t *)&new_slot);
    ret = __sync_bool_compare_and_swap((uint64_t*)target_slot_addr,
                                       *(uint64_t*)&empty_slot,
                                       *(uint64_t*)&new_slot);
    if (ret != true) {
      // printf("Insert - Set conflict!\n");
      goto cliquemap_set_retry;
    }

    match_slot = target_slot_addr;
    memset(&match_slot->meta, 0, sizeof(SlotMeta));
    match_slot->meta.acc_info.key_hash = key_hash;
  } else {
    // printf("server evict bucket!\n");
    // evict bucket
    if (slotIdx_prio_list.size() == 0) {
      // Failed to evict bucket
      printd(L_ERROR, "server failed to evict bucket!");
      abort();
    }
    int success_idx = 0;
    std::sort(slotIdx_prio_list.begin(), slotIdx_prio_list.end());
    int target_slot_id = slotIdx_prio_list[success_idx].second;
    Slot* target_slot_addr = &slot_ptr[target_slot_id];
    Slot target_slot;
    memcpy(&target_slot, target_slot_addr, sizeof(Slot));

    uint64_t evict_addr =
        HashIndexConvert48To64Bits(target_slot.atomic.pointer);

    // atomic cas
    ret = __sync_bool_compare_and_swap((uint64_t*)target_slot_addr,
                                       *(uint64_t*)&target_slot,
                                       *(uint64_t*)&new_slot);
    if (ret != true) {
      // printf("Evict Bucket - Set conflict!\n");
      goto cliquemap_set_retry;
    }

    if (evict_addr != 0)
      c_mm_[worker_id]->free(evict_addr, mm_->get_rkey(), block_size_, 0);

    match_slot = target_slot_addr;
    memset(&match_slot->meta, 0, sizeof(SlotMeta));
    match_slot->meta.acc_info.key_hash = key_hash;
  }

  // update priority
  update_priority(match_slot);

  return set_miss ? -1 : 0;
}

void Server::merge_priority(Slot* slot, uint64_t ts) {
  if (is_evict_precise_)
    return merge_priority_precise(slot, ts);
  return merge_priority_sample(slot, ts);
}

void Server::merge_priority_precise(Slot* slot, uint64_t ts) {
  uint32_t info_update_mask = priority_->info_update_mask(&slot->meta);
  double old_priority =
      priority_->parse_priority(&slot->meta, slot->atomic.kv_len);
  merge_acc_info(slot, info_update_mask, ts);
  double new_priority =
      priority_->parse_priority(&slot->meta, slot->atomic.kv_len);

  adjust_prio_slot_map(slot, old_priority, new_priority);
}

void Server::merge_priority_sample(Slot* slot, uint64_t ts) {
  // update access information
  uint32_t info_update_mask = priority_->info_update_mask(&slot->meta);
  merge_acc_info(slot, info_update_mask, ts);
}

void Server::update_priority(Slot* slot) {
  if (is_evict_precise_)
    return update_priority_precise(slot);
  return update_priority_sample(slot);
}

void Server::update_priority_precise(Slot* slot) {
  uint32_t info_update_mask = priority_->info_update_mask(&slot->meta);
  double old_priority =
      priority_->parse_priority(&slot->meta, slot->atomic.kv_len);
  update_acc_info(slot, info_update_mask);
  double new_priority =
      priority_->parse_priority(&slot->meta, slot->atomic.kv_len);

  adjust_prio_slot_map(slot, old_priority, new_priority);
}

void Server::update_priority_sample(Slot* slot) {
  // update access information
  uint32_t info_update_mask = priority_->info_update_mask(&slot->meta);
  update_acc_info(slot, info_update_mask);
}

void* Server::thread_main() {
  struct sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(struct sockaddr_in);
  UDPMsg request;
  int rc = 0;
  while (!need_stop_) {
    rc = nm_->recv_udp_msg(&request, &client_addr, &client_addr_len);
    if (rc) {
      continue;
    }

    deserialize_udpmsg(&request);

    if (request.type == UDPMSG_REQ_CONNECT) {
      rc = server_on_connect(&request, &client_addr, client_addr_len);
      assert(rc == 0);
    } else if (request.type == UDPMSG_REQ_ALLOC) {
      rc = server_on_alloc(&request, &client_addr, client_addr_len);
      assert(rc == 0);
    } else if (request.type == UDPMSG_REQ_TS) {
      rc = server_on_get_ts(&request, &client_addr, client_addr_len);
    } else {
      printd(L_ERROR, "Unsupported message type: %d", request.type);
    }
  }
  return NULL;
}

int Server::server_on_recv_msg_priority(const struct ibv_wc* wc) {
  uint16_t req_sid = *(uint16_t*)((uint64_t)recv_msg_buffer_ +
                                  wc->wr_id * MSG_BUF_SIZE + sizeof(uint8_t));
  uint64_t msg_laddr = (uint64_t)recv_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE +
                       sizeof(uint8_t) + sizeof(uint16_t);
  uint8_t num_keys = *(uint32_t*)msg_laddr;

  msg_laddr += sizeof(uint32_t);
  uint64_t st_addr = msg_laddr;
  for (int i = 0; i < num_keys; i++) {
    uint64_t ts = *(uint64_t*)st_addr;
    uint8_t key_size = *(uint8_t*)(st_addr + sizeof(uint64_t));
    void* key = (void*)(st_addr + sizeof(uint64_t) + sizeof(uint8_t));

    Slot* match_slot = kv_search_slot(key, key_size);
    if (match_slot != NULL) {
      merge_priority(match_slot, ts);
    }

    st_addr += sizeof(uint64_t) + sizeof(uint8_t) + key_size;
  }

  // reply
  int rep = 0;
  int ret =
      server_reply_to_sid(wc, req_sid, &rep, sizeof(int), IBMSG_REP_PRIORITY);
  assert(ret == 0);
  return 0;
}

int Server::server_on_recv_msg_get(const struct ibv_wc* wc) {
  printd(L_INFO, "server get");
  uint16_t req_sid = *(uint16_t*)((uint64_t)recv_msg_buffer_ +
                                  wc->wr_id * MSG_BUF_SIZE + sizeof(uint8_t));
  uint64_t msg_laddr = (uint64_t)recv_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE +
                       sizeof(uint8_t) +
                       sizeof(uint16_t);  // +1 is the msg type (uint8) size, +
                                          // sizeof(uint16_t) is the server id
  uint32_t key_size = *(uint32_t*)msg_laddr;
  void* key = (void*)(msg_laddr + sizeof(uint32_t));

  char tmp_buf[1024];
  uint32_t tmp_size = 0;
  int ret = get(key, key_size, tmp_buf, &tmp_size);

  char ret_msg[1024] = {0};
  if (ret == 0) {
    assert(tmp_size != 0);
    *(uint32_t*)ret_msg = tmp_size;
    memcpy((void*)((uint64_t)ret_msg + sizeof(uint32_t)), tmp_buf, tmp_size);
  }
  ret = server_reply_to_sid(wc, req_sid, ret_msg, tmp_size + sizeof(uint32_t),
                            IBMSG_REP_GET);
  assert(ret == 0);
  return 0;
}

int Server::server_on_recv_msg_set(uint32_t worker_id,
                                   const struct ibv_wc* wc) {
  uint16_t req_sid = *(uint16_t*)((uint64_t)recv_msg_buffer_ +
                                  wc->wr_id * MSG_BUF_SIZE + sizeof(uint8_t));
  uint64_t msg_laddr = (uint64_t)recv_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE +
                       sizeof(uint8_t) +
                       sizeof(uint16_t);  // +1 is the msg type (uint8) size, +
                                          // sizeof(uint16_t) is the server id
  uint32_t key_size = *(uint32_t*)msg_laddr;
  uint32_t val_size = *(uint32_t*)(msg_laddr + sizeof(uint32_t));
  void* key = (void*)(msg_laddr + 2 * sizeof(uint32_t));
  void* val = (void*)(msg_laddr + 2 * sizeof(uint32_t) + key_size);

  int ret = set(worker_id, key, key_size, val, val_size);

  // reply
  ret = server_reply_to_sid(wc, req_sid, &ret, sizeof(int), IBMSG_REP_SET);
  assert(ret == 0);
  return 0;
}

int Server::server_on_recv_msg_pset(uint32_t worker_id,
                                    const struct ibv_wc* wc) {
  uint16_t req_sid = *(uint16_t*)((uint64_t)recv_msg_buffer_ +
                                  wc->wr_id * MSG_BUF_SIZE + sizeof(uint8_t));
  uint64_t msg_laddr = (uint64_t)recv_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE +
                       sizeof(uint8_t) +
                       sizeof(uint16_t);  // +1 is the msg type (uint8) size, +
                                          // sizeof(uint16_t) is the server id
  uint32_t key_size = *(uint32_t*)msg_laddr;
  uint32_t val_size = *(uint32_t*)(msg_laddr + sizeof(uint32_t));
  void* key = (void*)(msg_laddr + 2 * sizeof(uint32_t));
  void* val = (void*)(msg_laddr + 2 * sizeof(uint32_t) + key_size);

  int ret = p_set(worker_id, key, key_size, val, val_size);

  // reply
  ret = server_reply_to_sid(wc, req_sid, &ret, sizeof(int), IBMSG_REP_PSET);
  assert(ret == 0);
  return 0;
}

int Server::server_on_recv_msg_alloc(const struct ibv_wc* wc) {
  uint16_t req_sid = *(uint16_t*)((uint64_t)recv_msg_buffer_ +
                                  wc->wr_id * MSG_BUF_SIZE + sizeof(uint8_t));
  uint64_t msg_laddr = (uint64_t)recv_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE +
                       sizeof(uint8_t) + sizeof(uint16_t);

  SegmentInfo info;
  int ret = 0;
  ret = mm_->alloc_segment(req_sid, &info);
  if (ret == -1)
    printd(L_INFO, "Server run out-of-memory");
  printd(L_DEBUG, "Allocated addr: 0x%lx to %d", info.addr, info.allocated_to);

  ret = server_reply_to_sid(wc, req_sid, &info.addr, sizeof(uint64_t),
                            IBMSG_REP_ALLOC);
  assert(ret == 0);
  return 0;
}

int Server::server_on_recv_msg_merge(const struct ibv_wc* wc) {
  uint16_t req_sid = *(uint16_t*)((uint64_t)recv_msg_buffer_ +
                                  wc->wr_id * MSG_BUF_SIZE + sizeof(uint8_t));
  uint64_t msg_laddr = (uint64_t)recv_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE +
                       sizeof(uint8_t) + sizeof(uint16_t);
  uint32_t reward_sync_size_ =
      sizeof(float) * num_experts_ + sizeof(uint8_t) + sizeof(uint16_t);

  float* rewards = (float*)msg_laddr;
  float sum = 0;
  for (int i = 0; i < num_experts_; i++) {
    expert_weights_[i] *= std::exp(rewards[i]);
    // expert_weights_[i] = expert_weights_[i] > 0.99 ? 0.99 :
    // expert_weights_[i]; expert_weights_[i] = expert_weights_[i] < 0.01 ? 0.01
    // : expert_weights_[i];
    sum += expert_weights_[i];
  }
  for (int i = 0; i < num_experts_; i++)
    expert_weights_[i] /= sum;
  sum = 0;
  // clipe the maximum value
  for (int i = 0; i < num_experts_; i++) {
    expert_weights_[i] = expert_weights_[i] > 0.99 ? 0.99 : expert_weights_[i];
    expert_weights_[i] = expert_weights_[i] < 0.01 ? 0.01 : expert_weights_[i];
    sum += expert_weights_[i];
  }
  for (int i = 0; i < num_experts_; i++)
    expert_weights_[i] /= sum;
  int ret = server_reply_to_sid(wc, req_sid, expert_weights_,
                                sizeof(float) * num_experts_, IBMSG_REP_MERGE);
  assert(ret == 0);

  // std::vector<float> tmp_vec;
  // for (int i = 0; i < num_experts_; i ++)
  //     tmp_vec.push_back(expert_weights_[i]);
  // weight_vec_.push_back(tmp_vec);
  return 0;
}

void* Server::worker_main(uint32_t worker_id) {
  printd(L_INFO, "Worker main started!");
  int ret = 0;
  int num_polled = 0;
  uint64_t ops_executed = 0;
  while (!need_stop_) {
    struct ibv_wc wc[256 / num_workers_];
    num_polled = nm_->rdma_poll_recv_completion_async(wc, 256 / num_workers_);
    for (int i = 0; i < num_polled; i++) {
      if (wc[i].status != IBV_WC_SUCCESS) {
        printd(L_ERROR, "%lx", rr_list_[wc[i].wr_id].sg_list->addr);
        printd(L_ERROR,
               "OP (%ld): Server %d polled one error wc(wr_id: %ld, status: "
               "%d, opcode: %d)",
               ops_executed, worker_id, wc[i].wr_id, wc[i].status,
               wc[i].opcode);
        assert(0);
      }
      if (wc[i].opcode != IBV_WC_RECV) {
        printd(L_ERROR, "Expected WR %d Polled %d, wr_id: %ld", IBV_WC_RECV,
               wc[i].opcode, wc[i].wr_id);
        assert(0);
      }
      // process request
      uint64_t msg_lbuf =
          (uint64_t)recv_msg_buffer_ + wc[i].wr_id * MSG_BUF_SIZE;
      uint8_t msg_type = *(uint8_t*)msg_lbuf;
      uint16_t req_sid = *(uint16_t*)(msg_lbuf + sizeof(uint8_t));
      printd(L_DEBUG, "msg: %d", msg_type);
      switch (msg_type) {
        case IBMSG_REQ_PRIORITY:
          ret = server_on_recv_msg_priority(&wc[i]);
          assert(ret == 0);
          break;
        case IBMSG_REQ_SET:
          ret = server_on_recv_msg_set(worker_id, &wc[i]);
          assert(ret == 0);
          break;
        case IBMSG_REQ_GET:
          ret = server_on_recv_msg_get(&wc[i]);
          assert(ret == 0);
          break;
        case IBMSG_REQ_MERGE:
          ret = server_on_recv_msg_merge(&wc[i]);
          assert(ret == 0);
          break;
        case IBMSG_REQ_ALLOC:
          ret = server_on_recv_msg_alloc(&wc[i]);
          assert(ret == 0);
          break;
        case IBMSG_REQ_PSET:
          ret = server_on_recv_msg_pset(worker_id, &wc[i]);
          assert(ret == 0);
          break;
        default:
          printd(L_ERROR, "Message %d not supported!", msg_type);
          abort();
      }
      ops_executed++;
    }
  }
  return NULL;
}

void Server::stop() {
  // set stop flag
  need_stop_ = 1;

  // wait worker to exit
  for (int i = 0; i < num_workers_; i++) {
    pthread_join(worker_tid_[i], NULL);
  }
}

int Server::server_reply_to_sid(const struct ibv_wc* wc,
                                uint16_t sid,
                                void* ret_val,
                                uint32_t val_size,
                                uint8_t ibmsg_type) {
  assert(rr_list_[wc->wr_id].wr_id == wc->wr_id);
  int ret = nm_->rdma_post_recv_sid_async(&rr_list_[wc->wr_id], sid);
  assert(ret == 0);

  uint64_t msg_buffer = (uint64_t)send_msg_buffer_ + wc->wr_id * MSG_BUF_SIZE;
  *(uint8_t*)msg_buffer = ibmsg_type;
  memcpy((void*)((uint64_t)msg_buffer + sizeof(uint8_t)), ret_val, val_size);

  struct ibv_send_wr* sr = &sr_list_[wc->wr_id];
  struct ibv_sge* sge = &sr_sge_list_[wc->wr_id];
  sge->length = sizeof(uint8_t) + val_size;
  ret = nm_->rdma_post_send_sid_sync(sr, sid);
  if (ret != 0) {
    printd(L_ERROR, "rdma_post_send_sid_async return %d(%s) %d", ret,
           strerror(ret), send_counter_);
  }
  assert(ret == 0);
  return 0;
}

void Server::get_adaptive_weights(std::vector<float>& adaptive_weights) {
  if (eviction_type_ != EVICT_SAMPLE_ADAPTIVE)
    return;
  adaptive_weights.clear();
  for (int i = 0; i < num_experts_; i++)
    adaptive_weights.push_back(expert_weights_[i]);
}

void Server::get_adaptive_weight_vec(
    std::vector<std::vector<float>>& weight_vec) {
  if (eviction_type_ != EVICT_SAMPLE_ADAPTIVE)
    return;
  weight_vec = weight_vec_;
}

void Server::update_acc_info(Slot* slot, uint32_t info_update_mask) {
  if (info_update_mask & UPD_FREQ) {
    __sync_fetch_and_add(&slot->meta.acc_info.freq, 1);
  }
  if (info_update_mask & UPD_COST || info_update_mask & UPD_LAT ||
      info_update_mask & UPD_TS) {
    slot->meta.acc_info.acc_ts = new_ts();
  }
}

void Server::merge_acc_info(Slot* slot,
                            uint32_t info_update_mask,
                            uint64_t ts) {
  if (info_update_mask & UPD_FREQ) {
    slot->meta.acc_info.freq++;
  }
  if (info_update_mask & UPD_COST || info_update_mask & UPD_LAT ||
      info_update_mask & UPD_TS) {
    slot->meta.acc_info.acc_ts =
        slot->meta.acc_info.acc_ts > ts ? slot->meta.acc_info.acc_ts : ts;
  }
}

void Server::adjust_prio_slot_map(Slot* slot,
                                  double old_priority,
                                  double new_priority) {
#ifdef USE_SHARD_PQUEUE
  uint64_t slot_addr = (uint64_t)slot;
  uint32_t shard_id =
      hash_->hash_func1(&slot_addr, sizeof(uint64_t)) % NUM_PQUEUE_SHARDS;
  spin_lock(&shard_prio_slot_addr_map_lock_[shard_id]);
  // erase the item from the old priority
  shard_prio_slot_addr_map_[shard_id][old_priority].erase((uint64_t)slot);

  // insert the item to the new priority
  shard_prio_slot_addr_map_[shard_id][new_priority].insert((uint64_t)slot);
  spin_unlock(&shard_prio_slot_addr_map_lock_[shard_id]);
#else
  spin_lock(&prio_slot_addr_map_lock_);
  // erase the item from the old priority
  prio_slot_addr_map_[old_priority].erase((uint64_t)slot);

  // insert the item to the new priority
  prio_slot_addr_map_[new_priority].insert((uint64_t)slot);
  spin_unlock(&prio_slot_addr_map_lock_);
#endif
}

uint32_t Server::get_slot_id(Slot* slot) {
  return ((uint64_t)slot - mm_->get_base_addr()) / sizeof(Slot);
}