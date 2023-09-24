#include "client.h"
#include "debug.h"
#include "dmc_table.h"
#include "ib.h"

#include <assert.h>
#include <stddef.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <vector>

DMCClient::DMCClient(const DMCConfig* conf) {
  evict_bucket_cnt_.clear();
  srand(conf->server_id);
  my_sid_ = conf->server_id;
  num_servers_ = conf->memory_num;
  server_base_addr_ = conf->server_base_addr;
  eviction_type_ = conf->eviction_type;
  priority_type_ = conf->eviction_priority;
  num_samples_ = conf->num_samples;
  server_oom_ = false;
  block_size_ = conf->block_size;
  log_f_ = fopen("op_log.log", "w");
  assert(log_f_ != NULL);

  // counters
  clear_counters();

  // freq_cache
  use_freq_cache_ = conf->use_freq_cache;
  freq_cache_size_ = conf->freq_cache_size;
  freq_cache_ = new FreqCache(freq_cache_size_);

  // local kv operation buffer
  local_buf_size_ = conf->client_local_size;
  local_buf_ = malloc(local_buf_size_);
  assert(local_buf_ != NULL);

  transient_buf_ = malloc(1024 * 1024);
  assert(transient_buf_ != NULL);

  nm_ = new UDPNetworkManager(conf);
  mm_ = new ClientUniformMM(conf);
  hash_ = dmc_new_hash(conf->hash_type);
  assert(hash_ != NULL);

  if (!is_evict_adaptive(eviction_type_)) {
    priority_ = dmc_new_priority(priority_type_);
    assert(priority_ != NULL);
  }

  int ret = connect_all_rc_qp();
  assert(ret == 0);

  sys_start_ts_ = get_sys_start_ts();
  printd(L_INFO, "start ts: %lx", sys_start_ts_);
  set_sys_start_ts(sys_start_ts_);
  printd(L_INFO, "new_ts32: %x", new_ts32());

  // allocate local mr
  struct ibv_pd* pd = nm_->get_ib_pd();
  local_buf_mr_ =
      ibv_reg_mr(pd, local_buf_, local_buf_size_,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  assert(local_buf_mr_ != NULL);
  transient_buf_mr_ =
      ibv_reg_mr(pd, transient_buf_, 1024 * 1024,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  printd(L_DEBUG, "Client mr allocated (%p, %ld, lkey: 0x%x, rkey: 0x%x)",
         local_buf_mr_->addr, local_buf_mr_->length, local_buf_mr_->lkey,
         local_buf_mr_->rkey);

  if (eviction_type_ != EVICT_CLIQUEMAP) {
    KVOpsCtx tmp_ctx;
    memset(&tmp_ctx, 0, sizeof(KVOpsCtx));
    tmp_ctx.op_laddr = (uint64_t)local_buf_;
#ifndef DEFER_ALLOC
    ret = alloc_segment(&tmp_ctx);
#endif
    assert(ret == 0);
  }

  init_eviction(conf);
}

DMCClient::~DMCClient() {
  delete nm_;
  delete mm_;
}

int DMCClient::init_eviction(const DMCConfig* conf) {
  if (eviction_type_ == EVICT_CLIQUEMAP) {
    gettimeofday(&last_sync_time_, NULL);
    num_experts_ = 1;
    struct ibv_pd* pd = nm_->get_ib_pd();
    // clique map initialization
    ts_buf_ = malloc(MSG_BUF_SIZE);
    assert(ts_buf_ != NULL);
    ts_buf_mr_ = ibv_reg_mr(pd, ts_buf_, MSG_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE);
    assert(ts_buf_mr_ != NULL);
    // construct ts message buffer
    // format [8b: msg_type, 16b: server_id, 8b: number of record]
    memset(ts_buf_, 0, MSG_BUF_SIZE);
    *(uint8_t*)ts_buf_ = IBMSG_REQ_PRIORITY;
    *(uint16_t*)((uint64_t)ts_buf_ + sizeof(uint8_t)) = my_sid_;
    *(uint32_t*)((uint64_t)ts_buf_ + sizeof(uint8_t) + sizeof(uint16_t)) = 0;
    ts_buf_off_ = sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint32_t);

    ts_rep_buf_ = malloc(block_size_);
    assert(ts_rep_buf_ != NULL);
    ts_rep_buf_mr_ =
        ibv_reg_mr(pd, ts_rep_buf_, block_size_, IBV_ACCESS_LOCAL_WRITE);
    assert(ts_rep_buf_mr_ != NULL);
  } else if (eviction_type_ == EVICT_PRECISE) {
    num_experts_ = 1;
    use_freq_cache_ = false;
    struct ibv_pd* pd = nm_->get_ib_pd();
    list_op_buf_ = malloc(1024 * 1024);
    assert(list_op_buf_ != NULL);
    list_op_buf_mr_ =
        ibv_reg_mr(pd, list_op_buf_, 1024 * 1024, IBV_ACCESS_LOCAL_WRITE);
    assert(list_op_buf_mr_ != NULL);
    prio_list_ =
        new RList(HASH_BUCKET_ASSOC_NUM * HASH_NUM_BUCKETS,
                  server_base_addr_ + HASH_SPACE_SIZE, server_rkey_map_[0],
                  list_op_buf_, 1024 * 1024U, list_op_buf_mr_->lkey, CLIENT);
    printd(L_INFO, "list end at %lx",
           server_base_addr_ + HASH_SPACE_SIZE + prio_list_->size());
  } else if (eviction_type_ == EVICT_NON) {
    num_experts_ = 1;
    experts_[0] = priority_;
    num_samples_ = 1;
  } else if (is_evict_adaptive(eviction_type_)) {
    use_async_weight_ = conf->use_async_weight;
    num_experts_ = conf->num_experts;
    for (int i = 0; i < conf->num_experts; i++)
      experts_[i] = dmc_new_priority(conf->experts[i]);
    learning_rate_ = conf->learning_rate;

    struct ibv_pd* pd = nm_->get_ib_pd();

    weights_raddr_ = server_base_addr_ + HASH_SPACE_SIZE;
    weights_sync_size = sizeof(float) * num_experts_ + sizeof(uint8_t);
    weights_sync_buf_ = malloc(weights_sync_size);
    assert(weights_sync_buf_ != NULL);
    r_weights_ = (float*)((uint64_t)weights_sync_buf_ + sizeof(uint8_t));
    weights_sync_buf_mr_ = ibv_reg_mr(pd, weights_sync_buf_, weights_sync_size,
                                      IBV_ACCESS_LOCAL_WRITE);
    assert(weights_sync_buf_mr_ != NULL);

    uint32_t cache_size = (HASH_NUM_BUCKETS * HASH_BUCKET_ASSOC_NUM) / 8;
    base_reward_ = std::pow(0.005, (1.0 / cache_size));
    reward_sync_size_ =
        sizeof(float) * num_experts_ + sizeof(uint8_t) + sizeof(uint16_t);
    reward_sync_size_ = MSG_BUF_SIZE;
    reward_sync_buf_ = malloc(reward_sync_size_);
    memset(reward_sync_buf_, 0, reward_sync_size_);
    reward_buf_ = (float*)((uint64_t)reward_sync_buf_ + sizeof(uint8_t) +
                           sizeof(uint16_t));
    memset(reward_buf_, 0, sizeof(float) * num_experts_);
    *(uint8_t*)reward_sync_buf_ = IBMSG_REQ_MERGE;
    *(uint16_t*)((uint64_t)reward_sync_buf_ + sizeof(uint8_t)) = my_sid_;
    reward_sync_buf_mr_ = ibv_reg_mr(pd, reward_sync_buf_, reward_sync_size_,
                                     IBV_ACCESS_LOCAL_WRITE);
    assert(reward_sync_buf_mr_ != NULL);
    local_reward_cntr_ = 0;

    // allocate history
    uint64_t history_raddr =
        server_base_addr_ + HASH_SPACE_SIZE + sizeof(float) * num_experts_;
    if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE) {
      history_size_ = cache_size;
      printd(L_INFO, "history_raddr: %lx", history_raddr);
      lw_history_ = new LWHistory(history_size_, history_raddr, CLIENT);
    } else {
      assert(eviction_type_ == EVICT_SAMPLE_ADAPTIVE_NAIVE ||
             eviction_type_ == EVICT_SAMPLE_ADAPTIVE_HEAVY);
      history_size_ = conf->history_size;
      history_op_buf_ = malloc(1024 * 1024);
      history_op_buf_mr_ =
          ibv_reg_mr(pd, history_op_buf_, 1024 * 1024, IBV_ACCESS_LOCAL_WRITE);
      naive_history_ =
          new FIFOHistory(history_size_, server_base_addr_, history_raddr,
                          server_rkey_map_[0], (uint64_t)history_op_buf_,
                          1024 * 1024, history_op_buf_mr_->lkey, CLIENT);
    }

    nm_->rdma_read_sid_sync(0, weights_raddr_, server_rkey_map_[0],
                            (uint64_t)r_weights_, weights_sync_buf_mr_->lkey,
                            sizeof(float) * num_experts_);
    memcpy(l_weights_, r_weights_, sizeof(float) * num_experts_);
    memset(r_weights_, 0, sizeof(float) * num_experts_);
    for (int i = 0; i < num_experts_; i++)
      printf("expert %d %f\n", i, l_weights_[i]);
  } else if (eviction_type_ == EVICT_SAMPLE) {
    // for simplicity of maintianing local pool
    num_experts_ = 1;
    experts_[0] = priority_;
  }
  evict_1s_lat_ = 10;
  return 0;
}

void DMCClient::clear_counters() {
  num_cliquemap_sync_ = 0;
  num_evict_ = 0;
  num_success_evict_ = 0;
  num_bucket_evict_ = 0;
  num_success_bucket_evict_ = 0;
  num_evict_read_bucket_ = 0;
  num_set_retry_ = 0;
  num_rdma_send_ = 0;
  num_rdma_recv_ = 0;
  num_rdma_read_ = 0;
  num_rdma_write_ = 0;
  num_rdma_cas_ = 0;
  num_rdma_faa_ = 0;
  num_udp_req_ = 0;
  num_read_hist_head_ = 0;
  num_hist_access_ = 0;
  num_hist_overwite_ = 0;
  num_ada_access_inconsistent_ = 0;
  num_ada_evict_inconsistent_ = 0;
  num_hist_match_ = 0;
  num_adaptive_weight_sync_ = 0;
  num_adaptive_adjust_weights_ = 0;
  num_bucket_evict_history_ = 0;
  n_set_miss_ = 0;
  weight_vec_.clear();
  expert_evict_cnt_.clear();
  for (int i = 0; i < 10; i++)
    expert_evict_cnt_.push_back(0);
#ifdef USE_REWARDS
  num_hit_rewards_ = 0;
  expert_reward_cnt_.resize(MAX_NUM_EXPERTS);
#endif
}

void DMCClient::get_init_bucket_raddr(uint64_t hash,
                                      __OUT uint64_t* r_addr,
                                      __OUT uint16_t* server) {
  uint64_t bucket_id = hash % HASH_NUM_BUCKETS;
  *server = hash % num_servers_;
  *r_addr = bucket_id * sizeof(Bucket) + server_base_addr_;
}

void DMCClient::get_slot_raddr(const KVOpsCtx* ctx,
                               uint64_t slot_laddr,
                               __OUT uint64_t* slot_raddr,
                               __OUT uint16_t* server) {
  uint32_t slot_gid =
      (slot_laddr - ctx->bucket_laddr) / sizeof(Slot);  // the global offset
  uint32_t bucket_id = slot_gid / HASH_BUCKET_ASSOC_NUM;
  uint32_t slot_id = slot_gid % HASH_BUCKET_ASSOC_NUM;
  assert(bucket_id == 0);

  uint64_t bucket_raddr = 0;
  uint64_t bucket_sid = 0;
  bucket_raddr = ctx->init_bucket_raddr;
  bucket_sid = ctx->init_bucket_sid;
  assert(bucket_raddr != 0);
  *slot_raddr = bucket_raddr + slot_id * sizeof(Slot);
  *server = bucket_sid;
}

uint64_t DMCClient::get_sys_start_ts() {
  UDPMsg request;
  memset(&request, 0, sizeof(UDPMsg));
  request.type = UDPMSG_REQ_TS;
  request.id = my_sid_;
  serialize_udpmsg(&request);

  int ret = nm_->send_udp_msg_to_server(&request, 0);
  assert(ret == 0);

  UDPMsg reply;
  struct sockaddr_in r_addr;
  socklen_t r_addr_len;
  int num_retry = 3;
  do {
    ret = nm_->recv_udp_msg(&reply, &r_addr, &r_addr_len);
    num_retry--;
  } while (ret && num_retry > 0);
  assert(ret == 0);
  deserialize_udpmsg(&reply);

  return reply.body.sys_start_ts;
}

int DMCClient::alloc_segment(KVOpsCtx* ctx) {
  if (server_oom_ == true) {
    return -1;
  }
  num_udp_req_++;
  static uint32_t to_alloc_sid;

  printd(L_DEBUG, "Client allocating new segment");

#ifdef ALLOC_SEGMENT_UDP
  uint16_t sid = to_alloc_sid % num_servers_;
  UDPMsg request;
  memset(&request, 0, sizeof(UDPMsg));
  request.type = UDPMSG_REQ_ALLOC;
  request.id = my_sid_;
  serialize_udpmsg(&request);

  int ret = nm_->send_udp_msg_to_server(&request, sid);
  assert(ret == 0);

  UDPMsg reply;
  struct sockaddr_in r_addr;
  socklen_t r_addr_len;
  int num_retry = 3;
  do {
    ret = nm_->recv_udp_msg(&reply, &r_addr, &r_addr_len);
    if (ret != 0)
      printd(L_INFO, "recv failed");
    num_retry--;
  } while (ret && num_retry > 0);
  if (ret != 0) {
    server_oom_ = true;
    return -1;
  }
  deserialize_udpmsg(&reply);

  // check if a real segment is allocated
  if (reply.body.mr_info.addr == 0) {
    printd(L_INFO, "Server run out-of-memory");
    server_oom_ = true;
    return -1;
  }
  mm_->add_segment(reply.body.mr_info.addr, server_rkey_map_[sid], sid);

  to_alloc_sid++;
  return 0;
#else
  *(uint8_t*)ctx->op_laddr = IBMSG_REQ_ALLOC;
  *(uint16_t*)((uint64_t)ctx->op_laddr + sizeof(uint8_t)) = my_sid_;

  // post rr first
  struct ibv_recv_wr rr;
  struct ibv_sge recv_sge;
  memset(&rr, 0, sizeof(struct ibv_recv_wr));
  ib_create_sge(ctx->op_laddr + block_size_, local_buf_mr_->lkey, block_size_,
                &recv_sge);
  rr.wr_id = 1;
  rr.next = NULL;
  rr.sg_list = &recv_sge;
  rr.num_sge = 1;
  int ret = nm_->rdma_post_recv_sid_async(&rr, 0);
  assert(ret == 0);

  struct ibv_send_wr sr;
  struct ibv_sge send_sge;
  memset(&sr, 0, sizeof(struct ibv_send_wr));
  ib_create_sge(ctx->op_laddr, local_buf_mr_->lkey, block_size_, &send_sge);
  sr.wr_id = 0;
  sr.next = NULL;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_SEND;
  sr.send_flags = IBV_SEND_SIGNALED;
  ret = nm_->rdma_post_send_sid_sync(&sr, 0);
  assert(ret == 0);

  struct ibv_wc recv_wc;
  ret = nm_->rdma_poll_one_recv_completion_sync(&recv_wc);
  assert(ret == 0);
  assert(recv_wc.status == IBV_WC_SUCCESS);
  assert(recv_wc.opcode == IBV_WC_RECV);
  assert(recv_wc.wr_id == 1);
  assert(*(uint8_t*)((uint64_t)ctx->op_laddr + block_size_) == IBMSG_REP_ALLOC);

  uint64_t alloc_addr =
      *(uint64_t*)(ctx->op_laddr + block_size_ + sizeof(uint8_t));
  if (alloc_addr == 0) {
    printd(L_INFO, "Server out-of-memory");
    server_oom_ = true;
    return -1;
  }
  mm_->add_segment(alloc_addr, server_rkey_map_[0], 0);
  to_alloc_sid++;
  return 0;
#endif
}

int DMCClient::connect_all_rc_qp() {
  int ret;
  for (int i = 0; i < num_servers_; i++) {
    MrInfo mr;
    ret = nm_->client_connect_one_rc_qp(i, &mr);
    assert(ret == 0);
    server_rkey_map_[i] = mr.rkey;
  }
  return 0;
}

void DMCClient::create_op_ctx(__OUT KVOpsCtx* ctx,
                              void* key,
                              uint32_t key_size,
                              void* val,
                              uint32_t val_size,
                              uint8_t op_type) {
  memset(ctx, 0, sizeof(KVOpsCtx));
  ctx->op = op_type;
  ctx->key = key;
  ctx->val = val;
  ctx->key_size = key_size;
  ctx->val_size = val_size;
  ctx->kv_size = key_size + val_size + sizeof(ObjHeader);

  ctx->ret = DMC_SUCCESS;
  ctx->key_found = false;

  // calculate hash
  ctx->key_hash = hash_->hash_func1(key, key_size);
  ctx->fp = HashIndexComputeFp(ctx->key_hash);
  get_init_bucket_raddr(ctx->key_hash, &ctx->init_bucket_raddr,
                        &ctx->init_bucket_sid);

  // construct local write buf
  memset(local_buf_, 0, sizeof(ObjHeader));
  (*(ObjHeader*)local_buf_).key_size = key_size;
  (*(ObjHeader*)local_buf_).val_size = val_size;
  uint64_t key_addr = (uint64_t)local_buf_ + sizeof(ObjHeader);
  uint64_t val_addr = key_addr + key_size;
  memcpy((void*)key_addr, key, key_size);
  memcpy((void*)val_addr, val, val_size);
  ctx->write_buf_laddr = (uint64_t)local_buf_;
  ctx->write_buf_size = 1024;
  ctx->target_block_laddr =
      ctx->write_buf_laddr;  // set the match block to point to the initial
                             // block for update priority
  ctx->op_laddr = ctx->write_buf_laddr + ctx->write_buf_size;
  ctx->op_size = 4 * 1024;
  ctx->bucket_laddr = ctx->op_laddr + ctx->op_size;
  ctx->read_buf_laddr = ctx->bucket_laddr + sizeof(Bucket) * MAX_CHAIN_LEN;
}

void DMCClient::kv_set_alloc_rblock(KVOpsCtx* ctx) {
  printd(L_DEBUG, "kv_set_alloc_rblock");
  int ret = 0;
  ret = mm_->alloc(ctx->kv_size, &ctx->kv_remote_block);
  if (ret == -1) {
    ret = alloc_segment(ctx);
    while (ret == -1) {
      ret = evict(ctx);
    }
    ret = mm_->alloc(ctx->kv_size, &ctx->kv_remote_block);
  }
  assert(ret == 0);
}

void DMCClient::kv_set_read_index_write_kv(KVOpsCtx* ctx) {
  printd(L_DEBUG, "kv_set_read_index_write_kv");
  int ret = 0;

  // write KV
  struct ibv_send_wr* head_wr = NULL;
  struct ibv_send_wr write_sr;
  struct ibv_sge write_sge;
  memset(&write_sr, 0, sizeof(struct ibv_send_wr));
  ib_create_sge(ctx->write_buf_laddr, local_buf_mr_->lkey, block_size_,
                &write_sge);
  write_sr.wr_id = 0;
  write_sr.next = head_wr;
  write_sr.sg_list = &write_sge;
  write_sr.num_sge = 1;
  write_sr.opcode = IBV_WR_RDMA_WRITE;
  write_sr.send_flags = IBV_SEND_SIGNALED;
  write_sr.wr.rdma.remote_addr = ctx->kv_remote_block.addr;
  write_sr.wr.rdma.rkey = ctx->kv_remote_block.rkey;
  head_wr = &write_sr;
  num_rdma_write_++;

  printd(L_DEBUG, "Writing (k_size: %d v_size: %d) to %d:0x%lx",
         ((ObjHeader*)ctx->write_buf_laddr)->key_size,
         ((ObjHeader*)ctx->write_buf_laddr)->val_size,
         ctx->kv_remote_block.server, ctx->kv_remote_block.addr);

  // read bucket request
  struct ibv_send_wr read_sr;
  struct ibv_sge read_sge;
  memset(&read_sr, 0, sizeof(struct ibv_send_wr));
  ib_create_sge(ctx->bucket_laddr, local_buf_mr_->lkey, sizeof(Bucket),
                &read_sge);
  read_sr.wr_id = 1;
  read_sr.next = head_wr;
  read_sr.sg_list = &read_sge;
  read_sr.num_sge = 1;
  read_sr.opcode = IBV_WR_RDMA_READ;
  read_sr.send_flags = 0;
  read_sr.wr.rdma.remote_addr = ctx->init_bucket_raddr;
  read_sr.wr.rdma.rkey = server_rkey_map_[ctx->init_bucket_sid];
  head_wr = &read_sr;
  num_rdma_read_++;

  struct ibv_send_wr read_hist_head_sr;
  struct ibv_sge read_hist_head_sge;
  if (is_evict_adaptive(eviction_type_)) {
    memset(&read_hist_head_sr, 0, sizeof(struct ibv_send_wr));
    ib_create_sge(ctx->op_laddr, local_buf_mr_->lkey, sizeof(uint64_t),
                  &read_hist_head_sge);
    read_hist_head_sr.wr_id = 1230;
    read_hist_head_sr.next = head_wr;
    read_hist_head_sr.sg_list = &read_hist_head_sge;
    read_hist_head_sr.num_sge = 1;
    read_hist_head_sr.opcode = IBV_WR_RDMA_READ;
    read_hist_head_sr.send_flags = 0;
    read_hist_head_sr.wr.rdma.remote_addr = get_hist_head_raddr();
    read_hist_head_sr.wr.rdma.rkey = server_rkey_map_[0];
    head_wr = &read_hist_head_sr;
    num_rdma_read_++;
  }

  assert(head_wr != NULL);
  nm_->rdma_post_send_sid_sync(head_wr, 0);
  assert(ret == 0);

  if (is_evict_adaptive(eviction_type_)) {
    // record history head
    ctx->has_read_hist_head = true;
    ctx->read_hist_head = *(uint64_t*)ctx->op_laddr;
  }
}

void DMCClient::match_fp_and_find_empty(KVOpsCtx* ctx) {
  printd(L_DEBUG, "match_fp_and_find_empty");
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;
  ctx->num_fp_match = 0;
  ctx->num_free_slot = 0;

  Slot* slot = (Slot*)ctx->bucket_laddr;
  std::vector<std::pair<double, uint64_t>> prio_slot_raddr[num_experts_];
  for (int j = 0; j < HASH_BUCKET_ASSOC_NUM; j++) {
    uint64_t kv_raddr = HashIndexConvert48To64Bits(slot[j].atomic.pointer);
    uint64_t slot_raddr = ctx->init_bucket_raddr + sizeof(Slot) * j;
    if ((eviction_type_ == EVICT_SAMPLE_ADAPTIVE_NAIVE ||
         eviction_type_ == EVICT_SAMPLE_ADAPTIVE_HEAVY) &&
        *(uint64_t*)&slot[j] == ADAPTIVE_TMP_SLOT)
      continue;  // continue if adaptive naive and is internal state
    if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE &&
        lw_history_->is_in_history(&slot[j])) {
      num_hist_access_++;
      if (lw_history_->has_overwritten(ctx->read_hist_head, kv_raddr)) {
        num_hist_overwite_++;
        ctx->remote_free_slot_id[ctx->num_free_slot] = j;
        ctx->num_free_slot++;
      } else if (slot[j].meta.acc_info.key_hash == ctx->key_hash) {
        num_hist_match_++;
        ctx->hist_match = true;
        ctx->hist_match_head = kv_raddr;
        ctx->hist_expert_bmap = *(uint8_t*)&(slot[j].meta.acc_info.ins_ts);
      }
      continue;
    }
    if (kv_raddr == 0) {
      // empty slot
      assert(slot[j].atomic.fp == 0 && slot[j].atomic.kv_len == 0);
      ctx->remote_free_slot_id[ctx->num_free_slot] = j;
      ctx->num_free_slot++;
      continue;
    }
    if (ctx->fp == slot[j].atomic.fp) {
      // fp match slot
      ctx->remote_data_slot_id[ctx->num_fp_match] = j;
      ctx->num_fp_match++;
      continue;
    }
  }
}

void DMCClient::read_and_find_kv(KVOpsCtx* ctx) {
  printd(L_DEBUG, "read_and_find_kv");

  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  Slot* slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < ctx->num_fp_match; i++) {
    uint32_t idx = ctx->remote_data_slot_id[i];
    uint64_t kv_raddr = HashIndexConvert48To64Bits(slot[idx].atomic.pointer);
    num_rdma_read_++;
    nm_->rdma_read_sid_sync(0, kv_raddr, rkey, ctx->read_buf_laddr, lkey,
                            block_size_);

    if ((eviction_type_ == EVICT_SAMPLE_ADAPTIVE_NAIVE ||
         eviction_type_ == EVICT_SAMPLE_ADAPTIVE_HEAVY) &&
        naive_history_->is_in_history(kv_raddr)) {
      HistEntry* histEntry = (HistEntry*)ctx->read_buf_laddr;
      if (histEntry->key_hash == ctx->key_hash) {
        num_hist_match_++;
        ctx->hist_match = true;
        memcpy(&ctx->histEntry, histEntry, sizeof(HistEntry));
      }
      continue;
    }

    // compare key
    ObjHeader* header = (ObjHeader*)ctx->read_buf_laddr;
    uint32_t key_len = header->key_size;
    void* key = (void*)(ctx->read_buf_laddr + sizeof(ObjHeader));
    if (is_key_match(key, key_len, ctx->key, ctx->key_size)) {
      ctx->key_found = true;
      ctx->target_slot_laddr = idx * sizeof(Slot) + ctx->bucket_laddr;
      ctx->target_block_raddr = kv_raddr;
      ctx->target_block_laddr = ctx->read_buf_laddr;
      get_slot_raddr(ctx, ctx->target_slot_laddr, &ctx->target_slot_raddr,
                     &ctx->target_slot_sid);
      break;
    }
  }
}

void DMCClient::kv_set_delete_duplicate(KVOpsCtx* ctx) {
  printd(L_DEBUG, "kv_set_delete_duplicate");

  if (*(uint64_t*)ctx->target_slot_laddr != *(uint64_t*)&ctx->new_slot) {
    // my own key is evicted before
    return;
  }

  Slot* slot = (Slot*)ctx->bucket_laddr;
  uint64_t key_match_slot_laddr = 0;
  for (int i = 0; i < ctx->num_fp_match; i++) {
    num_rdma_read_++;
    uint32_t idx = ctx->remote_data_slot_id[i];
    uint64_t kv_raddr = HashIndexConvert48To64Bits(slot[idx].atomic.pointer);
    if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE)
      assert(lw_history_->is_in_history(&slot[idx]) == false);
    nm_->rdma_read_sid_sync(0, kv_raddr, server_rkey_map_[0],
                            ctx->read_buf_laddr, local_buf_mr_->lkey,
                            block_size_);
    num_rdma_read_++;
    // compare key
    ObjHeader* header = (ObjHeader*)(ctx->read_buf_laddr);
    uint32_t key_len = header->key_size;
    void* key = (void*)(ctx->read_buf_laddr + sizeof(ObjHeader));
    if (is_key_match(key, key_len, ctx->key, ctx->key_size)) {
      key_match_slot_laddr = idx * sizeof(Slot) + ctx->bucket_laddr;
      break;
    }
  }

  if (key_match_slot_laddr == ctx->target_slot_laddr ||
      key_match_slot_laddr == 0) {
    // my key is first or all keys are evicted evicted before
    return;
  }

  // CAS my slot to 0 and free the remote block
  Slot* l_slot = (Slot*)ctx->target_slot_laddr;
  uint64_t slot_raddr;
  uint16_t slot_rsid;
  get_slot_raddr(ctx, ctx->target_slot_laddr, &slot_raddr, &slot_rsid);
  uint64_t slotMeta_raddr = slot_raddr + SLOT_META_OFF;
  num_rdma_cas_++;
  nm_->rdma_cas_sid_sync(0, slot_raddr, server_rkey_map_[slot_rsid],
                         ctx->op_laddr, local_buf_mr_->lkey, *(uint64_t*)l_slot,
                         0);
  // reclaim the block if success
  if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)l_slot) {
    mm_->free(HashIndexConvert48To64Bits(l_slot->atomic.pointer),
              server_rkey_map_[0], block_size_, 0);

    // clear the slotMeta
    SlotMeta new_meta;
    memset(&new_meta, 0, sizeof(SlotMeta));
    nm_->rdma_inl_write_sid_async(0, slotMeta_raddr, server_rkey_map_[0],
                                  (uint64_t)&new_meta, 0, sizeof(SlotMeta));
  }
}

int DMCClient::evict_bucket(KVOpsCtx* ctx) {
  num_bucket_evict_++;
  int ret = 0;
  switch (eviction_type_) {
    case EVICT_NON:
    case EVICT_SAMPLE:
      ret = evict_bucket_sample(ctx);
      break;
    case EVICT_SAMPLE_NAIVE:
      ret = evict_bucket_sample_naive(ctx);
      break;
    case EVICT_SAMPLE_ADAPTIVE:
      ret = evict_bucket_sample_adaptive(ctx);
      break;
    case EVICT_SAMPLE_ADAPTIVE_NAIVE:
      ret = evict_bucket_sample_adaptive_naive(ctx);
      break;
    case EVICT_SAMPLE_ADAPTIVE_HEAVY:
      ret = evict_bucket_sample_adaptive_heavy(ctx);
      break;
    case EVICT_PRECISE:
      ret = evict_bucket_precise(ctx);
      break;
    default:
      printd(L_ERROR, "No supported bucket eviction");
      abort();
  }
  num_success_bucket_evict_ += (ret == 0);
  return ret;
}

int DMCClient::evict_bucket_sample_adaptive_heavy(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict Bucket Sample");
  int ret = 0;

  uint64_t bucket_raddr = ctx->init_bucket_raddr;
  uint64_t bucket_laddr = ctx->bucket_laddr;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  std::vector<std::pair<double, int>> prio_hist_id;
  Slot* l_slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    if (*(uint64_t*)&l_slot[i] == ADAPTIVE_TMP_SLOT)
      continue;
    uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
    if (kv_raddr == 0)
      return 0;
    if (naive_history_->is_in_history(kv_raddr)) {
      double prio = l_slot[i].meta.acc_info.freq;
      prio_hist_id.emplace_back(prio, i);
    } else {
      for (int j = 0; j < num_experts_; j++) {
        double prio = experts_[j]->parse_priority(&l_slot[i].meta,
                                                  l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, i);
      }
    }
  }

  // evict history slot first if there are more than half of history slots in a
  // bucket
  std::sort(prio_hist_id.begin(), prio_hist_id.end());
  auto hist_it = prio_hist_id.begin();
  for (; prio_hist_id.size() > HASH_BUCKET_ASSOC_NUM / 8 &&
         hist_it != prio_hist_id.end();
       hist_it++) {
    int idx = hist_it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + idx * sizeof(Slot);
    ret = naive_history_->try_evict(nm_, target_slot_raddr, &l_slot[idx]);
    num_rdma_cas_++;
    if (ret == 0) {
      num_rdma_write_ += 2;
      memset(&l_slot[idx], 0, sizeof(Slot));
      return 1;
    }
  }

  // evict normal slot otherwise
  uint8_t expert_bmap = 0;
  int best_expert = adaptive_get_best_expert(&expert_bmap);
  std::sort(prio_slot_id[best_expert].begin(), prio_slot_id[best_expert].end());
  auto slot_it = prio_slot_id[best_expert].begin();
  for (; slot_it != prio_slot_id[best_expert].end(); slot_it++) {
    num_rdma_cas_++;
    int idx = slot_it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + sizeof(Slot) * idx;
    Slot* target_slot = &l_slot[idx];
    ret = nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr,
                                 lkey, *(uint64_t*)target_slot, 0);

    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);
      memset(target_slot, 0, sizeof(Slot));

      // clear slotMeta
      SlotMeta* new_meta = (SlotMeta*)(ctx->op_laddr + 8);
      memset(new_meta, 0, sizeof(SlotMeta));
      nm_->rdma_inl_write_sid_async(0, target_slot_raddr + SLOT_META_OFF, rkey,
                                    (uint64_t)new_meta, lkey, sizeof(SlotMeta));
      num_rdma_write_++;
      return 1;
    }
  }

  // read the bucket again on failure
  nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, bucket_laddr, lkey,
                          sizeof(Bucket));
  num_rdma_read_++;
  return -1;
}

int DMCClient::evict_bucket_sample_adaptive_naive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "evict bucket sample adaptive naive");
  int ret = 0;

  uint64_t bucket_raddr = ctx->init_bucket_raddr;
  uint64_t bucket_laddr = ctx->bucket_laddr;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  std::vector<std::pair<double, int>> prio_hist_id;
  Slot* l_slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    if (*(uint64_t*)&l_slot[i] == ADAPTIVE_TMP_SLOT)
      continue;

    uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
    nm_->rdma_read_sid_sync(0, kv_raddr, rkey, ctx->op_laddr, lkey,
                            block_size_);
    if (kv_raddr == 0)
      return 0;
    if (naive_history_->is_in_history(kv_raddr)) {
      HistEntry* entry = (HistEntry*)ctx->op_laddr;
      double prio = entry->head;
      prio_hist_id.emplace_back(prio, i);
    } else {
      ObjHeader* header = (ObjHeader*)ctx->op_laddr;
      SlotMeta* meta = &header->meta;
      for (int j = 0; j < num_experts_; j++) {
        double prio =
            experts_[j]->parse_priority(meta, l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, i);
      }
    }
  }

  // evict history first if there are more than half of history entries
  std::sort(prio_hist_id.begin(), prio_hist_id.end());
  auto hist_it = prio_hist_id.begin();
  for (; prio_hist_id.size() > (HASH_BUCKET_ASSOC_NUM / 2) &&
         hist_it != prio_hist_id.end();
       hist_it++) {
    int idx = hist_it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + idx * sizeof(Slot);
    ret = naive_history_->try_evict(nm_, target_slot_raddr, &l_slot[idx]);
    num_rdma_cas_++;
    if (ret == 0) {
      num_rdma_write_ += 2;
      memset(&l_slot[idx], 0, sizeof(Slot));
      return 1;
    }
  }

  // evict normal slots otherwise
  uint8_t expert_bmap = 0;
  int best_expert = adaptive_get_best_expert(&expert_bmap);
  std::sort(prio_slot_id[best_expert].begin(), prio_slot_id[best_expert].end());
  auto slot_it = prio_slot_id[best_expert].begin();
  for (; slot_it != prio_slot_id[best_expert].end(); slot_it++) {
    num_rdma_cas_++;
    int idx = slot_it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + sizeof(Slot) * idx;
    Slot* target_slot = &l_slot[idx];
    ret = nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr,
                                 lkey, *(uint64_t*)target_slot, 0);

    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);
      memset(target_slot, 0, sizeof(Slot));
      return 1;
    }
  }

  // read the bucket again on failure
  nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, bucket_laddr, lkey,
                          sizeof(Bucket));
  num_rdma_read_++;
  return -1;
}

int DMCClient::evict_bucket_sample_naive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict Bucket sample naive");
  int ret = 0;

  uint64_t bucket_raddr = ctx->init_bucket_raddr;
  uint64_t bucket_laddr = ctx->bucket_laddr;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;
  nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, bucket_laddr, lkey,
                          sizeof(Bucket));
  num_rdma_read_++;

  uint64_t sampled_key_slot_raddr[HASH_BUCKET_ASSOC_NUM];
  uint64_t sampled_key_slot_rsid[HASH_BUCKET_ASSOC_NUM];
  std::vector<std::pair<double, int>> prio_slot_id;
  Slot* l_slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
    if (kv_raddr == 0)
      return 1;
    nm_->rdma_read_sid_sync(0, kv_raddr, rkey, ctx->op_laddr, lkey,
                            block_size_);
    num_rdma_read_++;
    ObjHeader* header = (ObjHeader*)ctx->op_laddr;
    double prio =
        priority_->parse_priority(&header->meta, l_slot[i].atomic.kv_len);
    prio_slot_id.emplace_back(prio, i);
  }

  // construct new slot
  ctx->new_slot.atomic.fp = ctx->fp;
  ctx->new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(ctx->kv_remote_block.addr,
                             ctx->new_slot.atomic.pointer);

  std::sort(prio_slot_id.begin(), prio_slot_id.end());
  auto it = prio_slot_id.begin();
  for (; it != prio_slot_id.end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + idx * sizeof(Slot);
    uint64_t target_slot_rsid = ctx->init_bucket_sid;
    Slot* target_slot = &l_slot[idx];
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, *(uint64_t*)&ctx->new_slot);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      ctx->target_slot_laddr = (uint64_t)target_slot;
      ctx->target_slot_raddr = target_slot_raddr;
      ctx->target_slot_sid = 0;
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);
      memset(target_slot, 0, sizeof(Slot));
      break;
    }
  }
  if (it == prio_slot_id.end())
    return -1;
  return 0;
}

int DMCClient::evict_bucket_sample_adaptive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict Bucket Sample");

  uint64_t bucket_raddr = ctx->init_bucket_raddr;
  uint64_t bucket_laddr = ctx->bucket_laddr;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;
  uint64_t bucket_id =
      (ctx->init_bucket_raddr - server_base_addr_) / sizeof(Bucket);
  evict_bucket_cnt_[bucket_id]++;

  if (!use_async_weight_)
    adaptive_read_weights();

  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  std::vector<std::pair<double, int>> prio_hist_id;
  Slot* l_slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
    if (l_slot[i].atomic.kv_len == 0) {
      assert(kv_raddr == 0 && l_slot[i].atomic.fp == 0);
      return 1;
    }
    if (lw_history_->is_in_history(&l_slot[i])) {
      if (lw_history_->has_overwritten(ctx->read_hist_head, kv_raddr))
        return 1;
      double prio = kv_raddr;
      prio_hist_id.emplace_back(prio, i);
    } else {
      for (int j = 0; j < num_experts_; j++) {
        double prio = experts_[j]->parse_priority(&l_slot[i].meta,
                                                  l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, i);
      }
    }
  }

  // construct target slot
  ctx->new_slot.atomic.fp = ctx->fp;
  ctx->new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(ctx->kv_remote_block.addr,
                             ctx->new_slot.atomic.pointer);

  // evict history slot first if there are more than half of history slots in a
  // bucket
  std::sort(prio_hist_id.begin(), prio_hist_id.end());
  auto hist_it = prio_hist_id.begin();
  for (; prio_hist_id.size() >= HASH_BUCKET_ASSOC_NUM / 2 &&
         hist_it != prio_hist_id.end();
       hist_it++) {
    int idx = hist_it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + idx * sizeof(Slot);
    uint64_t target_slotMeta_raddr = target_slot_raddr + SLOT_META_OFF;
    Slot* target_slot = &l_slot[idx];
    num_rdma_cas_++;
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, *(uint64_t*)&ctx->new_slot);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      ctx->target_slot_laddr = (uint64_t)target_slot;
      ctx->target_slot_raddr = target_slot_raddr;
      ctx->target_slot_sid = 0;
      ctx->key_found = false;
      num_bucket_evict_history_++;
      memset(target_slot, 0, sizeof(Slot));
      // clear slotMeta
      SlotMeta* new_meta = (SlotMeta*)ctx->op_laddr;
      memset(new_meta, 0, sizeof(SlotMeta));
      nm_->rdma_inl_write_sid_async(0, target_slotMeta_raddr, rkey,
                                    (uint64_t)new_meta, 0, sizeof(SlotMeta));
      num_rdma_write_++;
      assert(ctx->target_slot_laddr != 0);
      return 0;
    }
  }

  // evict normal slot otherwise
  uint8_t expert_bmap = 0;
  int best_expert = adaptive_get_best_expert(&expert_bmap);
  std::sort(prio_slot_id[best_expert].begin(), prio_slot_id[best_expert].end());
  auto slot_it = prio_slot_id[best_expert].begin();
  for (; slot_it != prio_slot_id[best_expert].end(); slot_it++) {
    num_rdma_cas_++;
    int idx = slot_it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + sizeof(Slot) * idx;
    uint64_t target_slotMeta_raddr = target_slot_raddr + SLOT_META_OFF;
    Slot* target_slot = &l_slot[idx];
    assert(lw_history_->is_in_history(target_slot) == false);
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, *(uint64_t*)&ctx->new_slot);

    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      ctx->target_slot_laddr = (uint64_t)target_slot;
      ctx->target_slot_raddr = target_slot_raddr;
      ctx->target_slot_sid = 0;
      ctx->key_found = false;
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);
      memset(target_slot, 0, sizeof(Slot));

      // clear slotMeta
      SlotMeta* new_meta = (SlotMeta*)(ctx->op_laddr + 8);
      memset(new_meta, 0, sizeof(SlotMeta));
      nm_->rdma_inl_write_sid_async(0, target_slotMeta_raddr, rkey,
                                    (uint64_t)new_meta, 0, sizeof(SlotMeta));
      num_rdma_write_++;
      assert(ctx->target_slot_laddr != 0);
      return 0;
    }
  }

  // read the bucket and hist head again
  uint64_t r_addr_list[2] = {bucket_raddr, lw_history_->hist_cntr_raddr()};
  uint64_t l_addr_list[2] = {bucket_laddr, ctx->op_laddr};
  uint32_t lkey_list[2] = {lkey, lkey};
  uint32_t len_list[2] = {sizeof(Bucket), sizeof(uint64_t)};
  nm_->rdma_batch_read_sid_sync(0, r_addr_list, rkey, l_addr_list, lkey_list,
                                len_list, 2);
  num_rdma_read_ += 2;
  ctx->has_read_hist_head = true;
  ctx->read_hist_head = *(uint64_t*)ctx->op_laddr;
  return -1;
}

int DMCClient::evict_bucket_sample(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict Bucket Sample");
  int ret = 0;

  // read entire bucket
  uint64_t bucket_raddr = ctx->init_bucket_raddr;
  uint64_t bucket_laddr = ctx->bucket_laddr;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  std::vector<std::pair<double, int>> prio_slot_id;
  Slot* l_slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    if (HashIndexConvert48To64Bits(l_slot[i].atomic.pointer) == 0)
      return 1;
    double prio =
        priority_->parse_priority(&l_slot[i].meta, l_slot[i].atomic.kv_len);
    prio_slot_id.emplace_back(prio, i);
  }

  // construct new slot
  ctx->new_slot.atomic.fp = ctx->fp;
  ctx->new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(ctx->kv_remote_block.addr,
                             ctx->new_slot.atomic.pointer);

  std::sort(prio_slot_id.begin(), prio_slot_id.end());
  auto it = prio_slot_id.begin();
  for (; it != prio_slot_id.end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    uint64_t target_slot_raddr = ctx->init_bucket_raddr + sizeof(Slot) * idx;
    uint64_t target_slotMeta_raddr = target_slot_raddr + SLOT_META_OFF;
    uint16_t target_slot_rsid = ctx->init_bucket_sid;
    Slot* target_slot = &l_slot[idx];
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, *(uint64_t*)&ctx->new_slot);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      ctx->target_slot_laddr = (uint64_t)target_slot;
      ctx->target_slot_raddr = target_slot_raddr;
      ctx->target_slot_sid = 0;
      // Eviction success
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);
      memset(target_slot, 0, sizeof(Slot));

      // clear corresponding metadata
      SlotMeta* new_meta = (SlotMeta*)(ctx->op_laddr + 8);
      memset(new_meta, 0, sizeof(SlotMeta));
      nm_->rdma_inl_write_sid_async(0, target_slotMeta_raddr, rkey,
                                    (uint64_t)new_meta, lkey, sizeof(SlotMeta));
      num_rdma_write_++;

      return 0;
    }
  }

  // read bucket again on failure
  nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, bucket_laddr, lkey,
                          sizeof(Bucket));
  return -1;
}

int DMCClient::evict_bucket_precise(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict bucket precise");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  // 1. read the bucket again
  nm_->rdma_read_sid_sync(0, ctx->init_bucket_raddr, rkey, ctx->bucket_laddr,
                          lkey, sizeof(Bucket));
  num_rdma_read_++;
  match_fp_and_find_empty(ctx);
  if (ctx->num_free_slot > 0)
    return 1;

  // construct new slot
  ctx->new_slot.atomic.fp = ctx->fp;
  ctx->new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(ctx->kv_remote_block.addr,
                             ctx->new_slot.atomic.pointer);

  // 2. cas a random victim
  int victim_idx = prio_list_->get_bucket_min(
      nm_, get_slot_id(ctx->init_bucket_raddr), HASH_BUCKET_ASSOC_NUM);
  if (victim_idx < 0)
    return -1;  // the bucket is not consistent with the list retry
  uint64_t victim_raddr = ctx->init_bucket_raddr + sizeof(Slot) * victim_idx;
  Slot* victim_slot = (Slot*)(ctx->bucket_laddr + sizeof(Slot) * victim_idx);
  assert(HashIndexConvert48To64Bits(victim_slot->atomic.pointer) != 0);
  nm_->rdma_cas_sid_sync(0, victim_raddr, rkey, ctx->op_laddr, lkey,
                         *(uint64_t*)victim_slot, *(uint64_t*)&ctx->new_slot);

  // 3. check cas result
  // 3.1 return and retry on failure
  if (*(uint64_t*)ctx->op_laddr != *(uint64_t*)victim_slot) {
    printd(L_DEBUG, "Evict Bucket Failed!");
    return -1;
  }
  ctx->target_slot_laddr = (uint64_t)victim_slot;
  ctx->target_slot_raddr = victim_raddr;
  ctx->target_slot_sid = 0;

  // 3.2 free the bucket and delete the list slot on success
  ret = prio_list_->delete_slot(nm_, get_slot_id(victim_raddr), victim_raddr);
  uint64_t free_addr = HashIndexConvert48To64Bits(victim_slot->atomic.pointer);
  assert(free_addr != 0);
  mm_->free(free_addr, rkey, block_size_, 0);
  memset(victim_slot, 0, sizeof(Slot));
  printd(L_DEBUG, "Evict Bucket Success!");

  // 3.3 clear slotMeta
  uint32_t victim_id = get_slot_id(victim_raddr);
  uint64_t slotMeta_raddr = get_slot_raddr(victim_id) + SLOT_META_OFF;
  SlotMeta emptyMeta;
  memset(&emptyMeta, 0, sizeof(SlotMeta));
  ret = nm_->rdma_inl_write_sid_sync(0, slotMeta_raddr, rkey,
                                     (uint64_t)&emptyMeta, sizeof(SlotMeta));
  assert(ret == 0);

  return 0;
}

void DMCClient::find_empty(KVOpsCtx* ctx) {
  printd(L_DEBUG, "find_empty");
  assert(ctx->num_free_slot > 0);
  printd(L_DEBUG, "%d free slots", ctx->num_free_slot);
  uint32_t free_slot_idx = random() % ctx->num_free_slot;
  ctx->target_slot_laddr =
      ctx->remote_free_slot_id[free_slot_idx] * sizeof(Slot) +
      ctx->bucket_laddr;
  get_slot_raddr(ctx, ctx->target_slot_laddr, &ctx->target_slot_raddr,
                 &ctx->target_slot_sid);
  printd(L_DEBUG, "free slot lid %d", ctx->remote_free_slot_id[free_slot_idx]);
  printd(L_DEBUG, "free slot found 0x%lx -> @%d:0x%lx", ctx->target_slot_laddr,
         ctx->target_slot_sid, ctx->target_slot_raddr);
  return;
}

void DMCClient::kv_set_update_index(KVOpsCtx* ctx) {
  printd(L_DEBUG, "kv_set_update_index");
  Slot* slot = (Slot*)(ctx->target_slot_laddr);
  if (ctx->key_found) {
    assert(slot->atomic.fp == ctx->fp);
  } else if (eviction_type_ != EVICT_SAMPLE_ADAPTIVE) {
    assert(*(uint64_t*)ctx->target_slot_laddr == 0);
  }

  // construct new_slot
  ctx->new_slot.atomic.fp = ctx->fp;
  ctx->new_slot.atomic.kv_len = 1;
  HashIndexConvert64To48Bits(ctx->kv_remote_block.addr,
                             ctx->new_slot.atomic.pointer);

  // construct cas pointer request
  num_rdma_cas_++;
  nm_->rdma_cas_sid_sync(0, ctx->target_slot_raddr, server_rkey_map_[0],
                         ctx->op_laddr, local_buf_mr_->lkey, *(uint64_t*)slot,
                         *(uint64_t*)&ctx->new_slot);

  if (*(uint64_t*)ctx->op_laddr != *(uint64_t*)slot) {
    if (ctx->key_found == false) {
      // retry if insert failure
      ctx->ret = DMC_SET_RETRY;
    } else if (*(uint64_t*)ctx->op_laddr == 0) {
      // return if the key is evicted
      mm_->free(&ctx->kv_remote_block);
      ctx->ret = DMC_SET_RETURN;
    } else {
      // continue if the update is slow
      mm_->free(&ctx->kv_remote_block);
    }
    return;
  }
  if (ctx->key_found == true) {
    if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE)
      assert(lw_history_->is_in_history(slot) == false);
    uint64_t free_addr = HashIndexConvert48To64Bits(slot->atomic.pointer);
    mm_->free(free_addr, server_rkey_map_[0], block_size_, 0);
    assert(free_addr != 0);
  }
  memcpy((void*)ctx->target_slot_laddr, &ctx->new_slot, sizeof(Slot));
}

void DMCClient::gen_info_update_mask(const SlotMeta* meta,
                                     __OUT uint32_t* info_update_mask) {
  *info_update_mask = 0;
  if (is_evict_adaptive(eviction_type_)) {
    for (int i = 0; i < num_experts_; i++)
      *info_update_mask |= experts_[i]->info_update_mask(meta);
  } else {
    *info_update_mask = priority_->info_update_mask(meta);
  }
  if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE_NAIVE)
    *info_update_mask |= UPD_CNTR;
}

void DMCClient::gen_info_meta(KVOpsCtx* ctx,
                              uint32_t info_update_mask,
                              __OUT SlotMeta* meta) {
  // memset(meta, 0, sizeof(SlotMeta));
  meta->acc_info.key_hash = ctx->key_hash;
  meta->acc_info.ins_ts = new_ts();
  if (info_update_mask & UPD_TS) {
    meta->acc_info.acc_ts = new_ts();
  }
  if (info_update_mask & UPD_FREQ) {
    meta->acc_info.freq++;
  }
  if (info_update_mask & UPD_CNTR) {
    Priority* cur_prio;
    if (is_evict_adaptive(eviction_type_))
      cur_prio = experts_[0];
    else
      cur_prio = priority_;
    meta->acc_info.counter = cur_prio->get_counter_val(meta, 1);
  }
}

void DMCClient::update_priority_sample_naive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "update_priority_naive");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;
  uint64_t block_raddr = 0;
  if (ctx->op == GET)
    block_raddr = ctx->target_block_raddr;
  else
    block_raddr = ctx->kv_remote_block.addr;

  ObjHeader* old_header = (ObjHeader*)ctx->op_laddr;
  memset(old_header, 0, sizeof(ObjHeader));
  if (ctx->op == SET && ctx->key_found == true) {
    memcpy(old_header, (void*)ctx->read_buf_laddr, sizeof(ObjHeader));
  }

  SlotMeta* old_meta = &old_header->meta;
  uint32_t info_update_mask = 0;
  gen_info_update_mask(old_meta, &info_update_mask);
  gen_info_meta(ctx, info_update_mask, old_meta);

  struct ibv_send_wr write_cntr_wr;
  struct ibv_sge write_cntr_sge;
  struct ibv_send_wr write_ts_wr;
  struct ibv_sge write_ts_sge;
  struct ibv_send_wr faa_wr;
  struct ibv_sge faa_sge;
  struct ibv_send_wr* head_wr = NULL;

  if (info_update_mask & UPD_CNTR) {
    memset(&write_cntr_wr, 0, sizeof(struct ibv_send_wr));
    ib_create_sge((uint64_t)old_meta + SLOTM_INFO_ACC_TS_OFF, lkey,
                  sizeof(uint64_t), &write_cntr_sge);
    write_cntr_wr.wr_id = 12800;
    write_cntr_wr.next = head_wr;
    write_cntr_wr.sg_list = &write_cntr_sge;
    write_cntr_wr.num_sge = 1;
    write_cntr_wr.opcode = IBV_WR_RDMA_WRITE;
    write_cntr_wr.send_flags = 0;
    write_cntr_wr.wr.rdma.remote_addr =
        block_raddr + OBJ_META_OFF + SLOTM_INFO_CNTR_OFF;
    write_cntr_wr.wr.rdma.rkey = rkey;
    head_wr = &write_cntr_wr;
    num_rdma_write_++;
  }
  if (info_update_mask & UPD_TS) {
    memset(&write_ts_wr, 0, sizeof(struct ibv_send_wr));
    ib_create_sge((uint64_t)old_meta + SLOTM_INFO_ACC_TS_OFF, lkey,
                  sizeof(uint64_t), &write_ts_sge);
    write_ts_wr.wr_id = 12801;
    write_ts_wr.next = head_wr;
    write_ts_wr.sg_list = &write_ts_sge;
    write_ts_wr.num_sge = 1;
    write_ts_wr.opcode = IBV_WR_RDMA_WRITE;
    write_ts_wr.send_flags = 0;
    write_ts_wr.wr.rdma.remote_addr =
        block_raddr + OBJ_META_OFF + SLOTM_INFO_ACC_TS_OFF;
    write_ts_wr.wr.rdma.rkey = rkey;
    head_wr = &write_ts_wr;
    num_rdma_write_++;
  }
  if (info_update_mask & UPD_FREQ) {
    memset(&faa_wr, 0, sizeof(struct ibv_send_wr));
    ib_create_sge((uint64_t)transient_buf_, transient_buf_mr_->lkey,
                  sizeof(uint64_t), &faa_sge);
    faa_wr.wr_id = 12802;
    faa_wr.next = head_wr;
    faa_wr.sg_list = &faa_sge;
    faa_wr.num_sge = 1;
    faa_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    faa_wr.send_flags = 0;
    faa_wr.wr.atomic.compare_add = old_meta->acc_info.freq;
    faa_wr.wr.atomic.remote_addr =
        block_raddr + OBJ_META_OFF + SLOTM_INFO_FREQ_OFF;
    faa_wr.wr.atomic.rkey = rkey;
    head_wr = &faa_wr;
    num_rdma_faa_++;
  }
  assert(head_wr != NULL);
  nm_->rdma_post_send_sid_async(head_wr, 0);
}

void DMCClient::update_priority_sample_adaptive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "update_priority");
  uint32_t info_update_mask = 0;
  SlotMeta* old_meta = &((Slot*)ctx->target_slot_laddr)->meta;
  if (ctx->key_found != true)
    memset(old_meta, 0, sizeof(SlotMeta));
  for (int i = 0; i < num_experts_; i++)
    info_update_mask |= experts_[i]->info_update_mask(old_meta);
  update_priority_common(ctx, info_update_mask);
}

void DMCClient::update_priority_common(KVOpsCtx* ctx, uint32_t info_upd_mask) {
  printd(L_DEBUG, "update_priority_common");
  std::vector<std::pair<uint64_t, uint64_t>> freq_upd_vec;
  std::string str_key = get_str_key(ctx);
  uint64_t slotMeta_raddr = ctx->target_slot_raddr + SLOT_META_OFF;
  printd(L_DEBUG, "slotMeta_raddr: %lx", slotMeta_raddr);
  if (info_upd_mask & UPD_FREQ) {
    uint64_t freq_raddr = slotMeta_raddr + SLOTM_INFO_FREQ_OFF;
    if (use_freq_cache_)
      freq_cache_->add(str_key, freq_raddr, freq_upd_vec);
    else
      freq_upd_vec.emplace_back(freq_raddr, 1);
  }
  struct ibv_send_wr* head_wr = NULL;
  struct ibv_send_wr faa_wr[MAX_NUM_FAA];
  struct ibv_sge faa_sge[MAX_NUM_FAA];
  memset(faa_wr, 0, sizeof(struct ibv_send_wr) * MAX_NUM_FAA);
  memset(faa_sge, 0, sizeof(struct ibv_sge) * MAX_NUM_FAA);
  for (int i = 0; i < freq_upd_vec.size(); i++) {
    ib_create_sge((uint64_t)transient_buf_, transient_buf_mr_->lkey,
                  sizeof(uint64_t), &faa_sge[i]);
    faa_wr[i].wr_id = 11550 + i;
    faa_wr[i].next = head_wr;
    faa_wr[i].sg_list = &faa_sge[i];
    faa_wr[i].num_sge = 1;
    faa_wr[i].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    faa_wr[i].send_flags = 0;
    faa_wr[i].wr.atomic.compare_add = freq_upd_vec[i].second;
    faa_wr[i].wr.atomic.remote_addr = freq_upd_vec[i].first;
    faa_wr[i].wr.atomic.rkey = server_rkey_map_[0];
    head_wr = &faa_wr[i];
    num_rdma_faa_++;
  }
  struct ibv_send_wr write_wr;
  struct ibv_sge write_sge;
  if (info_upd_mask & UPD_TS || info_upd_mask & UPD_CNTR ||
      (ctx->op == SET && ctx->key_found == false)) {
    SlotMeta* new_meta = (SlotMeta*)ctx->op_laddr;
    gen_info_meta(ctx, info_upd_mask, new_meta);
    memset(&write_wr, 0, sizeof(struct ibv_send_wr));
    if (ctx->op == SET && ctx->key_found == false) {
      uint32_t write_size = sizeof(uint64_t) * 3;
#ifdef MULTI_POLICY
      write_size += sizeof(uint64_t) * (!!(info_upd_mask & UPD_CNTR));
#endif
      ib_create_sge((uint64_t)new_meta + SLOTM_INFO_HASH_OFF,
                    local_buf_mr_->lkey, write_size, &write_sge);
      write_wr.wr.rdma.remote_addr = slotMeta_raddr + SLOTM_INFO_HASH_OFF;
    } else {
      uint32_t write_size = sizeof(uint64_t);
#ifdef MULTI_POLICY
      write_size = sizeof(uint64_t) * (!!(info_upd_mask & UPD_CNTR) +
                                       !!(info_upd_mask & UPD_FREQ));
#endif
      ib_create_sge((uint64_t)new_meta + SLOTM_INFO_ACC_TS_OFF,
                    local_buf_mr_->lkey, write_size, &write_sge);
      if (info_upd_mask & UPD_TS)
        write_wr.wr.rdma.remote_addr = slotMeta_raddr + SLOTM_INFO_ACC_TS_OFF;
      else
        write_wr.wr.rdma.remote_addr = slotMeta_raddr + SLOTM_INFO_CNTR_OFF;
    }
    write_wr.wr_id = 115511;
    write_wr.next = head_wr;
    write_wr.sg_list = &write_sge;
    write_wr.num_sge = 1;
    write_wr.opcode = IBV_WR_RDMA_WRITE;
    write_wr.send_flags = IBV_SEND_INLINE;
    write_wr.wr.rdma.rkey = server_rkey_map_[0];
    head_wr = &write_wr;
    num_rdma_write_++;
  }
  // assert(head_wr != NULL);
  nm_->rdma_post_send_sid_async(head_wr, 0);
}

void DMCClient::update_priority_sample(KVOpsCtx* ctx) {
  printd(L_DEBUG, "update_priority");
  SlotMeta* old_meta = &((Slot*)ctx->target_slot_laddr)->meta;
  if (ctx->key_found == false)
    memset(old_meta, 0, sizeof(SlotMeta));
  uint32_t info_update_mask = priority_->info_update_mask(old_meta);
  update_priority_common(ctx, info_update_mask);
}

void DMCClient::update_priority_precise(KVOpsCtx* ctx) {
  printd(L_DEBUG, "update_priority_precise");

  // 1. update slot meta
  SlotMeta* old_meta = &((Slot*)ctx->target_slot_laddr)->meta;
  if (ctx->key_found == false)
    memset(old_meta, 0, sizeof(SlotMeta));
  uint32_t info_update_mask = priority_->info_update_mask(old_meta);
  update_priority_common(ctx, info_update_mask);

  Slot* target_slot = (Slot*)ctx->target_slot_laddr;
  SlotMeta* new_meta = (SlotMeta*)(ctx->op_laddr + 16);
  memcpy(new_meta, &target_slot->meta, sizeof(SlotMeta));
  gen_info_meta(ctx, info_update_mask, new_meta);

  // 2. update list
  double new_priority =
      priority_->parse_priority(new_meta, target_slot->atomic.kv_len);
  uint32_t slot_id = get_slot_id(ctx->target_slot_raddr);
  prio_list_->update(nm_, slot_id, new_priority, ctx->target_slot_raddr,
                     *(uint64_t*)target_slot);
}

void DMCClient::update_priority_cliquemap(KVOpsCtx* ctx) {
  int ret = 0;
  uint32_t required_size = ctx->key_size + sizeof(uint8_t) + sizeof(uint64_t);
  struct timeval now;
  gettimeofday(&now, NULL);
  uint64_t sync_interval = diff_ts_us(&now, &last_sync_time_);
  if (ts_buf_off_ + required_size > MSG_BUF_SIZE ||
      sync_interval >= CLIQUEMAP_SYNC_INTERVAL_US) {
    // printf("sync interval: %ld\n", sync_interval);
    struct ibv_recv_wr rr;
    struct ibv_sge rr_sge;
    memset(&rr, 0, sizeof(struct ibv_recv_wr));
    memset(&rr_sge, 0, sizeof(struct ibv_sge));
    rr_sge.addr = (uint64_t)ts_rep_buf_;
    rr_sge.length = block_size_;
    rr_sge.lkey = ts_rep_buf_mr_->lkey;
    rr.wr_id = 0;
    rr.next = NULL;
    rr.sg_list = &rr_sge;
    rr.num_sge = 1;
    ret = nm_->rdma_post_recv_sid_async(&rr, 0);
    assert(ret == 0);

    struct ibv_send_wr sr;
    struct ibv_sge sr_sge;
    memset(&sr, 0, sizeof(struct ibv_send_wr));
    memset(&sr_sge, 0, sizeof(struct ibv_sge));
    sr_sge.addr = (uint64_t)ts_buf_;
    sr_sge.length = ts_buf_off_;
    sr_sge.lkey = ts_buf_mr_->lkey;
    sr.wr_id = 1;
    sr.next = NULL;
    sr.sg_list = &sr_sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_SEND;
    sr.send_flags = IBV_SEND_SIGNALED;
    ret = nm_->rdma_post_send_sid_sync(&sr, 0);
    assert(ret == 0);

    struct ibv_wc wc;
    ret = nm_->rdma_poll_one_recv_completion_sync(&wc);
    if (wc.opcode != IBV_WC_RECV) {
      printd(L_INFO, "opcode: %d", wc.opcode);
      printf("opcode: %d, status: %d, wrid: %ld\n", wc.opcode, wc.status,
             wc.wr_id);
    }
    assert(ret == 0);
    assert(wc.status == IBV_WC_SUCCESS);
    assert(wc.opcode == IBV_WC_RECV);
    assert(wc.wr_id == 0);
    assert(*(uint8_t*)ts_rep_buf_ == IBMSG_REP_PRIORITY);
    assert(*(int*)((uint64_t)ts_rep_buf_ + sizeof(uint8_t)) == 0);

    // reset ts message buffer
    memset(ts_buf_, 0, MSG_BUF_SIZE);
    *(uint8_t*)ts_buf_ = IBMSG_REQ_PRIORITY;
    *(uint16_t*)((uint64_t)ts_buf_ + sizeof(uint8_t)) = my_sid_;
    *(uint32_t*)((uint64_t)ts_buf_ + sizeof(uint8_t) + sizeof(uint16_t)) = 0;
    ts_buf_off_ = sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint32_t);
    gettimeofday(&last_sync_time_, NULL);
    num_cliquemap_sync_++;
  }
  assert(ctx->key_size < 256);
  *(uint32_t*)((uint64_t)ts_buf_ + sizeof(uint8_t) + sizeof(uint16_t)) += 1;
  *(uint64_t*)((uint64_t)ts_buf_ + ts_buf_off_) = new_ts();
  *(uint8_t*)((uint64_t)ts_buf_ + ts_buf_off_ + sizeof(uint64_t)) =
      (uint8_t)ctx->key_size;
  memcpy((void*)((uint64_t)ts_buf_ + ts_buf_off_ + sizeof(uint64_t) +
                 sizeof(uint8_t)),
         ctx->key, ctx->key_size);
  ts_buf_off_ += required_size;
}

void DMCClient::update_priority(KVOpsCtx* ctx) {
  switch (eviction_type_) {
    case EVICT_NON:
      return;
    case EVICT_CLIQUEMAP:
      return update_priority_cliquemap(ctx);
    case EVICT_SAMPLE:
      return update_priority_sample(ctx);
    case EVICT_SAMPLE_ADAPTIVE_NAIVE:
    case EVICT_SAMPLE_NAIVE:
      return update_priority_sample_naive(ctx);
    case EVICT_SAMPLE_ADAPTIVE_HEAVY:
    case EVICT_SAMPLE_ADAPTIVE:
      return update_priority_sample_adaptive(ctx);
    case EVICT_PRECISE:
      return update_priority_precise(ctx);
    default:
      printd(L_ERROR, "Unsupported eviction type %d", eviction_type_);
      abort();
  }
}

void DMCClient::kv_get_read_index(KVOpsCtx* ctx) {
  printd(L_DEBUG, "kv_get");
  // construct rdma read bucket request
  struct ibv_send_wr* head_wr = NULL;
  struct ibv_send_wr read_bucket_wr;
  struct ibv_sge read_bucket_sge;
  memset(&read_bucket_wr, 0, sizeof(struct ibv_send_wr));
  ib_create_sge(ctx->bucket_laddr, local_buf_mr_->lkey, sizeof(Bucket),
                &read_bucket_sge);
  read_bucket_wr.wr_id = 12301;
  read_bucket_wr.next = head_wr;
  read_bucket_wr.sg_list = &read_bucket_sge;
  read_bucket_wr.num_sge = 1;
  read_bucket_wr.opcode = IBV_WR_RDMA_READ;
  read_bucket_wr.send_flags = IBV_SEND_SIGNALED;
  read_bucket_wr.wr.rdma.remote_addr = ctx->init_bucket_raddr;
  read_bucket_wr.wr.rdma.rkey = server_rkey_map_[0];
  head_wr = &read_bucket_wr;
  num_rdma_read_++;

  struct ibv_send_wr read_hist_head_wr;
  struct ibv_sge read_hist_head_sge;
  if (is_evict_adaptive(eviction_type_)) {
    memset(&read_hist_head_wr, 0, sizeof(struct ibv_send_wr));
    ib_create_sge(ctx->op_laddr, local_buf_mr_->lkey, sizeof(uint64_t),
                  &read_hist_head_sge);
    read_hist_head_wr.wr_id = 12302;
    read_hist_head_wr.next = head_wr;
    read_hist_head_wr.sg_list = &read_hist_head_sge;
    read_hist_head_wr.num_sge = 1;
    read_hist_head_wr.opcode = IBV_WR_RDMA_READ;
    read_hist_head_wr.send_flags = 0;
    read_hist_head_wr.wr.rdma.remote_addr = get_hist_head_raddr();
    read_hist_head_wr.wr.rdma.rkey = server_rkey_map_[0];
    head_wr = &read_hist_head_wr;
    num_rdma_read_++;
  }

  assert(head_wr != NULL);
  nm_->rdma_post_send_sid_sync(head_wr, 0);

  if (is_evict_adaptive(eviction_type_)) {
    ctx->has_read_hist_head = true;
    ctx->read_hist_head = *(uint64_t*)ctx->op_laddr;
  }
}

void DMCClient::kv_get_copy_value(KVOpsCtx* ctx,
                                  __OUT void* val,
                                  __OUT uint32_t* val_len) {
  ObjHeader* header = (ObjHeader*)ctx->target_block_laddr;
  uint32_t rval_len = header->val_size;
  uint32_t rkey_len = header->key_size;
  memcpy(val, (void*)(ctx->target_block_laddr + rkey_len + sizeof(ObjHeader)),
         rval_len);
  *val_len = rval_len;
}

int DMCClient::kv_set_1s(void* key,
                         uint32_t key_size,
                         void* val,
                         uint32_t val_size) {
  KVOpsCtx ctx;
  int ret = 0;
  bool set_miss = true;
  create_op_ctx(&ctx, key, key_size, val, val_size, SET);
  kv_set_alloc_rblock(&ctx);
  kv_set_read_index_write_kv(&ctx);
kv_set_1s_retry:
  match_fp_and_find_empty(&ctx);
  if (ctx.num_fp_match > 0) {
    read_and_find_kv(&ctx);
  }
  if (ctx.key_found == true) {
    kv_set_update_index(&ctx);
    set_miss = false;
  } else if (ctx.num_free_slot > 0) {
    find_empty(&ctx);
    kv_set_update_index(&ctx);
  } else {
    // key not found and there is no empty slot
    do {
      ret = evict_bucket(&ctx);
    } while (ret == -1);
    if (ret == 1) {
      match_fp_and_find_empty(&ctx);
      find_empty(&ctx);
      kv_set_update_index(&ctx);
    }
  }
  // adjust weights on set miss
  if (ctx.key_found == false && is_evict_adaptive(eviction_type_)) {
    adaptive_update_weights(&ctx);
  }
  // check if index is successfully modified
  if (ctx.ret == DMC_SET_RETRY) {
    // retry when failed to insert new object
    num_set_retry_++;
    kv_get_read_index(&ctx);
    ctx.ret = DMC_SUCCESS;  // reset to prevent loop
    goto kv_set_1s_retry;
  } else if (ctx.ret == DMC_SET_RETURN) {
#ifdef USE_REWARDS
    // if (ctx.key_found == true)
    // hit_adjust_weights(&ctx);
#endif
    // return when the object to update have been evicted
    return set_miss ? -1 : 0;
  }
  assert(ctx.target_slot_laddr != 0);
  update_priority(&ctx);

  if (ctx.key_found == false) {
    ctx.num_fp_match = 0;
    kv_get_read_index(&ctx);
    match_fp_and_find_empty(&ctx);
    if (ctx.num_fp_match <= 1) {
      return set_miss ? -1 : 0;
    }
    kv_set_delete_duplicate(&ctx);
  }

  return set_miss ? -1 : 0;
}

int DMCClient::kv_get_1s(void* key,
                         uint32_t key_size,
                         __OUT void* val,
                         __OUT uint32_t* val_size) {
  char key_buf[256] = {0};
  memcpy(key_buf, key, key_size);
  printd(L_DEBUG, "get %s", key_buf);
  KVOpsCtx ctx;
  create_op_ctx(&ctx, key, key_size, NULL, 0, GET);
  kv_get_read_index(&ctx);
  match_fp_and_find_empty(&ctx);
  if (ctx.num_fp_match == 0) {
    printd(L_DEBUG, "No match fp found");
    return -1;  // not found
  } else {
    read_and_find_kv(&ctx);
    if (ctx.key_found == false) {
      printd(L_DEBUG, "miss %s", key_buf);
      if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE) {
        adaptive_update_weights(&ctx);
      }
      return -1;  // not found
    }
  }
#ifdef USE_REWARDS
  hit_adjust_weights(&ctx);
#endif
  update_priority(&ctx);

  // copy value
  kv_get_copy_value(&ctx, val, val_size);
  return 0;
}

int DMCClient::evict(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict");
  num_evict_++;
  int ret = 0;
  switch (eviction_type_) {
    case EVICT_NON:
    case EVICT_SAMPLE:
      ret = evict_sample(ctx);
      break;
    case EVICT_PRECISE:
      ret = evict_precise(ctx);
      break;
    case EVICT_SAMPLE_NAIVE:
      ret = evict_sample_naive(ctx);
      break;
    case EVICT_SAMPLE_ADAPTIVE:
      ret = evict_sample_adaptive(ctx);
      break;
    case EVICT_SAMPLE_ADAPTIVE_NAIVE:
      ret = evict_sample_adaptive_naive(ctx);
      break;
    case EVICT_SAMPLE_ADAPTIVE_HEAVY:
      ret = evict_sample_adaptive_heavy(ctx);
      break;
    default:
      printd(L_ERROR, "Unsupported eviction type %d", eviction_type_);
      abort();
  }
  num_success_evict_ += (ret == 0);
  return ret;
}

int DMCClient::evict_sample_adaptive_heavy(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  uint64_t sampled_key_slot_raddr[MAX_NUM_SAMPLES];
  Slot sampled_key_slot[MAX_NUM_SAMPLES];
  int num_sampled_keys = 0;
  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  while (num_sampled_keys < num_samples_) {
    num_rdma_read_++;
    // read a random bucket
    uint64_t rand_hash = random() % (HASH_NUM_BUCKETS - 4);
    uint64_t bucket_raddr;
    uint16_t bucket_rsid;
    get_init_bucket_raddr(rand_hash, &bucket_raddr, &bucket_rsid);
    nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, ctx->op_laddr, lkey,
                            sizeof(Bucket) * 4);

    Slot* l_slot = (Slot*)ctx->op_laddr;
    for (int i = 0;
         i < HASH_BUCKET_ASSOC_NUM * 4 && num_sampled_keys < MAX_NUM_SAMPLES;
         i++) {
      if (*(uint64_t*)&l_slot[i] == ADAPTIVE_TMP_SLOT)
        continue;
      uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
      if (kv_raddr == 0 || naive_history_->is_in_history(kv_raddr))
        continue;
      memcpy(&sampled_key_slot[num_sampled_keys], &l_slot[i], sizeof(Slot));
      sampled_key_slot_raddr[num_sampled_keys] =
          bucket_raddr + i * sizeof(Slot);
      for (int j = 0; j < num_experts_; j++) {
        double prio = experts_[j]->parse_priority(&l_slot[i].meta,
                                                  l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, num_sampled_keys);
      }
      num_sampled_keys++;
    }
  }

  // read weights if do not use async weights
  adaptive_read_weights();

  // try evict best candiate
  std::vector<int> best_candidate_list;
  for (int i = 0; i < num_experts_; i++) {
    std::sort(prio_slot_id[i].begin(), prio_slot_id[i].end());
    best_candidate_list.push_back(prio_slot_id[i].begin()->second);
  }
  uint8_t expert_bmap = 0;
  int best_candidate =
      adaptive_get_best_candidate(best_candidate_list, &expert_bmap);
  uint64_t best_slot_raddr = sampled_key_slot_raddr[best_candidate];
  Slot* best_lslot = &sampled_key_slot[best_candidate];

  num_rdma_cas_++;
  nm_->rdma_cas_sid_sync(0, best_slot_raddr, rkey, ctx->op_laddr, lkey,
                         *(uint64_t*)best_lslot, ADAPTIVE_TMP_SLOT);
  if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)best_lslot) {
    printd(L_DEBUG, "evict best success");
    uint64_t free_addr = HashIndexConvert48To64Bits(
        sampled_key_slot[best_candidate].atomic.pointer);
    mm_->free(free_addr, rkey, block_size_, 0);

    // clear slotMeta
    SlotMeta* new_slotMeta = (SlotMeta*)(ctx->op_laddr + sizeof(uint64_t));
    memset(new_slotMeta, 0, sizeof(SlotMeta));
    ret = nm_->rdma_inl_write_sid_async(0, best_slot_raddr + SLOT_META_OFF,
                                        rkey, (uint64_t)new_slotMeta, lkey,
                                        sizeof(SlotMeta));
    assert(ret == 0);
    num_rdma_write_++;

    // add to history
    // 1. get key
    uint64_t kv_laddr = ctx->op_laddr + sizeof(uint64_t) + sizeof(SlotMeta);
    ret = nm_->rdma_read_sid_sync(0, free_addr, rkey, kv_laddr, lkey,
                                  block_size_);
    num_rdma_read_++;
    ObjHeader* header = (ObjHeader*)kv_laddr;
    uint64_t key_laddr = kv_laddr + sizeof(ObjHeader);
    uint64_t key_hash = hash_->hash_func1((void*)key_laddr, header->key_size);
    naive_history_->insert(nm_, best_slot_raddr, key_hash, expert_bmap,
                           &best_lslot->meta);
    num_rdma_faa_++;
    num_rdma_read_++;
    num_rdma_write_++;
    num_rdma_cas_ += 2;
    return 0;
  }

  // resort to evict according to the order of best expert
  int best_expert = adaptive_get_best_expert(&expert_bmap);
  auto it = prio_slot_id[best_expert].begin();
  for (; it != prio_slot_id[best_expert].end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    if (idx == best_candidate)
      continue;
    uint64_t target_slot_raddr = sampled_key_slot_raddr[idx];
    Slot* target_slot = &sampled_key_slot[idx];
    *(uint64_t*)ctx->op_laddr = 0;
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, ADAPTIVE_TMP_SLOT);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      // Eviction success
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, server_rkey_map_[0], block_size_, 0);

      // clear the corresponding access information slot (async)
      SlotMeta* new_slotMeta = (SlotMeta*)(ctx->op_laddr + sizeof(uint64_t));
      memset(new_slotMeta, 0, sizeof(SlotMeta));
      nm_->rdma_inl_write_sid_async(0, target_slot_raddr + SLOT_META_OFF, rkey,
                                    (uint64_t)new_slotMeta, lkey,
                                    sizeof(SlotMeta));
      num_rdma_write_++;

      // insert to history
      // 1. read kv
      uint64_t kv_laddr = ctx->op_laddr + sizeof(uint64_t) + sizeof(SlotMeta);
      ret = nm_->rdma_read_sid_sync(0, free_addr, rkey, kv_laddr, lkey,
                                    block_size_);
      num_rdma_read_++;
      ObjHeader* header = (ObjHeader*)kv_laddr;
      uint64_t key_laddr = kv_laddr + sizeof(ObjHeader);
      uint64_t key_hash = hash_->hash_func1((void*)key_laddr, header->key_size);
      naive_history_->insert(nm_, target_slot_raddr, key_hash, expert_bmap,
                             &target_slot->meta);
      num_rdma_faa_++;
      num_rdma_read_++;
      num_rdma_write_++;
      num_rdma_cas_ += 2;
      return 0;
    }
  }
  return -1;
}

int DMCClient::evict_sample_adaptive_naive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "evict adaptive naive");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  uint64_t sampled_key_slot_raddr[MAX_NUM_SAMPLES];
  uint64_t sampled_key_hash[MAX_NUM_SAMPLES];
  Slot sampled_key_slot[MAX_NUM_SAMPLES];
  SlotMeta sampled_key_slotM[MAX_NUM_SAMPLES];
  int num_sampled_keys = 0;
  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  while (num_sampled_keys < num_samples_) {
    num_rdma_read_++;
    uint64_t rand_hash = random() % (HASH_NUM_BUCKETS - 4);
    uint64_t bucket_raddr;
    uint16_t bucket_rsid;
    get_init_bucket_raddr(rand_hash, &bucket_raddr, &bucket_rsid);
    nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, ctx->op_laddr, lkey,
                            sizeof(Bucket) * 4);

    Slot* l_slot = (Slot*)ctx->op_laddr;
    for (int i = 0;
         i < 4 * HASH_BUCKET_ASSOC_NUM && num_samples_ < MAX_NUM_SAMPLES; i++) {
      if (*(uint64_t*)&l_slot[i] == ADAPTIVE_TMP_SLOT)
        continue;

      uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
      if (kv_raddr == 0 || naive_history_->is_in_history(kv_raddr))
        continue;

      uint64_t kv_laddr = ctx->op_laddr + sizeof(Bucket) * 4;
      num_rdma_read_++;
      nm_->rdma_read_sid_sync(0, kv_raddr, rkey, kv_laddr, lkey, block_size_);
      ObjHeader* header = (ObjHeader*)kv_laddr;
      void* cur_key = (void*)(kv_laddr + sizeof(ObjHeader));

      memcpy(&sampled_key_slot[num_sampled_keys], &l_slot[i], sizeof(Slot));
      memcpy(&sampled_key_slotM[num_sampled_keys], &header->meta,
             sizeof(SlotMeta));
      sampled_key_slot_raddr[num_sampled_keys] =
          bucket_raddr + i * sizeof(Slot);
      sampled_key_hash[num_sampled_keys] =
          hash_->hash_func1(cur_key, header->key_size);

      for (int j = 0; j < num_experts_; j++) {
        double prio =
            experts_[j]->parse_priority(&header->meta, l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, num_sampled_keys);
      }
      num_sampled_keys++;
    }
  }

  // read weights
  adaptive_read_weights();

  // try evict the best candidate
  std::vector<int> best_candidate_list;
  for (int i = 0; i < num_experts_; i++) {
    std::sort(prio_slot_id[i].begin(), prio_slot_id[i].end());
    best_candidate_list.push_back(prio_slot_id[i].begin()->second);
  }
  uint8_t expert_bmap = 0;
  int best_candidate =
      adaptive_get_best_candidate(best_candidate_list, &expert_bmap);
  uint64_t best_slot_raddr = sampled_key_slot_raddr[best_candidate];
  Slot* best_slot = &sampled_key_slot[best_candidate];

  num_rdma_cas_++;
  nm_->rdma_cas_sid_sync(0, best_slot_raddr, rkey, ctx->op_laddr, lkey,
                         *(uint64_t*)best_slot, ADAPTIVE_TMP_SLOT);
  if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)best_slot) {
    printd(L_DEBUG, "evict best success");
    uint64_t free_addr = HashIndexConvert48To64Bits(
        sampled_key_slot[best_candidate].atomic.pointer);
    mm_->free(free_addr, rkey, block_size_, 0);

    // add to history
    naive_history_->insert(nm_, best_slot_raddr,
                           sampled_key_hash[best_candidate], expert_bmap,
                           &sampled_key_slotM[best_candidate]);
    num_rdma_faa_++;
    num_rdma_read_++;
    num_rdma_write_++;
    num_rdma_cas_ += 2;
    return 0;
  }

  // resort to evict according to the best candidate
  int best_expert = adaptive_get_best_expert(&expert_bmap);
  auto it = prio_slot_id[best_expert].begin();
  for (; it != prio_slot_id[best_expert].end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    uint64_t target_slot_raddr = sampled_key_slot_raddr[idx];
    Slot* target_slot = &sampled_key_slot[idx];
    *(uint64_t*)ctx->op_laddr = 0;
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, ADAPTIVE_TMP_SLOT);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      printd(L_DEBUG, "evict resort success");
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);

      // insert into history
      naive_history_->insert(nm_, target_slot_raddr, sampled_key_hash[idx],
                             expert_bmap, &sampled_key_slotM[best_candidate]);
      num_rdma_faa_++;
      num_rdma_read_++;
      num_rdma_write_++;
      num_rdma_cas_ += 2;
      return 0;
    }
  }
  return -1;
}

int DMCClient::evict_precise(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict precise!");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  // 1. get the victim
  uint32_t victim_id = prio_list_->get_list_min(nm_);
  uint64_t victim_raddr = get_slot_raddr(victim_id);
  Slot* victim_lslot = (Slot*)(ctx->op_laddr);
  nm_->rdma_read_sid_sync(0, victim_raddr, rkey, (uint64_t)victim_lslot, lkey,
                          sizeof(Slot));
  if (HashIndexConvert48To64Bits(victim_lslot->atomic.pointer) == 0) {
    return -1;
  }

  // 2. cas the victim
  Slot* swap_back = (Slot*)(ctx->op_laddr + sizeof(Slot));
  nm_->rdma_cas_sid_sync(0, victim_raddr, rkey, (uint64_t)swap_back, lkey,
                         *(uint64_t*)victim_lslot, 0);
  if (*(uint64_t*)swap_back != *(uint64_t*)victim_lslot) {
    return -1;
  }

  // 3. free the block
  uint64_t free_raddr =
      HashIndexConvert48To64Bits(victim_lslot->atomic.pointer);
  assert(free_raddr != 0);
  mm_->free(free_raddr, rkey, block_size_, 0);

  // 4. delete the entry from list
  ret = prio_list_->delete_slot(nm_, victim_id, victim_raddr);
  assert(ret == 0);

  // 5. clear slotMeta
  uint32_t slotMeta_raddr = get_slot_raddr(victim_id) + SLOT_META_OFF;
  SlotMeta emptyMeta;
  memset(&emptyMeta, 0, sizeof(SlotMeta));
  nm_->rdma_inl_write_sid_sync(0, slotMeta_raddr, rkey, (uint64_t)&emptyMeta,
                               sizeof(SlotMeta));
  return 0;
}

int DMCClient::evict_sample_naive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;
  uint64_t bucket_laddr = ctx->op_laddr;
  uint64_t kv_laddr = ctx->op_laddr + sizeof(Bucket);

  uint64_t sampled_key_slot_raddr[MAX_NUM_SAMPLES];
  uint16_t sampled_key_slot_rsid[MAX_NUM_SAMPLES];
  Slot sampled_key_slot[MAX_NUM_SAMPLES];
  std::vector<std::pair<double, int>> prio_slot_id;
  int num_sampled_keys = 0;
  while (num_sampled_keys < num_samples_) {
    num_evict_read_bucket_++;
    num_rdma_read_++;
    // read a random bucket
    uint64_t rand_hash = random();
    uint64_t bucket_raddr;
    uint16_t bucket_rsid;
    get_init_bucket_raddr(rand_hash, &bucket_raddr, &bucket_rsid);
    ret = nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, bucket_laddr, lkey,
                                  sizeof(Bucket));
    assert(ret == 0);

    Slot* l_slot = (Slot*)ctx->op_laddr;
    for (int i = 0;
         i < HASH_BUCKET_ASSOC_NUM && num_sampled_keys < MAX_NUM_SAMPLES; i++) {
      uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
      if (kv_raddr == 0)
        continue;
      ret = nm_->rdma_read_sid_sync(0, kv_raddr, rkey, kv_laddr, lkey,
                                    block_size_);
      assert(ret == 0);

      ObjHeader* header = (ObjHeader*)kv_laddr;
      double prio =
          priority_->parse_priority(&header->meta, l_slot[i].atomic.kv_len);
      memcpy(&sampled_key_slot[num_sampled_keys], &l_slot[i], sizeof(Slot));
      sampled_key_slot_raddr[num_sampled_keys] =
          bucket_raddr + i * sizeof(Slot);
      sampled_key_slot_rsid[num_sampled_keys] = bucket_rsid;
      prio_slot_id.emplace_back(prio, num_sampled_keys);
      num_sampled_keys++;
    }
  }

  std::sort(prio_slot_id.begin(), prio_slot_id.end());
  auto it = prio_slot_id.begin();
  for (; it != prio_slot_id.end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    uint64_t target_slot_raddr = sampled_key_slot_raddr[idx];
    uint16_t target_slot_rsid = sampled_key_slot_rsid[idx];
    Slot* target_slot = &sampled_key_slot[idx];
    *(uint64_t*)ctx->op_laddr = 0;
    ret = nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr,
                                 lkey, *(uint64_t*)target_slot, 0);
    assert(ret == 0);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, rkey, block_size_, 0);
      break;
    }
  }
  if (it == prio_slot_id.end()) {
    printd(L_DEBUG, "Eviction failed");
    return -1;
  }
  return 0;
}

int DMCClient::evict_sample_adaptive(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  uint64_t sampled_key_slot_raddr[MAX_NUM_SAMPLES];
  uint16_t sampled_key_slot_rsid[MAX_NUM_SAMPLES];
  Slot sampled_key_slot[MAX_NUM_SAMPLES];
  int num_sampled_keys = 0;
  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  while (num_sampled_keys < num_samples_) {
    num_rdma_read_++;
    // read a random bucket
    uint64_t rand_hash = random() % (HASH_NUM_BUCKETS - 4);
    uint64_t bucket_raddr;
    uint16_t bucket_rsid;
    get_init_bucket_raddr(rand_hash, &bucket_raddr, &bucket_rsid);
    if (ctx->has_faa_hist_head == false) {
      // FAA hist head and read bucket
      struct ibv_send_wr sr[2];
      struct ibv_sge sge[2];
      memset(sr, 0, sizeof(struct ibv_send_wr) * 2);
      // read bucket sr
      ib_create_sge(ctx->op_laddr, lkey, sizeof(Bucket) * 4, &sge[0]);
      sr[0].wr_id = 2331;
      sr[0].next = &sr[1];
      sr[0].sg_list = &sge[0];
      sr[0].num_sge = 1;
      sr[0].opcode = IBV_WR_RDMA_READ;
      sr[0].send_flags = 0;
      sr[0].wr.rdma.remote_addr = bucket_raddr;
      sr[0].wr.rdma.rkey = rkey;

      // faa hist_head sr
      num_rdma_faa_++;
      ib_create_sge(ctx->op_laddr + sizeof(Bucket) * 4, lkey, sizeof(uint64_t),
                    &sge[1]);
      sr[1].wr_id = 2332;
      sr[1].next = NULL;
      sr[1].sg_list = &sge[1];
      sr[1].num_sge = 1;
      sr[1].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
      sr[1].send_flags = IBV_SEND_SIGNALED;
      sr[1].wr.atomic.compare_add = 1;
      sr[1].wr.atomic.remote_addr = lw_history_->hist_cntr_raddr();
      sr[1].wr.atomic.rkey = rkey;
      nm_->rdma_post_send_sid_sync(sr, 0);
      ctx->faa_hist_head = *(uint64_t*)(ctx->op_laddr + sizeof(Bucket) * 4);
      ctx->has_faa_hist_head = true;
    } else {
      nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, ctx->op_laddr, lkey,
                              sizeof(Bucket) * 4);
    }

    Slot* l_slot = (Slot*)(ctx->op_laddr);
    for (int i = 0;
         i < HASH_BUCKET_ASSOC_NUM * 4 && num_sampled_keys < MAX_NUM_SAMPLES;
         i++) {
      uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
      uint64_t slot_raddr = bucket_raddr + i * sizeof(Slot);
      if (kv_raddr == 0 || lw_history_->is_in_history(&l_slot[i]))
        continue;
      memcpy(&sampled_key_slot[num_sampled_keys], &l_slot[i], sizeof(Slot));
      sampled_key_slot_raddr[num_sampled_keys] = slot_raddr;
      sampled_key_slot_rsid[num_sampled_keys] = bucket_rsid;
      for (int j = 0; j < num_experts_; j++) {
        double prio = experts_[j]->parse_priority(&l_slot[i].meta,
                                                  l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, num_sampled_keys);
      }
      num_sampled_keys++;
    }
  }

  // read weights if do not use asychrounous weight update
  if (!use_async_weight_)
    adaptive_read_weights();

  // construct new slot value
  assert(ctx->has_faa_hist_head == true);
  Slot hist_slot;
  hist_slot.atomic.kv_len = 0xF;
  HashIndexConvert64To48Bits(ctx->faa_hist_head, hist_slot.atomic.pointer);

  // try evict best candiate
  // get the best candidate
  std::vector<int> best_candidate_list;
  for (int i = 0; i < num_experts_; i++) {
    std::sort(prio_slot_id[i].begin(), prio_slot_id[i].end());
    best_candidate_list.push_back(prio_slot_id[i].begin()->second);
  }

  uint8_t expert_bmap = 0;
  int best_candidate =
      adaptive_get_best_candidate(best_candidate_list, &expert_bmap);

  // evict best candidate slot
  uint64_t best_slot_raddr = sampled_key_slot_raddr[best_candidate];
  uint64_t best_slotMeta_raddr = best_slot_raddr + SLOT_META_OFF;
  Slot* best_lslot = &sampled_key_slot[best_candidate];
  hist_slot.atomic.fp = best_lslot->atomic.fp;
  num_rdma_cas_++;
  nm_->rdma_cas_sid_sync(0, best_slot_raddr, rkey, ctx->op_laddr, lkey,
                         *(uint64_t*)best_lslot, *(uint64_t*)&hist_slot);
  if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)best_lslot) {
    count_expert_evict(expert_bmap);
    printd(L_DEBUG, "evict best success");
    uint64_t free_addr = HashIndexConvert48To64Bits(best_lslot->atomic.pointer);
    mm_->free(free_addr, rkey, block_size_, 0);

    // modify slotMeta to record expert_bmap
    nm_->rdma_inl_write_sid_async(
        0, best_slotMeta_raddr + SLOTM_INFO_INS_TS_OFF, rkey,
        (uint64_t)&expert_bmap, 0, sizeof(uint8_t));
    num_rdma_write_++;

    return 0;
  }

  // resort to evict according to the order of best expert
  int best_expert = adaptive_get_best_expert(&expert_bmap);
  auto it = prio_slot_id[best_expert].begin();
  for (; it != prio_slot_id[best_expert].end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    if (idx == best_candidate)
      continue;
    uint64_t target_slot_raddr = sampled_key_slot_raddr[idx];
    uint64_t target_slotMeta_raddr = target_slot_raddr + SLOT_META_OFF;
    uint16_t target_slot_rsid = sampled_key_slot_rsid[idx];
    Slot* target_slot = &sampled_key_slot[idx];
    *(uint64_t*)ctx->op_laddr = 0;
    hist_slot.atomic.fp = target_slot->atomic.fp;
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, *(uint64_t*)&hist_slot);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      count_expert_evict(expert_bmap);
      // Eviction success
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      mm_->free(free_addr, server_rkey_map_[0], block_size_, 0);

      // record the expert_bmap in slotMeta
      nm_->rdma_inl_write_sid_async(
          0, target_slotMeta_raddr + SLOTM_INFO_INS_TS_OFF, rkey,
          (uint64_t)&expert_bmap, 0, sizeof(uint8_t));
      num_rdma_write_++;

      return 0;
    }
  }
  return -1;
}

int DMCClient::evict_sample(KVOpsCtx* ctx) {
  printd(L_DEBUG, "Evict");
  int ret = 0;
  uint32_t rkey = server_rkey_map_[0];
  uint32_t lkey = local_buf_mr_->lkey;

  uint64_t sampled_key_slot_raddr[MAX_NUM_SAMPLES];
  uint16_t sampled_key_slot_rsid[MAX_NUM_SAMPLES];
  Slot sampled_key_slot[MAX_NUM_SAMPLES];
  std::vector<std::pair<double, int>> prio_slot_id;
  int num_sampled_keys = 0;
  while (num_sampled_keys < num_samples_) {
    num_evict_read_bucket_++;
    num_rdma_read_++;
    // read a random bucket
    uint64_t rand_hash = random() % (HASH_NUM_BUCKETS - 4);
    uint64_t bucket_raddr;
    uint16_t bucket_rsid;
    get_init_bucket_raddr(rand_hash, &bucket_raddr, &bucket_rsid);
    ret = nm_->rdma_read_sid_sync(0, bucket_raddr, rkey, ctx->op_laddr, lkey,
                                  sizeof(Bucket) * 4);
    assert(ret == 0);

    Slot* l_slot = (Slot*)ctx->op_laddr;
    for (int i = 0;
         i < HASH_BUCKET_ASSOC_NUM * 4 && num_sampled_keys < MAX_NUM_SAMPLES;
         i++) {
      if (HashIndexConvert48To64Bits(l_slot[i].atomic.pointer) == 0)
        continue;
      double prio =
          priority_->parse_priority(&l_slot[i].meta, l_slot[i].atomic.kv_len);
      uint64_t slot_raddr = bucket_raddr + i * sizeof(Slot);
      memcpy(&sampled_key_slot[num_sampled_keys], &l_slot[i], sizeof(Slot));
      sampled_key_slot_raddr[num_sampled_keys] = slot_raddr;
      sampled_key_slot_rsid[num_sampled_keys] = bucket_rsid;
      prio_slot_id.emplace_back(prio, num_sampled_keys);
      num_sampled_keys++;
    }
  }

  // cas the slot to delete the item
  std::sort(prio_slot_id.begin(), prio_slot_id.end());

  auto it = prio_slot_id.begin();
  for (; it != prio_slot_id.end(); it++) {
    num_rdma_cas_++;
    int idx = it->second;
    uint64_t target_slot_raddr = sampled_key_slot_raddr[idx];
    uint64_t target_slotMeta_raddr = target_slot_raddr + SLOT_META_OFF;
    uint16_t target_slot_rsid = sampled_key_slot_rsid[idx];
    Slot* target_slot = &sampled_key_slot[idx];
    *(uint64_t*)ctx->op_laddr = 0;
    nm_->rdma_cas_sid_sync(0, target_slot_raddr, rkey, ctx->op_laddr, lkey,
                           *(uint64_t*)target_slot, 0);
    if (*(uint64_t*)ctx->op_laddr == *(uint64_t*)target_slot) {
      priority_->evict_callback(it->first);
      printd(L_DEBUG, "evicti priority: %lf", it->first);
      // Eviction success
      uint64_t free_addr =
          HashIndexConvert48To64Bits(target_slot->atomic.pointer);
      printd(L_DEBUG, "free kv block @%d:%lx", 0, free_addr);
      mm_->free(free_addr, server_rkey_map_[0], block_size_, 0);

      // clear the corresponding access information slot (async)
      SlotMeta* new_slotMeta = (SlotMeta*)(ctx->op_laddr + sizeof(uint64_t));
      memset(new_slotMeta, 0, sizeof(SlotMeta));
      nm_->rdma_inl_write_sid_async(0, target_slotMeta_raddr, rkey,
                                    (uint64_t)new_slotMeta, lkey,
                                    sizeof(SlotMeta));

      // // TODO: remove this after debug
      // nm_->rdma_read_sid_sync(0, free_addr, server_rkey_map_[0],
      //     ctx->op_laddr, lkey, block_size_);
      // ObjHeader * header = (ObjHeader *)ctx->op_laddr;
      // uint32_t key_len = header->key_size;
      // void * key = (void *)(ctx->op_laddr + sizeof(ObjHeader));
      // log_op("EVICT", key, key_len, true);
      // // TODO: remove the above

      return 0;
    }
  }
  return -1;
}

int DMCClient::kv_get_2s(void* key,
                         uint32_t key_size,
                         __OUT void* val,
                         __OUT uint32_t* val_size) {
  *(uint8_t*)local_buf_ = IBMSG_REQ_GET;
  *(uint16_t*)((uint64_t)local_buf_ + sizeof(uint8_t)) = my_sid_;
  uint64_t key_addr = (uint64_t)local_buf_ + sizeof(uint8_t) + sizeof(uint16_t);

  *(uint32_t*)key_addr = key_size;
  memcpy((void*)(key_addr + sizeof(uint32_t)), key, key_size);

  // post rr fist
  struct ibv_recv_wr rr;
  struct ibv_sge recv_sge;
  memset(&rr, 0, sizeof(struct ibv_recv_wr));
  memset(&recv_sge, 0, sizeof(struct ibv_sge));
  recv_sge.addr = (uint64_t)local_buf_ + block_size_;
  recv_sge.length = block_size_;
  recv_sge.lkey = local_buf_mr_->lkey;
  rr.wr_id = 1;
  rr.next = NULL;
  rr.sg_list = &recv_sge;
  rr.num_sge = 1;
  int ret = nm_->rdma_post_recv_sid_async(&rr, 0);
  assert(ret == 0);

  struct ibv_send_wr sr;
  struct ibv_sge send_sge;
  memset(&sr, 0, sizeof(struct ibv_send_wr));
  memset(&send_sge, 0, sizeof(struct ibv_sge));
  send_sge.addr = (uint64_t)local_buf_;
  send_sge.length = block_size_;
  send_sge.lkey = local_buf_mr_->lkey;
  sr.wr_id = 0;
  sr.next = NULL;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_SEND;
  sr.send_flags = IBV_SEND_SIGNALED;
  ret = nm_->rdma_post_send_sid_sync(&sr, 0);
  assert(ret == 0);

  struct ibv_wc recv_wc;
  ret = nm_->rdma_poll_one_recv_completion_sync(&recv_wc);
  assert(ret == 0);
  assert(recv_wc.status == IBV_WC_SUCCESS);
  assert(recv_wc.opcode == IBV_WC_RECV);
  assert(recv_wc.wr_id == 1);

  uint64_t ret_msg_addr = (uint64_t)local_buf_ + block_size_;
  *val_size = *(uint32_t*)ret_msg_addr;
  if (*val_size != 0) {
    memcpy(val, (void*)(ret_msg_addr + sizeof(uint32_t)), *val_size);
    return 0;
  }
  return -1;
}

int DMCClient::kv_set_2s(void* key,
                         uint32_t key_size,
                         void* val,
                         uint32_t val_size) {
  *(uint8_t*)local_buf_ = IBMSG_REQ_SET;
  *(uint16_t*)((uint64_t)local_buf_ + sizeof(uint8_t)) = my_sid_;
  uint64_t kv_addr = (uint64_t)local_buf_ + sizeof(uint8_t) + sizeof(uint16_t);

  *(uint32_t*)kv_addr = key_size;
  *(uint32_t*)(kv_addr + sizeof(uint32_t)) = val_size;
  memcpy((void*)(kv_addr + 2 * sizeof(uint32_t)), key, key_size);
  memcpy((void*)(kv_addr + 2 * sizeof(uint32_t) + key_size), val, val_size);

  // post rr fist
  uint64_t rand_rr_id = rand();
  struct ibv_recv_wr rr;
  struct ibv_sge recv_sge;
  memset(&rr, 0, sizeof(struct ibv_recv_wr));
  memset(&recv_sge, 0, sizeof(struct ibv_sge));
  recv_sge.addr = (uint64_t)local_buf_ + block_size_;
  recv_sge.length = block_size_;
  recv_sge.lkey = local_buf_mr_->lkey;
  rr.wr_id = rand_rr_id;
  rr.next = NULL;
  rr.sg_list = &recv_sge;
  rr.num_sge = 1;
  int ret = nm_->rdma_post_recv_sid_async(&rr, 0);
  assert(ret == 0);

  struct ibv_send_wr sr;
  struct ibv_sge send_sge;
  uint64_t rand_sr_id = rand();
  memset(&sr, 0, sizeof(struct ibv_send_wr));
  memset(&send_sge, 0, sizeof(struct ibv_sge));
  send_sge.addr = (uint64_t)local_buf_;
  send_sge.length = block_size_;
  send_sge.lkey = local_buf_mr_->lkey;
  sr.wr_id = rand_sr_id;
  sr.next = NULL;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_SEND;
  sr.send_flags = IBV_SEND_SIGNALED;
  // ret = nm_->rdma_post_send_sid_sync(&sr, 0);
  ret = nm_->rdma_post_send_sid_async(&sr, 0);
  struct ibv_wc send_wc;
  ret = nm_->rdma_poll_one_send_completion_sync(&send_wc);
  assert(send_wc.status == IBV_WC_SUCCESS);
  assert(send_wc.opcode == IBV_WC_SEND);
  assert(send_wc.wr_id == rand_sr_id);
  assert(ret == 0);

  struct ibv_wc recv_wc;
  ret = nm_->rdma_poll_one_recv_completion_sync(&recv_wc);
  assert(ret == 0);
  assert(recv_wc.status == IBV_WC_SUCCESS);
  assert(recv_wc.opcode == IBV_WC_RECV);
  assert(recv_wc.wr_id == rand_rr_id);
  assert(*(uint8_t*)((uint64_t)local_buf_ + block_size_) == IBMSG_REP_SET);
  return *(int*)((uint64_t)local_buf_ + block_size_ + sizeof(uint8_t));
}

int DMCClient::kv_get(void* key,
                      uint32_t key_size,
                      __OUT void* val,
                      __OUT uint32_t* val_size) {
  return kv_get_1s(key, key_size, val, val_size);
}

int DMCClient::kv_p_set(void* key,
                        uint32_t key_size,
                        void* val,
                        uint32_t val_size) {
  *(uint8_t*)local_buf_ = IBMSG_REQ_PSET;
  *(uint16_t*)((uint64_t)local_buf_ + sizeof(uint8_t)) = my_sid_;
  uint64_t kv_addr = (uint64_t)local_buf_ + sizeof(uint8_t) + sizeof(uint16_t);

  *(uint32_t*)kv_addr = key_size;
  *(uint32_t*)(kv_addr + sizeof(uint32_t)) = val_size;
  memcpy((void*)(kv_addr + 2 * sizeof(uint32_t)), key, key_size);
  memcpy((void*)(kv_addr + 2 * sizeof(uint32_t) + key_size), val, val_size);

  // post rr fist
  struct ibv_recv_wr rr;
  struct ibv_sge recv_sge;
  memset(&rr, 0, sizeof(struct ibv_recv_wr));
  memset(&recv_sge, 0, sizeof(struct ibv_sge));
  recv_sge.addr = (uint64_t)local_buf_ + block_size_;
  recv_sge.length = block_size_;
  recv_sge.lkey = local_buf_mr_->lkey;
  rr.wr_id = 1;
  rr.next = NULL;
  rr.sg_list = &recv_sge;
  rr.num_sge = 1;
  int ret = nm_->rdma_post_recv_sid_async(&rr, 0);
  assert(ret == 0);

  struct ibv_send_wr sr;
  struct ibv_sge send_sge;
  memset(&sr, 0, sizeof(struct ibv_send_wr));
  memset(&send_sge, 0, sizeof(struct ibv_sge));
  send_sge.addr = (uint64_t)local_buf_;
  send_sge.length = block_size_;
  send_sge.lkey = local_buf_mr_->lkey;
  sr.wr_id = 0;
  sr.next = NULL;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_SEND;
  sr.send_flags = IBV_SEND_SIGNALED;
  ret = nm_->rdma_post_send_sid_sync(&sr, 0);
  assert(ret == 0);

  struct ibv_wc recv_wc;
  ret = nm_->rdma_poll_one_recv_completion_sync(&recv_wc);
  assert(ret == 0);
  assert(recv_wc.status == IBV_WC_SUCCESS);
  assert(recv_wc.opcode == IBV_WC_RECV);
  assert(recv_wc.wr_id == 1);
  assert(*(uint8_t*)((uint64_t)local_buf_ + block_size_) == IBMSG_REP_PSET);
  return *(int*)((uint64_t)local_buf_ + block_size_ + sizeof(uint8_t));
}

int DMCClient::kv_set(void* key,
                      uint32_t key_size,
                      void* val,
                      uint32_t val_size) {
  int ret = 0;
  if (eviction_type_ == EVICT_CLIQUEMAP) {
    ret = kv_set_2s(key, key_size, val, val_size);
  } else {
    ret = kv_set_1s(key, key_size, val, val_size);
  }
  n_set_miss_ += (ret < 0);
  return ret;
}

void DMCClient::check_priority(KVOpsCtx* ctx) {
#if FALSE
  int ret = nm_->rdma_read_sid_sync(0, ctx->init_bucket_raddr,
                                    server_rkey_map_[0], ctx->op_laddr,
                                    local_buf_mr_->lkey, sizeof(Bucket));

  Slot* slot = (Slot*)(ctx->op_laddr);
  SlotMeta* slotMeta = (SlotMeta*)(ctx->op_laddr + sizeof(SlotBucket));
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    if (HashIndexConvert48To64Bits(slot[i].pointer) == 0)
      continue;
    printd(L_INFO, "check %lx %lx %ld",
           ctx->init_bucket_raddr + sizeof(Slot) * i,
           ctx->init_bucket_raddr + sizeof(SlotBucket) + sizeof(SlotMeta) * i,
           slotMeta[i].meta.acc_info.acc_ts);
    if (priority_type_ == EVICT_PRIO_LRU)
      assert(slotMeta[i].meta.acc_info.acc_ts != 0);
    else if (priority_type_ == EVICT_PRIO_LFU)
      assert(slotMeta[i].meta.acc_info.freq != 0);
  }
#endif
}

uint32_t DMCClient::get_slot_id(uint64_t slot_raddr) {
  return (slot_raddr - server_base_addr_) / sizeof(Slot);
}

uint64_t DMCClient::get_slot_raddr(uint32_t slot_id) {
  return server_base_addr_ + sizeof(Slot) * slot_id;
}

uint64_t DMCClient::adaptive_get_best_candidate(
    const std::vector<uint64_t>& candidates,
    __OUT uint8_t* expert_bmap) {
  std::map<uint64_t, float> candidate_weight_map;
  std::map<uint64_t, uint8_t> candidate_bmap_map;
  for (int i = 0; i < num_experts_; i++) {
    uint64_t candidate = candidates[i];
    candidate_weight_map[candidate] += l_weights_[i];
    candidate_bmap_map[candidate] |= (1 << i);
  }

  float rand_val = (float)(rand() % 1000000) / 1000000;
  float cur = 0;
  int num_candidates = candidate_weight_map.size();
  auto it = candidate_weight_map.begin();
  for (; it != candidate_weight_map.end(); it++) {
    float w = it->second;
    cur += (1 - learning_rate_) * w + learning_rate_ / num_candidates;
    if (rand_val <= cur) {
      *expert_bmap = candidate_bmap_map[it->first];
      return it->first;
    }
  }
  it = (--candidate_weight_map.end());
  *expert_bmap = candidate_bmap_map[it->first];
  return it->first;
}

int DMCClient::adaptive_get_best_candidate(const std::vector<int>& candidates,
                                           __OUT uint8_t* expert_bmap) {
  std::map<int, float> candidate_weight_map;
  std::map<int, uint8_t> candidate_bmap_map;
  for (int i = 0; i < num_experts_; i++) {
    int candidate = candidates[i];
    candidate_weight_map[candidate] += l_weights_[i];
    candidate_bmap_map[candidate] |= (1 << i);
  }

  float rand_val = (float)(rand() % 1000000) / 1000000;
  float cur = 0;
  int num_candidates = candidate_weight_map.size();
  auto it = candidate_weight_map.begin();
  for (; it != candidate_weight_map.end(); it++) {
    float w = it->second;
    cur += (1 - learning_rate_) * w + learning_rate_ / num_candidates;
    if (rand_val <= cur) {
      *expert_bmap = candidate_bmap_map[it->first];
      return it->first;
    }
  }
  it = (--candidate_weight_map.end());
  *expert_bmap = candidate_bmap_map[it->first];
  return it->first;
}

int DMCClient::adaptive_get_best_expert(__OUT uint8_t* expert_bmap) {
  float max_weight = l_weights_[0];
  int max_idx = 0;
  for (int i = 1; i < num_experts_; i++) {
    if (l_weights_[i] > max_weight) {
      max_weight = l_weights_[i];
      max_idx = i;
    }
  }
  *expert_bmap = (1 << max_idx);
  return max_idx;
}

void DMCClient::adaptive_update_weights(KVOpsCtx* ctx) {
  printd(L_DEBUG, "update weights");
  if (use_async_weight_)
    adaptive_update_weights_async(ctx);
  else
    adaptive_update_weights_sync(ctx);
}

void DMCClient::adaptive_update_weights_sync(KVOpsCtx* ctx) {
  if (ctx->hist_match == false)
    return;
  printd(L_DEBUG, "update weights sync");
  num_adaptive_adjust_weights_++;

  uint8_t expert_bmap;
  double hist_depth;
  if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE) {
    expert_bmap = ctx->hist_expert_bmap;
    hist_depth = ctx->read_hist_head - ctx->hist_match_head;
  } else {
    expert_bmap = ctx->histEntry.expert_bmap;
    hist_depth = ctx->read_hist_head - ctx->histEntry.head;
  }
  float rewards[num_experts_];
  memset(rewards, 0, sizeof(float) * num_experts_);
  for (int i = 0; i < num_experts_; i++) {
    if ((1 << i) & expert_bmap) {
      rewards[i] = -std::pow(base_reward_, hist_depth);
    }
    reward_buf_[i] += rewards[i] * learning_rate_;
  }
  // sync weights to remote
  adaptive_sync_weights();
}

void DMCClient::adaptive_update_weights_async(KVOpsCtx* ctx) {
  if (ctx->hist_match == false)
    return;
  printd(L_DEBUG, "adjust weights async");
  num_adaptive_adjust_weights_++;

  uint8_t expert_bmap;
  double hist_depth;
  if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE) {
    expert_bmap = ctx->hist_expert_bmap;
    hist_depth = ctx->read_hist_head - ctx->hist_match_head;
  } else {
    expert_bmap = ctx->histEntry.expert_bmap;
    hist_depth = ctx->read_hist_head - ctx->histEntry.head;
  }
  float rewards[num_experts_];
  memset(rewards, 0, sizeof(float) * num_experts_);
  for (int i = 0; i < num_experts_; i++) {
    if ((1 << i) & expert_bmap) {
      rewards[i] = -std::pow(base_reward_, hist_depth);
    }
    reward_buf_[i] += rewards[i] * learning_rate_;
  }

  double sum = 0;
  for (int i = 0; i < num_experts_; i++) {
    l_weights_[i] *= std::exp(learning_rate_ * rewards[i]);
    if (l_weights_[i] > 0.99)
      l_weights_[i] = 0.99;
    if (l_weights_[i] < 0.01)
      l_weights_[i] = 0.01;
    sum += l_weights_[i];
  }
  for (int i = 0; i < num_experts_; i++)
    l_weights_[i] /= sum;

  // record weight changing
  std::vector<float> tmp_vec;
  for (int i = 0; i < num_experts_; i++)
    tmp_vec.push_back(l_weights_[i]);
  weight_vec_.push_back(tmp_vec);

  // check local rewards
  if (local_reward_cntr_ < ADAPTIVE_NUM_LOCAL_REWARD) {
    local_reward_cntr_++;
  } else {
    adaptive_sync_weights();
    local_reward_cntr_ = 0;
  }
}

void DMCClient::adaptive_read_weights() {
  num_rdma_read_++;
  nm_->rdma_read_sid_sync(0, weights_raddr_, server_rkey_map_[0],
                          (uint64_t)r_weights_, weights_sync_buf_mr_->lkey,
                          sizeof(float) * num_experts_);
  memcpy(l_weights_, r_weights_, sizeof(float) * num_experts_);
  memset(r_weights_, 0, sizeof(float) * num_experts_);
}

void DMCClient::adaptive_sync_weights() {
  num_adaptive_weight_sync_++;
  int ret = 0;

  // 1. post recv first
  struct ibv_recv_wr rr;
  struct ibv_sge r_sge;
  memset(&rr, 0, sizeof(struct ibv_recv_wr));
  ib_create_sge((uint64_t)weights_sync_buf_, weights_sync_buf_mr_->lkey,
                weights_sync_size, &r_sge);
  rr.wr_id = 12302;
  rr.next = NULL;
  rr.sg_list = &r_sge;
  rr.num_sge = 1;
  ret = nm_->rdma_post_recv_sid_async(&rr, 0);
  assert(ret == 0);
  num_rdma_recv_++;

  // 2. send reward to server
  struct ibv_send_wr send_wr;
  struct ibv_sge send_sge;
  memset(&send_wr, 0, sizeof(struct ibv_send_wr));
  memset(&send_sge, 0, sizeof(struct ibv_sge));
  ib_create_sge(
      (uint64_t)reward_sync_buf_, reward_sync_buf_mr_->lkey,
      sizeof(float) * num_experts_ + sizeof(uint8_t) + sizeof(uint16_t),
      &send_sge);
  send_wr.wr_id = 12301;
  send_wr.next = NULL;
  send_wr.sg_list = &send_sge;
  send_wr.num_sge = 1;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_INLINE;
  ret = nm_->rdma_post_send_sid_async(&send_wr, 0);
  assert(ret == 0);
  num_rdma_send_++;

  struct ibv_wc wc;
  ret = nm_->rdma_poll_one_recv_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_INFO, "wc status: %d", wc.status);
  }
  assert(ret == 0);
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 12302);
  assert(*(uint8_t*)weights_sync_buf_ == IBMSG_REP_MERGE);
  memcpy(l_weights_, r_weights_, sizeof(float) * num_experts_);
  memset(weights_sync_buf_, 0, weights_sync_size);
  memset(reward_buf_, 0, sizeof(float) * num_experts_);
}

std::string DMCClient::get_str_key(KVOpsCtx* ctx) {
  char key_buf[256] = {0};
  memcpy(key_buf, ctx->key, ctx->key_size);
  return std::string(key_buf);
}

void DMCClient::get_adaptive_weight_vec(
    std::vector<std::vector<float>>& weight_vec) {
  if (eviction_type_ != EVICT_SAMPLE_ADAPTIVE)
    return;
  weight_vec = weight_vec_;
}

void DMCClient::get_adaptive_weights(std::vector<float>& adaptive_weights) {
  if (eviction_type_ != EVICT_SAMPLE_ADAPTIVE)
    return;
  adaptive_weights.clear();
  for (int i = 0; i < num_experts_; i++)
    adaptive_weights.push_back(l_weights_[i]);
}

void DMCClient::adaptive_get_expert_rank(
    std::vector<std::pair<double, int>>& expert_rank) {
  expert_rank.clear();
  for (int i = 0; i < num_experts_; i++) {
    expert_rank.emplace_back(l_weights_[i], i);
  }
  std::sort(expert_rank.begin(), expert_rank.end());
}

void DMCClient::count_expert_evict(uint8_t expert_bmap) {
  for (int i = 0; i < num_experts_; i++)
    expert_evict_cnt_[i] += ((1 << i) & expert_bmap);
}

uint64_t DMCClient::get_hist_head_raddr() {
  if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE)
    return lw_history_->hist_cntr_raddr();
  else
    return naive_history_->hist_cntr_raddr();
}

void DMCClient::get_expert_evict_cnt(std::vector<uint32_t>& evict_cnt) {
  if (eviction_type_ == EVICT_SAMPLE_ADAPTIVE)
    evict_cnt = expert_evict_cnt_;
}

void DMCClient::log_op(const char* op,
                       void* key,
                       uint32_t key_size,
                       bool miss) {
  char key_buf[256] = {0};
  memcpy(key_buf, key, key_size);
  if (miss)
    fprintf(log_f_, "%s %s MISS\n", op, key_buf);
  else
    fprintf(log_f_, "%s %s HIT\n", op, key_buf);
}

#ifdef USE_REWARDS
void DMCClient::hit_adjust_weights(KVOpsCtx* ctx) {
  uint32_t target_slot_id =
      (ctx->target_slot_laddr - ctx->bucket_laddr) / sizeof(Slot);
  uint64_t bucket_laddr = ctx->bucket_laddr;

  std::vector<std::pair<double, int>> prio_slot_id[num_experts_];
  Slot* l_slot = (Slot*)ctx->bucket_laddr;
  for (int i = 0; i < HASH_BUCKET_ASSOC_NUM; i++) {
    uint64_t kv_raddr = HashIndexConvert48To64Bits(l_slot[i].atomic.pointer);
    if (l_slot[i].atomic.kv_len == 0) {
      continue;
    }
    if (lw_history_->is_in_history(&l_slot[i])) {
      if (lw_history_->has_overwritten(ctx->read_hist_head, kv_raddr))
        continue;
    } else {
      for (int j = 0; j < num_experts_; j++) {
        double prio = experts_[j]->parse_priority(&l_slot[i].meta,
                                                  l_slot[i].atomic.kv_len);
        prio_slot_id[j].emplace_back(prio, i);
      }
    }
  }

  // sort all prio lists
  for (int i = 0; i < num_experts_; i++) {
    std::sort(prio_slot_id[i].begin(), prio_slot_id[i].end());
  }

  // get the rank of the object in each expert
  std::vector<int> expert_target_rank;
  expert_target_rank.resize(num_experts_);
  for (int i = 0; i < num_experts_; i++) {
    for (int j = 0; j < prio_slot_id[i].size(); j++) {
      if (prio_slot_id[i][j].second == target_slot_id) {
        expert_target_rank[i] = j;
        break;
      }
    }
  }

  // find experts with the highest rank
  int _max = 0;
  for (int i = 0; i < num_experts_; i++) {
    if (_max < expert_target_rank[i])
      _max = expert_target_rank[i];
  }
  std::vector<int> reward_expert_list;
  for (int i = 0; i < num_experts_; i++) {
    if (_max == expert_target_rank[i])
      reward_expert_list.push_back(i);
  }

  // reward experts
  if (reward_expert_list.size() == num_experts_)
    return;  // return if all expert has the same weight
  num_hit_rewards_++;
  float rewards[num_experts_];
  memset(rewards, 0, sizeof(float) * num_experts_);
  for (int i = 0; i < num_experts_; i++) {
    // add probabilistic reward
    bool need_reward = l_weights_[i] < 0.1;
    auto it =
        std::find(reward_expert_list.begin(), reward_expert_list.end(), i);
    if (need_reward && it != reward_expert_list.end()) {
      rewards[i] = std::pow(base_reward_, history_size_ * 300);
      expert_reward_cnt_[i]++;
    }
    reward_buf_[i] += rewards[i] * learning_rate_;
  }

  double sum = 0;
  for (int i = 0; i < num_experts_; i++) {
    l_weights_[i] *= std::exp(learning_rate_ * rewards[i]);
    if (l_weights_[i] > 0.99)
      l_weights_[i] = 0.99;
    if (l_weights_[i] < 0.01)
      l_weights_[i] = 0.01;
    sum += l_weights_[i];
  }
  for (int i = 0; i < num_experts_; i++)
    l_weights_[i] /= sum;

  if (local_reward_cntr_ < ADAPTIVE_NUM_LOCAL_REWARD) {
    local_reward_cntr_++;
  } else {
    adaptive_sync_weights();
    local_reward_cntr_ = 0;
  }
}

void DMCClient::get_expert_reward_cnt(std::vector<uint32_t>& reward_cnt) {
  reward_cnt = expert_reward_cnt_;
}
#endif