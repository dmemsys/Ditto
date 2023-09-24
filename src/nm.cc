#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <unistd.h>

#include "debug.h"
#include "ib.h"
#include "nm.h"

int UDPNetworkManager::UDPNMInitServer(const DMCConfig* conf) {
  int ret = 0;
  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;
  if (setsockopt(udp_sock_, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                 sizeof(timeout)) == -1)
    perror("setsockopt failed:");

  num_server_ = 1;
  server_addr_list_ =
      (struct sockaddr_in*)malloc(num_server_ * sizeof(struct sockaddr_in));
  assert(server_addr_list_ != NULL);
  memset(server_addr_list_, 0, sizeof(struct sockaddr_in) * num_server_);

  server_addr_list_[0].sin_family = AF_INET;
  server_addr_list_[0].sin_port = htons(udp_port_);
  server_addr_list_[0].sin_addr.s_addr = htonl(INADDR_ANY);

  // create a single cq
  ib_recv_cq_ = ibv_create_cq(ib_ctx_, 1024, NULL, NULL, 0);
  ib_send_cq_ = ibv_create_cq(ib_ctx_, 1024, NULL, NULL, 0);
  assert(ib_send_cq_ != NULL);
  assert(ib_recv_cq_ != NULL);

  ret = bind(udp_sock_, (struct sockaddr*)&server_addr_list_[0],
             sizeof(struct sockaddr_in));
  assert(ret >= 0);

  return 0;
}

int UDPNetworkManager::UDPNMInitClient(const DMCConfig* conf) {
  num_server_ = conf->memory_num;
  server_addr_list_ =
      (struct sockaddr_in*)malloc(num_server_ * sizeof(struct sockaddr_in));
  assert(server_addr_list_ != NULL);
  memset(server_addr_list_, 0, sizeof(struct sockaddr_in) * num_server_);

  rc_qp_list_.resize(num_server_);
  mr_info_list_.resize(num_server_);
  ib_send_cq_ = ibv_create_cq(ib_ctx_, 1024, NULL, NULL, 0);
  ib_recv_cq_ = ibv_create_cq(ib_ctx_, 4096, NULL, NULL, 0);
  // ib_recv_cq_ = ib_send_cq_;
  assert(ib_send_cq_ != NULL);
  assert(ib_recv_cq_ != NULL);

  for (int i = 0; i < num_server_; i++) {
    server_addr_list_[i].sin_family = AF_INET;
    server_addr_list_[i].sin_port = htons(udp_port_);
    server_addr_list_[i].sin_addr.s_addr = inet_addr(conf->memory_ip_list[i]);
  }
  return 0;
}

UDPNetworkManager::UDPNetworkManager(const DMCConfig* conf) {
  int ret = 0;
  udp_sock_ = socket(AF_INET, SOCK_DGRAM, 0);
  assert(udp_sock_ >= 0);

  role_ = conf->role;
  server_id_ = conf->server_id;
  conn_type_ = conf->conn_type;
  udp_port_ = conf->udp_port;
  ib_port_id_ = conf->ib_port_id;
  ib_gid_idx_ = conf->ib_gid_idx;
  printd(L_INFO, "ib_gid_idx: %d", ib_gid_idx_);
  printd(L_INFO, "ib_dev_id: %d", conf->ib_dev_id);

  ib_ctx_ = ib_get_ctx(conf->ib_dev_id, conf->ib_port_id);
  assert(ib_ctx_ != NULL);

  ib_pd_ = ibv_alloc_pd(this->ib_ctx_);
  assert(ib_pd_ != NULL);

  ret = ibv_query_port(ib_ctx_, ib_port_id_, &ib_port_attr_);
  assert(ret == 0);

  ret = ibv_query_device(ib_ctx_, &ib_device_attr_);
  assert(ret == 0);

  if (conn_type_ == ROCE) {
    ret = ibv_query_gid(ib_ctx_, ib_port_id_, ib_gid_idx_, &ib_gid_);
    ib_print_gid((uint8_t*)&ib_gid_);
    assert(ret == 0);
  } else {
    assert(conn_type_ == IB);
    memset(&ib_gid_, 0, sizeof(union ibv_gid));
  }

  if (role_ == CLIENT) {
    ret = UDPNMInitClient(conf);
  } else {
    ret = UDPNMInitServer(conf);
  }
  assert(ret == 0);
}

UDPNetworkManager::~UDPNetworkManager() {
  close_udp_sock();
  return;
}

int UDPNetworkManager::recv_udp_msg(__OUT UDPMsg* msg,
                                    __OUT struct sockaddr_in* src_addr,
                                    __OUT socklen_t* src_addr_len) {
  int rc = recvfrom(udp_sock_, msg, sizeof(UDPMsg), 0,
                    (struct sockaddr*)src_addr, src_addr_len);
  if (rc != sizeof(UDPMsg)) {
    return -1;
  }
  return 0;
}

int UDPNetworkManager::send_udp_msg(UDPMsg* msg,
                                    struct sockaddr_in* dest_addr,
                                    socklen_t dest_addr_len) {
  int rc = sendto(udp_sock_, msg, sizeof(UDPMsg), 0,
                  (struct sockaddr*)dest_addr, dest_addr_len);
  if (rc != sizeof(UDPMsg)) {
    return -1;
  }
  return 0;
}

int UDPNetworkManager::send_udp_msg_to_server(UDPMsg* msg, uint16_t server_id) {
  struct sockaddr_in* dest_addr = &server_addr_list_[server_id];
  socklen_t dest_addr_len = sizeof(struct sockaddr_in);
  return send_udp_msg(msg, dest_addr, dest_addr_len);
}

void UDPNetworkManager::close_udp_sock() {
  close(udp_sock_);
}

int UDPNetworkManager::get_qp_info(struct ibv_qp* qp, __OUT QPInfo* qp_info) {
  qp_info->qp_num = qp->qp_num;
  qp_info->lid = ib_port_attr_.lid;
  qp_info->port_num = ib_port_id_;
  if (this->conn_type_ == ROCE) {
    memcpy(qp_info->gid, &ib_gid_, 16);
    qp_info->gid_idx = ib_gid_idx_;
  } else {
    memset(qp_info->gid, 0, 16);
  }
  return 0;
}

struct ibv_qp* UDPNetworkManager::create_rc_qp() {
  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(struct ibv_qp_init_attr));
  if (role_ == CLIENT) {
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = ib_send_cq_;
    qp_init_attr.recv_cq = ib_recv_cq_;
    qp_init_attr.cap.max_send_wr = 512;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 16;
    qp_init_attr.cap.max_recv_sge = 16;
    qp_init_attr.cap.max_inline_data = 256;
  } else {
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = ib_send_cq_;
    qp_init_attr.recv_cq = ib_recv_cq_;
    qp_init_attr.cap.max_send_wr = 512;
    qp_init_attr.cap.max_recv_wr = 512;
    qp_init_attr.cap.max_send_sge = 16;
    qp_init_attr.cap.max_recv_sge = 16;
    qp_init_attr.cap.max_inline_data = 256;
  }

  return ib_create_rc_qp(ib_pd_, &qp_init_attr);
}

int UDPNetworkManager::client_connect_one_rc_qp(uint16_t dest_server_id,
                                                __OUT MrInfo* mr_info) {
  int rc = 0;
  struct ibv_qp* new_rc_qp = create_rc_qp();
  assert(new_rc_qp != NULL);

  UDPMsg request;
  memset(&request, 0, sizeof(UDPMsg));
  request.type = UDPMSG_REQ_CONNECT;
  request.id = server_id_;
  rc = get_qp_info(new_rc_qp, &request.body.conn_info.qp_info);
  assert(rc == 0);
  serialize_udpmsg(&request);

  rc = sendto(udp_sock_, &request, sizeof(UDPMsg), 0,
              (struct sockaddr*)&server_addr_list_[dest_server_id],
              sizeof(struct sockaddr_in));
  assert(rc == sizeof(UDPMsg));

  UDPMsg reply;
  int len;
  rc = recvfrom(udp_sock_, &reply, sizeof(UDPMsg), 0, NULL, NULL);
  assert(rc == sizeof(UDPMsg));
  deserialize_udpmsg(&reply);
  deserialize_udpmsg(&request);

  assert(reply.type == UDPMSG_REP_CONNECT);

  rc = ib_connect_qp(new_rc_qp, &request.body.conn_info.qp_info,
                     &reply.body.conn_info.qp_info, conn_type_);
  assert(rc == 0);

  // record this rc_qp
  MrInfo* new_mr_info = (MrInfo*)malloc(sizeof(MrInfo));
  memcpy(new_mr_info, &reply.body.conn_info.mr_info, sizeof(MrInfo));
  rc_qp_list_[dest_server_id] = new_rc_qp;
  mr_info_list_[dest_server_id] = new_mr_info;
  // return the memory info
  memcpy(mr_info, &reply.body.conn_info.mr_info, sizeof(MrInfo));
  return 0;
}

int UDPNetworkManager::nm_on_connect_new_qp(const UDPMsg* request,
                                            __OUT QPInfo* qp_info) {
  int rc = 0;
  struct ibv_qp* new_rc_qp = this->create_rc_qp();
  assert(new_rc_qp != NULL);

  if (this->rc_qp_list_.size() <= request->id) {
    this->rc_qp_list_.resize(request->id + 1);
  }
  if (this->rc_qp_list_[request->id] != NULL) {
    ibv_destroy_qp(rc_qp_list_[request->id]);
  }
  this->rc_qp_list_[request->id] = new_rc_qp;

  rc = this->get_qp_info(new_rc_qp, qp_info);
  assert(rc == 0);
  return 0;
}

int UDPNetworkManager::nm_on_connect_connect_qp(uint32_t client_id,
                                                const QPInfo* local_qp_info,
                                                const QPInfo* remote_qp_info) {
  int rc = 0;
  struct ibv_qp* qp = this->rc_qp_list_[client_id];
  ib_print_gid(local_qp_info->gid);
  ib_print_gid(remote_qp_info->gid);
  rc = ib_connect_qp(qp, local_qp_info, remote_qp_info, this->conn_type_);
  assert(rc == 0);
  return 0;
}

int UDPNetworkManager::rdma_poll_one_send_completion_sync(struct ibv_wc* wc) {
  int num_polled = 0;
  while (num_polled == 0) {
#ifdef USE_FIBER
    boost::this_fiber::yield();
#endif
    num_polled = ibv_poll_cq(ib_send_cq_, 1, wc);
  }
  assert(num_polled == 1);
  return 0;
}

int UDPNetworkManager::rdma_poll_one_recv_completion_async(struct ibv_wc* wc) {
#ifdef USE_FIBER
  boost::this_fiber::yield();
#endif
  return ibv_poll_cq(ib_recv_cq_, 1, wc);
}

int UDPNetworkManager::rdma_poll_one_recv_completion_sync(struct ibv_wc* wc) {
  int num_polled = 0;
  while (num_polled == 0) {
#ifdef USE_FIBER
    boost::this_fiber::yield();
#endif
    num_polled = ibv_poll_cq(ib_recv_cq_, 1, wc);
  }
  assert(num_polled == 1);
  return 0;
}

int UDPNetworkManager::rdma_poll_recv_completion_async(struct ibv_wc* wc,
                                                       int num_wc) {
#ifdef USE_FIBER
  boost::this_fiber::yield();
#endif
  return ibv_poll_cq(ib_recv_cq_, num_wc, wc);
}

int UDPNetworkManager::rdma_write_sid_sync(uint16_t server,
                                           uint64_t remote_addr,
                                           uint32_t rkey,
                                           uint64_t local_addr,
                                           uint32_t lkey,
                                           uint32_t len) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, lkey, len, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 1010;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.next = NULL;
  sr.opcode = IBV_WR_RDMA_WRITE;
  sr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;
  sr.wr.rdma.remote_addr = remote_addr;
  sr.wr.rdma.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  printd(L_DEBUG, "write %d@0x%lx with rkey %x", server, remote_addr, rkey);
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);

  struct ibv_wc wc;
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status(%d) wrid(%ld)", wc.status, wc.wr_id);
  }
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 1010);
  assert(wc.opcode == IBV_WC_RDMA_WRITE);
  return 0;
}

int UDPNetworkManager::rdma_inl_write_sid_sync(uint16_t server,
                                               uint64_t remote_addr,
                                               uint32_t rkey,
                                               uint64_t local_addr,
                                               uint32_t len) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, 0, len, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 100;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.next = NULL;
  sr.opcode = IBV_WR_RDMA_WRITE;
  sr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE | IBV_SEND_INLINE;
  sr.wr.rdma.remote_addr = remote_addr;
  sr.wr.rdma.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);

  struct ibv_wc wc;
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status(%d) wrid(%ld)", wc.status, wc.wr_id);
  }
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 100);
  assert(wc.opcode == IBV_WC_RDMA_WRITE);
  return 0;
}

int UDPNetworkManager::rdma_faa_size_sync(uint16_t server,
                                          uint64_t remote_addr,
                                          uint32_t rkey,
                                          uint64_t local_addr,
                                          uint32_t lkey,
                                          uint64_t add) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, lkey, 8, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 123341;
  sr.next = NULL;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  sr.send_flags = IBV_SEND_SIGNALED;
  sr.wr.atomic.compare_add = add;
  sr.wr.atomic.remote_addr = remote_addr;
  sr.wr.atomic.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);

  struct ibv_wc wc;
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status (%d) wrid (%ld)", wc.status, wc.wr_id);
  }
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 123341);
  assert(wc.opcode == IBV_WC_FETCH_ADD);
  return 0;
}

int UDPNetworkManager::rdma_cas_sid_sync(uint16_t server,
                                         uint64_t remote_addr,
                                         uint32_t rkey,
                                         uint64_t local_addr,
                                         uint32_t lkey,
                                         uint64_t expect_val,
                                         uint64_t swap_val) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, lkey, 8, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 121321;
  sr.next = NULL;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  sr.send_flags = IBV_SEND_SIGNALED;
  sr.wr.atomic.compare_add = expect_val;
  sr.wr.atomic.swap = swap_val;
  sr.wr.atomic.remote_addr = remote_addr;
  sr.wr.atomic.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);

  struct ibv_wc wc;
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status(%d) wrid(%ld)", wc.status, wc.wr_id);
  }
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 121321);
  assert(wc.opcode == IBV_WC_COMP_SWAP);
  return 0;
}

int UDPNetworkManager::rdma_write_sid_async(uint16_t server,
                                            uint64_t remote_addr,
                                            uint32_t rkey,
                                            uint64_t local_addr,
                                            uint32_t lkey,
                                            uint32_t len) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, lkey, len, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 120;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.next = NULL;
  sr.opcode = IBV_WR_RDMA_WRITE;
  sr.send_flags = 0;
  sr.wr.rdma.remote_addr = remote_addr;
  sr.wr.rdma.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);
  return 0;
}

int UDPNetworkManager::rdma_inl_write_sid_async(uint16_t server,
                                                uint64_t remote_addr,
                                                uint32_t rkey,
                                                uint64_t local_addr,
                                                uint32_t lkey,
                                                uint32_t len) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, lkey, len, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 120;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.next = NULL;
  sr.opcode = IBV_WR_RDMA_WRITE;
  sr.send_flags = IBV_SEND_INLINE;
  sr.wr.rdma.remote_addr = remote_addr;
  sr.wr.rdma.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);
  return 0;
}

int UDPNetworkManager::rdma_batch_read_sid_sync(uint16_t server,
                                                uint64_t* remote_addr,
                                                uint32_t rkey,
                                                uint64_t* local_addr,
                                                uint32_t* lkey,
                                                uint32_t* len,
                                                int batch_size) {
  struct ibv_send_wr sr[batch_size];
  struct ibv_send_wr* bad_wr;
  struct ibv_sge sge[batch_size];
  for (int i = 0; i < batch_size; i++) {
    ib_create_sge(local_addr[i], lkey[i], len[i], &sge[i]);
    memset(&sr[i], 0, sizeof(struct ibv_send_wr));
    sr[i].wr_id = 3012 * 10 + i;
    sr[i].next = i == batch_size - 1 ? NULL : &sr[i + 1];
    sr[i].sg_list = &sge[i];
    sr[i].num_sge = 1;
    sr[i].opcode = IBV_WR_RDMA_READ;
    sr[i].send_flags = i == batch_size - 1 ? IBV_SEND_SIGNALED : 0;
    sr[i].wr.rdma.remote_addr = remote_addr[i];
    sr[i].wr.rdma.rkey = rkey;
  }

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, sr, &bad_wr);
  assert(ret == 0);

  struct ibv_wc wc;
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status(%d) wrid(%ld)", wc.status, wc.wr_id);
  }
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 3012 * 10 + batch_size - 1);
  assert(wc.opcode == IBV_WC_RDMA_READ);
  return 0;
}

int UDPNetworkManager::rdma_read_sid_sync(uint16_t server,
                                          uint64_t remote_addr,
                                          uint32_t rkey,
                                          uint64_t local_addr,
                                          uint32_t lkey,
                                          uint32_t len) {
  struct ibv_send_wr sr;
  struct ibv_send_wr* bad_wr;
  struct ibv_sge send_sge;
  ib_create_sge(local_addr, lkey, len, &send_sge);

  memset(&sr, 0, sizeof(struct ibv_send_wr));
  sr.wr_id = 100;
  sr.sg_list = &send_sge;
  sr.num_sge = 1;
  sr.next = NULL;
  sr.opcode = IBV_WR_RDMA_READ;
  sr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_FENCE;
  sr.wr.rdma.remote_addr = remote_addr;
  sr.wr.rdma.rkey = rkey;

  struct ibv_qp* qp = rc_qp_list_[server];
  int ret = ibv_post_send(qp, &sr, &bad_wr);
  assert(ret == 0);

  struct ibv_wc wc;
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status(%d) wrid(%ld)", wc.status, wc.wr_id);
  }
  assert(wc.status == IBV_WC_SUCCESS);
  assert(wc.wr_id == 100);
  assert(wc.opcode == IBV_WC_RDMA_READ);
  return 0;
}

int UDPNetworkManager::rdma_post_send_sid_async(struct ibv_send_wr* sr,
                                                uint16_t server) {
  struct ibv_qp* qp = rc_qp_list_[server];
  struct ibv_send_wr* bad_wr;
  int ret = ibv_post_send(qp, sr, &bad_wr);
  return ret;
}

// **The sr list only signal once!**
int UDPNetworkManager::rdma_post_send_sid_sync(struct ibv_send_wr* sr,
                                               uint16_t server) {
  struct ibv_qp* qp = rc_qp_list_[server];
  struct ibv_send_wr* bad_wr;
  int ret = ibv_post_send(qp, sr, &bad_wr);
  if (ret != 0) {
    printd(L_DEBUG, "ibv_post_send return %d %s", ret, strerror(ret));
  }
  assert(ret == 0);

  struct ibv_wc wc;
  memset(&wc, 0, sizeof(struct ibv_wc));
  ret = rdma_poll_one_send_completion_sync(&wc);
  if (wc.status != IBV_WC_SUCCESS) {
    printd(L_ERROR, "WC status(%d) wrid(%ld) opcode(%d)", wc.status, wc.wr_id,
           wc.opcode);
    ib_print_wr(sr);
    return -1;
  }
  return 0;
}

int UDPNetworkManager::rdma_post_recv_sid_async(struct ibv_recv_wr* rr,
                                                uint16_t server) {
  struct ibv_qp* qp = rc_qp_list_[server];
  struct ibv_recv_wr* bad_rr;
  int ret = ibv_post_recv(qp, rr, &bad_rr);
  if (ret != 0) {
    printd(L_DEBUG, "ibv_post_recv return %d %s", ret, strerror(ret));
  }
  assert(ret == 0);
  return 0;
}