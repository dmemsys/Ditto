#include "test_nm.h"

#include <sys/time.h>
#include <cstdlib>

#include "debug.h"
#include "dmc_table.h"

void NMTest::SetUp() {
  setup_server_conf();
  setup_client_conf();
  server_nm_ = new UDPNetworkManager(&server_conf_);
  client_nm_ = new UDPNetworkManager(&client_conf_);
}

void NMTest::TearDown() {
  delete server_nm_;
  delete client_nm_;
}

int NMTest::rdma_connect(MrInfo* mr_info) {
  pthread_t server_tid;
  int ret;
  pthread_create(&server_tid, NULL, rdma_connect_server, server_nm_);

  ret = client_nm_->client_connect_one_rc_qp(0, mr_info);
  assert(ret == 0);

  pthread_join(server_tid, NULL);
  return 0;
}

void* udp_send_recv_client(void* args) {
  UDPNetworkManager* nm = (UDPNetworkManager*)args;
  UDPMsg request;
  request.type = UDPMSG_REQ_CONNECT;
  request.id = 1;
  serialize_udpmsg(&request);
  int ret = nm->send_udp_msg_to_server(&request, 0);
  assert(ret == 0);
  UDPMsg reply;
  ret = nm->recv_udp_msg(&reply, NULL, NULL);
  assert(ret == 0);
  deserialize_udpmsg(&reply);
  assert(reply.id == 0);
  assert(reply.type == UDPMSG_REP_CONNECT);
  return NULL;
}

void* udp_send_recv_server(void* args) {
  UDPNetworkManager* nm = (UDPNetworkManager*)args;
  UDPMsg request;
  struct sockaddr_in src_addr;
  socklen_t src_addr_len = sizeof(struct sockaddr_in);
  int ret = nm->recv_udp_msg(&request, &src_addr, &src_addr_len);
  assert(ret == 0);
  deserialize_udpmsg(&request);
  assert(request.type == UDPMSG_REQ_CONNECT);
  assert(request.id == 1);
  UDPMsg reply;
  reply.type = UDPMSG_REP_CONNECT;
  reply.id = 0;
  serialize_udpmsg(&reply);
  ret = nm->send_udp_msg(&reply, &src_addr, src_addr_len);
  assert(ret == 0);
  return NULL;
}

void* rdma_connect_server(void* args) {
  UDPNetworkManager* nm = (UDPNetworkManager*)args;
  UDPMsg request;
  struct sockaddr_in client_addr;
  socklen_t client_addr_len = sizeof(struct sockaddr_in);
  int rc = nm->recv_udp_msg(&request, &client_addr, &client_addr_len);
  assert(rc == 0);
  deserialize_udpmsg(&request);

  assert(request.type == UDPMSG_REQ_CONNECT);
  assert(request.id == 1);
  UDPMsg reply;
  reply.id = 0;
  reply.type = UDPMSG_REP_CONNECT;
  rc = nm->nm_on_connect_new_qp(&request, &reply.body.conn_info.qp_info);
  assert(rc == 0);

  struct ibv_pd* pd;
  pd = nm->get_ib_pd();
  void* buf = malloc(1024);
  struct ibv_mr* mr =
      ibv_reg_mr(pd, buf, 1024,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC);
  reply.body.conn_info.mr_info.addr = (uint64_t)buf;
  reply.body.conn_info.mr_info.rkey = mr->rkey;
  serialize_udpmsg(&reply);

  rc = nm->send_udp_msg(&reply, &client_addr, client_addr_len);
  assert(rc == 0);
  deserialize_udpmsg(&reply);
  rc = nm->nm_on_connect_connect_qp(request.id, &reply.body.conn_info.qp_info,
                                    &request.body.conn_info.qp_info);
  assert(rc == 0);
  return NULL;
}

TEST_F(NMTest, udp_send_recv) {
  pthread_t server_tid, client_tid;
  pthread_create(&server_tid, NULL, udp_send_recv_server, (void*)server_nm_);
  pthread_create(&client_tid, NULL, udp_send_recv_client, (void*)client_nm_);
  pthread_join(server_tid, NULL);
  pthread_join(client_tid, NULL);
  server_nm_->close_udp_sock();
  client_nm_->close_udp_sock();
}

TEST_F(NMTest, rdma_connect) {
  MrInfo mrInfo;
  rdma_connect(&mrInfo);
  ASSERT_TRUE(true);
}

TEST_F(NMTest, rdma_write) {
  MrInfo server_mr_info;
  rdma_connect(&server_mr_info);
  ASSERT_TRUE(true);

  struct ibv_pd* client_pd = client_nm_->get_ib_pd();
  char buf[128];
  struct ibv_mr* client_mr =
      ibv_reg_mr(client_pd, buf, 128, IBV_ACCESS_LOCAL_WRITE);

  // iteratively write and check
  srand((int)time(NULL));
  for (int i = 0; i < 100; i++) {
    uint64_t r0 = rand();
    uint64_t r1 = rand();
    *(uint64_t*)buf = r0;
    *(uint64_t*)&buf[8] = r1;
    client_nm_->rdma_write_sid_sync(0, server_mr_info.addr, server_mr_info.rkey,
                                    (uint64_t)buf, client_mr->lkey, 16);
    ASSERT_TRUE((*(uint64_t*)server_mr_info.addr) == r0);
    ASSERT_TRUE((*(uint64_t*)(server_mr_info.addr + 8)) == r1);
  }

  for (int i = 0; i < 100; i++) {
    uint64_t r = rand();
    *(uint64_t*)buf = r;
    client_nm_->rdma_inl_write_sid_sync(0, server_mr_info.addr,
                                        server_mr_info.rkey, (uint64_t)buf, 8);
    ASSERT_TRUE((*(uint64_t*)server_mr_info.addr) == r);
  }
}

TEST_F(NMTest, rdma_read) {
  MrInfo server_mr_info;
  rdma_connect(&server_mr_info);
  ASSERT_TRUE(true);

  struct ibv_pd* client_pd = client_nm_->get_ib_pd();
  char buf[128];
  struct ibv_mr* client_mr =
      ibv_reg_mr(client_pd, buf, 128, IBV_ACCESS_LOCAL_WRITE);

  // iteratively write and check
  srand((int)time(NULL));
  for (int i = 0; i < 100; i++) {
    uint64_t r0 = rand();
    uint64_t r1 = rand();
    *(uint64_t*)server_mr_info.addr = r0;
    *(uint64_t*)(server_mr_info.addr + 8) = r1;
    client_nm_->rdma_read_sid_sync(0, server_mr_info.addr, server_mr_info.rkey,
                                   (uint64_t)buf, client_mr->lkey, 16);
    ASSERT_TRUE((*(uint64_t*)buf) == r0);
    ASSERT_TRUE((*(uint64_t*)&buf[8]) == r1);
  }
}

TEST_F(NMTest, rdma_combined_ts) {
  MrInfo server_mr_info;
  rdma_connect(&server_mr_info);
  ASSERT_TRUE(true);

  struct ibv_pd* client_pd = client_nm_->get_ib_pd();
  char buf[128];
  struct ibv_mr* client_mr =
      ibv_reg_mr(client_pd, buf, 128, IBV_ACCESS_LOCAL_WRITE);

  struct timeval cur;
  gettimeofday(&cur, NULL);
  struct ibv_send_wr write_wr;
  struct ibv_send_wr cas_wr;
  struct ibv_send_wr faa_wr;
  struct ibv_send_wr read_wr;
  struct ibv_sge write_sge;
  struct ibv_sge cas_sge;
  struct ibv_sge faa_sge;
  struct ibv_sge read_sge;
  memset(&write_wr, 0, sizeof(struct ibv_send_wr));
  memset(&cas_wr, 0, sizeof(struct ibv_send_wr));
  memset(&faa_wr, 0, sizeof(struct ibv_send_wr));
  memset(&read_wr, 0, sizeof(struct ibv_send_wr));
  memset(&write_sge, 0, sizeof(struct ibv_sge));
  memset(&cas_sge, 0, sizeof(struct ibv_sge));
  memset(&faa_sge, 0, sizeof(struct ibv_sge));
  memset(&read_sge, 0, sizeof(struct ibv_sge));

  uint64_t ts = 0x1234;

  write_sge.addr = (uint64_t)&ts;
  write_sge.length = 6;
  write_sge.lkey = 0;
  write_wr.wr_id = 0;
  write_wr.next = NULL;
  write_wr.sg_list = &write_sge;
  write_wr.num_sge = 1;
  write_wr.opcode = IBV_WR_RDMA_WRITE;
  write_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  write_wr.wr.rdma.remote_addr = server_mr_info.addr;
  write_wr.wr.rdma.rkey = server_mr_info.rkey;
  int ret = client_nm_->rdma_post_send_sid_sync(&write_wr, 0);
  ASSERT_TRUE(ret == 0);
  ASSERT_TRUE(*(uint64_t*)server_mr_info.addr == ts);
  printf("%lx\n", *(uint64_t*)server_mr_info.addr);

  faa_sge.addr = (uint64_t)client_mr->addr;
  faa_sge.length = 8;
  faa_sge.lkey = client_mr->lkey;
  faa_wr.wr_id = 1;
  faa_wr.next = NULL;
  faa_wr.sg_list = &faa_sge;
  faa_wr.num_sge = 1;
  faa_wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  faa_wr.send_flags = IBV_SEND_SIGNALED;
  faa_wr.wr.atomic.compare_add = (2ll << 48);
  faa_wr.wr.atomic.remote_addr = ((uint64_t)server_mr_info.addr);
  faa_wr.wr.atomic.rkey = server_mr_info.rkey;
  ret = client_nm_->rdma_post_send_sid_sync(&faa_wr, 0);
  ASSERT_TRUE(ret == 0);
  printd(L_INFO, "server content: 0x%lx", *(uint64_t*)server_mr_info.addr);
  ASSERT_TRUE(*(uint64_t*)server_mr_info.addr == 0x0002000000001234ll);

  cas_sge.addr = (uint64_t)client_mr->addr;
  cas_sge.length = 8;
  cas_sge.lkey = client_mr->lkey;
  cas_wr.wr_id = 0;
  cas_wr.next = NULL;
  cas_wr.sg_list = &cas_sge;
  cas_wr.num_sge = 1;
  cas_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  cas_wr.send_flags = IBV_SEND_SIGNALED;
  cas_wr.wr.atomic.remote_addr = server_mr_info.addr;
  cas_wr.wr.atomic.rkey = server_mr_info.rkey;
  cas_wr.wr.atomic.compare_add = *(uint64_t*)server_mr_info.addr;
  cas_wr.wr.atomic.swap = (((*(uint64_t*)server_mr_info.addr) >> 56) + 1) << 56;
  ret = client_nm_->rdma_post_send_sid_sync(&cas_wr, 0);
  ASSERT_TRUE(ret == 0);
  printd(L_INFO, "server content: %lx", *(uint64_t*)server_mr_info.addr);
  ASSERT_TRUE(*(uint64_t*)server_mr_info.addr == 0x0100000000000000ll);

  faa_wr.wr.atomic.compare_add = (1ll << 48);
  ret = client_nm_->rdma_post_send_sid_sync(&faa_wr, 0);
  ASSERT_TRUE(ret == 0);
  printd(L_INFO, "server content: 0x%lx", *(uint64_t*)server_mr_info.addr);
  ASSERT_TRUE(*(uint64_t*)server_mr_info.addr == 0x0101000000000000ll);

  faa_wr.wr.atomic.compare_add = (-1ull << 56);
  ret = client_nm_->rdma_post_send_sid_sync(&faa_wr, 0);
  ASSERT_TRUE(ret == 0);
  printd(L_INFO, "server content: 0x%lx", *(uint64_t*)server_mr_info.addr);
  ASSERT_TRUE(*(uint64_t*)server_mr_info.addr == 0x0001000000000000ll);

  read_sge.addr = (uint64_t)buf;
  read_sge.length = 8;
  read_sge.lkey = client_mr->lkey;
  read_wr.wr_id = 2;
  read_wr.next = NULL;
  read_wr.sg_list = &read_sge;
  read_wr.num_sge = 1;
  read_wr.opcode = IBV_WR_RDMA_READ;
  read_wr.send_flags = IBV_SEND_SIGNALED;
  read_wr.wr.rdma.remote_addr = server_mr_info.addr;
  read_wr.wr.rdma.rkey = server_mr_info.rkey;
  ret = client_nm_->rdma_post_send_sid_sync(&read_wr, 0);
  ASSERT_TRUE(ret == 0);
  printd(L_INFO, "local content: 0x%lx", *(uint64_t*)buf);
}