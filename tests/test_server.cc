#include "test_server.h"
#include "dmc_table.h"

void ServerTest::SetUp() {
  setup_server_conf();
  setup_client_conf();
  server_ = new Server(&server_conf_);
  client_nm_ = new UDPNetworkManager(&client_conf_);
}

void ServerTest::TearDown() {
  delete client_nm_;
  delete server_;
}

void ServerTest::start_server() {
  server_main_args_.server = server_;
  server_main_args_.core_id = 0;
  pthread_create(&server_tid_, NULL, server_main, (void*)&server_main_args_);
}

void ServerTest::stop_server() {
  server_->stop();
  pthread_join(server_tid_, NULL);
}

TEST_F(ServerTest, initialization) {
  ASSERT_TRUE(true);
}

TEST_F(ServerTest, server_start_stop) {
  start_server();
  stop_server();
}

TEST_F(ServerTest, rdma_connect) {
  start_server();
  MrInfo mr_info;
  int ret = client_nm_->client_connect_one_rc_qp(0, &mr_info);
  ASSERT_TRUE(ret == 0);
  ASSERT_TRUE(mr_info.addr == server_conf_.server_base_addr);
  stop_server();
}

TEST_F(ServerTest, server_alloc_segment) {
  start_server();
  MrInfo mr_info;
  int ret = client_nm_->client_connect_one_rc_qp(0, &mr_info);
  ASSERT_TRUE(ret == 0);
  ASSERT_TRUE(mr_info.addr == server_conf_.server_base_addr);

  // iteratively allocate segment
  for (int i = 0; i < 10; i++) {
    UDPMsg request;
    memset(&request, 0, sizeof(UDPMsg));
    request.type = UDPMSG_REQ_ALLOC;
    request.id = 1;
    serialize_udpmsg(&request);
    ret = client_nm_->send_udp_msg_to_server(&request, 0);
    ASSERT_TRUE(ret == 0);

    UDPMsg reply;
    struct sockaddr_in sockaddr;
    socklen_t sockaddr_len;
    ret = client_nm_->recv_udp_msg(&reply, &sockaddr, &sockaddr_len);
    ASSERT_TRUE(ret == 0);
    deserialize_udpmsg(&reply);
    ASSERT_TRUE(reply.type == UDPMSG_REP_ALLOC);
    ASSERT_TRUE(reply.id == 0);
    ASSERT_TRUE(reply.body.mr_info.addr == server_conf_.server_base_addr +
                                               HASH_SPACE_SIZE +
                                               i * server_conf_.segment_size);
  }
  stop_server();
}

TEST_F(ServerTest, server_rdma_rdwr) {
  start_server();
  MrInfo mr_info;
  int ret = client_nm_->client_connect_one_rc_qp(0, &mr_info);
  ASSERT_TRUE(ret == 0);
  ASSERT_TRUE(mr_info.addr == server_conf_.server_base_addr);

  struct ibv_pd* pd = client_nm_->get_ib_pd();
  char send_buf[128];
  char recv_buf[128];
  struct ibv_mr* client_send_mr =
      ibv_reg_mr(pd, send_buf, 128, IBV_ACCESS_LOCAL_WRITE);
  struct ibv_mr* client_recv_mr =
      ibv_reg_mr(pd, recv_buf, 128, IBV_ACCESS_LOCAL_WRITE);
  uint64_t r_addr = mr_info.addr + HASH_SPACE_SIZE;
  for (int i = 0; i < 10; i++) {
    sprintf(send_buf, "message %d!", i);
    memset(recv_buf, 0, 128);
    ASSERT_TRUE(std::string(send_buf) != std::string(recv_buf));
    ret = client_nm_->rdma_write_sid_sync(
        0, r_addr, mr_info.rkey, (uint64_t)send_buf, client_send_mr->lkey, 128);
    ASSERT_TRUE(ret == 0);

    ret = client_nm_->rdma_read_sid_sync(
        0, r_addr, mr_info.rkey, (uint64_t)recv_buf, client_recv_mr->lkey, 128);
    ASSERT_TRUE(ret == 0);

    ASSERT_TRUE(std::string(recv_buf) == std::string(send_buf));
    r_addr += 128;
  }
  ibv_dereg_mr(client_send_mr);
  ibv_dereg_mr(client_recv_mr);
  stop_server();
}

TEST_F(ServerTest, server_set_get) {
  start_server();
  sleep(2);
  char key_buf[128];
  char val_buf[128];
  char tmp_buf[128];
  int ret = 0;
  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d", i);
    ret = server_->set(0, key_buf, strlen(key_buf) + 1, val_buf,
                       strlen(val_buf) + 1);
    ASSERT_TRUE(ret == 0);
  }

  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d", i);
    uint32_t val_len;
    ret = server_->get(key_buf, strlen(key_buf) + 1, tmp_buf, &val_len);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(val_buf, tmp_buf) == 0);
  }
  stop_server();
}

TEST_F(ServerTest, server_dup_set) {
  start_server();
  sleep(2);
  char key_buf[128];
  char val_buf[128];
  char tmp_buf[128];
  int ret = 0;
  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d", i);
    ret = server_->set(0, key_buf, strlen(key_buf) + 1, val_buf,
                       strlen(val_buf) + 1);
    ASSERT_TRUE(ret == 0);
  }
  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d-n", i);
    ret = server_->set(0, key_buf, strlen(key_buf) + 1, val_buf,
                       strlen(val_buf) + 1);
    ASSERT_TRUE(ret == 0);
  }

  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d-n", i);
    uint32_t val_len;
    ret = server_->get(key_buf, strlen(key_buf) + 1, tmp_buf, &val_len);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(val_buf, tmp_buf) == 0);
  }
  stop_server();
}

TEST_F(ServerTest, server_dumb_set) {
  // TODO: finish this
  delete server_;
  server_conf_.hash_type = HASH_DUMB;
  server_ = new Server(&server_conf_);
  int ret = 0;

  start_server();

  char key_buf[128];
  char val_buf[128];
  char tmp_buf[128];
  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d", i);
    ret = server_->set(0, key_buf, strlen(key_buf) + 1, val_buf,
                       strlen(val_buf) + 1);
    ASSERT_TRUE(ret == 0);
  }

  for (int i = 0; i < 100; i++) {
    sprintf(key_buf, "Server-Set-Key-%d", i);
    sprintf(val_buf, "Server-Set-Val-%d", i);
    uint32_t val_len;
    ret = server_->get(key_buf, strlen(key_buf) + 1, tmp_buf, &val_len);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(strcmp(val_buf, tmp_buf) == 0);
  }
  stop_server();
}