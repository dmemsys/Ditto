#include "test_rlist.h"

void RListTest::SetUp() {
  MrInfo server_mr_info;
  setup_server_conf();
  setup_client_conf();
  server_nm_ = new UDPNetworkManager(&server_conf_);
  client_nm_ = new UDPNetworkManager(&client_conf_);
  rdma_connect(&server_mr_info);

  list_buf_ = malloc(1024 * 1024 * 1024LL);
  assert(list_buf_ != NULL);
  struct ibv_pd* pd = server_nm_->get_ib_pd();
  list_buf_mr_ =
      ibv_reg_mr(pd, list_buf_, 1024 * 1024 * 1024LL,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  assert(list_buf_mr_ != NULL);

  check_buf_ = malloc(1024 * 1024);
  assert(check_buf_ != NULL);
  check_buf_mr_ =
      ibv_reg_mr(pd, check_buf_, 1024 * 1024,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC |
                     IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
  assert(check_buf_mr_ != NULL);
  memset(check_buf_, 0, 1024 * 1024);

  op_buf_ = malloc(1024 * 1024LL);
  assert(op_buf_ != NULL);
  pd = client_nm_->get_ib_pd();
  op_buf_mr_ = ibv_reg_mr(pd, op_buf_, 1024 * 1024LL, IBV_ACCESS_LOCAL_WRITE);
  server_rList_ = new RList(1024 * 1024 * 16U, (uint64_t)list_buf_,
                            list_buf_mr_->rkey, NULL, 0, 0, SERVER);
  client_rList_ =
      new RList(1024 * 1024 * 16U, (uint64_t)list_buf_, list_buf_mr_->rkey,
                op_buf_, 1024 * 1024, op_buf_mr_->lkey, CLIENT);
}

void RListTest::TearDown() {
  ibv_dereg_mr(op_buf_mr_);
  ibv_dereg_mr(list_buf_mr_);
  free(op_buf_);
  free(list_buf_);
}

TEST_F(RListTest, rlist_insert) {
  for (int i = 0; i < 10; i++) {
    client_rList_->update(client_nm_, i, i, (uint64_t)check_buf_, 0);
  }
  client_rList_->printList(client_nm_);
  for (int i = 0; i < 10; i++) {
    client_rList_->update(client_nm_, i, 123121, (uint64_t)check_buf_, 0);
  }
  client_rList_->printList(client_nm_);

  *(uint64_t*)check_buf_ = 0;
  client_rList_->delete_slot(client_nm_, 9, (uint64_t)check_buf_);
  client_rList_->delete_slot(client_nm_, 4, (uint64_t)check_buf_);
  client_rList_->printList(client_nm_);
}