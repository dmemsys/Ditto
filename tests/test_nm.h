#ifndef _DMC_TEST_NM_H_
#define _DMC_TEST_NM_H_

#include <gtest/gtest.h>

#include "dmc_test.h"
#include "dmc_utils.h"
#include "nm.h"

typedef struct TagSrReqCtx {
  struct ibv_send_wr sr_list_1[2];
  struct ibv_send_wr sr_list_2[2];
  struct ibv_sge sg_list_1[2];
  struct ibv_sge sg_list_2[2];
} SrReqCtx;

class NMTest : public DMCTest {
 protected:
  void SetUp() override;
  void TearDown() override;

 public:
  UDPNetworkManager* server_nm_;
  UDPNetworkManager* client_nm_;

  uint64_t test_source_data_[4];

  int rdma_connect(MrInfo* mr_info);
  SrReqCtx* gen_sr_reqs(MrInfo* mr_info);
};

void* udp_send_recv_server(void* args);
void* udp_send_recv_client(void* args);

void* rdma_connect_server(void* args);

#endif