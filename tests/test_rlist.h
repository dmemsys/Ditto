#ifndef _DMC_TEST_RLIST_H_
#define _DMC_TEST_RLIST_H_

#include "nm.h"
#include "rlist.h"
#include "test_nm.h"

#include <gtest/gtest.h>

class RListTest : public NMTest {
 protected:
  void SetUp() override;
  void TearDown() override;

 public:
  void* list_buf_;
  struct ibv_mr* list_buf_mr_;
  void* op_buf_;
  struct ibv_mr* op_buf_mr_;
  void* check_buf_;
  struct ibv_mr* check_buf_mr_;
  RList* server_rList_;
  RList* client_rList_;
};

#endif