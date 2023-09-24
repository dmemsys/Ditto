#ifndef _DMC_TEST_SERVER_MM_H_
#define _DMC_TEST_SERVER_MM_H_

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

#include "dmc_test.h"
#include "dmc_utils.h"
#include "nm.h"
#include "server_mm.h"

class ServerMMTest : public DMCTest {
 protected:
  void SetUp() override;
  void TearDown() override;

 public:
  ServerMM* mm_;
  UDPNetworkManager* nm_;
};

#endif