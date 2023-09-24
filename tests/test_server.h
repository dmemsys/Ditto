#ifndef _DMC_TEST_SERVER_H_
#define _DMC_TEST_SERVER_H_

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <pthread.h>

#include "dmc_test.h"
#include "nm.h"
#include "server.h"

class ServerTest : public DMCTest {
 protected:
  void SetUp() override;
  void TearDown() override;

 private:
  pthread_t server_tid_;
  ServerMainArgs server_main_args_;

 public:
  Server* server_;
  UDPNetworkManager* client_nm_;
  void start_server();
  void stop_server();
};

#endif