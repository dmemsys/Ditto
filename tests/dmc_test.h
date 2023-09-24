#ifndef DMC_TEST_H_
#define DMC_TEST_H_

#include <gtest/gtest.h>

#include "dmc_utils.h"

#define GB (1024ll * 1024 * 1024)
#define MB (1024ll * 1024)
#define KB (1024ll)

class DMCTest : public ::testing::Test {
 protected:
  void setup_server_conf();
  void setup_client_conf();

 public:
  DMCConfig server_conf_;
  DMCConfig client_conf_;
};

#endif