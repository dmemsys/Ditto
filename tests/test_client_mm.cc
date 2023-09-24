#include <gtest/gtest.h>

#include "client_mm.h"
#include "dmc_table.h"
#include "dmc_test.h"
#include "dmc_utils.h"

class TestClientMM : public DMCTest {
 protected:
  void SetUp() override;
  void TearDown() override;
};

void TestClientMM::SetUp() {
  setup_client_conf();
}

void TestClientMM::TearDown() {
  return;
}

TEST_F(TestClientMM, initialization) {
  ClientMM* mm = new ClientUniformMM(&client_conf_);
  delete mm;
  ASSERT_TRUE(true);
}

TEST_F(TestClientMM, add_segment) {
  ClientMM* mm = new ClientUniformMM(&client_conf_);

  uint32_t rkey = 0x123;
  uint64_t st_addr = client_conf_.server_base_addr + HASH_SPACE_SIZE;
  for (int i = 0; i < 10; i++) {
    mm->add_segment(st_addr + i * client_conf_.segment_size, rkey, 0);
  }
  // get free size
  ASSERT_TRUE(mm->check_integrity() == true);
  ASSERT_TRUE(mm->get_free_size() == 10 * client_conf_.segment_size);
  delete mm;
}

TEST_F(TestClientMM, alloc) {
  ClientMM* mm = new ClientUniformMM(&client_conf_);
  int ret;

  RemoteBlock rb;
  ret = mm->alloc(10, &rb);
  ASSERT_TRUE(ret == -1);

  uint32_t rkey = 0x123;
  uint64_t st_addr = client_conf_.server_base_addr + HASH_SPACE_SIZE;
  for (int i = 0; i < 10; i++) {
    mm->add_segment(st_addr + i * client_conf_.segment_size, rkey, 0);
  }
  // get free size
  ASSERT_TRUE(mm->check_integrity() == true);
  ASSERT_TRUE(mm->get_free_size() == 10 * client_conf_.segment_size);

  for (int i = 0; i < 10; i++) {
    ret = mm->alloc(10, &rb);
    ASSERT_TRUE(rb.addr == st_addr + i * client_conf_.block_size);
    ASSERT_TRUE(rb.server == 0);
    ASSERT_TRUE(rb.rkey == rkey);
    ASSERT_TRUE(rb.size == client_conf_.block_size);
    ASSERT_TRUE(ret == 0);
  }
  ASSERT_TRUE(mm->check_integrity() == true);
  ASSERT_TRUE(mm->get_free_size() ==
              10 * client_conf_.segment_size - 10 * client_conf_.block_size);
}