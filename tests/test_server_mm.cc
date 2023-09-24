#include "test_server_mm.h"
#include "dmc_table.h"

void ServerMMTest::SetUp() {
  setup_server_conf();
  nm_ = new UDPNetworkManager(&server_conf_);
  mm_ = new ServerMM(&server_conf_, nm_->get_ib_pd());
}

void ServerMMTest::TearDown() {
  delete mm_;
  delete nm_;
}

TEST_F(ServerMMTest, test_init) {
  ASSERT_TRUE(true);
}

TEST_F(ServerMMTest, test_alloc) {
  int ret = 0;
  uint64_t allocated_bytes = 0;
  for (int i = 0; i < 30; i++) {
    SegmentInfo alloc_info;
    ret = mm_->alloc_segment(1, &alloc_info);
    allocated_bytes += server_conf_.segment_size;
    if (allocated_bytes > (server_conf_.server_data_len - HASH_SPACE_SIZE)) {
      ASSERT_TRUE(ret == -1);
      continue;
    } else {
      ASSERT_TRUE(ret == 0);
    }
    ASSERT_TRUE(alloc_info.allocated_to == 1);
    ASSERT_TRUE(mm_->check_num_segments() == true);
  }
}

TEST_F(ServerMMTest, test_free) {
  int ret = 0;
  ret = mm_->free_segment(0x1234ull);
  ASSERT_TRUE(ret == -1);

  std::vector<SegmentInfo> allocated_info_list;
  for (int i = 0; i < 10; i++) {
    SegmentInfo alloc_info;
    ret = mm_->alloc_segment(1, &alloc_info);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(alloc_info.allocated_to == 1);
    ASSERT_TRUE(mm_->check_num_segments() == true);
    allocated_info_list.push_back(alloc_info);
  }

  for (int i = 0; i < 10; i++) {
    ret = mm_->free_segment(allocated_info_list[i].addr);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(mm_->check_num_segments() == true);
  }
}