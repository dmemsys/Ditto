#include <gtest/gtest.h>
#include <stdio.h>
#include "cms.h"
#include "dmc_table.h"

TEST(test_cms, test_add_estimate) {
  void* buffer = malloc(8 * 1024 * 1024);
  ASSERT_TRUE(buffer != NULL);

  uint32_t lsize = 0;
  CMS cms(0.0005, 0.6, (uint64_t)buffer, &lsize);

  for (int i = 0; i < 100000; i++) {
    cms.add(i, 1);
  }

  uint64_t err = 0;
  for (int i = 0; i < 100000; i++) {
    ASSERT_TRUE(cms.estimate_count(i) > 0);
    err += cms.estimate_count(i);
  }
  printf("Average err: %f\n", (float)err / 100000);
}

TEST(test_cms, test_collision) {
  void* buffer = malloc(8 * 1024 * 1024);
  ASSERT_TRUE(buffer != NULL);

  uint32_t cms_size = 0;
  DMCHash* hash = new DefaultHash();

  for (int c = 0; c < 64; c++) {
    CMS cms = CMS(0.0001, 0.7, (uint64_t)buffer, &cms_size);
    char key_buf[128];
    char val_buf[128];
    std::map<uint64_t, bool> collision_map;
    int num_collision = 0;
    for (int i = 0; i < 100000; i++) {
      sprintf(key_buf, "%d-key-%d", 0, i);
      uint64_t keyhash = hash->hash_func1(key_buf, strlen(key_buf) + 1);
      if (collision_map.find(keyhash) == collision_map.end()) {
        collision_map[keyhash] = true;
      } else {
        num_collision++;
      }
      cms.add(keyhash, 1);
    }
    uint64_t max = 0;
    for (int i = 0; i < 100000; i++) {
      sprintf(key_buf, "%d-key-%d", 0, i);
      cms.add(hash->hash_func1(key_buf, strlen(key_buf) + 1), 1);
      uint64_t est =
          cms.estimate_count(hash->hash_func1(key_buf, strlen(key_buf) + 1));
      if (est > max)
        max = est;
    }
    printf("%d: max: %ld collisions: %d\n", c, max, num_collision);
  }
}