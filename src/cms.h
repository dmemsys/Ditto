#ifndef _DMC_CMS_H_
#define _DMC_CMS_H_

#include <iostream>

#include "debug.h"
#include "dmc_utils.h"

#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

class CMS {
 private:
  uint64_t PRIME;
  uint32_t* global_table_;
  uint32_t* delta_table_;
  uint32_t depth_;
  uint32_t width_;
  uint64_t g_cnter;
  uint64_t* hashA_;

  int hash(uint64_t item, int i) {
    uint64_t hash = hashA_[i] * item;
    hash += hash >> 32;
    hash &= PRIME;
    return ((int)hash) % width_;
  }

 public:
  CMS(double eps, double confidence, uint64_t laddr, __OUT uint32_t* lsize) {
    PRIME = (1L << 31) - 1;
    width_ = (uint32_t)ceil(2 / eps);
    depth_ = (uint32_t)ceil(-log(1 - confidence) / log(2));
    global_table_ = (uint32_t*)laddr;
    delta_table_ = (uint32_t*)(laddr + sizeof(uint32_t) * width_ * depth_);
    hashA_ = (uint64_t*)malloc(sizeof(uint64_t) * depth_);
    assert(hashA_ != NULL);
    printd(L_INFO, "CMS (%d x %d) size: %ldKB", width_, depth_,
           (sizeof(uint32_t) * width_ * depth_) / 1024);

    memset(global_table_, 0, sizeof(uint32_t) * width_ * depth_);
    memset(delta_table_, 0, sizeof(uint32_t) * width_ * depth_);

    // initialize hash functions (a*x+b)mod p
    srand((int)time(NULL));
    for (int i = 0; i < depth_; i++) {
      hashA_[i] = random();
    }

    *lsize = sizeof(uint32_t) * width_ * depth_;
    g_cnter = 0;
  }

  uint64_t estimate_count(uint64_t item) {
    uint64_t count = UINT64_MAX;
    for (int i = 0; i < depth_; i++) {
      int slot = hash(item, i);
      uint64_t compare = (uint64_t)global_table_[i * width_ + slot] +
                         (uint64_t)delta_table_[i * width_ + slot];
      count = std::min(count, compare);
    }
    return count;
  }

  void add(uint64_t item, long count) {
    for (int i = 0; i < depth_; i++) {
      delta_table_[i * width_ + hash(item, i)] += count;
    }
    g_cnter += count;
  }

  void merge_delta() {
    for (int i = 0; i < width_ * depth_; i++) {
      global_table_[i] += delta_table_[i];
    }
    memset(delta_table_, 0, sizeof(uint32_t) * width_ * depth_);
  }

  float average_delta() { return (float)g_cnter / width_; }

  uint32_t max_delta() {
    uint32_t count = 0;
    for (int i = 0; i < width_ * depth_; i++) {
      if (delta_table_[i] > count) {
        count = delta_table_[i];
      }
    }
    return count;
  }
};

#endif