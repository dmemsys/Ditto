#ifndef _DMC_FREQ_CACHE_H_
#define _DMC_FREQ_CACHE_H_

#include "dmc_utils.h"

#include <stdint.h>

#include <string>
#include <unordered_map>
#include <vector>

class FreqCache {
 private:
  uint32_t cache_size_;
  uint32_t free_size_;
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> cache_;

 public:
  FreqCache(uint32_t cache_size) {
    printd(L_INFO, "create freq cache of size: %d", cache_size);
    cache_size_ = cache_size;
    free_size_ = cache_size;
  }

  inline uint32_t get_consumed_size(const std::string& key) {
    return key.size() + sizeof(uint64_t) * 2;
  }

  std::pair<uint64_t, uint64_t> evict() {
    auto it_1 = cache_.begin();
    auto it_2 = it_1++;
    auto victim = it_1;
    if (it_1->second.second > it_2->second.second) {
      victim = it_1;
    } else {
      victim = it_2;
    }
    std::pair<uint64_t, uint64_t> ret = victim->second;
    free_size_ += get_consumed_size(victim->first);
    cache_.erase(victim);
    return ret;
  }

  void add(std::string key,
           uint64_t r_addr,
           std::vector<std::pair<uint64_t, uint64_t>>& target_freq_cnters) {
    uint32_t consumed_size = get_consumed_size(key);

    while (free_size_ < consumed_size) {
      std::pair<uint64_t, uint64_t> victim = evict();
      target_freq_cnters.push_back(victim);
    }

    auto key_it = cache_.find(key);
    if (key_it == cache_.end()) {
      cache_[key] = std::make_pair(r_addr, 1);
      free_size_ -= consumed_size;
      return;
    }
    key_it->second.second++;
    if (key_it->second.second > LOCAL_FREQ_THRESH) {
      target_freq_cnters.push_back(key_it->second);
      free_size_ += consumed_size;
      cache_.erase(key_it);
    }
  }
};

#endif