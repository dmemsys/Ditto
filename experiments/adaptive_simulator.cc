#include "client.h"
#include "dmc_table.h"
#include "dmc_utils.h"
#include "priority.h"
#include "third_party/json.hpp"
#include "workload.h"

#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <map>
#include <unordered_map>

using json = nlohmann::json;

#define NUM_PRIORITY (2)

// static uint8_t prio_opt[NUM_PRIORITY] = {EVICT_SAMPLE_LRU, EVICT_SAMPLE_LFU};
static uint8_t prio_opt[NUM_PRIORITY] = {EVICT_PRIO_LRU, EVICT_PRIO_LFU};
// static uint8_t prio_opt[NUM_PRIORITY] = {EVICT_SAMPLE_HYPERBOLIC,
// EVICT_SAMPLE_LFU}; static uint8_t prio_opt[NUM_PRIORITY] = {EVICT_SAMPLE_LFU,
// EVICT_SAMPLE_LFU};

static struct option opts[] = {
    {"workload", 1, NULL, 'w'},   {"num-buckets", 1, NULL, 'b'},
    {"num-slots", 1, NULL, 's'},  {"num-sample-buckets", 1, NULL, 'S'},
    {"cache-size", 1, NULL, 'C'}, {"adaptive", 0, NULL, 'A'},
};

// hash table setting
uint64_t num_buckets = 0;
uint32_t num_slots_per_bucket = 0;
uint64_t num_cached_kv = 0;
uint64_t* slot_priority_arr;
std::unordered_map<std::string, uint64_t> key_slot_map;
std::unordered_map<uint64_t, std::string> slot_key_map;
std::unordered_map<std::string, ObjHeader> key_header_map;

// workload and sample settings
char workload_name[128] = {0};
int num_sample_buckets = 0;

// profiler counters
uint64_t num_space_evict = 0;
uint64_t num_bucket_evict = 0;
uint64_t num_get_miss = 0;
uint64_t num_set_miss = 0;
uint32_t num_prio0 = 0;
uint32_t num_prio1 = 0;

// priority settings
bool use_adaptive = false;
float prio_miss_cnt[NUM_PRIORITY];
float prio_hit_cnt[NUM_PRIORITY];
float prio_alloc_ratio = 0.5;
Priority* prio_list[NUM_PRIORITY];
uint32_t dueling_group_num = 0;
float prio_weights[NUM_PRIORITY];

void parse_args(int argc, char** argv) {
  char c;
  while (1) {
    c = getopt_long(argc, argv, "w:b:s:S:C:A", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'w':
        strcpy(workload_name, optarg);
        break;
      case 'b':
        num_buckets = atoi(optarg);
        break;
      case 's':
        num_slots_per_bucket = atoi(optarg);
        break;
      case 'S':
        num_sample_buckets = atoi(optarg);
        break;
      case 'C':
        num_cached_kv = atoi(optarg);
        break;
      case 'A':
        use_adaptive = true;
        break;
      default:
        printf("Invalid argument %c\n", c);
        abort();
    }
  }
  assert(strlen(workload_name) > 0);
  assert(num_buckets > 0);
  assert(num_slots_per_bucket > 0);
  assert(num_sample_buckets > 0);
  assert(num_cached_kv > 0);

  if (use_adaptive == true)
    prio_alloc_ratio = 0.5;
  else
    prio_alloc_ratio = 1;
  dueling_group_num = num_buckets / 4 / 3;

  // initialize adaptive priorities
  for (int i = 0; i < NUM_PRIORITY; i++) {
    printd(L_INFO, "priority: %d", prio_opt[i]);
    prio_list[i] = dmc_new_priority(prio_opt[i]);
    prio_miss_cnt[i] = 0;
    prio_hit_cnt[i] = 0;
    prio_weights[i] = 0.5;
    assert(prio_list[i] != NULL);
  }

  slot_priority_arr =
      (uint64_t*)malloc(num_buckets * num_slots_per_bucket * sizeof(uint64_t));
  assert(slot_priority_arr != NULL);
  printf(
      "Running simulator with %ld buckets %d slots per bucket storing %ld keys "
      "and sampling %d buckets.\n",
      num_buckets, num_slots_per_bucket, num_cached_kv, num_sample_buckets);
}

inline int select_priority(uint64_t key_hash) {
  if (use_adaptive == false)
    return 0;
  uint32_t bucket_id = key_hash % num_buckets;
  uint32_t sample_group_id = bucket_id / 4;
  uint32_t dueling_group_id = sample_group_id / 3;
  uint32_t standard_alloc0 = dueling_group_num / 2 + (dueling_group_num % 2);
  uint32_t standard_alloc1 = dueling_group_num - standard_alloc0;
  if (sample_group_id % 3 == 0) {
    // fixed prio0 bucket
    return 0;
  } else if (sample_group_id % 3 == 2) {
    // fixed prio1 bucket
    return 1;
  } else if (sample_group_id % 6 == 1) {
    // dueling piro0 bucket
    uint32_t num_prio0_group = dueling_group_num * prio_alloc_ratio;
    if (dueling_group_id / 2 < num_prio0_group)
      return 0;
    return 1;
  } else if (sample_group_id % 6 == 4) {
    // dueling prio1 bucket
    uint32_t num_prio1_group =
        dueling_group_num - dueling_group_num * prio_alloc_ratio;
    if (dueling_group_id / 2 < num_prio1_group)
      return 1;
    return 0;
  }
}

void update_header(const std::string& key_str, int prio_idx) {
  assert(key_header_map.find(key_str) != key_header_map.end());
  Priority* prio_func = prio_list[prio_idx];
  ObjHeader* target_header = &key_header_map[key_str];
  printd(L_DEBUG, "old (freq, ts) = (%ld, %lx)", target_header->freq,
         target_header->last_ts);
  int mask = prio_func->info_update_mask();
  if (mask & UPD_FREQ)
    target_header->freq++;
  if (mask & UPD_TS)
    target_header->last_ts = new_ts();
  printd(L_DEBUG, "new (freq, ts) = (%ld, %lx)", target_header->freq,
         target_header->last_ts);
}

uint32_t get_sample_group_range(int prio_idx) {
  if (prio_idx == 0)
    return dueling_group_num + dueling_group_num * prio_alloc_ratio;
  else
    return 2 * dueling_group_num - dueling_group_num * prio_alloc_ratio;
}

uint32_t sample_group_to_bucket_id(uint32_t sample_group, int prio_idx) {
  if (sample_group < dueling_group_num) {
    if (prio_idx == 0)
      return sample_group * 12;
    else
      return sample_group * 12 + 8;
  }
  uint32_t standard_alloc0_num =
      dueling_group_num / 2 + (dueling_group_num % 2);
  uint32_t standard_alloc1_num = dueling_group_num - standard_alloc0_num;
  uint32_t dueling_group_id = sample_group - dueling_group_num;
  if (prio_idx == 0) {
    if (dueling_group_id < standard_alloc0_num)
      return dueling_group_id * 24 + 4;
    else {
      uint32_t n_group_id =
          standard_alloc1_num - (dueling_group_id - standard_alloc0_num);
      return n_group_id * 24 + 16;
    }
  } else {
    if (dueling_group_id < standard_alloc1_num)
      return dueling_group_id * 24 + 16;
    else {
      uint32_t n_group_id =
          standard_alloc0_num - (dueling_group_id - standard_alloc1_num);
      return n_group_id * 24 + 4;
    }
  }
}

uint32_t random_bucket_id(int prio_idx) {
  if (use_adaptive == false) {
    // return (random() % (num_buckets/4)) * 4;
    return random() % (num_buckets - 4);
  }
  uint32_t sample_group_range = get_sample_group_range(prio_idx);
  assert(sample_group_range >= dueling_group_num);
  uint32_t sample_group = random() % sample_group_range;
  return sample_group_to_bucket_id(sample_group, prio_idx);
}

uint64_t sample(int prio_idx) {
  Priority* prio_func = prio_list[prio_idx];
  // printd(L_INFO, "idx: %d", prio_idx);
  // decide sample bucket space

  // conduct sampling
  std::map<double, std::vector<uint64_t>> sorted_ts_slot;
  int num_sampled = 0;
  while (num_sampled < 5) {
    uint64_t bucket_id = random_bucket_id(prio_idx);
    // printd(L_DEBUG, "sampled bucket: %ld", bucket_id);
    uint64_t slot_id = bucket_id * num_slots_per_bucket;
    for (int i = 0; i < num_slots_per_bucket * num_sample_buckets; i++) {
      uint64_t cur_slot = slot_id + i;
      uint64_t raw_prio = slot_priority_arr[cur_slot];
      // printd(L_INFO, "slot: %ld -> %ld", cur_slot, raw_prio);
      if (raw_prio == 0)
        continue;
      uint64_t prio = prio_func->parse_priority(raw_prio);
      sorted_ts_slot[prio].push_back(cur_slot);
      num_sampled++;
    }
    // printd(L_INFO, "=========");
  }
  // getchar();
  return sorted_ts_slot.begin()->second[0];
}

void clear_slot(uint64_t slot_id) {
  std::string target_key = slot_key_map[slot_id];
  slot_key_map.erase(slot_id);
  key_slot_map.erase(target_key);
  key_header_map.erase(target_key);
  slot_priority_arr[slot_id] = 0;
}

void evict(int prio_idx) {
  num_space_evict++;
  uint64_t target_slot_id = sample(prio_idx);
  clear_slot(target_slot_id);
}

void insert_key(const std::string& key_str,
                uint64_t target_slot,
                int prio_idx) {
  assert(key_slot_map.find(key_str) == key_slot_map.end());
  assert(key_header_map.find(key_str) == key_header_map.end());
  Priority* prio_func = prio_list[prio_idx];
  ObjHeader new_header;
  memset(&new_header, 0, sizeof(ObjHeader));
  slot_priority_arr[target_slot] = prio_func->new_priority(0, &new_header);
  key_slot_map[key_str] = target_slot;
  slot_key_map[target_slot] = key_str;
  key_header_map[key_str] = new_header;
}

bool set(char* key, uint32_t key_size, DMCHash* hash) {
  std::string key_str(key);
  uint64_t key_hash = hash->hash_func1(key, key_size);
  int prio_idx = select_priority(key_hash);
  Priority* prio_func = prio_list[prio_idx];
  if (key_slot_map.find(key_str) != key_slot_map.end()) {
    uint64_t slot_id = key_slot_map[key_str];
    ObjHeader header = key_header_map[key_str];
    slot_priority_arr[slot_id] =
        prio_func->new_priority(slot_priority_arr[slot_id], &header);
    update_header(key_str, prio_idx);
    return true;
  }

  while (key_slot_map.size() >= num_cached_kv) {
    evict(prio_idx);
  }
  uint64_t bucket_id = key_hash % num_buckets;
  uint64_t slot_id = bucket_id * num_slots_per_bucket;
  std::map<double, uint64_t> sorted_slot;
  for (int i = 0; i < num_slots_per_bucket; i++) {
    uint64_t cur_slot = slot_id + i;
    uint64_t raw_prio = slot_priority_arr[cur_slot];
    if (raw_prio == 0) {
      insert_key(key_str, cur_slot, prio_idx);
      update_header(key_str, prio_idx);
      return false;
    }
    double cur_prio = prio_func->parse_priority(slot_priority_arr[cur_slot]);
    sorted_slot[cur_prio] = cur_slot;
  }

  // evict the old key
  num_bucket_evict++;
  uint64_t target_slot_id = sorted_slot.begin()->second;
  clear_slot(target_slot_id);

  // insert the new key
  insert_key(key_str, target_slot_id, prio_idx);
  update_header(key_str, prio_idx);
  return false;
}

bool get(char* key, uint32_t key_size, DMCHash* hash) {
  assert(strlen(key) == key_size);
  uint64_t key_hash = hash->hash_func1(key, key_size);
  int prio_idx = select_priority(key_hash);
  std::string key_str(key);
  auto key_slot_it = key_slot_map.find(key_str);
  if (key_slot_it == key_slot_map.end()) {
    return false;
  }
  uint64_t slot_id = key_slot_it->second;
  ObjHeader header = key_header_map[key_str];
  slot_priority_arr[slot_id] =
      prio_list[prio_idx]->new_priority(slot_priority_arr[slot_id], &header);
  update_header(key_str, prio_idx);
  return true;
}

void clear_counters() {
  num_space_evict = 0;
  num_bucket_evict = 0;
}

float adjust_hit_density() {
  float hd0 = prio_hit_cnt[0] / prio_alloc_ratio;
  float hd1 = prio_hit_cnt[1] / (1 - prio_alloc_ratio);
  printd(L_INFO, "hd0, hd1: %f %f", hd0, hd1);
  return hd0 / (hd0 + hd1);
  // if (hd0 > hd1) {
  //     return prio_alloc_ratio + 0.01;
  // } else {
  //     return prio_alloc_ratio - 0.01;
  // }
}

float adjust_hit_density_n() {
  float hr0 = prio_hit_cnt[0] / (prio_hit_cnt[0] + prio_miss_cnt[0]);
  float hr1 = prio_hit_cnt[1] / (prio_hit_cnt[1] + prio_miss_cnt[1]);
  float hd0 = hr0 / prio_alloc_ratio;
  float hd1 = hr1 / (1 - prio_alloc_ratio);
  printd(L_INFO, "hr0, hr1: %f %f", hr0, hr1);
  printd(L_INFO, "hd0, hd1: %f %f", hd0, hd1);
  printd(L_INFO, "w0, w1: %f %f", prio_weights[0], prio_weights[1]);
  printd(L_INFO, "acc0, acc1: %f %f", prio_hit_cnt[0] + prio_miss_cnt[0],
         prio_hit_cnt[1] + prio_miss_cnt[1]);
  if (hd0 < hd1)
    prio_weights[1] += 0.01;
  else if (hd1 < hd0)
    prio_weights[0] += 0.01;
  prio_weights[0] = prio_weights[0] / (prio_weights[0] + prio_weights[1]);
  prio_weights[1] = 1 - prio_weights[0];
  return prio_weights[0] / (prio_weights[0] + prio_weights[1]);
}

float adjust_miss_number() {
  if (prio_miss_cnt[0] > prio_miss_cnt[1]) {
    return prio_alloc_ratio - 0.01;
  } else {
    return prio_alloc_ratio + 0.01;
  }
  // return prio_miss_cnt[1] / (prio_miss_cnt[1] + prio_miss_cnt[0]);
}

uint32_t sampled_prio_hit[NUM_PRIORITY] = {0};
uint32_t sampled_prio_access[NUM_PRIORITY] = {0};

float adjust_sample_hit() {
  float hr0 = (float)sampled_prio_hit[0] / sampled_prio_access[0];
  float hr1 = (float)sampled_prio_hit[1] / sampled_prio_access[1];
  if (hr0 < hr1)
    prio_weights[1] += 0.01;
  else if (hr1 < hr0)
    prio_weights[0] += 0.01;
  prio_weights[0] = prio_weights[0] / (prio_weights[0] + prio_weights[1]);
  prio_weights[1] = 1 - prio_weights[0];
  return prio_weights[0];
}

std::vector<float> ratio_change;

void adjust_allocation() {
  ratio_change.push_back(prio_alloc_ratio);
  if (use_adaptive == false) {
    return;
  }
  if (num_space_evict == 0) {
    return;
  }
  float new_ratio = adjust_hit_density_n();
  // float new_ratio = adjust_miss_number();
  // float new_ratio = adjust_sample_hit();

  // adjust the newly allocated block
  uint32_t prio0_lower, prio0_upper;
  uint32_t standard_alloc0 = dueling_group_num / 2 + (dueling_group_num % 2);
  uint32_t standard_alloc1 = dueling_group_num - standard_alloc0;
  if (new_ratio < prio_alloc_ratio) {
    prio0_lower = dueling_group_num * new_ratio;
    prio0_upper = dueling_group_num * prio_alloc_ratio;
  } else {
    prio0_lower = dueling_group_num * prio_alloc_ratio;
    prio0_upper = dueling_group_num * new_ratio;
  }
  // deal with the normal mode
  for (int i = prio0_lower; i < standard_alloc0 && i < prio0_upper; i++) {
    uint32_t bucket_id = i * 24 + 4;
    uint32_t slot_id = bucket_id * num_slots_per_bucket;
    for (int j = 0; j < num_slots_per_bucket; j++) {
      clear_slot(slot_id + j);
    }
  }
  // deal with the expansion mode
  for (int i = standard_alloc0; i < prio0_upper; i++) {
    uint32_t n_group_id = standard_alloc1 - (i - standard_alloc0);
    uint32_t bucket_id = n_group_id * 24 + 16;
    uint32_t slot_id = bucket_id * num_slots_per_bucket;
    for (int j = 0; j < num_slots_per_bucket; j++) {
      clear_slot(slot_id + j);
    }
  }
  printd(L_INFO, "n_evict: %d %d", num_space_evict, num_bucket_evict);
  printd(L_INFO, "ratio: %f -> %f", prio_alloc_ratio, new_ratio);
  prio_alloc_ratio = new_ratio;
  // getchar();
}

void update_sample(uint64_t key_hash, int prio_idx, bool is_hit) {
  uint32_t bucket_id = key_hash % num_buckets;
  if (prio_idx == 0 && (bucket_id % 12) == 0) {
    sampled_prio_hit[0] += is_hit;
    sampled_prio_access[0]++;
  } else if (prio_idx == 1 && (bucket_id % 12) == 8) {
    sampled_prio_hit[1] += is_hit;
    sampled_prio_access[1]++;
  }
}

int main(int argc, char** argv) {
  srand(time(NULL));
  parse_args(argc, argv);
  DMCHash* hash = new DefaultHash();
  DMCWorkload wl;
  load_workload(workload_name, -1, 1, 1, &wl);

  // warmup
  printf("Warmup start!\n");
  int num_warmup = wl.num_ops / 10;
  for (int i = 0; i < num_warmup; i++) {
    char* key_ptr = (char*)((uint64_t)wl.key_buf + i * 128);
    uint32_t key_size = wl.key_size_list[i];
    uint64_t key_hash = hash->hash_func1(key_ptr, key_size);
    int prio_idx = select_priority(key_hash);
    assert(strlen(key_ptr) == key_size);
    if (wl.op_list[i] == GET) {
      bool is_hit = get(key_ptr, key_size, hash);
      if (is_hit == false) {
        set(key_ptr, key_size, hash);
        prio_miss_cnt[prio_idx]++;
      } else {
        prio_hit_cnt[prio_idx]++;
      }
      update_sample(key_hash, prio_idx, is_hit);
    } else {
      bool is_hit = set(key_ptr, key_size, hash);
      if (is_hit == false) {
        prio_miss_cnt[prio_idx]++;
      } else {
        prio_hit_cnt[prio_idx]++;
      }
      update_sample(key_hash, prio_idx, is_hit);
    }
    if (key_slot_map.size() > num_cached_kv) {
      printf("cached %ld size %ld\n", key_slot_map.size(), num_cached_kv);
    }
    assert(key_slot_map.size() <= num_cached_kv);
    if (i % 1000 == 0 && i > 0) {
      adjust_allocation();
    }
  }

  clear_counters();

  // start simulate
  printf("Simulation start!\n");
  uint32_t num_miss = 0;
  for (int i = num_warmup; i < wl.num_ops; i++) {
    char* key_ptr = (char*)((uint64_t)wl.key_buf + i * 128);
    uint32_t key_size = wl.key_size_list[i];
    uint64_t key_hash = hash->hash_func1(key_ptr, key_size);
    int prio_idx = select_priority(key_hash);
    assert(strlen(key_ptr) == key_size);
    if (wl.op_list[i] == GET) {
      bool is_hit = get(key_ptr, key_size, hash);
      if (is_hit == false) {
        num_get_miss++;
        num_miss++;
        set(key_ptr, key_size, hash);
        prio_miss_cnt[prio_idx]++;
      } else {
        prio_hit_cnt[prio_idx]++;
      }
      update_sample(key_hash, prio_idx, is_hit);
    } else {
      bool is_hit = set(key_ptr, key_size, hash);
      if (is_hit == false) {
        num_set_miss++;
        num_miss++;
        prio_miss_cnt[prio_idx]++;
      } else {
        prio_hit_cnt[prio_idx]++;
      }
      update_sample(key_hash, prio_idx, is_hit);
    }
    if (key_slot_map.size() > num_cached_kv) {
      printf("cached %ld size %ld\n", key_slot_map.size(), num_cached_kv);
    }
    assert(key_slot_map.size() <= num_cached_kv);
    if (i % 1000 == 0) {
      adjust_allocation();
    }
  }

  printf("num_miss: %d Get/Set: %ld/%ld\n", num_miss, num_get_miss,
         num_set_miss);
  printf("num_prio: %d %d\n", num_prio0, num_prio1);
  printf("prio_ratio: %f\n", prio_alloc_ratio);
  printf("num_space_evict: %ld\n", num_space_evict);
  printf("num_bucket_evict: %ld\n", num_bucket_evict);
  printf("num_ops:  %d\n", wl.num_ops - num_warmup);
  printf("miss_rate: %f\n", (float)num_miss / (wl.num_ops - num_warmup));
  printf("bucket_evict_rate: %f\n",
         (float)num_bucket_evict / (wl.num_ops - num_warmup));
  printf("space_evict_rate: %f\n",
         (float)num_space_evict / (wl.num_ops - num_warmup));

  json res_vec(ratio_change);
  FILE* of = fopen("ratio_change.json", "w");
  assert(of != NULL);
  fprintf(of, "%s", res_vec.dump().c_str());
}