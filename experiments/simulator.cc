#include "client.h"
#include "dmc_table.h"
#include "dmc_utils.h"
#include "workload.h"

#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <map>
#include <unordered_map>

#define POOL_SIZE (5)

static struct option opts[] = {
    {"workload", 1, NULL, 'w'},      {"num-buckets", 1, NULL, 'b'},
    {"num-slots", 1, NULL, 's'},     {"num-sample-buckets", 1, NULL, 'S'},
    {"cache-size", 1, NULL, 'C'},    {"evict-type", 1, NULL, 'e'},
    {"random-bucket", 0, NULL, 'R'}, {"local-pool", 0, NULL, 'P'},
    {"num-clients", 1, NULL, 'n'}};

char workload_name[128] = {0};

uint64_t num_buckets = 0;
uint32_t num_slots_per_bucket = 0;
Slot* table = NULL;

int num_samples = 0;
int eviction_type = EVICT_PRIO_LRU;
uint64_t* slot_priority;
bool is_random_bucket = false;
bool use_local_pool = false;
uint32_t num_cached_key = 0;
uint32_t cache_size = 0;
uint32_t num_clients = 1;

std::vector<std::pair<uint64_t, uint32_t>>* sample_pool;

// counters
uint64_t num_space_evict = 0;
uint64_t num_bucket_evict = 0;
uint64_t num_sample_read_bucket = 0;
uint64_t num_pool_evict = 0;
uint64_t num_pool_evict_read_slot = 0;

void parse_args(int argc, char** argv) {
  char c;
  while (1) {
    c = getopt_long(argc, argv, "w:b:s:S:C:e:RPn:", opts, NULL);
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
        num_samples = atoi(optarg);
        break;
      case 'C':
        cache_size = atoi(optarg);
        break;
      case 'e':
        if (strcmp(optarg, "lru") == 0) {
          printf("lru simulator\n");
          eviction_type = EVICT_PRIO_LRU;
        } else {
          assert(strcmp(optarg, "lfu") == 0);
          printf("lfu simulator\n");
          eviction_type = EVICT_PRIO_LFU;
        }
        break;
      case 'R':
        is_random_bucket = true;
        break;
      case 'P':
        use_local_pool = true;
        break;
      case 'n':
        num_clients = atoi(optarg);
        break;
      default:
        printf("Invalid argument %c\n", c);
        abort();
    }
  }
  assert(strlen(workload_name) > 0);
  assert(num_buckets > 0);
  assert(num_slots_per_bucket > 0);
  assert(num_samples > 0);
  assert(cache_size > 0);
  assert(num_clients > 0);

  sample_pool = new std::vector<std::pair<uint64_t, uint32_t>>[num_clients];

  slot_priority =
      (uint64_t*)malloc(num_buckets * num_slots_per_bucket * sizeof(uint64_t));
  assert(slot_priority != NULL);
  memset(slot_priority, 0,
         sizeof(uint64_t) * num_buckets * num_slots_per_bucket);
  table = (Slot*)malloc(num_buckets * num_slots_per_bucket * sizeof(Slot));
  assert(table != NULL);
  memset(table, 0, sizeof(Slot) * num_slots_per_bucket * num_buckets);
  printf(
      "Running simulator with %d clients %ld buckets %d slots per bucket "
      "storing %d keys and sampling %d keys.\n"
      "Random bucket eviction: %d\n"
      "Use local pool: %d\n",
      num_clients, num_buckets, num_slots_per_bucket, cache_size, num_samples,
      is_random_bucket, use_local_pool);
}

void update_priority(uint32_t slot_idx) {
  if (eviction_type == EVICT_PRIO_LRU) {
    slot_priority[slot_idx] = new_ts();
  } else {
    assert(eviction_type == EVICT_PRIO_LFU);
    slot_priority[slot_idx]++;
  }
}

void maintain_pool(const std::vector<std::pair<uint64_t, uint32_t>>& prio_slot,
                   int cid) {
  int i = 0, j = 0;
  std::vector<std::pair<uint64_t, uint32_t>> tmp_pool;
  while (i < prio_slot.size() && j < sample_pool[cid].size() &&
         tmp_pool.size() < num_samples) {
    if (prio_slot[i].first < sample_pool[cid][j].first) {
      tmp_pool.push_back(prio_slot[i]);
      i++;
    } else {
      tmp_pool.push_back(sample_pool[cid][j]);
      j++;
    }
  }

  for (; i < prio_slot.size() && tmp_pool.size() < num_samples; i++)
    tmp_pool.push_back(prio_slot[i]);
  for (; j < sample_pool[cid].size() && tmp_pool.size() < num_samples; j++)
    tmp_pool.push_back(sample_pool[cid][j]);
  sample_pool[cid] = tmp_pool;
}

uint64_t sample(int cid) {
  std::vector<std::pair<uint64_t, uint32_t>> prio_slotidx;
  prio_slotidx.clear();
  while (prio_slotidx.size() < num_samples) {
    num_sample_read_bucket++;
    uint64_t bucket_id = rand() % num_buckets;
    uint64_t slot_id = bucket_id * num_slots_per_bucket;
    for (int i = 0; i < num_slots_per_bucket; i++) {
      uint64_t cur_slot = slot_id + i;
      uint64_t kv_addr =
          HashIndexConvert48To64Bits(table[cur_slot].atomic.pointer);
      if (kv_addr == 0)
        continue;
      uint64_t prio = slot_priority[cur_slot];
      prio_slotidx.emplace_back(prio, cur_slot);
    }
  }
  std::sort(prio_slotidx.begin(), prio_slotidx.end());
  std::pair<uint64_t, uint32_t> target = prio_slotidx[0];
  prio_slotidx.erase(prio_slotidx.begin());
  if (use_local_pool) {
    maintain_pool(prio_slotidx, cid);
  }
  return target.second;
}

void clear_slot(uint32_t slot_idx) {
  Slot* target_slot = &table[slot_idx];
  uint64_t kv_addr = HashIndexConvert48To64Bits(target_slot->atomic.pointer);
  *(uint64_t*)target_slot = 0;
  free((void*)kv_addr);
  slot_priority[slot_idx] = 0;
  num_cached_key--;
}

void insert_slot(char* key,
                 uint32_t key_size,
                 uint64_t key_hash,
                 uint32_t slot_idx) {
  uint64_t kv_addr = (uint64_t)malloc(sizeof(ObjHeader) + key_size);
  memset((void*)kv_addr, 0, sizeof(ObjHeader) + key_size);
  void* key_addr = (void*)(kv_addr + sizeof(ObjHeader));
  memcpy(key_addr, key, key_size);

  ObjHeader* header = (ObjHeader*)kv_addr;
  header->key_size = key_size;

  Slot* target_slot = &table[slot_idx];
  assert(*(uint64_t*)target_slot == 0);
  target_slot->atomic.fp = HashIndexComputeFp(key_hash);
  target_slot->atomic.kv_len = 1;
  HashIndexConvert64To48Bits(kv_addr, target_slot->atomic.pointer);

  update_priority(slot_idx);
  num_cached_key++;
}

void evict(int cid) {
  num_space_evict++;
  for (auto it = sample_pool[cid].begin();
       use_local_pool && it != sample_pool[cid].end();) {
    num_pool_evict_read_slot++;
    uint64_t target_slot_idx = it->second;
    if (*(uint64_t*)&table[target_slot_idx] == 0) {
      auto del_it = it;
      it = sample_pool[cid].erase(del_it);
    } else {
      num_pool_evict++;
      clear_slot(target_slot_idx);
      auto del_it = it;
      it = sample_pool[cid].erase(del_it);
    }
  }
  uint64_t target_slot_id = sample(cid);
  clear_slot(target_slot_id);
}

uint64_t new_priority(uint64_t old_priority) {
  if (eviction_type == EVICT_PRIO_LRU)
    return new_ts();
  else if (eviction_type == EVICT_PRIO_LFU)
    return old_priority + 1;
  abort();
}

void set(char* key, uint32_t key_size, DMCHash* hash, int cid) {
  uint64_t key_hash = hash->hash_func1(key, key_size);
  uint32_t bucket_id = key_hash % num_buckets;
  uint64_t bucket_init_idx = bucket_id * num_slots_per_bucket;

  std::vector<int> empty_slots;
  std::vector<std::pair<uint64_t, uint32_t>> priority_slots;
  bool found = false;
  for (int i = 0; i < num_slots_per_bucket; i++) {
    uint32_t slot_idx = bucket_init_idx + i;
    uint64_t kv_addr =
        HashIndexConvert48To64Bits(table[slot_idx].atomic.pointer);
    if (kv_addr == 0) {
      assert(*(uint64_t*)&table[slot_idx] == 0);
      empty_slots.push_back(i);
      continue;
    }

    ObjHeader* header = (ObjHeader*)kv_addr;
    char* target_key = (char*)((uint64_t)kv_addr + sizeof(ObjHeader));
    if (is_key_match(target_key, header->key_size, key, key_size)) {
      // update match return
      update_priority(slot_idx);
      found = true;
    } else {
      uint64_t prio = slot_priority[slot_idx];
      priority_slots.emplace_back(prio, slot_idx);
    }
  }

  if (found) {
    if (use_local_pool) {
      std::sort(priority_slots.begin(), priority_slots.end());
      maintain_pool(priority_slots, cid);
    }
    return;
  }

  if (empty_slots.size() == 0) {
    num_bucket_evict++;
    // evict bucket and insert one key
    std::sort(priority_slots.begin(), priority_slots.end());
    int victim_idx = priority_slots[0].second;
    if (is_random_bucket) {
      victim_idx = priority_slots[random() % priority_slots.size()].second;
    }
    clear_slot(victim_idx);
    insert_slot(key, key_size, key_hash, victim_idx);
    return;
  }

  if (use_local_pool) {
    std::sort(priority_slots.begin(), priority_slots.end());
    maintain_pool(priority_slots, cid);
  }

  while (num_cached_key >= cache_size) {
    evict(cid);
  }

  // insert one key
  uint64_t rand_id = empty_slots[random() % empty_slots.size()];
  uint64_t target_idx = rand_id + num_slots_per_bucket * bucket_id;
  insert_slot(key, key_size, key_hash, target_idx);
  return;
}

bool get(char* key, uint32_t key_size, DMCHash* hash, int cid) {
  assert(strlen(key) == key_size);
  uint64_t key_hash = hash->hash_func1(key, key_size);
  uint32_t bucket_id = key_hash % num_buckets;
  uint32_t bucket_init_idx = bucket_id * num_slots_per_bucket;
  bool found = false;
  std::vector<std::pair<uint64_t, uint32_t>> prio_slotidx;
  for (int i = 0; i < num_slots_per_bucket; i++) {
    uint32_t slot_idx = bucket_init_idx + i;
    uint64_t kv_addr =
        HashIndexConvert48To64Bits(table[slot_idx].atomic.pointer);
    if (kv_addr == 0) {
      assert(*(uint64_t*)&table[slot_idx] == 0);
      continue;
    }
    ObjHeader* header = (ObjHeader*)kv_addr;
    char* target_key = (char*)((uint64_t)kv_addr + sizeof(ObjHeader));
    if (is_key_match(target_key, header->key_size, key, key_size)) {
      update_priority(slot_idx);
      found = true;
    } else {
      uint64_t prio = slot_priority[slot_idx];
      prio_slotidx.emplace_back(prio, slot_idx);
    }
  }

  if (use_local_pool) {
    std::sort(prio_slotidx.begin(), prio_slotidx.end());
    maintain_pool(prio_slotidx, cid);
  }

  return found;
}

void clear_counters() {
  num_space_evict = 0;
  num_bucket_evict = 0;
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
    int cid = i % num_clients;
    char* key_ptr = (char*)((uint64_t)wl.key_buf + i * 128);
    uint32_t key_size = wl.key_size_list[i];
    assert(strlen(key_ptr) == key_size);
    wl.op_list[i] = GET;
    if (wl.op_list[i] == GET) {
      bool is_hit = get(key_ptr, key_size, hash, cid);
      if (is_hit == false)
        set(key_ptr, key_size, hash, cid);
    } else {
      set(key_ptr, key_size, hash, cid);
    }
    if (num_cached_key > cache_size) {
      printf("cached %d size %d\n", num_cached_key, cache_size);
    }
    assert(num_cached_key <= cache_size);
  }

  clear_counters();

  // start simulate
  printf("Simulation start!\n");
  uint32_t num_miss = 0;
  for (int i = num_warmup; i < wl.num_ops; i++) {
    int cid = i % num_clients;
    char* key_ptr = (char*)((uint64_t)wl.key_buf + i * 128);
    uint32_t key_size = wl.key_size_list[i];
    assert(strlen(key_ptr) == key_size);
    wl.op_list[i] = GET;
    if (wl.op_list[i] == GET) {
      bool is_hit = get(key_ptr, key_size, hash, cid);
      if (is_hit == false) {
        num_miss++;
        set(key_ptr, key_size, hash, cid);
      }
    } else {
      set(key_ptr, key_size, hash, cid);
    }
    if (num_cached_key > cache_size) {
      printf("cached %d size %d\n", num_cached_key, cache_size);
    }
    assert(num_cached_key <= cache_size);
  }

  int sum = 0;
  for (int i = 0; i < num_slots_per_bucket * num_buckets; i++) {
    uint64_t kv_addr = HashIndexConvert48To64Bits(table[i].atomic.pointer);
    if (*(uint64_t*)&table[i] == 0) {
      assert(kv_addr == 0);
      continue;
    }
    sum++;
  }
  printf("num_cached_key: %d %d\n", num_cached_key, cache_size);
  printf("num occupied slots: %d\n", sum);
  assert(sum == num_cached_key);

  printf("num_miss: %d\n", num_miss);
  printf("num_space_evict: %ld\n", num_space_evict);
  printf("num_bucket_evict: %ld\n", num_bucket_evict);
  printf("avg_bucket_sampled: %f\n",
         (float)num_sample_read_bucket / num_space_evict);
  printf("num_pool_evict: %ld\n", num_pool_evict);
  printf("avg_pool_evict_read: %f\n",
         (float)num_pool_evict_read_slot / num_pool_evict);
  printf("num_ops:  %d\n", wl.num_ops - num_warmup);
  printf("miss_rate: %f\n", (float)num_miss / (wl.num_ops - num_warmup));
  uint64_t total_evicts = num_bucket_evict + num_space_evict + num_pool_evict;
  printf("bucket_evict_rate: %f\n", (float)num_bucket_evict / (total_evicts));
  printf("space_evict_rate: %f\n", (float)num_space_evict / (total_evicts));
  printf("pool_evict_rate: %f\n", (float)num_pool_evict / (total_evicts));
}