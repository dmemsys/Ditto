#ifndef _DMC_CLIENT_MM_H_
#define _DMC_CLIENT_MM_H_

#include <dmc_utils.h>
#include <queue>
#include <set>
#include <vector>

#define CLIENT_MM_WATERMARK 16

typedef struct _RemoteSegment {
  uint64_t addr;
  uint32_t rkey;
  uint16_t server;
} RemoteSegment;

typedef struct _RemoteBlock {
  uint64_t addr;
  uint64_t rkey;
  uint32_t size;
  uint16_t server;
} RemoteBlock;

class ClientMM {
 protected:
  uint64_t segment_size_;
  std::vector<RemoteSegment> remote_segment_list_;

 public:
  ClientMM(const DMCConfig* conf);
  ~ClientMM();

  virtual void add_segment(uint64_t r_addr, uint32_t rkey, uint16_t server);

  virtual int alloc(uint32_t size, __OUT RemoteBlock* r_block) { return 0; };
  virtual int free(const RemoteBlock* r_block) { return 0; };
  virtual int free(uint64_t r_addr,
                   uint32_t rkey,
                   uint32_t size,
                   uint16_t server) {
    return 0;
  }
  virtual uint64_t get_free_size() { return 0; };
  virtual bool check_integrity() { return false; };
  virtual bool need_amortize() = 0;
};

class ClientUniformMM : public ClientMM {
  uint32_t uni_block_size_;  // Uniform size of each allocation
  std::queue<RemoteBlock> free_block_list_;
  std::vector<RemoteBlock> used_block_map_;

 public:
  ClientUniformMM(const DMCConfig* conf);
  ~ClientUniformMM();

  void add_segment(uint64_t r_addr, uint32_t rkey, uint16_t server);
  int alloc(uint32_t size, __OUT RemoteBlock* r_block);
  int free(const RemoteBlock* r_block);
  int free(uint64_t r_addr, uint32_t rkey, uint32_t size, uint16_t server);
  uint64_t get_free_size();
  bool check_integrity();
  bool need_amortize();
};

#endif