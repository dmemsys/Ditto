#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "client_mm.h"
#include "debug.h"

ClientMM::ClientMM(const DMCConfig* conf) {
  segment_size_ = conf->segment_size;
  remote_segment_list_.clear();
}

ClientMM::~ClientMM() {
  remote_segment_list_.clear();
}

void ClientMM::add_segment(uint64_t r_addr, uint32_t rkey, uint16_t server) {
  for (int i = 0; i < remote_segment_list_.size(); i++) {
    if (remote_segment_list_[i].addr == r_addr) {
      printd(L_ERROR, "Duplicated remote segment 0x%lx", r_addr);
      assert(false);
    }
  }
  RemoteSegment new_segment;
  new_segment.addr = r_addr;
  new_segment.rkey = rkey;
  new_segment.server = server;
  remote_segment_list_.push_back(new_segment);
}

ClientUniformMM::ClientUniformMM(const DMCConfig* conf) : ClientMM(conf) {
  uni_block_size_ = conf->block_size;
  free_block_list_ = std::queue<RemoteBlock>();
  used_block_map_.clear();
}

ClientUniformMM::~ClientUniformMM() {
  return;
}

void ClientUniformMM::add_segment(uint64_t r_addr,
                                  uint32_t rkey,
                                  uint16_t server) {
  ClientMM::add_segment(r_addr, rkey, server);
  uint32_t num_blocks = segment_size_ / uni_block_size_;
  for (int i = 0; i < num_blocks; i++) {
    RemoteBlock new_block;
    new_block.addr = r_addr + i * uni_block_size_;
    new_block.rkey = rkey;
    new_block.size = uni_block_size_;
    new_block.server = server;

    free_block_list_.push(new_block);
  }
}

int ClientUniformMM::alloc(uint32_t size, __OUT RemoteBlock* r_block) {
  if (size > uni_block_size_) {
    printd(L_ERROR, "Unsupported block size %d", size);
    return -1;
  }
  if (free_block_list_.size() == 0) {
    printd(L_DEBUG, "No enough memory");
    return -1;
  }
  RemoteBlock block = free_block_list_.front();
  free_block_list_.pop();
  used_block_map_.push_back(block);
  memcpy(r_block, &block, sizeof(RemoteBlock));
  return 0;
}

int ClientUniformMM::free(const RemoteBlock* r_block) {
  printd(L_DEBUG, "Free rb @%d:0x%lx", r_block->server, r_block->addr);
  RemoteBlock rb = *r_block;
  rb.size = uni_block_size_;
  free_block_list_.push(rb);
  return 0;
}

int ClientUniformMM::free(uint64_t r_addr,
                          uint32_t rkey,
                          uint32_t size,
                          uint16_t server) {
  printd(L_DEBUG, "Free rb @%d:0x%lx", server, r_addr);
  RemoteBlock rb;
  rb.addr = r_addr;
  rb.rkey = rkey;
  rb.size = uni_block_size_;
  rb.server = server;
  free_block_list_.push(rb);
  return 0;
}

uint64_t ClientUniformMM::get_free_size() {
  uint64_t totoal_size = remote_segment_list_.size() * segment_size_;
  uint64_t allocated_size = used_block_map_.size() * uni_block_size_;
  return totoal_size - allocated_size;
}

bool ClientUniformMM::check_integrity() {
  uint64_t sum = used_block_map_.size() + free_block_list_.size();
  uint64_t total =
      remote_segment_list_.size() * segment_size_ / uni_block_size_;
  if (sum != total) {
    printd(L_ERROR, "sum: %ld != total: %ld", sum, total);
    return false;
  }
  return true;
}

bool ClientUniformMM::need_amortize() {
  return free_block_list_.size() < CLIENT_MM_WATERMARK;
}