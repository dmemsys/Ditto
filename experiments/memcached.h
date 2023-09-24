#ifndef _DMC_EXP_MEMCACHED_H_
#define _DMC_EXP_MEMCACHED_H_

#include <libmemcached/memcached.h>

#define MEMCACHED_MAX_NAME_LEN 256

class DMCMemcachedClient {
  char memcached_ip_[64];
  memcached_st* memc_;
  int num_sync_;
  int num_result_;

  void memcached_publish(const char* key, void* val, uint32_t len);

 public:
  DMCMemcachedClient(const char* memcached_ip);
  // for client statistics
  void memcached_put_result(void* result,
                            uint32_t result_size,
                            uint32_t server_id);
  void memcached_put_result_fiber(void* result,
                                  uint32_t result_size,
                                  uint32_t server_id,
                                  uint32_t fb_id);
  void memcached_sync_ready(uint32_t server_id);

  // for server statistics
  void memcached_sync_server_stop();
  void memcached_put_server_result(void* result,
                                   uint32_t result_size,
                                   uint32_t server_id);

  // for special use
  void increase_result_cntr();

  // for general purpose syncs
  void memcached_wait(const char* msg);
  void set_msg(const char* msg);
};

#endif