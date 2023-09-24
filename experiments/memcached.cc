#include <assert.h>
#include <unistd.h>

#include "debug.h"
#include "memcached.h"

DMCMemcachedClient::DMCMemcachedClient(const char* memcached_ip) {
  strcpy(memcached_ip_, memcached_ip);

  // init memc
  memcached_server_st* servers = NULL;
  memcached_return rc;

  memc_ = memcached_create(NULL);

  servers = memcached_server_list_append(servers, memcached_ip_,
                                         MEMCACHED_DEFAULT_PORT, &rc);
  printd(L_DEBUG, "Connectin to server %s", memcached_ip_);
  rc = memcached_server_push(memc_, servers);
  if (rc != MEMCACHED_SUCCESS) {
    printd(L_ERROR, "Couldn't add memcached server %s", memcached_ip_);
    assert(0);
  }

  num_sync_ = 0;
  num_result_ = 0;
}

void DMCMemcachedClient::memcached_publish(const char* key,
                                           void* val,
                                           uint32_t len) {
  assert(key != NULL && val != NULL);
  memcached_return rc;

  rc = memcached_set(memc_, key, strlen(key), (const char*)val, len, (time_t)0,
                     (uint32_t)0);
  if (rc != MEMCACHED_SUCCESS) {
    printd(L_ERROR, "Failed to publish key %s to %s", key, memcached_ip_);
    assert(0);
  }
}

void DMCMemcachedClient::memcached_put_result_fiber(void* result,
                                                    uint32_t result_size,
                                                    uint32_t server_id,
                                                    uint32_t fb_id) {
  assert(result != NULL);
  assert(server_id >= 1);
  char key_buf[MEMCACHED_MAX_NAME_LEN];
  sprintf(key_buf, "client-%d-fb-%d-result-%d", server_id, fb_id, num_result_);
  printf("publish key: %s\n", key_buf);
  memcached_publish(key_buf, result, result_size);
  num_result_++;
}

void DMCMemcachedClient::memcached_put_result(void* result,
                                              uint32_t result_size,
                                              uint32_t server_id) {
  assert(result != NULL);
  assert(server_id >= 1);
  char key_buf[MEMCACHED_MAX_NAME_LEN];
  sprintf(key_buf, "client-%d-result-%d", server_id, num_result_);
  memcached_publish(key_buf, result, result_size);
  num_result_++;
}

void DMCMemcachedClient::increase_result_cntr() {
  num_result_++;
}

void DMCMemcachedClient::memcached_sync_ready(uint32_t server_id) {
  assert(server_id >= 1);

  char key_buf[MEMCACHED_MAX_NAME_LEN];
  sprintf(key_buf, "client-%d-ready-%d", server_id, num_sync_);
  memcached_publish(key_buf, (void*)&num_sync_, sizeof(int));

  // iteratively get the ready signal
  char* val_buf = NULL;
  size_t val_len;
  uint32_t flags;
  memcached_return rc;
  sprintf(key_buf, "all-client-ready-%d", num_sync_);

  do {
    val_buf =
        memcached_get(memc_, key_buf, strlen(key_buf), &val_len, &flags, &rc);
  } while (val_buf == NULL);
  free(val_buf);
  num_sync_++;
}

void DMCMemcachedClient::memcached_wait(const char* msg) {
  char* val_buf = NULL;
  size_t val_len;
  uint32_t flags;
  memcached_return rc;

  do {
    val_buf = memcached_get(memc_, msg, strlen(msg), &val_len, &flags, &rc);
  } while (val_buf == NULL);
  free(val_buf);
}

void DMCMemcachedClient::set_msg(const char* msg) {
  int dumb_val = 1;
  memcached_publish(msg, (void*)&dumb_val, sizeof(int));
}

void DMCMemcachedClient::memcached_sync_server_stop() {
  // iteratively get the stop signal
  char* val_buf = NULL;
  char key_buf[MEMCACHED_MAX_NAME_LEN];
  size_t val_len;
  uint32_t flags;
  memcached_return rc;
  sprintf(key_buf, "server-stop");

  do {
    val_buf =
        memcached_get(memc_, key_buf, strlen(key_buf), &val_len, &flags, &rc);
    sleep(1);
  } while (val_buf == NULL);
  free(val_buf);
  num_sync_++;
}

void DMCMemcachedClient::memcached_put_server_result(void* result,
                                                     uint32_t result_size,
                                                     uint32_t server_id) {
  assert(result != NULL);
  assert(server_id == 0);
  char key_buf[MEMCACHED_MAX_NAME_LEN];
  sprintf(key_buf, "server-result");
  memcached_publish(key_buf, result, result_size);
}