#include "workload.h"
#include "client.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>

static int load_ycsb_workload(char* workload_name,
                              int num_load_ops,
                              uint32_t server_id,
                              uint32_t all_client_num,
                              __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/ycsb/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == server_id)
      wl_list.emplace_back(buf);
    cnt++;
  }

  if (num_load_ops == -1)
    wl->num_ops = wl_list.size();
  else
    wl->num_ops = num_load_ops;
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  char ops_buf[64];
  char key_buf[64];
  for (int i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%s %s", ops_buf, key_buf);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), key_buf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(int));
    wl->key_size_list[i] = strlen(key_buf);
    wl->val_size_list[i] = sizeof(int);
    if (strcmp("READ", ops_buf) == 0) {
      wl->op_list[i] = GET;
    } else {
      wl->op_list[i] = SET;
    }
  }
  return 0;
}

static int load_wiki_workload(char* workload_name,
                              int num_load_ops,
                              uint32_t server_id,
                              uint32_t all_client_num,
                              __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/wiki/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  int ts;
  uint32_t item_size;
  char keybuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == server_id) {
      wl_list.emplace_back(buf);
    }
    cnt++;
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  for (int i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%d %s %d %d", &ts, keybuf, &item_size,
           &feature);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(int));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(int);
    wl->op_list[i] = GET;
  }
  return 0;
}

static int load_cphy_workload(char* workload_name,
                              int num_load_ops,
                              uint32_t server_id,
                              uint32_t all_client_num,
                              __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/cphy/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  uint64_t ts;
  uint32_t obj_size;
  char keybuf[128];
  char opbuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == server_id) {
      wl_list.emplace_back(buf);
    }
    cnt++;
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  uint64_t next_acc_time;
  for (uint64_t i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%ld %s %d %ld", &ts, keybuf, &obj_size,
           &next_acc_time);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(uint64_t));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(uint64_t);  // we write 256 byt anyway
    wl->op_list[i] = GET;
  }
  printd(L_INFO, "load done");
  return 0;
}

static int load_ibm_workload(char* workload_name,
                             int num_load_ops,
                             uint32_t server_id,
                             uint32_t all_client_num,
                             __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/ibm/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  uint64_t ts;
  uint32_t obj_size;
  char keybuf[128];
  char opbuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == server_id) {
      wl_list.emplace_back(buf);
    }
    cnt++;
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  for (uint64_t i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%lld %s %s %d", &ts, opbuf, keybuf, &obj_size);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(uint64_t));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(uint64_t);  // we write 256 byt anyway
    if (strcmp(opbuf, "REST.PUT.OBJECT") == 0) {
      wl->op_list[i] = SET;
    } else {
      wl->op_list[i] = GET;
    }
  }
  printd(L_INFO, "load done");
  return 0;
}

static int load_fiu_workload(char* workload_name,
                             int num_load_ops,
                             uint32_t server_id,
                             uint32_t all_client_num,
                             __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/fiu/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  int ts;
  uint32_t item_size;
  char keybuf[128];
  char opbuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == server_id) {
      wl_list.emplace_back(buf);
    }
    cnt++;
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  for (uint64_t i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%s %s", keybuf, opbuf);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(uint64_t));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(uint64_t);
    wl->op_list[i] = GET;
  }
  printd(L_INFO, "load done");
  return 0;
}

static int load_mix_workload(char* workload_name,
                             int num_load_ops,
                             uint32_t server_id,
                             uint32_t all_client_num,
                             uint32_t n_lru_client,
                             uint32_t n_lfu_client,
                             __OUT DMCWorkload* wl) {
  assert(all_client_num == 10);
  assert(n_lfu_client + n_lru_client == all_client_num);

  char wl_fname[128];
  uint32_t tmp_id = 0;
  if (server_id <= n_lru_client) {
    strcpy(wl_fname, "../workloads/mix/lru-acc-trace-n.trc");
    tmp_id = server_id;
  } else {
    strcpy(wl_fname, "../workloads/mix/lfu-acc-trace-n.trc");
    tmp_id = server_id - n_lru_client;
  }

  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d->%d loading %s", server_id, tmp_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  int ts;
  uint32_t item_size;
  char keybuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == tmp_id) {
      wl_list.emplace_back(buf);
    }
    cnt++;
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  for (uint64_t i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%s", keybuf);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(uint64_t));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(uint64_t);
    wl->op_list[i] = GET;
  }
  return 0;
}

static int load_changing_workload(char* workload_name,
                                  int num_load_ops,
                                  uint32_t server_id,
                                  uint32_t all_client_num,
                                  __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/changing/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "Client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  int ts;
  uint32_t item_size;
  char keybuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    if ((cnt % all_client_num) + 1 == server_id) {
      wl_list.emplace_back(buf);
    }
    cnt++;
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  for (uint64_t i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%s", keybuf);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(uint64_t));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(uint64_t);
    wl->op_list[i] = GET;
  }
  return 0;
}

static int load_twitter_workload(char* workload_name,
                                 int num_load_ops,
                                 uint32_t server_id,
                                 uint32_t all_client_num,
                                 __OUT DMCWorkload* wl) {
  char wl_fname[128];
  sprintf(wl_fname, "../workloads/twitter/%s", workload_name);
  FILE* f = fopen(wl_fname, "r");
  assert(f != NULL);
  printd(L_INFO, "client %d loading %s", server_id, workload_name);

  std::vector<std::string> wl_list;
  char buf[2048];
  int ts;
  uint32_t key_size;
  uint32_t val_size;
  uint32_t cid;
  char keybuf[128];
  char opbuf[64];
  int ttl;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    sscanf(buf, "%d %s %d %d %d %s %d", &ts, keybuf, &key_size, &val_size, &cid,
           opbuf, &ttl);
    if ((cid % all_client_num) + 1 != server_id) {
      continue;
    }
    wl_list.emplace_back(buf);
  }

  if (num_load_ops == -1) {
    wl->num_ops = wl_list.size();
  } else {
    wl->num_ops = num_load_ops;
  }
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client %d loading %ld ops\n", server_id, wl_list.size());
  for (int i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%d %s %d %d %d %s %d", &ts, keybuf, &key_size,
           &val_size, &cid, opbuf, &ttl);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(int));
    wl->key_size_list[i] = strlen(keybuf) > 128 ? 128 : strlen(keybuf);
    wl->val_size_list[i] = sizeof(int);
    if (strcmp(opbuf, "get") == 0 || strcmp(opbuf, "gets") == 0) {
      wl->op_list[i] = GET;
    } else {
      wl->op_list[i] = SET;
    }
  }
  return 0;
}

int load_workload(char* workload_name,
                  int num_load_ops,
                  uint32_t server_id,
                  uint32_t all_client_num,
                  __OUT DMCWorkload* wl) {
  if (memcmp(workload_name, "twitter", strlen("twitter")) == 0) {
    load_twitter_workload(workload_name, num_load_ops, server_id,
                          all_client_num, wl);
  } else if (memcmp(workload_name, "wiki", strlen("wiki")) == 0) {
    load_wiki_workload(workload_name, num_load_ops, server_id, all_client_num,
                       wl);
  } else if (memcmp(workload_name, "changing", strlen("changing")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s.trc", workload_name);
    load_changing_workload(fname_buf, num_load_ops, server_id, all_client_num,
                           wl);
  } else if (memcmp(workload_name, "webmail", strlen("webmail")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s.fiu", workload_name);
    load_fiu_workload(fname_buf, num_load_ops, server_id, all_client_num, wl);
  } else if (memcmp(workload_name, "ibm", strlen("ibm")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s", workload_name);
    load_ibm_workload(fname_buf, num_load_ops, server_id, all_client_num, wl);
  } else if (memcmp(workload_name, "cphy", strlen("cphy")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s", workload_name);
    load_cphy_workload(fname_buf, num_load_ops, server_id, all_client_num, wl);
  }
  return 0;
}

int load_workload_ycsb(char* workload_name,
                       int num_load_ops,
                       uint32_t server_id,
                       uint32_t all_client_num,
                       uint8_t evict_type,
                       __OUT DMCWorkload* load_wl,
                       __OUT DMCWorkload* trans_wl) {
  char fname_buf[256];
  sprintf(fname_buf, "%s.load", workload_name);
  // server_id starts with 1
  if (evict_type == EVICT_PRECISE || all_client_num < YCSB_LOAD_NUM) {
    if (server_id <= YCSB_LOAD_NUM) {
      load_ycsb_workload(fname_buf, num_load_ops, server_id, YCSB_LOAD_NUM,
                         load_wl);
    } else {
      load_wl->num_ops = 0;
    }
  } else {
    load_ycsb_workload(fname_buf, num_load_ops, server_id, all_client_num,
                       load_wl);
  }

  sprintf(fname_buf, "%s.trans", workload_name);
  load_ycsb_workload(fname_buf, num_load_ops, server_id, all_client_num,
                     trans_wl);
  return 0;
}

int load_workload_hit_rate(char* workload_name,
                           int num_load_ops,
                           uint32_t server_id,
                           uint32_t all_client_num,
                           uint32_t n_lru_client,
                           uint32_t n_lfu_client,
                           __OUT DMCWorkload* wl) {
  char real_workload_name[256];
  sscanf(workload_name, "hit-rate-%s", real_workload_name);
  printf("%s\n", real_workload_name);
  if (memcmp(real_workload_name, "twitter", strlen("twitter")) == 0) {
    load_twitter_workload(real_workload_name, num_load_ops, server_id,
                          all_client_num, wl);
  } else if (memcmp(real_workload_name, "wiki", strlen("wiki")) == 0) {
    load_wiki_workload(real_workload_name, num_load_ops, server_id,
                       all_client_num, wl);
  } else if (memcmp(real_workload_name, "mix", strlen("mix")) == 0) {
    load_mix_workload(real_workload_name, num_load_ops, server_id,
                      all_client_num, n_lru_client, n_lfu_client, wl);
  } else if (memcmp(real_workload_name, "webmail", strlen("webmail")) == 0) {
    char fname_buf[512];
    sprintf(fname_buf, "%s.fiu", real_workload_name);
    load_fiu_workload(fname_buf, num_load_ops, server_id, all_client_num, wl);
  } else if (memcmp(real_workload_name, "changing", strlen("changing")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s.trc", real_workload_name);
    load_changing_workload(fname_buf, num_load_ops, server_id, all_client_num,
                           wl);
  } else if (memcmp(real_workload_name, "ibm", strlen("ibm")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s", real_workload_name);
    load_ibm_workload(fname_buf, num_load_ops, server_id, all_client_num, wl);
  } else if (memcmp(real_workload_name, "cphy", strlen("cphy")) == 0) {
    char fname_buf[256];
    sprintf(fname_buf, "%s", real_workload_name);
    load_cphy_workload(fname_buf, num_load_ops, server_id, all_client_num, wl);
  }
  return 0;
}

static int load_mix_individual_all(char* fname, __OUT DMCWorkload* wl) {
  FILE* f = fopen(fname, "r");
  assert(f != NULL);
  printf("Client loading %s\n", fname);

  std::vector<std::string> wl_list;
  char buf[2048];
  int ts;
  uint32_t item_size;
  char keybuf[128];
  int feature;
  uint32_t cnt = 0;
  while (fgets(buf, 2048, f) == buf) {
    if (buf[0] == '\n')
      continue;
    wl_list.emplace_back(buf);
    cnt++;
  }

  wl->num_ops = wl_list.size();
  wl->key_buf = malloc(128 * wl->num_ops);
  wl->val_buf = malloc(120 * wl->num_ops);
  wl->key_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->val_size_list = (uint32_t*)malloc(sizeof(uint32_t) * wl->num_ops);
  wl->op_list = (uint8_t*)malloc(sizeof(uint8_t) * wl->num_ops);

  printf("Client loading %ld ops\n", wl->num_ops);
  for (uint64_t i = 0; i < wl->num_ops; i++) {
    sscanf(wl_list[i].c_str(), "%s", keybuf);
    memcpy((void*)((uint64_t)wl->key_buf + i * 128), keybuf, 128);
    memcpy((void*)((uint64_t)wl->val_buf + i * 120), &i, sizeof(uint64_t));
    wl->key_size_list[i] = strlen(keybuf);
    wl->val_size_list[i] = sizeof(uint64_t);
    wl->op_list[i] = GET;
  }
  return 0;
}

int load_mix_all(__OUT DMCWorkload* lru_wl, __OUT DMCWorkload* lfu_wl) {
  char wl_fname[128];
  strcpy(wl_fname, "../workloads/mix/lru-acc-trace-n.trc");
  // strcpy(wl_fname, "../workloads/mix/lru-acc-trace-nnn.trc");
  load_mix_individual_all(wl_fname, lru_wl);
  strcpy(wl_fname, "../workloads/mix/lfu-acc-trace-n.trc");
  // strcpy(wl_fname, "../workloads/mix/lfu-acc-trace-nnn.trc");
  load_mix_individual_all(wl_fname, lfu_wl);
  return 0;
}

void free_workload(DMCWorkload* wl) {
  free(wl->key_buf);
  free(wl->val_buf);
  free(wl->key_size_list);
  free(wl->val_size_list);
  free(wl->op_list);
}