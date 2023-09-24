#include "init.h"
#include "client.h"
#include "dmc_utils.h"
#include "memcached.h"
#include "run_client.h"
#include "server.h"
#include "third_party/json.hpp"

#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <unistd.h>

using json = nlohmann::json;

static struct option opts[] = {
    {"config-file", 1, NULL, 'c'},
    {"num-client-threads", 1, NULL, 't'},
    {"all-clients", 1, NULL, 'A'},
    {"server", 0, NULL, 'S'},
    {"client", 0, NULL, 'C'},
    {"workload", 1, NULL, 'w'},
    {"num-load-ops", 1, NULL, 'n'},
    {"run-time", 1, NULL, 'T'},
    {"id", 1, NULL, 'i'},
    {"validate", 0, NULL, 'v'},
    {"memcached-ip", 1, NULL, 'm'},
    {"lru-clients", 1, NULL, 'r'},
    {"lfu-clients", 1, NULL, 'f'},
    {"use-warmup", 1, NULL, 'W'},
    {"elastic-compute", 1, NULL, 'E'},
};

InitArgs g_init_args;

int init_parse_args(int argc, char** argv) {
  memset(&g_init_args, 0, sizeof(_InitArgs));
  char c;
  bool is_server = false;
  bool is_client = false;
  while (1) {
    c = getopt_long(argc, argv, "c:t:SCi:n:T:w:vm:A:r:f:WE:", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'c':
        strcpy(g_init_args.config_fname, optarg);
        break;
      case 't':
        g_init_args.num_client_threads = atoi(optarg);
        break;
      case 'S':
        is_server = true;
        g_init_args.role = SERVER;
        break;
      case 'C':
        is_client = true;
        g_init_args.role = CLIENT;
        break;
      case 'i':
        g_init_args.server_id = atoi(optarg);
        break;
      case 'n':
        g_init_args.num_load_ops = atoi(optarg);
        break;
      case 'T':
        g_init_args.run_time = atoi(optarg);
        break;
      case 'w':
        strcpy(g_init_args.workload_name, optarg);
        printd(L_INFO, "workload name: %s", g_init_args.workload_name);
        break;
      case 'v':
        g_init_args.validate = true;
        break;
      case 'm':
        strcpy(g_init_args.memcached_ip, optarg);
        break;
      case 'A':
        g_init_args.all_client_num = atoi(optarg);
        break;
      case 'r':
        g_init_args.n_lru_client = atoi(optarg);
        break;
      case 'f':
        g_init_args.n_lfu_client = atoi(optarg);
        break;
      case 'W':
        g_init_args.use_warmup = true;
        break;
      case 'E':
        assert(g_init_args.elastic == ELA_NON);
        if (strcmp(optarg, "cpu") == 0)
          g_init_args.elastic = ELA_CPU;
        else if (strcmp(optarg, "mem") == 0)
          g_init_args.elastic = ELA_MEM;
        break;
      default:
        printf("Invalid argument %c\n", c);
        return -1;
    }
  }
  if (is_server == is_client) {
    printf("Error: is_server == is_client\n");
    return -1;
  }
  return 0;
}

void run_server(const InitArgs* args);

int main(int argc, char** argv) {
  printf("bucket_assoc_num: %lld, bucket_num: %lld\n", HASH_BUCKET_ASSOC_NUM,
         HASH_NUM_BUCKETS);
  int ret = 0;
  ret = init_parse_args(argc, argv);
  if (ret) {
    printf("Failed to parse args\n");
    return ret;
  }
  if (g_init_args.role == SERVER) {
    run_server(&g_init_args);
  } else {
    assert(g_init_args.role == CLIENT);
    run_client(&g_init_args);
  }

  return 0;
}

void run_server(const InitArgs* args) {
  int ret = 0;
  DMCConfig server_conf;
  ret = load_config(args->config_fname, &server_conf);
  DMCMemcachedClient con_client(args->memcached_ip);

  if (args->elastic == ELA_MEM)
    server_conf.elastic_mem = true;
  else
    server_conf.elastic_mem = false;

  Server server(&server_conf);
  ServerMainArgs server_thread_args;
  server_thread_args.core_id = server_conf.core_id;
  server_thread_args.server = &server;
  pthread_t server_tid;
  ret = pthread_create(&server_tid, NULL, server_main, &server_thread_args);
  assert(ret == 0);

#ifdef ELA_MEM_TPT
  for (int i = 0; i < 1 && server_conf.elastic_mem; i++) {  // for ycsb
#else
  for (int i = 0; i < 4 && server_conf.elastic_mem; i++) {  // for hit-rate
#endif
    char sync_msg[256];
    sprintf(sync_msg, "server-scale-memory-%d", i);
    con_client.memcached_wait(sync_msg);
    server.scale_memory(i + 1);
    sprintf(sync_msg, "server-scale-memory-ok-%d", i);
    con_client.set_msg(sync_msg);
  }

  con_client.memcached_sync_server_stop();
  std::vector<float> expert_weights;
  server.get_adaptive_weights(expert_weights);
  json res;
  res["num_evict_dequeue"] = server.num_evict_dequeue_;
  res["num_evict"] = server.num_evict_;
  // std::vector<std::vector<float> > weight_vec;
  // server.get_adaptive_weight_vec(weight_vec);
  // json jweight_vec(weight_vec);
  // res["adaptive_weights"] = jweight_vec;
  std::string res_str = res.dump();
  con_client.memcached_put_server_result((void*)res_str.c_str(),
                                         strlen(res_str.c_str()), 0);
  server.stop();
  pthread_join(server_tid, NULL);
}
