#include <fstream>
#include <sstream>
#include <string>

#include <arpa/inet.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "debug.h"
#include "dmc_utils.h"

inline static uint64_t htonll(uint64_t val) {
  return (((uint64_t)htonl(val)) << 32) + htonl(val >> 32);
}

inline static uint64_t ntohll(uint64_t val) {
  return (((uint64_t)ntohl(val)) << 32) + ntohl(val >> 32);
}

static int parse_priority(const std::string& priority_str) {
  if (priority_str == std::string("EVICT_PRIO_NON")) {
    return EVICT_PRIO_NON;
  } else if (priority_str == std::string("EVICT_PRIO_LRU")) {
    return EVICT_PRIO_LRU;
  } else if (priority_str == std::string("EVICT_PRIO_LFU")) {
    return EVICT_PRIO_LFU;
  } else if (priority_str == std::string("EVICT_PRIO_GDSF")) {
    return EVICT_PRIO_GDSF;
  } else if (priority_str == std::string("EVICT_PRIO_GDS")) {
    return EVICT_PRIO_GDS;
  } else if (priority_str == std::string("EVICT_PRIO_LIRS")) {
    return EVICT_PRIO_LIRS;
  } else if (priority_str == std::string("EVICT_PRIO_LRFU")) {
    return EVICT_PRIO_LRFU;
  } else if (priority_str == std::string("EVICT_PRIO_FIFO")) {
    return EVICT_PRIO_FIFO;
  } else if (priority_str == std::string("EVICT_PRIO_LFUDA")) {
    return EVICT_PRIO_LFUDA;
  } else if (priority_str == std::string("EVICT_PRIO_LRUK")) {
    return EVICT_PRIO_LRUK;
  } else if (priority_str == std::string("EVICT_PRIO_SIZE")) {
    return EVICT_PRIO_SIZE;
  } else if (priority_str == std::string("EVICT_PRIO_MRU")) {
    return EVICT_PRIO_MRU;
  } else if (priority_str == std::string("EVICT_PRIO_HYPERBOLIC")) {
    return EVICT_PRIO_HYPERBOLIC;
  }
  return -1;
}

int load_config(const char* fname, __OUT DMCConfig* config) {
  std::fstream config_fs(fname);
  assert(config_fs.is_open());

  boost::property_tree::ptree pt;
  try {
    boost::property_tree::read_json(config_fs, pt);
  } catch (boost::property_tree::ptree_error& e) {
    perror("read_json failed\n");
    return -1;
  }

  try {
    std::string role_str = pt.get<std::string>("role");
    if (role_str == std::string("SERVER")) {
      config->role = SERVER;
    } else {
      assert(role_str == std::string("CLIENT"));
      config->role = CLIENT;
    }

    std::string conn_type_str = pt.get<std::string>("conn_type");
    if (conn_type_str == std::string("IB")) {
      config->conn_type = IB;
    } else {
      config->conn_type = ROCE;
    }

    config->server_id = pt.get<uint32_t>("server_id");
    config->udp_port = pt.get<uint16_t>("udp_port");
    config->memory_num = pt.get<uint16_t>("memory_num");

    int i = 0;
    BOOST_FOREACH (boost::property_tree::ptree::value_type& v,
                   pt.get_child("memory_ip_list")) {
      std::string ip = v.second.get<std::string>("");
      assert(ip.length() > 0 && ip.length() < 16);
      strcpy(config->memory_ip_list[i], ip.c_str());
      i++;
    }
    assert(i == config->memory_num);

    std::string hash_type_str = pt.get<std::string>("hash_type");
    if (hash_type_str == std::string("DEFAULT")) {
      config->hash_type = HASH_DEFAULT;
    } else if (hash_type_str == std::string("DUMB")) {
      config->hash_type = HASH_DUMB;
    } else {
      printd(L_ERROR, "Unsupported hash_type %s", hash_type_str.c_str());
      assert(0);
    }

    std::string eviction_type_str = pt.get<std::string>("eviction_type");
    if (eviction_type_str == std::string("EVICT_NON")) {
      config->eviction_type = EVICT_NON;
    } else if (eviction_type_str == std::string("EVICT_SAMPLE")) {
      config->eviction_type = EVICT_SAMPLE;
    } else if (eviction_type_str == std::string("EVICT_PRECISE")) {
      config->eviction_type = EVICT_PRECISE;
    } else if (eviction_type_str == std::string("EVICT_CLIQUEMAP")) {
      config->eviction_type = EVICT_CLIQUEMAP;
    } else if (eviction_type_str == std::string("EVICT_SAMPLE_NAIVE")) {
      config->eviction_type = EVICT_SAMPLE_NAIVE;
    } else if (eviction_type_str == std::string("EVICT_SAMPLE_ADAPTIVE")) {
      config->eviction_type = EVICT_SAMPLE_ADAPTIVE;
    } else if (eviction_type_str ==
               std::string("EVICT_SAMPLE_ADAPTIVE_NAIVE")) {
      config->eviction_type = EVICT_SAMPLE_ADAPTIVE_NAIVE;
    } else if (eviction_type_str ==
               std::string("EVICT_SAMPLE_ADAPTIVE_HEAVY")) {
      config->eviction_type = EVICT_SAMPLE_ADAPTIVE_HEAVY;
    } else {
      printd(L_ERROR, "Unsupportd eviction_type %s", eviction_type_str.c_str());
      abort();
    }

    if (!is_evict_adaptive(config->eviction_type)) {
      std::string eviction_prio_str = pt.get<std::string>("eviction_priority");
      int priority = parse_priority(eviction_prio_str);
      if (priority == -1) {
        printd(L_ERROR, "Unsupported priority %s", eviction_prio_str.c_str());
        abort();
      }
      config->eviction_priority = priority;
    } else {
      i = 0;
      BOOST_FOREACH (boost::property_tree::ptree::value_type& v,
                     pt.get_child("experts")) {
        assert(i < MAX_NUM_EXPERTS);
        std::string expert = v.second.get<std::string>("");
        assert(expert.length() > 0);
        int priority = parse_priority(expert);
        if (priority == -1) {
          printd(L_ERROR, "Unsupported priority %s", expert.c_str());
          abort();
        }
        config->experts[i] = priority;
        i++;
      }
      config->num_experts = i;
      config->learning_rate = pt.get<float>("learning_rate");
      config->history_size = pt.get<uint32_t>("history_size");
      config->use_async_weight = pt.get<bool>("use_async_weight");
    }

    if (config->eviction_type == EVICT_CLIQUEMAP) {
      config->is_server_precise = pt.get<bool>("is_server_precise");
    }

    config->num_samples = pt.get<uint32_t>("num_samples");
    config->use_freq_cache = pt.get<bool>("use_freq_cache");
    config->freq_cache_size = pt.get<uint32_t>("freq_cache_size");

    config->ib_dev_id = pt.get<uint32_t>("ib_dev_id");
    config->ib_port_id = pt.get<uint32_t>("ib_port_id");
    config->ib_gid_idx = pt.get<uint32_t>("ib_gid_idx", -1);

    std::string server_base_addr_str = pt.get<std::string>("server_base_addr");
    sscanf(server_base_addr_str.c_str(), "0x%lx", &config->server_base_addr);

    config->server_data_len = pt.get<uint64_t>("server_data_len");
    config->segment_size = pt.get<uint64_t>("segment_size");
    config->block_size = pt.get<uint64_t>("block_size");

    config->client_local_size = pt.get<uint32_t>("client_local_size");

    config->core_id = pt.get<uint32_t>("core_id");
    config->num_cores = pt.get<uint32_t>("num_cores");

    config->testing = pt.get<bool>("testing", false);
    config->num_server_threads = pt.get<uint32_t>("num_server_threads", 1);
  } catch (boost::property_tree::ptree_error& e) {
    perror("parse failed\n");
    return -1;
  }
  return 0;
}

void serialize_udpmsg(__OUT UDPMsg* msg) {
  switch (msg->type) {
    case UDPMSG_REQ_CONNECT:
    case UDPMSG_REP_CONNECT:
      serialize_conn_info(&msg->body.conn_info);
      break;
    case UDPMSG_REQ_ALLOC:
    case UDPMSG_REP_ALLOC:
      serialize_mr_info(&msg->body.mr_info);
      break;
    case UDPMSG_REQ_TS:
    case UDPMSG_REP_TS:
      msg->body.sys_start_ts = htonll(msg->body.sys_start_ts);
      break;
    default:
      break;
  }
  msg->type = htons(msg->type);
  msg->id = htons(msg->id);
}

void deserialize_udpmsg(__OUT UDPMsg* msg) {
  msg->type = ntohs(msg->type);
  msg->id = ntohs(msg->id);
  switch (msg->type) {
    case UDPMSG_REQ_CONNECT:
    case UDPMSG_REP_CONNECT:
      deserialize_conn_info(&msg->body.conn_info);
      break;
    case UDPMSG_REQ_ALLOC:
    case UDPMSG_REP_ALLOC:
      serialize_mr_info(&msg->body.mr_info);
      break;
    case UDPMSG_REQ_TS:
    case UDPMSG_REP_TS:
      msg->body.sys_start_ts = ntohll(msg->body.sys_start_ts);
      break;
    default:
      break;
  }
}

void serialize_qp_info(__OUT QPInfo* qp_info) {
  qp_info->qp_num = htonl(qp_info->qp_num);
  qp_info->lid = htons(qp_info->lid);
}

void deserialize_qp_info(__OUT QPInfo* qp_info) {
  qp_info->qp_num = ntohl(qp_info->qp_num);
  qp_info->lid = ntohs(qp_info->lid);
}

void serialize_mr_info(__OUT MrInfo* mr_info) {
  mr_info->addr = htonll(mr_info->addr);
  mr_info->rkey = htonl(mr_info->rkey);
}

void deserialize_mr_info(__OUT MrInfo* mr_info) {
  mr_info->addr = ntohll(mr_info->addr);
  mr_info->rkey = ntohl(mr_info->rkey);
}

void serialize_conn_info(__OUT ConnInfo* conn_info) {
  serialize_qp_info(&conn_info->qp_info);
  serialize_mr_info(&conn_info->mr_info);
}

void deserialize_conn_info(__OUT ConnInfo* conn_info) {
  deserialize_qp_info(&conn_info->qp_info);
  deserialize_mr_info(&conn_info->mr_info);
}

int stick_this_thread_to_core(int core_id) {
  int num_cores = sysconf(_SC_NPROCESSORS_CONF);
  if (core_id < 0 || core_id >= num_cores) {
    return -1;
  }

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t current_thread = pthread_self();
  return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

static uint64_t sys_start_ts = 0;

void set_sys_start_ts(uint64_t ts) {
  sys_start_ts = ts;
}

uint32_t new_ts32() {
  uint64_t cur_ts = new_ts();
  return (uint32_t)(cur_ts - sys_start_ts);
}