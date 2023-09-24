#ifndef _DMC_IB_H_
#define _DMC_IB_H_

#include <infiniband/verbs.h>
#include <stdlib.h>

#include "dmc_utils.h"

struct ibv_context* ib_get_ctx(uint32_t dev_id, uint32_t port_id);
struct ibv_qp* ib_create_rc_qp(struct ibv_pd* ib_pd,
                               struct ibv_qp_init_attr* qp_init_attr);
int ib_connect_qp(struct ibv_qp* local_qp,
                  const QPInfo* local_qp_info,
                  const QPInfo* remote_qp_info,
                  uint8_t conn_type);

void ib_print_gid(const uint8_t* gid);

void ib_print_wr(struct ibv_send_wr* wr_list);

inline void ib_create_sge(uint64_t local_addr,
                          uint32_t lkey,
                          uint32_t len,
                          __OUT struct ibv_sge* sge) {
  memset(sge, 0, sizeof(struct ibv_sge));
  sge->addr = local_addr;
  sge->lkey = lkey;
  sge->length = len;
}

enum IB_RES_TYPE {
  IB_SR,
  IB_RR,
  IB_SGE,
};

static inline void* ib_zalloc(uint8_t ib_res_type, uint32_t num) {
  void* ret_ptr = NULL;
  switch (ib_res_type) {
    case IB_SR:
      ret_ptr = malloc(sizeof(struct ibv_send_wr) * num);
      memset(ret_ptr, 0, sizeof(struct ibv_send_wr) * num);
      break;
    case IB_RR:
      ret_ptr = malloc(sizeof(struct ibv_recv_wr) * num);
      memset(ret_ptr, 0, sizeof(struct ibv_recv_wr));
    case IB_SGE:
      ret_ptr = malloc(sizeof(struct ibv_sge) * num);
      memset(ret_ptr, 0, sizeof(struct ibv_sge) * num);
      break;
    default:
      ret_ptr = NULL;
  }
  return ret_ptr;
}

#endif