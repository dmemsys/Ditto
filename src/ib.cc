#include <assert.h>
#include <stdio.h>

#include "debug.h"
#include "dmc_utils.h"
#include "ib.h"

static void dump_qp_info(const QPInfo* info, const char* msg) {
  printd(L_DEBUG, "%s qp_num: %d", msg, info->qp_num);
  printd(L_DEBUG, "%s lid: %x", msg, info->lid);
  printd(L_DEBUG, "%s gid: %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d",
         msg, info->gid[0], info->gid[1], info->gid[2], info->gid[3],
         info->gid[4], info->gid[5], info->gid[6], info->gid[7], info->gid[8],
         info->gid[9], info->gid[10], info->gid[11], info->gid[12],
         info->gid[13], info->gid[14], info->gid[15]);
  printd(L_DEBUG, "%s gid_idx: %d", msg, info->gid_idx);
}

static int modify_qp_to_rts(struct ibv_qp* local_qp) {
  struct ibv_qp_attr attr;
  int attr_mask;
  int rc;
  memset(&attr, 0, sizeof(struct ibv_qp_attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0x12;
  attr.retry_cnt = 6;
  attr.rnr_retry = 0;
  attr.sq_psn = 0;
  attr.max_rd_atomic = 16;
  attr_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
              IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp(local_qp, &attr, attr_mask);
  assert(rc == 0);
  return 0;
}

static int modify_qp_to_init(struct ibv_qp* qp, const QPInfo* local_qp_info) {
  struct ibv_qp_attr attr;
  int attr_mask;
  int rc;
  memset(&attr, 0, sizeof(struct ibv_qp_attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = local_qp_info->port_num;
  attr.pkey_index = 0;
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  attr_mask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp(qp, &attr, attr_mask);
  assert(rc == 0);
  return 0;
}

static int modify_qp_to_rtr(struct ibv_qp* local_qp,
                            const QPInfo* local_qp_info,
                            const QPInfo* remote_qp_info,
                            uint8_t conn_type) {
  dump_qp_info(local_qp_info, "local");
  dump_qp_info(remote_qp_info, "remote");
  struct ibv_qp_attr attr;
  int attr_mask;
  int rc;
  memset(&attr, 0, sizeof(struct ibv_qp_attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = remote_qp_info->qp_num;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 16;
  attr.min_rnr_timer = 0x12;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = remote_qp_info->lid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = local_qp_info->port_num;
  if (conn_type == ROCE) {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = local_qp_info->port_num;
    memcpy(&attr.ah_attr.grh.dgid, remote_qp_info->gid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.sgid_index = local_qp_info->gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }
  attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
              IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp(local_qp, &attr, attr_mask);
  assert(rc == 0);
  return 0;
}

struct ibv_context* ib_get_ctx(uint32_t dev_id, uint32_t port_id) {
  struct ibv_device** ib_dev_list;
  struct ibv_device* ib_dev;
  int num_devices;

  ib_dev_list = ibv_get_device_list(&num_devices);
  for (int i = 0; i < num_devices; i++) {
    printd(L_INFO, "dev[%d]: %s", i, ibv_get_device_name(ib_dev_list[i]));
  }
  assert(ib_dev_list != NULL && num_devices > dev_id);
  ib_dev = ib_dev_list[dev_id];

  struct ibv_context* ret = ibv_open_device(ib_dev);
  assert(ret != NULL);
  ibv_free_device_list(ib_dev_list);
  return ret;
}

struct ibv_qp* ib_create_rc_qp(struct ibv_pd* ib_pd,
                               struct ibv_qp_init_attr* qp_init_attr) {
  return ibv_create_qp(ib_pd, qp_init_attr);
}

int ib_connect_qp(struct ibv_qp* local_qp,
                  const QPInfo* local_qp_info,
                  const QPInfo* remote_qp_info,
                  uint8_t conn_type) {
  int rc = 0;
  rc = modify_qp_to_init(local_qp, local_qp_info);
  assert(rc == 0);

  rc = modify_qp_to_rtr(local_qp, local_qp_info, remote_qp_info, conn_type);
  assert(rc == 0);

  rc = modify_qp_to_rts(local_qp);
  assert(rc == 0);
  return 0;
}

void ib_print_gid(const uint8_t* gid) {
  printd(L_DEBUG, "gid: %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d",
         gid[0], gid[1], gid[2], gid[3], gid[4], gid[5], gid[6], gid[7], gid[8],
         gid[9], gid[10], gid[11], gid[12], gid[13], gid[14], gid[15]);
}

void ib_print_wr(struct ibv_send_wr* wr_list) {
  struct ibv_send_wr* p;
  for (p = wr_list; p != NULL; p = p->next) {
    if (p->opcode == IBV_WR_RDMA_WRITE || p->opcode == IBV_WR_RDMA_READ) {
      printd(L_INFO, "wr_id: %ld, opcode: %d, raddr: 0x%lx, rkey: 0x%x",
             p->wr_id, p->opcode, p->wr.rdma.remote_addr, p->wr.rdma.rkey);
    } else if (p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
      printd(L_INFO,
             "wr_id: %ld, opcode: %d, raddr: 0x%lx, rkey: 0x%x, cmp: 0x%lx, "
             "swap: 0x%lx",
             p->wr_id, p->opcode, p->wr.atomic.remote_addr, p->wr.atomic.rkey,
             p->wr.atomic.compare_add, p->wr.atomic.swap);
    }
  }
}