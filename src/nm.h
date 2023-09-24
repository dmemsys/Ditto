#ifndef _DMC_NM_H_
#define _DMC_NM_H_

#include <infiniband/verbs.h>
#include <netdb.h>
#include <stdint.h>

#include <map>
#include <vector>

#include "dmc_utils.h"

// Network manager
// Provide a UDP socket for RPC
// Manage an RC QP for each client/server
class UDPNetworkManager {
 private:
  uint32_t udp_sock_;
  uint16_t udp_port_;
  uint8_t role_;
  uint8_t conn_type_;
  struct sockaddr_in* server_addr_list_;
  uint32_t num_server_;
  uint32_t server_id_;

  struct ibv_context* ib_ctx_;
  struct ibv_pd* ib_pd_;
  struct ibv_cq* ib_send_cq_;
  struct ibv_cq* ib_recv_cq_;
  uint8_t ib_port_id_;
  struct ibv_port_attr ib_port_attr_;
  struct ibv_device_attr ib_device_attr_;
  int32_t ib_gid_idx_;
  union ibv_gid ib_gid_;
  std::vector<struct ibv_qp*> rc_qp_list_;
  std::vector<MrInfo*> mr_info_list_;

  int UDPNMInitClient(const DMCConfig* conf);
  int UDPNMInitServer(const DMCConfig* conf);

  // private methods
 private:
  struct ibv_qp* create_rc_qp();
  int get_qp_info(struct ibv_qp* qp, __OUT QPInfo* qp_info);

  // inline public functions
 public:
  inline uint32_t get_server_rkey(uint8_t server_id) {
    return mr_info_list_[server_id]->rkey;
  }

  inline struct ibv_pd* get_ib_pd() { return ib_pd_; }

 public:
  UDPNetworkManager(const DMCConfig* conf);
  ~UDPNetworkManager();

  // common udp functions
  int recv_udp_msg(__OUT UDPMsg* udpmsg,
                   __OUT sockaddr_in* src_addr,
                   __OUT socklen_t* src_addr_len);
  int send_udp_msg(UDPMsg* udpmsg,
                   struct sockaddr_in* dest_addr,
                   socklen_t dest_addr_len);
  int send_udp_msg_to_server(UDPMsg* udpmsg, uint16_t server_id);
  void close_udp_sock();

  // for server
  int nm_on_connect_new_qp(const UDPMsg* request, __OUT QPInfo* qp_info);
  int nm_on_connect_connect_qp(uint32_t client_id,
                               const QPInfo* local_qp_info,
                               const QPInfo* remote_qp_info);

  // for client
  int client_connect_one_rc_qp(uint16_t server_id, __OUT MrInfo* mr_info);

  // rdma operations
  int rdma_write_sid_sync(uint16_t server,
                          uint64_t remote_addr,
                          uint32_t rkey,
                          uint64_t local_addr,
                          uint32_t lkey,
                          uint32_t len);
  int rdma_inl_write_sid_sync(uint16_t server,
                              uint64_t remote_addr,
                              uint32_t rkey,
                              uint64_t local_addr,
                              uint32_t len);
  int rdma_read_sid_sync(uint16_t server,
                         uint64_t remote_addr,
                         uint32_t rkey,
                         uint64_t local_addr,
                         uint32_t lkey,
                         uint32_t len);
  int rdma_batch_read_sid_sync(uint16_t server,
                               uint64_t* remote_addr,
                               uint32_t rkey,
                               uint64_t* local_addr,
                               uint32_t* lkey,
                               uint32_t* len,
                               int batch_size);
  int rdma_cas_sid_sync(uint16_t server,
                        uint64_t remote_addr,
                        uint32_t rkey,
                        uint64_t local_addr,
                        uint32_t lkey,
                        uint64_t expected_val,
                        uint64_t swap_val);
  int rdma_faa_size_sync(uint16_t server,
                         uint64_t remote_addr,
                         uint32_t rkey,
                         uint64_t local_addr,
                         uint32_t lkey,
                         uint64_t add);
  int rdma_write_sid_async(uint16_t server,
                           uint64_t remote_addr,
                           uint32_t rkey,
                           uint64_t local_addr,
                           uint32_t lkey,
                           uint32_t len);
  int rdma_inl_write_sid_async(uint16_t server,
                               uint64_t remote_addr,
                               uint32_t rkey,
                               uint64_t local_addr,
                               uint32_t lkey,
                               uint32_t len);
  int rdma_post_send_sid_async(struct ibv_send_wr* sr_list, uint16_t server);
  int rdma_post_send_sid_sync(struct ibv_send_wr* sr_list, uint16_t server);
  int rdma_post_recv_sid_async(struct ibv_recv_wr* rr_list, uint16_t server);

  int rdma_poll_one_send_completion_sync(struct ibv_wc* wc);
  int rdma_poll_one_recv_completion_async(struct ibv_wc* wc);
  int rdma_poll_one_recv_completion_sync(struct ibv_wc* wc);
  int rdma_poll_recv_completion_async(struct ibv_wc* wc, int num_wc);
};

#endif