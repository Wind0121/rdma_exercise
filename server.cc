#include "rdma.h"
#include <cstdint>
#include <cstdlib>
#include <infiniband/verbs.h>
#include <iostream>
#include <jsonrpccpp/common/procedure.h>
#include <jsonrpccpp/common/specification.h>
#include <jsonrpccpp/server.h>
#include <jsonrpccpp/server/connectors/tcpsocketserver.h>
#include <malloc.h>
#include <string>
#include <sys/time.h>
#include <vector>

using jsonrpc::JSON_STRING;
using jsonrpc::PARAMS_BY_NAME;
using jsonrpc::Procedure;
using std::cerr;
using std::endl;
using std::string;

constexpr int64_t kShowInterval = 2000000;

struct ServerContext {
  int link_type; // IBV_LINK_LAYER_XX
  RdmaDeviceInfo dev_info;
  char *buf;
  ibv_mr *mr; // 只是创建删除时候使用
  ibv_cq *cq;
  ibv_qp *qp;

  void BuildRdmaEnvironment(const string &dev_name) {
    // 1. dev_info and pd
    link_type = IBV_LINK_LAYER_UNSPECIFIED;
    auto dev_infos = RdmaGetRdmaDeviceInfoByNames({dev_name}, link_type);
    if (dev_infos.size() != 1 || link_type == IBV_LINK_LAYER_UNSPECIFIED) {
      cerr << "query " << dev_name << "failed" << endl;
      exit(0);
    }
    dev_info = dev_infos[0];

    // 2. mr and buffer
    buf =
        reinterpret_cast<char *>(memalign(4096, kBufferSize * kRdmaQueueSize));
    mr = ibv_reg_mr(dev_info.pd, buf, kBufferSize * kRdmaQueueSize,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                        IBV_ACCESS_REMOTE_READ);
    if (mr == nullptr) {
      cerr << "register mr failed" << endl;
      exit(0);
    }

    // 3. create cq
    cq = dev_info.CreateCq(kRdmaQueueSize * 2);
    if (cq == nullptr) {
      cerr << "create cq failed" << endl;
      exit(0);
    }
    qp = nullptr;
  }

  void DestroyRdmaEnvironment() {
    if (qp != nullptr) {
      ibv_destroy_qp(qp);
      qp = nullptr;
    }
    ibv_destroy_cq(cq);
    ibv_dereg_mr(mr);
    free(buf);
    ibv_dealloc_pd(dev_info.pd);
    ibv_close_device(dev_info.ctx);
  }
} s_ctx;

class ServerJrpcServer : public jsonrpc::AbstractServer<ServerJrpcServer> {
public:
  explicit ServerJrpcServer(jsonrpc::TcpSocketServer &server)
      : AbstractServer<ServerJrpcServer>(server) {
    this->bindAndAddMethod(
        Procedure("ExchangeQP", PARAMS_BY_NAME, JSON_STRING, nullptr),
        &ServerJrpcServer::ExchangeQP);
  }

  void ExchangeQP(const Json::Value &req, Json::Value &resp) { // NOLINT
    if (s_ctx.qp != nullptr) {
      cerr << "qp already inited" << endl;
    }
    s_ctx.qp = RdmaCreateQp(s_ctx.dev_info.pd, s_ctx.cq, s_ctx.cq,
                            kRdmaQueueSize, IBV_QPT_RC);
    if (s_ctx.qp == nullptr) {
      cerr << "create qp failed" << endl;
      exit(0);
    }
    RdmaQpExchangeInfo local_info;
    local_info.lid = s_ctx.dev_info.port_attr.lid;
    local_info.qpNum = s_ctx.qp->qp_num;
    ibv_query_gid(s_ctx.dev_info.ctx, kRdmaDefaultPort, kGidIndex,
                  &local_info.gid);
    local_info.gid_index = kGidIndex;
    printf("local lid %d qp_num %d gid %s gid_index %d\n", local_info.lid,
           local_info.qpNum, RdmaGid2Str(local_info.gid).c_str(),
           local_info.gid_index);

    RdmaQpExchangeInfo remote_info = {
        .lid = static_cast<uint16_t>(req["lid"].asUInt()),
        .qpNum = req["qp_num"].asUInt(),
        .gid = RdmaStr2Gid(req["gid"].asString()),
        .gid_index = req["gid_index"].asInt()};
    printf("remote lid %d qp_num %d gid %s gid_index %d\n", local_info.lid,
           local_info.qpNum, RdmaGid2Str(local_info.gid).c_str(),
           local_info.gid_index);

    RdmaModifyQp2Rts(s_ctx.qp, local_info, remote_info);

    for (int i = 0; i < kRdmaQueueSize; i++) {
      RdmaPostRecv(kBufferSize, s_ctx.mr->lkey, i, s_ctx.qp,
                   s_ctx.buf + i * kBufferSize);
    }

    resp["lid"] = local_info.lid;
    resp["qp_num"] = local_info.qpNum;
    resp["gid"] = RdmaGid2Str(local_info.gid);
    resp["gid_index"] = local_info.gid_index;
  }
};

ServerJrpcServer *jrpc_server = nullptr;

ibv_wc wc[kPollCqSize];

int64_t GetUs() {
  timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_usec + tv.tv_sec * 1000000L;
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    printf("Usage: %s <dev_name> <port>\n", argv[0]);
    return 0;
  }
  string dev_name = argv[1];
  int port = atoi(argv[2]);

  s_ctx.BuildRdmaEnvironment(dev_name);

  jsonrpc::TcpSocketServer server("0.0.0.0", port);
  jrpc_server = new ServerJrpcServer(server);
  jrpc_server->StartListening();
  printf("server start listening...\n");

  int recv_cnt = 0;
  while (recv_cnt < kSendTaskNum) {
    int n = ibv_poll_cq(s_ctx.cq, kPollCqSize, wc);
    for (int i = 0; i < n; i++) {
      if (wc[i].status == IBV_WC_SUCCESS) {
        if (wc[i].opcode == IBV_WC_RECV) {
          recv_cnt++;
          RdmaPostRecv(kBufferSize, s_ctx.mr->lkey, wc[i].wr_id, s_ctx.qp,
                       s_ctx.buf + wc[i].wr_id * kBufferSize);
        } else {
          fprintf(stderr, "ERROR: wc[i] opcode %d", wc[i].opcode);
        }
      } else {
        fprintf(stderr, "ERROR: wc[i] status %d", wc[i].status);
      }
    }
  }

  s_ctx.DestroyRdmaEnvironment();

  return 0;
}