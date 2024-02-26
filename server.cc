#include "rdma.h"
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <infiniband/verbs.h>
#include <iostream>
#include <istream>
#include <jsonrpccpp/common/procedure.h>
#include <jsonrpccpp/common/specification.h>
#include <jsonrpccpp/server.h>
#include <jsonrpccpp/server/connectors/tcpsocketserver.h>
#include <malloc.h>
#include <queue>
#include <string>
#include <sys/time.h>
#include <vector>
#include <fstream>

using jsonrpc::JSON_STRING;
using jsonrpc::PARAMS_BY_NAME;
using jsonrpc::Procedure;
using std::cerr;
using std::string;

constexpr int64_t kShowInterval = 2000000;

struct ServerContext {
  int link_type; // IBV_LINK_LAYER_XX
  RdmaDeviceInfo dev_info;
  char *buf; // 存放 write/send 的 buffer, kWriteSize * kRdmaQueueSize 长
             // 前 kRdmaQueueSize / 2 做接收 write 的 buffer，后 kRdmaQueueSize
             // / 2 做 recv buffer
  ibv_mr *mr; // 只是创建删除时候使用
  ibv_cq *cq;
  ibv_qp *qp;
  ibv_mr *flag_mr;

  void BuildRdmaEnvironment(const string &dev_name) {
    // 1. dev_info and pd
    link_type = IBV_LINK_LAYER_UNSPECIFIED;
    auto dev_infos = RdmaGetRdmaDeviceInfoByNames({dev_name}, link_type);
    if (dev_infos.size() != 1 || link_type == IBV_LINK_LAYER_UNSPECIFIED) {
      cerr << "query " << dev_name << "failed" << "\n";
      exit(0);
    }
    dev_info = dev_infos[0];

    // 2. mr and buffer
    buf = reinterpret_cast<char *>(memalign(4096, kWriteSize * kRdmaQueueSize));
    memset(buf,0,kWriteSize * kRdmaQueueSize);
    mr = ibv_reg_mr(dev_info.pd, buf, kWriteSize * kRdmaQueueSize,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                        IBV_ACCESS_REMOTE_READ);
    if (mr == nullptr) {
      cerr << "register mr failed" << "\n";
      exit(0);
    }

    // 3. flag_mr
    char* flag_buf = reinterpret_cast<char *>(memalign(4096,kWriteSize));
    memset(flag_buf,0,kWriteSize);
    flag_mr = ibv_reg_mr(dev_info.pd,flag_buf,kWriteSize,IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                        IBV_ACCESS_REMOTE_READ);

    // 4. create cq
    cq = dev_info.CreateCq(kRdmaQueueSize);
    if (cq == nullptr) {
      cerr << "create cq failed" << "\n";
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
    // 1. create qp
    if (s_ctx.qp != nullptr) {
      cerr << "qp already inited" << "\n";
    }
    s_ctx.qp = RdmaCreateQp(s_ctx.dev_info.pd, s_ctx.cq, s_ctx.cq,
                            kRdmaQueueSize, IBV_QPT_RC);
    if (s_ctx.qp == nullptr) {
      cerr << "create qp failed" << "\n";
      exit(0);
    }

    // 2. get local_info 
    RdmaQpExchangeInfo local_info;
    local_info.lid = s_ctx.dev_info.port_attr.lid;
    local_info.qpNum = s_ctx.qp->qp_num;
    ibv_query_gid(s_ctx.dev_info.ctx, kRdmaDefaultPort, kGidIndex,
                  &local_info.gid);
    local_info.gid_index = kGidIndex;
    printf("local lid %d qp_num %d gid %s gid_index %d\n", local_info.lid,
           local_info.qpNum, RdmaGid2Str(local_info.gid).c_str(),
           local_info.gid_index);

    // 3. get remote_info
    RdmaQpExchangeInfo remote_info = {
        .lid = static_cast<uint16_t>(req["lid"].asUInt()),
        .qpNum = req["qp_num"].asUInt(),
        .gid = RdmaStr2Gid(req["gid"].asString()),
        .gid_index = req["gid_index"].asInt()};
    printf("remote lid %d qp_num %d gid %s gid_index %d\n", remote_info.lid,
           remote_info.qpNum, RdmaGid2Str(remote_info.gid).c_str(),
           remote_info.gid_index);

    // 4. change qp state
    RdmaModifyQp2Rts(s_ctx.qp, local_info, remote_info);
    RdmaPostRecv(kWriteSize,s_ctx.flag_mr->lkey,114514,s_ctx.qp,s_ctx.flag_mr->addr);
    // 5. send resp
    resp["lid"] = local_info.lid;
    resp["qp_num"] = local_info.qpNum;
    resp["gid"] = RdmaGid2Str(local_info.gid);
    resp["gid_index"] = local_info.gid_index;
    resp["rkey"] = s_ctx.mr->rkey;
    resp["remote_addr"] = reinterpret_cast<uint64_t>(s_ctx.buf); // NOLINT
  }
};

ServerJrpcServer *jrpc_server = nullptr;

void HandleCtrlc(int /*signum*/) {
  if (jrpc_server != nullptr) {
    jrpc_server->StopListening();
  }
  exit(0);
}

ibv_wc wc[kRdmaQueueSize];

int main(int argc, char *argv[]) {
  signal(SIGINT, HandleCtrlc);
  signal(SIGTERM, HandleCtrlc);

  // 1. get args
  if (argc != 3) {
    printf("Usage: %s <dev_name> <port>\n", argv[0]);
    return 0;
  }
  string dev_name = argv[1];
  int port = atoi(argv[2]);

  s_ctx.BuildRdmaEnvironment(dev_name);

  // 2. start listening
  jsonrpc::TcpSocketServer server("0.0.0.0", port);
  jrpc_server = new ServerJrpcServer(server);
  jrpc_server->StartListening();
  printf("server start listening...\n");
  
  int cnt = 0;
  // 3. wait finish flag
  while(true){
    int n = ibv_poll_cq(s_ctx.cq,kRdmaQueueSize,wc);
    if(n != 0){
      int k = 0;
      while(reinterpret_cast<char*>(s_ctx.flag_mr->addr)[k] != '\0'){
        cnt *= 10;
        cnt += reinterpret_cast<char*>(s_ctx.flag_mr->addr)[k++] - '0';
      }
      break;
    }
  }

  // 4. print content
  for(int i = 0;i < cnt;i++){
    printf("%s\n",s_ctx.buf + i * kWriteSize);
  }

  // 5. prepare for read
  std::ifstream pd;
  pd.open("./install_deps.sh",std::ios::in);
  if(!pd.is_open()){
    cerr << "server: can't open file" << "\n";
    exit(0);
  }
  cnt = 0;
  memset(s_ctx.buf,0,kWriteSize * kRdmaQueueSize);
  while(pd.getline(s_ctx.buf + cnt * kWriteSize,kWriteSize)){
    cnt++;
  }
  snprintf(reinterpret_cast<char*>(s_ctx.flag_mr->addr),kWriteSize,"%d",cnt);
  RdmaPostSend(kWriteSize, s_ctx.flag_mr->lkey, 666, 23333, s_ctx.qp, s_ctx.flag_mr->addr);

  // 6. wait for completion
  RdmaPostRecv(kWriteSize, s_ctx.flag_mr->lkey, 667, s_ctx.qp, s_ctx.flag_mr->addr);
  bool flag = true;
  while(flag){
    int n = ibv_poll_cq(s_ctx.cq,kRdmaQueueSize,wc);
    for(int i = 0;i < n;i++)
      if(wc[i].status == IBV_WC_SUCCESS && wc[i].opcode == IBV_WC_RECV)
        flag = false;
  }

  // 7. destroy enviroment
  s_ctx.DestroyRdmaEnvironment();

  return 0;
}