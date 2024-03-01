#include "rdma.h"
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <infiniband/verbs.h>
#include <ios>
#include <iostream>
#include <json/value.h>
#include <jsonrpccpp/client.h>
#include <jsonrpccpp/client/connectors/tcpsocketclient.h>
#include <jsonrpccpp/common/procedure.h>
#include <jsonrpccpp/common/specification.h>
#include <malloc.h>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>

using std::cerr;
using std::string;

struct ClientContext {
  int link_type; // IBV_LINK_LAYER_XX
  RdmaDeviceInfo dev_info;
  char *buf; // 存放 write/send 的 buffer, kWriteSize * kRdmaQueueSize 长
             // 前 kRdmaQueueSize / 2 做 write 的 buffer，后 kRdmaQueueSize
             // / 2 做 send buffer
  ibv_mr *mr; // 只是创建删除时候使用
  ibv_cq *cq;
  ibv_qp *qp;
  ibv_mr *flag_mr;
  char *ip;
  int port;
  uint32_t rkey; // 对面的 rkey
  uint64_t remote_addr;

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

    char* flag_buf = reinterpret_cast<char *>(memalign(4096,kWriteSize));
    memset(flag_buf,0,kWriteSize);
    flag_mr = ibv_reg_mr(dev_info.pd, flag_buf, kWriteSize,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                          IBV_ACCESS_REMOTE_READ);
    if(flag_mr == nullptr){
      cerr << "register flag_mr failed" << "\n";
      exit(0);
    }

    // 3. create cq
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
    char* tmp = reinterpret_cast<char*>(flag_mr->addr);
    ibv_dereg_mr(flag_mr);
    free(tmp);
    ibv_dealloc_pd(dev_info.pd);
    ibv_close_device(dev_info.ctx);
  }
} c_ctx;

void ExchangeQP() { // NOLINT
  // 1. create qp
  if (c_ctx.qp != nullptr) {
    cerr << "qp already inited" << "\n";
  }
  c_ctx.qp = RdmaCreateQp(c_ctx.dev_info.pd, c_ctx.cq, c_ctx.cq, kRdmaQueueSize,
                          IBV_QPT_RC);
  if (c_ctx.qp == nullptr) {
    cerr << "create qp failed" << "\n";
    exit(0);
  }
  // 2. get local_info
  RdmaQpExchangeInfo local_info;
  local_info.lid = c_ctx.dev_info.port_attr.lid;
  local_info.qpNum = c_ctx.qp->qp_num;
  ibv_query_gid(c_ctx.dev_info.ctx, kRdmaDefaultPort, kGidIndex,
                &local_info.gid);
  local_info.gid_index = kGidIndex;
  printf("local lid %d qp_num %d gid %s gid_index %d\n", local_info.lid,
         local_info.qpNum, RdmaGid2Str(local_info.gid).c_str(),
         local_info.gid_index);
  
  // 3. ExchangeQP
  Json::Value req;
  req["lid"] = local_info.lid;
  req["qp_num"] = local_info.qpNum;
  req["gid"] = RdmaGid2Str(local_info.gid);
  req["gid_index"] = local_info.gid_index;

  jsonrpc::TcpSocketClient client(c_ctx.ip, c_ctx.port);
  jsonrpc::Client c(client);
  Json::Value resp = c.CallMethod("ExchangeQP", req);

  // 4. get remote_info
  RdmaQpExchangeInfo remote_info;
  remote_info.lid = static_cast<uint16_t>(resp["lid"].asUInt());
  remote_info.qpNum = resp["qp_num"].asUInt();
  remote_info.gid = RdmaStr2Gid(resp["gid"].asString());
  remote_info.gid_index = resp["gid_index"].asInt();
  printf("remote lid %d qp_num %d gid %s gid_index %d\n", local_info.lid,
         local_info.qpNum, RdmaGid2Str(local_info.gid).c_str(),
         local_info.gid_index);
  c_ctx.rkey = resp["rkey"].asUInt();
  c_ctx.remote_addr = resp["remote_addr"].asUInt64();

  // 5. change qp state
  RdmaModifyQp2Rts(c_ctx.qp, local_info, remote_info);
}


void HandleCtrlc(int /*signum*/) { exit(0); }

ibv_wc wc[kRdmaQueueSize];
int main(int argc, char *argv[]) {
  signal(SIGINT, HandleCtrlc);
  signal(SIGTERM, HandleCtrlc);

  // 1. get args
  if (argc != 4) {
    printf("Usage: %s <dev_name> <server_ip> <server_port>\n", argv[0]);
    return 0;
  }
  string dev_name = argv[1];
  c_ctx.ip = argv[2];
  c_ctx.port = atoi(argv[3]);

  c_ctx.BuildRdmaEnvironment(dev_name);

  std::ifstream pd;
  pd.open("./download_deps.sh",std::ios::in);
  if(!pd.is_open()){
    cerr << "client: can't open file" << "\n";
    exit(0);
  }

  // 2. start linking
  ExchangeQP();

  // 3. write data
  printf("start write data\n");
  int k = 0;
  while(pd.getline(c_ctx.buf + k * kWriteSize,kWriteSize)){
    RdmaPostWrite(kWriteSize,c_ctx.mr->lkey,k,c_ctx.qp,c_ctx.buf + k * kWriteSize,
                  c_ctx.remote_addr + k * kWriteSize,c_ctx.rkey);
    // printf("%d: %s\n",k,c_ctx.buf + k * kWriteSize);
    k++;
  }
  
  snprintf(reinterpret_cast<char*>(c_ctx.flag_mr->addr),kWriteSize,"%d",k);
  RdmaPostSend(kWriteSize, c_ctx.flag_mr->lkey, 666, 23333, c_ctx.qp, c_ctx.flag_mr->addr);

  // 4. read data
  printf("start read data\n");
  RdmaPostRecv(kWriteSize, c_ctx.flag_mr->lkey, 667, c_ctx.qp, c_ctx.flag_mr->addr);
  bool flag = true;
  int cnt = 0;
  while(flag){
    int n = ibv_poll_cq(c_ctx.cq, kRdmaQueueSize,wc);
    for(int i = 0;i < n;i++)
      if(wc[i].status == IBV_WC_SUCCESS && wc[i].opcode == IBV_WC_RECV){
        int k = 0;
        while(reinterpret_cast<char*>(c_ctx.flag_mr->addr)[k] != '\0'){
          cnt *= 10;
          cnt += reinterpret_cast<char*>(c_ctx.flag_mr->addr)[k++] - '0';
        }
        flag = false;
      }
  }
  memset(c_ctx.buf,0,kWriteSize * kRdmaQueueSize);
  for(int i = 0;i < cnt;i++){
    RdmaPostRead(kWriteSize,c_ctx.mr->lkey,i,c_ctx.qp,c_ctx.buf + i * kWriteSize,
                  c_ctx.remote_addr + i * kWriteSize,c_ctx.rkey);
  }
  for(int i = 0;i < cnt;i++){
    printf("%s\n",c_ctx.buf + i * kWriteSize);
  }

  RdmaPostSend(kWriteSize, c_ctx.flag_mr->lkey, 668, 23333, c_ctx.qp, c_ctx.flag_mr->addr);
  pd.close();
  c_ctx.DestroyRdmaEnvironment();

  return 0;
}