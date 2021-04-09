extern "C" {
#include <net/ip.h>
#include <runtime/runtime.h>
#include <runtime/tcp.h>
#include <runtime/thread.h>
}
#include "thread.h"

#include "device.hpp"
#include "helpers.hpp"
#include "object.hpp"
#include "server.hpp"
#include "RDMAManager.hpp"

#include <atomic>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>
#include <map>

using namespace far_memory;
using namespace std;

RDMAManager manager;
std::vector<rt::Thread> slave_threads;

std::atomic<bool> has_shutdown{true};
rt::Thread master_thread;
Server server;
map<uint8_t, struct ibv_mr*>	server_mr;


// Request:
//     |Opcode = Shutdown (1B)|
// Response:
//     |Ack (1B)|
void process_shutdown(tcpconn_t *c) {
  uint8_t ack;
  helpers::tcp_write_until(c, &ack, sizeof(ack));

  for (auto &thread : slave_threads) {
    thread.Join();
  }
  slave_threads.clear();
}

// Request:
// |Opcode = kOpConstruct (1B)|ds_type(1B)|ds_id(1B)|
// |param_len(1B)|params(param_len B)|
// Response:
// |Ack (1B)|
void process_construct(tcpconn_t *c) {
  uint8_t ds_type;
  uint8_t ds_id;
  uint8_t param_len;
  uint8_t *params;
  uint8_t req[sizeof(ds_type) + Object::kDSIDSize + sizeof(param_len) +
              std::numeric_limits<decltype(param_len)>::max()];

  helpers::tcp_read_until(
      c, req, sizeof(ds_type) + Object::kDSIDSize + sizeof(param_len));
  ds_type = *const_cast<uint8_t *>(&req[0]);
  ds_id = *const_cast<uint8_t *>(&req[sizeof(ds_type)]);
  param_len = *const_cast<uint8_t *>(&req[sizeof(ds_type) + Object::kDSIDSize]);
  helpers::tcp_read_until(
      c, &req[sizeof(ds_type) + Object::kDSIDSize + sizeof(param_len)],
      param_len);
  params = const_cast<uint8_t *>(
      &req[sizeof(ds_type) + Object::kDSIDSize + sizeof(param_len)]);

  /* TODO: handle different ds_type, currently only support pointer */
  /* register memory region */
  struct ibv_mr	*mr = NULL;
  map<uint8_t, struct ibv_mr*>::iterator	iter;
  iter = server_mr.find(ds_id);
  
  if(iter == server_mr.end()){
    char		*buff;
    uint64_t		size;

    BUG_ON(param_len != sizeof(decltype(size)));
    size = *(reinterpret_cast<decltype(size) *>(params));
    buff = reinterpret_cast<char*>(malloc(size));
    mr = manager.reg_addr(reinterpret_cast<uint64_t>(buff), size, false);
    if(mr == NULL)
      std::cerr << "reg addr return NULL" << std::endl;

    server_mr[ds_id] = mr;
  }
  else{
    mr = iter->second;
  }

  struct mr_data_t	mr_data;
  mr_data.addr = reinterpret_cast<uint64_t>(mr->addr);
  mr_data.rkey = mr->rkey;
  mr_data.len = mr->length;

  helpers::tcp_write_until(c, &mr_data, sizeof(struct mr_data_t));
}

// Request:
// |Opcode = kOpDeconstruct (1B)|ds_id(1B)|
// Response:
// |Ack (1B)|
void process_destruct(tcpconn_t *c) {
  uint8_t ds_id;

  helpers::tcp_read_until(c, &ds_id, Object::kDSIDSize);

  /* deregister memory region according ds_id */
  struct ibv_mr	*mr = NULL;
  map<uint8_t, struct ibv_mr*>::iterator	iter;
  iter = server_mr.find(ds_id);
  
  if(iter != server_mr.end()){
    mr = iter->second;
    server_mr.erase(ds_id);
    void *addr = mr->addr;

    if(ibv_dereg_mr(mr))
      cerr << "ibv_dereg_mr failed" << endl;
    free(addr);
  }

  uint8_t ack;
  helpers::tcp_write_until(c, &ack, sizeof(ack));
}

// Request:
// |Opcode = kOpCompute(1B)|ds_id(1B)|opcode(1B)|input_len(2B)|
// |input_buf(input_len)|
// Response:
// |output_len(2B)|output_buf(output_len B)|
void process_compute(tcpconn_t *c) {
  uint8_t opcode;
  uint16_t input_len;
  uint8_t req[Object::kDSIDSize + sizeof(opcode) + sizeof(input_len) +
              TCPDevice::kMaxComputeDataLen];

  helpers::tcp_read_until(
      c, req, Object::kDSIDSize + sizeof(opcode) + sizeof(input_len));

  auto ds_id = *reinterpret_cast<uint8_t *>(&req[0]);
  opcode = *reinterpret_cast<uint8_t *>(&req[Object::kDSIDSize]);
  input_len =
      *reinterpret_cast<uint16_t *>(&req[Object::kDSIDSize + sizeof(opcode)]);
  assert(input_len <= TCPDevice::kMaxComputeDataLen);

  if (input_len) {
    helpers::tcp_read_until(
        c, &req[Object::kDSIDSize + sizeof(opcode) + sizeof(input_len)],
        input_len);
  }

  auto *input_buf = const_cast<uint8_t *>(
      &req[Object::kDSIDSize + sizeof(opcode) + sizeof(input_len)]);

  uint16_t *output_len;
  uint8_t resp[sizeof(*output_len) + TCPDevice::kMaxComputeDataLen];
  output_len = reinterpret_cast<uint16_t *>(&resp[0]);
  uint8_t *output_buf = &resp[sizeof(*output_len)];
  server.compute(ds_id, opcode, input_len, input_buf, output_len, output_buf);

  helpers::tcp_write_until(c, resp, sizeof(*output_len) + *output_len);
}

void slave_fn(tcpconn_t *c) {
  // Run event loop.
  uint8_t opcode;
  int ret;
  while ((ret = tcp_read(c, &opcode, RDMADevice::kOpcodeSize)) > 0) {
    BUG_ON(ret != RDMADevice::kOpcodeSize);
    switch (opcode) {
    case RDMADevice::kOpConstruct:
      process_construct(c);
      break;
    case RDMADevice::kOpDeconstruct:
      process_destruct(c);
      break;
    case RDMADevice::kOpCompute:
      /*process_compute(c);*/
      break;
    default:
      BUG();
    }
  }
  tcp_close(c);
}

void master_fn(tcpconn_t *c) {
  uint8_t	opcode;
  
  manager.set_tcpconn(c);
  manager.resources_create(16, 24);
  manager.connect_qp(24);

  /*
  char			a, b, *buff;
  struct ibv_mr		*mr = NULL;
  struct mr_data_t	mr_data;
  buff = reinterpret_cast<char*>(malloc(128));
  memset(buff, 0, 128);
  memcpy(buff, "IAMServerFUck", 13);
  mr = manager.reg_addr(reinterpret_cast<uint64_t>(buff), 128);
  mr_data.addr = reinterpret_cast<uint64_t>(buff);
  mr_data.rkey = mr->rkey;
  mr_data.len = 128;

  helpers::tcp_write_until(c, &mr_data, sizeof(struct mr_data_t));

  manager.tcp_sync_data(1, &a, &b);
  std::cout << buff << std::endl;
  if(mr)
	  ibv_dereg_mr(mr);*/

  /* testcase for construct */
  /*char		a, b;
  struct ibv_mr	*mr;
  manager.tcp_sync_data(1, &a, &b);
  map<uint8_t, struct ibv_mr*>::iterator	iter;

  iter = server_mr.find(0);
  if(iter != server_mr.end()){
    mr = iter->second;
    char *buff = reinterpret_cast<char*>(mr->addr);
    memset(buff, 0, 128);
    memcpy(buff, "IAMServerfuck", 13);
    manager.tcp_sync_data(1, &a, &b);
    std::cout << buff << std::endl;
  }
  else
    cerr << "not found" << endl;*/


  /* wait for shutdown command */
  helpers::tcp_read_until(c, &opcode, TCPDevice::kOpcodeSize);
  BUG_ON(opcode != RDMADevice::kOpShutdown);
  process_shutdown(c);
  has_shutdown = true;
}

void do_work(uint16_t port) {
  tcpqueue_t *q;
  struct netaddr server_addr = {.ip = 0, .port = port};
  tcp_listen(server_addr, 1, &q);

  tcpconn_t *c;
  while (tcp_accept(q, &c) == 0) {
    if (has_shutdown) {
      master_thread = rt::Thread([c]() { master_fn(c); });
      has_shutdown = false;
    } else {
      slave_threads.emplace_back(rt::Thread([c]() { slave_fn(c); }));
    }
  }
}

int argc;

void my_main(void *arg) {
  char **argv = static_cast<char **>(arg);
  int port = atoi(argv[1]);
  do_work(port);
}

int main(int _argc, char *argv[]) {
  int ret;

  if (_argc < 3) {
    std::cerr << "usage: [cfg_file] [port]" << std::endl;
    return -EINVAL;
  }

  char conf_path[strlen(argv[1]) + 1];
  strcpy(conf_path, argv[1]);
  for (int i = 2; i < _argc; i++) {
    argv[i - 1] = argv[i];
  }
  argc = _argc - 1;

  ret = runtime_init(conf_path, my_main, argv);
  if (ret) {
    std::cerr << "failed to start runtime" << std::endl;
    return ret;
  }

  return 0;
}
