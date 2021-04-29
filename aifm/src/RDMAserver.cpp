extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <base/limits.h>
#include <base/stddef.h>
}

#include "RDMAserver.hpp"
/*#include "server_dataframe_vector.hpp"*/
#include "RDMA_hashtable.hpp"
#include "RDMA_ptr.hpp"

namespace far_memory {

std::unique_ptr<RDMADS> RDMAServer::server_ds_ptrs_[kMaxNumDSIDs];

RDMAServer::RDMAServer() {
  register_ds(kRDMAVanillaPtrDSType, new RDMAPtrFactory());
  register_ds(kHashTableDSType, new RDMAHashTableFactory());
  /*register_ds(kDataFrameVectorDSType, new ServerDataFrameVectorFactory());*/
}

void RDMAServer::register_ds(uint8_t ds_type, RDMADSFactory *factory) {
  registered_server_ds_factorys_[ds_type] = factory;
}

void RDMAServer::construct(uint8_t ds_type, uint8_t ds_id, uint32_t param_len, uint8_t *params
		, RDMAManager *manager, struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr) {
  auto factory = registered_server_ds_factorys_[ds_type];
  BUG_ON(server_ds_ptrs_[ds_id]);
  server_ds_ptrs_[ds_id].reset(factory->build(param_len, params, manager, mr, l_mr, dlen_mr));
}

void RDMAServer::destruct(uint8_t ds_id) {
  BUG_ON(!server_ds_ptrs_[ds_id]);
  server_ds_ptrs_[ds_id].reset();
}

void RDMAServer::read_object(uint8_t ds_id, uint8_t obj_id_len,
                         const uint8_t *obj_id, uint16_t *data_len,
                         uint8_t *data_buf) {
  auto ds_ptr = server_ds_ptrs_[ds_id].get();
  if (!ds_ptr) {
    ds_ptr = server_ds_ptrs_[kRDMAVanillaPtrDSID].get();
  }
  ds_ptr->read_object(obj_id_len, obj_id, data_len, data_buf);
}

void RDMAServer::write_object(uint8_t ds_id, uint8_t obj_id_len,
                          const uint8_t *obj_id, uint16_t data_len,
                          const uint8_t *data_buf) {
  auto ds_ptr = server_ds_ptrs_[ds_id].get();
  if (!ds_ptr) {
    ds_ptr = server_ds_ptrs_[kRDMAVanillaPtrDSID].get();
  }
  ds_ptr->write_object(obj_id_len, obj_id, data_len, data_buf);
}

bool RDMAServer::remove_object(uint64_t ds_id, uint8_t obj_id_len,
                           const uint8_t *obj_id) {
  auto ds_ptr = server_ds_ptrs_[ds_id].get();
  if (!ds_ptr) {
    ds_ptr = server_ds_ptrs_[kRDMAVanillaPtrDSID].get();
  }
  return ds_ptr->remove_object(obj_id_len, obj_id);
}

void RDMAServer::compute(uint8_t ds_id, uint8_t opcode, uint16_t input_len,
                     const uint8_t *input_buf, uint16_t *output_len,
                     uint8_t *output_buf) {
  auto ds_ptr = server_ds_ptrs_[ds_id].get();
  return ds_ptr->compute(opcode, input_len, input_buf, output_len, output_buf);
}

RDMADS *RDMAServer::get_server_ds(uint8_t ds_id) {
  return server_ds_ptrs_[ds_id].get();
}

} // namespace far_memory
