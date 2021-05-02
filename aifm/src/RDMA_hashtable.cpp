#include "RDMA_hashtable.hpp"
#include "helpers.hpp"

#include <cstring>
#include <iostream>

namespace far_memory {

RDMAHashTable::RDMAHashTable(uint32_t param_len, uint8_t *params, RDMAManager *manager
		, struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr) {
  auto remote_num_entries_shift = *reinterpret_cast<uint32_t *>(&params[0]);
  auto remote_data_size =
      *reinterpret_cast<uint64_t *>(&params[sizeof(remote_num_entries_shift)]);
  std::cout << "Data size: " << remote_data_size << std::endl;
  /*local_hopscotch_.reset(new LocalGenericConcurrentHopscotch(*/
      /*remote_num_entries_shift, remote_data_size));*/
  local_hopscotch_.reset(new RDMAGenericConcurrentHopscotch(
      remote_num_entries_shift, remote_data_size, manager, mr, l_mr, dlen_mr));
}

RDMAHashTable::~RDMAHashTable() {}

void RDMAHashTable::read_object(uint8_t obj_id_len, const uint8_t *obj_id,
                                  uint16_t *data_len, uint8_t *data_buf) {
#ifdef HASHTABLE_EXCLUSIVE
  local_hopscotch_->get(obj_id_len, obj_id, data_len, data_buf,
                        /* remove= */ true);
#else
  local_hopscotch_->get(obj_id_len, obj_id, data_len, data_buf,
                        /* remove= */ false);
#endif
}

void RDMAHashTable::write_object(uint8_t obj_id_len, const uint8_t *obj_id,
                                   uint16_t data_len, const uint8_t *data_buf) {
  local_hopscotch_->put(obj_id_len, obj_id, data_len, data_buf);
}

bool RDMAHashTable::remove_object(uint8_t obj_id_len, const uint8_t *obj_id) {
  return local_hopscotch_->remove(obj_id_len, obj_id);
}

void RDMAHashTable::compute(uint8_t opcode, uint16_t input_len,
                              const uint8_t *input_buf, uint16_t *output_len,
                              uint8_t *output_buf) {
  BUG();
}

RDMADS *RDMAHashTableFactory::build(uint32_t param_len, uint8_t *params, RDMAManager *manager
		, struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr) {
  return new RDMAHashTable(param_len, params, manager, mr, l_mr, dlen_mr);
}

} // namespace far_memory
