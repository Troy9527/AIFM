#pragma once

#include "local_concurrent_hopscotch.hpp"
#include "RDMAserver.hpp"
#include "RDMAManager.hpp"

#include <cstring>
#include <memory>

namespace far_memory {
class RDMAHashTable : public RDMADS {
private:
  std::unique_ptr<LocalGenericConcurrentHopscotch> local_hopscotch_;
  friend class RDMAHashTableFactory;

public:
  RDMAHashTable(uint32_t param_len, uint8_t *params, RDMAManager *manager
		  , struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr);
  ~RDMAHashTable();
  void read_object(uint8_t obj_id_len, const uint8_t *obj_id,
                   uint16_t *data_len, uint8_t *data_buf);
  void write_object(uint8_t obj_id_len, const uint8_t *obj_id,
                    uint16_t data_len, const uint8_t *data_buf);
  bool remove_object(uint8_t obj_id_len, const uint8_t *obj_id);
  void compute(uint8_t opcode, uint16_t input_len, const uint8_t *input_buf,
               uint16_t *output_len, uint8_t *output_buf);
};

class RDMAHashTableFactory : public RDMADSFactory {
public:
  RDMADS *build(uint32_t param_len, uint8_t *params, RDMAManager *manager
		  , struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr);
};

}; // namespace far_memory
