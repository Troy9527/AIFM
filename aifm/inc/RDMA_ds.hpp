#pragma once

#include <cstdint>
#include "RDMAManager.hpp"

namespace far_memory {
class RDMADS {
public:
  virtual ~RDMADS() {}
  virtual void read_object(uint8_t obj_id_len, const uint8_t *obj_id,
                           uint16_t *data_len, uint8_t *data_buf) = 0;
  virtual void write_object(uint8_t obj_id_len, const uint8_t *obj_id,
                            uint16_t data_len, const uint8_t *data_buf) = 0;
  virtual bool remove_object(uint8_t obj_id_len, const uint8_t *obj_id) = 0;
  virtual void compute(uint8_t opcode, uint16_t input_len,
                       const uint8_t *input_buf, uint16_t *output_len,
                       uint8_t *output_buf) = 0;
};

class RDMADSFactory {
public:
  virtual RDMADS *build(RDMAManager *manager, struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr) = 0;
};
} // namespace far_memory
