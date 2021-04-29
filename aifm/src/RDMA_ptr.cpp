extern "C" {
#include <base/assert.h>
#include <base/compiler.h>
#include <base/limits.h>
#include <base/stddef.h>
#include <runtime/preempt.h>
}

#include "object.hpp"
#include "RDMA_ptr.hpp"

namespace far_memory {

RDMAPtr::RDMAPtr(uint32_t param_len, uint8_t *params, RDMAManager *manager, struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr){
	manager_ = manager;
	remote_mr = mr;
	local_mr = l_mr;
	data_len_mr = dlen_mr;
}

RDMAPtr::~RDMAPtr() {}

void RDMAPtr::read_object(uint8_t obj_id_len, const uint8_t *obj_id,
			uint16_t *data_len, uint8_t *data_buf){
	struct ibv_mr		*mr;
	void			*buff;

	preempt_disable();
	mr = data_len_mr[get_core_num()];
	buff = mr->addr;
	manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
			, sizeof(uint16_t), mr->lkey, &remote_mr
			, *(reinterpret_cast<const uint64_t *>(obj_id)));
	manager_->poll_completion(get_core_num());
	*data_len = *(reinterpret_cast<uint16_t *>(buff));
	preempt_enable();

	if(*data_len){
		preempt_disable();
		manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(data_buf)
				, *data_len, (*local_mr)->lkey, &remote_mr
				, (*(reinterpret_cast<const uint64_t *>(obj_id))) + sizeof(uint16_t));
		manager_->poll_completion(get_core_num());
		preempt_enable();
	}
}

void RDMAPtr::write_object(uint8_t obj_id_len, const uint8_t *obj_id,
			uint16_t data_len, const uint8_t *data_buf){
	struct ibv_mr		*mr;
	void			*buff;

	preempt_disable();
	mr = data_len_mr[get_core_num()];
	buff = mr->addr;
	__builtin_memcpy(buff, &data_len, sizeof(uint16_t));
	manager_->post_send(get_core_num(), IBV_WR_RDMA_WRITE, reinterpret_cast<uint64_t>(buff)
			, sizeof(uint16_t), mr->lkey, &remote_mr
			, *(reinterpret_cast<const uint64_t *>(obj_id)));
	manager_->poll_completion(get_core_num());
	manager_->post_send(get_core_num(), IBV_WR_RDMA_WRITE, reinterpret_cast<uint64_t>(data_buf)
			, data_len, (*local_mr)->lkey, &remote_mr
			, (*(reinterpret_cast<const uint64_t *>(obj_id))) + sizeof(uint16_t));
	manager_->poll_completion(get_core_num());
	preempt_enable();
}

bool RDMAPtr::remove_object(uint8_t obj_id_len, const uint8_t *obj_id){
  BUG();
}

void RDMAPtr::compute(uint8_t opcode, uint16_t input_len, const uint8_t *input_buf,
			uint16_t *output_len, uint8_t *output_buf){
  BUG();
}

RDMADS *RDMAPtrFactory::build(uint32_t param_len, uint8_t *params, RDMAManager *manager, struct mr_data_t mr
		, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr){
  return new RDMAPtr(param_len, params, manager, mr, l_mr, dlen_mr);
}

}; // namespace far_memory
