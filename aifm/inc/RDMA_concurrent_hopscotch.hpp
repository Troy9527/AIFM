#pragma once

#include "sync.h"

#include "helpers.hpp"
#include "slab.hpp"
#include "RDMAManager.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <limits>

namespace far_memory {

class RDMAGenericConcurrentHopscotch {
private:
#pragma pack(push, 1)
  struct KVDataHeader {
    uint8_t key_len;
    uint16_t val_len;
  };
#pragma pack(pop)
  static_assert(sizeof(KVDataHeader) == 3);

#pragma pack(push, 1)
  struct BucketEntry {
    constexpr static uint64_t kBusyPtr = ULLONG_MAX - 1;
    constexpr static uint64_t kEmptyPtr = ULLONG_MAX;

    uint32_t bitmap;
    rt::Spin spin;
    uint64_t timestamp;
    KVDataHeader header;
    char *ptr;

    BucketEntry();
  };
#pragma pack(pop)
  static_assert(sizeof(BucketEntry) == 27);

  constexpr static uint32_t kNeighborhood = 32;
  constexpr static uint32_t kMaxRetries = 2;

  const uint32_t kHashMask_;
  const uint32_t kNumEntries_;
  std::unique_ptr<uint8_t> buckets_mem_;
  uint64_t slab_base_addr_;
  Slab slab_;
  BucketEntry *buckets_;
  friend class FarMemTest;

  void do_remove(BucketEntry *bucket, BucketEntry *entry);
  
  struct mr_data_t	remote_mr;
  struct ibv_mr 	**local_mr;
  struct ibv_mr 	**data_len_mr;
  RDMAManager		*manager_;

public:
  RDMAGenericConcurrentHopscotch(uint32_t num_entries_shift, uint64_t data_size, RDMAManager *manager
		, struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr);
  ~RDMAGenericConcurrentHopscotch();
  void get(uint8_t key_len, const uint8_t *key, uint16_t *val_len, uint8_t *val,
           bool remove = false);
  bool put(uint8_t key_len, const uint8_t *key, uint16_t val_len,
           const uint8_t *val);
  bool remove(uint8_t key_len, const uint8_t *key);
};

} // namespace far_memory
