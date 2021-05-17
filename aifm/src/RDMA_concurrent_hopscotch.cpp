extern "C" {
#include <runtime/preempt.h>
}
#include "RDMA_concurrent_hopscotch.hpp"
#include "hash.hpp"
#include "helpers.hpp"

#include <cstring>
#include <iostream>
#include <cstdio>
#include <cstdlib>

namespace far_memory {

FORCE_INLINE RDMAGenericConcurrentHopscotch::BucketEntry::BucketEntry() {
  bitmap = timestamp = 0;
  ptr = reinterpret_cast<char*>(BucketEntry::kEmptyPtr);
}

RDMAGenericConcurrentHopscotch::RDMAGenericConcurrentHopscotch(uint32_t num_entries_shift, uint64_t data_size, RDMAManager *manager
    , struct mr_data_t mr, struct ibv_mr **l_mr, struct ibv_mr **dlen_mr)
    : kHashMask_((1 << num_entries_shift) - 1),
      kNumEntries_((1 << num_entries_shift) + kNeighborhood),
      /*slab_base_addr_(reinterpret_cast<uint64_t>(helpers::allocate_hugepage(data_size))),*/
      slab_base_addr_(0),
      slab_(reinterpret_cast<uint8_t *>(slab_base_addr_), data_size) {
  // Check overflow.
  BUG_ON(((kHashMask_ + 1) >> num_entries_shift) != 1);

  // Allocate memory for buckets.
  auto size = kNumEntries_ * sizeof(BucketEntry);
  buckets_mem_.reset(
      reinterpret_cast<uint8_t *>(helpers::allocate_hugepage(size)));
  buckets_ = new (buckets_mem_.get()) BucketEntry[kNumEntries_];
	
  manager_ = manager;
  remote_mr = mr;
  local_mr = l_mr;
  data_len_mr = dlen_mr;
}

RDMAGenericConcurrentHopscotch::~RDMAGenericConcurrentHopscotch() {}

void RDMAGenericConcurrentHopscotch::do_remove(BucketEntry *bucket,
                                                BucketEntry *entry) {
  auto header = entry->header;
  auto old_data_size =  header.key_len + header.val_len;
  slab_.free(reinterpret_cast<uint8_t *>(entry->ptr), old_data_size);
  entry->ptr = reinterpret_cast<char*>(BucketEntry::kEmptyPtr);
  auto offset = entry - bucket;
  assert(bucket->bitmap & (1 << offset));
  bucket->bitmap ^= (1 << offset);
}

void RDMAGenericConcurrentHopscotch::get(uint8_t key_len, const uint8_t *key,
                                          uint16_t *val_len, uint8_t *val,
                                          bool remove) {
  uint32_t hash = hash_32(static_cast<const void *>(key), key_len);
  uint32_t bucket_idx = hash & kHashMask_;
  auto *bucket = buckets_ + bucket_idx;
  decltype(bucket) entry;
  uint64_t timestamp;
  uint32_t retry_counter = 0;
  struct ibv_mr	*mr;
  void	*buff;

  auto get_once = [&]<bool Lock>() -> bool {
    if constexpr (Lock) {
      while (unlikely(!bucket->spin.TryLockWp())) {
        thread_yield();
      }
    }
    auto spin_guard = helpers::finally([&]() {
      if constexpr (Lock) {
        bucket->spin.UnlockWp();
      }
    });
    timestamp = load_acquire(&(bucket->timestamp));
    uint32_t bitmap = bucket->bitmap;
    while (bitmap) {
      auto offset = helpers::bsf_32(bitmap);
      entry = &buckets_[bucket_idx + offset];
      auto header = entry->header;
      auto *slab_val_ptr = entry->ptr;
      rmb();
      if (header.key_len == key_len) {
	/* RDMA Read */
	preempt_disable();
	mr = data_len_mr[get_core_num()];
	buff = mr->addr;
	manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
			, header.key_len, mr->lkey, &remote_mr
			, (reinterpret_cast<uint64_t>(slab_val_ptr + header.val_len)));
	manager_->poll_completion(get_core_num());
        
	if (strncmp(reinterpret_cast<char *>(buff),
                    reinterpret_cast<const char *>(key), key_len) == 0) {
          *val_len = header.val_len;
	  
	  manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
	  		, header.val_len, mr->lkey, &remote_mr
	  		, (reinterpret_cast<uint64_t>(slab_val_ptr)));
	  manager_->poll_completion(get_core_num());
	  memcpy(val, buff, header.val_len);
	  preempt_enable();
	  
          /*memcpy(val, slab_val_ptr, *val_len);*/
          return true;
        }
	preempt_enable();
      }
      
      bitmap ^= (1 << offset);
    }
    return false;
  };

  // Fast path.
  do {
    if (get_once.template operator()<false>()) {
      if (remove) {
        goto remove;
      }
      return;
    }
  } while (timestamp != ACCESS_ONCE(bucket->timestamp) &&
           retry_counter++ < kMaxRetries);

  // Slow path.
  if (timestamp != ACCESS_ONCE(bucket->timestamp)) {
    if (get_once.template operator()<true>()) {
      if (remove) {
        goto remove;
      }
      return;
    }
  }
  *val_len = 0;
  return;

remove:
  bucket->spin.Lock();
  auto spin_guard = helpers::finally([&]() { bucket->spin.Unlock(); });
  /*if (likely(entry->ptr)) {*/
    if (likely(timestamp == ACCESS_ONCE(bucket->timestamp))) {
      // Fast path.
      do_remove(bucket, entry);
    } else {
      // Slow path.
      spin_guard.reset();
      this->remove(key_len, key);
    }
  /*}*/
}

bool RDMAGenericConcurrentHopscotch::put(uint8_t key_len, const uint8_t *key,
                                          uint16_t val_len,
                                          const uint8_t *val) {
  uint32_t hash = hash_32(static_cast<const void *>(key), key_len);
  uint32_t bucket_idx = hash & kHashMask_;
  auto *bucket = &(buckets_[bucket_idx]);
  auto orig_bucket_idx = bucket_idx;
  struct ibv_mr	*mr;
  void	*buff;

  while (unlikely(!bucket->spin.TryLockWp())) {
    thread_yield();
  }
  auto bucket_lock_guard = helpers::finally([&]() { bucket->spin.UnlockWp(); });

  uint32_t bitmap = load_acquire(&(bucket->bitmap));
  while (bitmap) {
    auto offset = helpers::bsf_32(bitmap);
    auto *bucket = &buckets_[bucket_idx];
    auto *entry = bucket + offset;
    auto header = entry->header;
    if (header.key_len == key_len) {
      auto *slab_val_ptr = reinterpret_cast<char *>(entry->ptr);
      
      preempt_disable();
      mr = data_len_mr[get_core_num()];
      buff = mr->addr;
      manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
      		, header.key_len, mr->lkey, &remote_mr
      		, reinterpret_cast<uint64_t>(entry->ptr + header.val_len));
      manager_->poll_completion(get_core_num());
      
      if (strncmp(reinterpret_cast<char *>(buff),
                  reinterpret_cast<const char *>(key), key_len) == 0) {
        if (unlikely(header.val_len != val_len)) {
          auto old_data_size = key_len + header.val_len;
          slab_.free(reinterpret_cast<uint8_t *>(entry->ptr), old_data_size);
          auto new_data_size = key_len + val_len;
          auto *new_header =
              reinterpret_cast<char *>(slab_.allocate(new_data_size));
          /*BUG_ON(!new_header);*/
          entry->ptr = new_header;
          entry->header = {.key_len = key_len, .val_len = val_len};
          slab_val_ptr =
              reinterpret_cast<char *>(new_header);
	  
	  memcpy(buff, key, key_len);
          manager_->post_send(get_core_num(), IBV_WR_RDMA_WRITE, reinterpret_cast<uint64_t>(buff)
      		, key_len, mr->lkey, &remote_mr
      		, reinterpret_cast<uint64_t>(slab_val_ptr + val_len));
          manager_->poll_completion(get_core_num());

          /*memcpy(slab_val_ptr + val_len, key, key_len);*/
        }

	memcpy(buff, val, val_len);
        manager_->post_send(get_core_num(), IBV_WR_RDMA_WRITE, reinterpret_cast<uint64_t>(buff)
      	      , val_len, mr->lkey, &remote_mr
      	      , reinterpret_cast<uint64_t>(slab_val_ptr));
        manager_->poll_completion(get_core_num());

	
        manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
        		, header.key_len + header.val_len, mr->lkey, &remote_mr
        		, reinterpret_cast<uint64_t>(entry->ptr));
        manager_->poll_completion(get_core_num());
	
	if(strncmp(reinterpret_cast<char *>(buff), reinterpret_cast<const char *>(key), key_len) != 0)
		std::cout << "wrong" << std::endl;

        preempt_enable();
	std::cout << "hello" << std::endl;
        /*memcpy(slab_val_ptr, val, val_len);*/
        return true;
      }
      preempt_enable();
    }
    bitmap ^= (1 << offset);
  }

  // The key does not exist. Use linear probing to find the first empty slot.
  while (bucket_idx < kNumEntries_) {
    auto *entry = &buckets_[bucket_idx];
    if (__sync_bool_compare_and_swap(reinterpret_cast<uint64_t *>(&entry->ptr),
                                     BucketEntry::kEmptyPtr, BucketEntry::kBusyPtr)) {
      break;
    }
    bucket_idx++;
  }

  if (very_unlikely(bucket_idx == kNumEntries_)) {
    // The bucket is full and we need to resize the hash table.
    // For now, we just throw out an error.
    BUG();
  }

  uint32_t distance_to_orig_bucket;
  // Now keep moving the empty slot until it becomes neighbors.
  while ((distance_to_orig_bucket = bucket_idx - orig_bucket_idx) >=
         kNeighborhood) {
    // Try to see if we can move things backward.
    uint32_t distance;
    for (distance = kNeighborhood - 1; distance > 0; distance--) {
      auto idx = bucket_idx - distance;
      auto *anchor_entry = &(buckets_[idx]);
      if (!anchor_entry->bitmap) {
        continue;
      }

      // Lock and recheck bitmap.
      while (unlikely(!anchor_entry->spin.TryLockWp())) {
        thread_yield();
      }
      auto lock_guard =
          helpers::finally([&]() { anchor_entry->spin.UnlockWp(); });
      auto bitmap = load_acquire(&(anchor_entry->bitmap));
      if (unlikely(!bitmap)) {
        continue;
      }

      // Get the offset of the first entry within the bucket.
      auto offset = helpers::bsf_32(bitmap);
      if (idx + offset >= bucket_idx) {
        continue;
      }

      // Swap entry [closest_bucket + offset] and [bucket_idx]
      auto *from_entry = &buckets_[idx + offset];
      auto *to_entry = &buckets_[bucket_idx];

      to_entry->ptr = from_entry->ptr;
      to_entry->header = from_entry->header;
      assert((anchor_entry->bitmap & (1 << distance)) == 0);
      anchor_entry->bitmap |= (1 << distance);
      anchor_entry->timestamp++;

      wmb();

      from_entry->ptr = reinterpret_cast<char *>(BucketEntry::kBusyPtr);
      assert(anchor_entry->bitmap & (1 << offset));
      anchor_entry->bitmap ^= (1 << offset);

      // Jump backward.
      bucket_idx = idx + offset;
      break;
    }

    // The bucket is full and we need to resize the hash table.
    // For now, we just throw out an error.
    if (very_unlikely(!distance)) {
      BUG();
    }
  }

  // Allocate memory.
  auto *final_entry = &buckets_[bucket_idx];
  auto *slab_addr = reinterpret_cast<char *>(
      slab_.allocate(key_len + val_len));
  /*BUG_ON(!slab_addr);*/
  final_entry->ptr = slab_addr;

  // Write object.
  final_entry->header = {.key_len = key_len, .val_len = val_len};
  auto *slab_val_ptr = reinterpret_cast<char *>(slab_addr);

  preempt_disable();
  mr = data_len_mr[get_core_num()];
  buff = mr->addr;
  memcpy(buff, val, val_len);
  memcpy(reinterpret_cast<char*>(buff) + val_len, key, key_len);
  manager_->post_send(get_core_num(), IBV_WR_RDMA_WRITE, reinterpret_cast<uint64_t>(buff)
        , val_len + key_len, mr->lkey, &remote_mr
        , reinterpret_cast<uint64_t>(slab_val_ptr));
  manager_->poll_completion(get_core_num());
        
  manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
  		, key_len + val_len, mr->lkey, &remote_mr
  		, reinterpret_cast<uint64_t>(final_entry->ptr));
  manager_->poll_completion(get_core_num());
  
  /*if(strncmp(reinterpret_cast<char *>(buff) + val_len, reinterpret_cast<const char *>(key), key_len) != 0)*/
  	/*std::cout << "wrong" << std::endl;*/
  /*if(strncmp(reinterpret_cast<char *>(buff), reinterpret_cast<const char *>(val), val_len) != 0)*/
  	/*std::cout << "fuck" << std::endl;*/
  /*fprintf(stdout, "%s\n", reinterpret_cast<const char *>(key));*/
  /*fflush(stdout);*/
  /*int	len = (key_len > 63) ? 63 : key_len;*/
  /*if(strncmp("illtbxgogzvurfawdvejnftyhclwjydrkqmnnsdwsyqleshjnnsdslbcppyzndq", reinterpret_cast<const char *>(key), len) == 0){*/
	  /*fprintf(stdout, "%p\n", final_entry->ptr);*/
	  /*fflush(stdout);*/
  /*}*/

  preempt_enable();

  /*memcpy(slab_val_ptr + val_len, key, key_len);*/
  /*memcpy(slab_val_ptr, val, val_len);*/
  wmb();

  // Update the bitmap of the final bucket.
  assert((bucket->bitmap & (1 << distance_to_orig_bucket)) == 0);
  bucket->bitmap |= (1 << distance_to_orig_bucket);
  return false;
}

bool RDMAGenericConcurrentHopscotch::remove(uint8_t key_len,
                                             const uint8_t *key) {
  uint32_t hash = hash_32(static_cast<const void *>(key), key_len);
  uint32_t bucket_idx = hash & kHashMask_;
  auto *bucket = &(buckets_[bucket_idx]);
  struct ibv_mr	*mr;
  void	*buff;
  char	tmp[256];

  while (unlikely(!bucket->spin.TryLockWp())) {
    thread_yield();
  }
  auto spin_guard = helpers::finally([&]() { bucket->spin.UnlockWp(); });

  uint32_t bitmap = load_acquire(&(bucket->bitmap));
  while (bitmap) {
    auto offset = helpers::bsf_32(bitmap);
    auto *entry = &buckets_[bucket_idx + offset];
    auto header = entry->header;
    if (header.key_len == key_len) {
      auto *slab_val_ptr =
          reinterpret_cast<const char *>(entry->ptr);
      
      preempt_disable();
      mr = data_len_mr[get_core_num()];
      buff = mr->addr;
      manager_->post_send(get_core_num(), IBV_WR_RDMA_READ, reinterpret_cast<uint64_t>(buff)
            , key_len, mr->lkey, &remote_mr
            , reinterpret_cast<uint64_t>(slab_val_ptr + header.val_len));
      manager_->poll_completion(get_core_num());

      if (strncmp(reinterpret_cast<char *>(buff),
                  reinterpret_cast<const char *>(key), key_len) == 0) {
        do_remove(bucket, entry);
        preempt_enable();
        return true;
      }
      preempt_enable();
    }
    bitmap ^= (1 << offset);
  }
  return false;
}

} // namespace far_memory
