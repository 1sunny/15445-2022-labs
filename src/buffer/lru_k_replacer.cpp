//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  Key_t evict = q_.Evict(evictable_);
  if (evict == -1){
    evict = qk_.Evict(evictable_);
  }
  if (evict != -1){
    curr_size_--;
    *frame_id = evict;
    accesses_.erase(evict);
    evictable_.erase(evict);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_ && frame_id >= 0, "frame_id Invalid");
  size_t cnt = ++accesses_[frame_id];
  std::shared_ptr<LinkList> &p = q_.mp_[frame_id];
  if (cnt <= k_) {
    if (p == nullptr){
      p = q_.mp_[frame_id] = std::make_shared<LinkList>(frame_id);
    } else {
      q_.Remove(p);
    }
    if (cnt == k_){
      qk_.MoveToEnd(p);
    }else{
      q_.MoveToEnd(p);
    }
  } else {
    BUSTUB_ASSERT(p != nullptr, "p == nullptr");
    qk_.Remove(p);
    qk_.MoveToEnd(p);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_ && frame_id >= 0, "frame_id Invalid");
  bool old = evictable_[frame_id];
  evictable_[frame_id] = set_evictable;
  if (set_evictable && !old){
    curr_size_++;
  }else if(!set_evictable && old){
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_ && frame_id >= 0, "frame_id Invalid");
  if (evictable_.find(frame_id) == evictable_.end()){
    return;
  }
  BUSTUB_ASSERT(evictable_[frame_id], "evictable_[frame_id] == false");
  if (accesses_.find(frame_id) == accesses_.end()){
    return;
  }
  size_t cnt = accesses_[frame_id];
  if (cnt < k_){
    if(q_.Remove(q_.mp_[frame_id])){
      curr_size_--;
    }
  } else {
    if(qk_.Remove(qk_.mp_[frame_id])){
      curr_size_--;
    }
  }
  accesses_.erase(frame_id);
  evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
