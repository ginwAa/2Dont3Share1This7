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

LRUKReplacer::LRUKReplacer(size_t num_frames, const size_t &k) : replacer_size_(num_frames), k_(k),
                                                                 dir_(num_frames, EMPTY_FRAME) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == replacer_size_) {
    BUSTUB_ASSERT(0, "LRUKReplacer is full of non-evictable!");
    return false;
  }
  if (curr_size_ == 0) {
//    BUSTUB_ASSERT(0, "LRUKReplacer is empty!");
    return false;
  }
  auto iter = data_.begin();
  while (iter != data_.end()) {
    auto &[stk, fid] = iter->second;
    if (!std::get<2>(dir_[fid])) {
      ++iter;
      continue;
    }
    if (frame_id != nullptr) {
      *frame_id = iter->second.second;
    }
    dir_[fid] = EMPTY_FRAME;
    data_.erase(iter);
    --curr_size_;
    return true;
  }
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in RecordAccess!");
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;

  if (dir_[frame_id] == EMPTY_FRAME) {
    dir_[frame_id] = std::make_tuple(1, current_timestamp_, false);
    std::stack<size_t> stk;
    stk.push(current_timestamp_);
    data_[{1, current_timestamp_}] = {stk, frame_id};
  } else {
    auto &[siz, las, evi] = dir_[frame_id];
    auto iter = data_.find({siz, las});
    auto &[stk, fid] = iter->second;
    stk.push(current_timestamp_);
    if (stk.size() > k_) {
      stk.pop();
    } else {
      ++siz;
    }
    las = current_timestamp_;
    data_[{siz, las}] = iter->second;
    data_.erase(iter);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in SetEvictable!");
  std::scoped_lock<std::mutex> lock(latch_);
  auto &p = std::get<2>(dir_[frame_id]);
  if (p ^ set_evictable) {
    if (p) {
      --curr_size_;
    } else {
      ++curr_size_;
    }
    p = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in Remove!");
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir_[frame_id] == EMPTY_FRAME) {
    return;
  }
  auto &[siz, las, evi] = dir_[frame_id];
  BUSTUB_ASSERT(evi, "Cannot remove a non-evictable frame!");

  data_.erase(data_.find({siz, las}));
  dir_[frame_id] = EMPTY_FRAME;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

//auto LRUKReplacer::Enough() -> bool {
//  std::scoped_lock<std::mutex> lock(latch_);
//  return total_size_ < replacer_size_;
//}

}  // namespace bustub
