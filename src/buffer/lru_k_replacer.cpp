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

LRUKReplacer::LRUKReplacer(size_t num_frames, const size_t &k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i < num_frames; ++i) {
    dir_.emplace_back(std::make_shared<LRUKReplacer::Info>());
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == replacer_size_) {
    return false;
  }
  if (curr_size_ == 0) {
    return false;
  }
  size_t j = 0;
  for (size_t i = 1; i < replacer_size_; ++i) {
    if (InfoLess(*dir_[i], *dir_[j])) {
      j = i;
    }
  }
  dir_[j] = std::make_shared<LRUKReplacer::Info>();
  *frame_id = static_cast<frame_id_t>(j);
  --curr_size_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in RecordAccess!");
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;

  if (dir_[frame_id]->record_.empty()) {
    dir_[frame_id]->record_.emplace_back(current_timestamp_);
    ++curr_size_;
  } else {
    dir_[frame_id]->record_.emplace_back(current_timestamp_);
    if (dir_[frame_id]->record_.size() > k_) {
      dir_[frame_id]->record_.pop_front();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in SetEvictable!");
  std::scoped_lock<std::mutex> lock(latch_);
  auto &p = dir_[frame_id]->pin_;
  if (p == set_evictable) {
    if (p) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
    p = !p;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in Remove!");
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir_[frame_id]->record_.empty()) {
    return;
  }
  if (dir_[frame_id]->pin_) {
    BUSTUB_ASSERT(0, "Cannot remove a non-evictable frame!");
  }
  dir_[frame_id] = std::make_shared<LRUKReplacer::Info>();
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}
auto LRUKReplacer::InfoLess(const LRUKReplacer::Info &lhs, const LRUKReplacer::Info &rhs) const -> bool {
  if (rhs.pin_ || rhs.record_.empty()) {
    return true;
  }
  if (lhs.pin_ || lhs.record_.empty()) {
    return false;
  }
  if (rhs.record_.size() == k_) {
    if (lhs.record_.size() < k_) {
      return true;
    }
    return lhs.record_.front() < rhs.record_.front();
  }
  if (lhs.record_.size() == k_) {
    return false;
  }
  return lhs.record_.front() < rhs.record_.front();
}

}  // namespace bustub
