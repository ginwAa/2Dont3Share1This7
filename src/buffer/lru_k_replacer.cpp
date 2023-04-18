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

LRUKReplacer::LRUKReplacer(size_t num_frames, const size_t &k)
    : replacer_size_(num_frames),
      k_(k),
      head0_(new Info),
      head1_(new Info),
      dir0_(num_frames, nullptr),
      dir1_(num_frames, nullptr) {
  head0_->pre_ = head0_;
  head0_->nxt_ = head0_;
  head1_->pre_ = head1_;
  head1_->nxt_ = head1_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  if (siz0_ > 0) {
    auto p = head0_;
    while (p->nxt_ != head0_) {
      p = p->nxt_;
      if (p->unpin_) {
        BUSTUB_ASSERT(p->frame_id_ >= 0, "Wrong frame id");
        p->pre_->nxt_ = p->nxt_;
        p->nxt_->pre_ = p->pre_;
        *frame_id = p->frame_id_;
        --curr_size_;
        --siz0_;
        dir0_[*frame_id] = nullptr;
        return true;
      }
    }
  }
  auto p = head1_;
  while (p->nxt_ != head1_) {
    p = p->nxt_;
    if (p->unpin_) {
      BUSTUB_ASSERT(p->frame_id_ >= 0, "Wrong frame id");
      p->pre_->nxt_ = p->nxt_;
      p->nxt_->pre_ = p->pre_;
      *frame_id = p->frame_id_;
      --curr_size_;
      --siz1_;
      dir1_[*frame_id] = nullptr;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id in RecordAccess!");
  std::scoped_lock<std::mutex> lock(latch_);
  ++current_timestamp_;

  if (dir1_[frame_id] != nullptr) {
    auto p = dir1_[frame_id];
    p->record_.emplace_back(current_timestamp_);
    p->record_.pop_front();
    p->pre_->nxt_ = p->nxt_;
    p->nxt_->pre_ = p->pre_;

    p->pre_ = head1_->pre_;
    p->nxt_ = head1_;
    head1_->pre_->nxt_ = p.get();
    head1_->pre_ = p.get();
  } else if (dir0_[frame_id] != nullptr) {
    auto p = dir0_[frame_id];
    p->record_.emplace_back(current_timestamp_);
    if (p->record_.size() == k_) {
      siz0_ -= static_cast<size_t>(p->unpin_);
      siz1_ += static_cast<size_t>(p->unpin_);
      p->pre_->nxt_ = p->nxt_;
      p->nxt_->pre_ = p->pre_;

      p->pre_ = head1_->pre_;
      p->nxt_ = head1_;
      head1_->pre_->nxt_ = p.get();
      head1_->pre_ = p.get();
      std::swap(dir0_[frame_id], dir1_[frame_id]);
    }
  } else {
    auto p = std::make_shared<LRUKReplacer::Info>();
    if (k_ == 1) {
      p->pre_ = head1_->pre_;
      p->nxt_ = head1_;
      head1_->pre_->nxt_ = p.get();
      head1_->pre_ = p.get();
      p->frame_id_ = frame_id;
      p->record_.emplace_back(current_timestamp_);
      ++siz1_;
      dir1_[frame_id] = p;
    } else {
      p->pre_ = head0_->pre_;
      p->nxt_ = head0_;
      head0_->pre_->nxt_ = p.get();
      head0_->pre_ = p.get();
      p->frame_id_ = frame_id;
      p->record_.emplace_back(current_timestamp_);
      ++siz0_;
      dir0_[frame_id] = p;
    }
    ++curr_size_;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in SetEvictable!");
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir0_[frame_id] != nullptr) {
    auto p = dir0_[frame_id];
    if (p->unpin_ ^ set_evictable) {
      p->unpin_ = set_evictable;
      siz0_ += set_evictable ? 1 : -1;
      curr_size_ += set_evictable ? 1 : -1;
    }
  } else if (dir1_[frame_id] != nullptr) {
    auto p = dir1_[frame_id];
    if (p->unpin_ ^ set_evictable) {
      p->unpin_ = set_evictable;
      siz1_ += set_evictable ? 1 : -1;
      curr_size_ += set_evictable ? 1 : -1;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<int>(replacer_size_), "Invalid frame_id_t in Remove!");
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir0_[frame_id] == nullptr) {
    if (dir1_[frame_id] == nullptr) {
      return;
    }
    if (dir1_[frame_id]->unpin_) {
      auto p = dir1_[frame_id];
      p->pre_->nxt_ = p->nxt_;
      p->nxt_->pre_ = p->pre_;
      dir1_[frame_id] = nullptr;
      --siz1_;
      --curr_size_;
      return;
    }
    BUSTUB_ASSERT(0, "Cannot remove a non-evictable frame!");
  } else {
    if (dir0_[frame_id]->unpin_) {
      auto p = dir0_[frame_id];
      p->pre_->nxt_ = p->nxt_;
      p->nxt_->pre_ = p->pre_;
      dir0_[frame_id] = nullptr;
      --siz0_;
      --curr_size_;
      return;
    }
    BUSTUB_ASSERT(0, "Cannot remove a non-evictable frame!");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}
LRUKReplacer::~LRUKReplacer() {
  delete head0_;
  delete head1_;
}

}  // namespace bustub