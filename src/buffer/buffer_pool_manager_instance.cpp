//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

//  // TODO(students): remove this line after you have implemented the buffer pool manager
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    page_id = nullptr;
    return nullptr;
  }
  *page_id = AllocatePage();
  frame_id_t fid = 0;
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
//    pages_[fid].WLatch();
    SetFrame(fid, *page_id);
//    pages_[fid].WUnlatch();
  } else {
//    pages_[fid].WLatch();
    PopFrame(fid);
    SetFrame(fid, *page_id);
//    pages_[fid].WUnlatch();
  }
  return &pages_[fid];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid = 0;
  bool in = page_table_->Find(page_id, fid);
  if (!in && free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
//  std::cout << "need to fetch from disk\n";
  if (!in) {
    if (!free_list_.empty()) {
//      std::cout << "ready to set Wrong frame\n";
      fid = free_list_.back();
      free_list_.pop_back();
//      pages_[fid].WLatch();
      SetFrame(fid, page_id);
//      pages_[fid].WUnlatch();
    } else {
//      pages_[fid].WLatch();
      PopFrame(fid);
//      std::cout << "ready to set frame\n";
      SetFrame(fid, page_id);
//      pages_[fid].WUnlatch();
    }
  }
  return &pages_[fid];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid = 0;
  if (!page_table_->Find(page_id, fid)) {
    return false;
  }
//  pages_[fid].WLatch();
  if (pages_[fid].pin_count_ > 0) {
    --pages_[fid].pin_count_;
    if (pages_[fid].pin_count_ == 0) {
//      std::cout << "Unpin page " << page_id << " set dirty " << is_dirty << " data = " << pages_[fid].GetData() << '\n';
      replacer_->SetEvictable(fid, true);
    }
  } else {
    return false;
  }
  pages_[fid].is_dirty_ |= is_dirty;
//  pages_[page_id].WUnlatch();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t fid = 0;
  if (!page_table_->Find(page_id, fid)) {
    return false;
  }
//  pages_[fid].WLatch();
  pages_[fid].is_dirty_ = false;
  disk_manager_->WritePage(page_id, pages_[fid].GetData());
//  pages_[fid].WUnlatch();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid = 0;
  if (!page_table_->Find(page_id, fid)) {
    return true;
  }
//  pages_[fid].WLatch();
  if (pages_[fid].GetPinCount() != 0) {
    return false;
  }
  page_table_->Remove(fid);
  replacer_->Remove(fid);
  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = 0;
  pages_[fid].is_dirty_ = false;
  DeallocatePage(page_id);
//  pages_[fid].WUnlatch();
  free_list_.push_back(fid);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

auto BufferPoolManagerInstance::SetFrame(const frame_id_t &fid, const page_id_t &pid) -> void {
  page_table_->Insert(pid, fid);
  pages_[fid].page_id_ = pid;
  ++pages_[fid].pin_count_;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  disk_manager_->ReadPage(pid, pages_[fid].GetData());
}

auto BufferPoolManagerInstance::PopFrame(frame_id_t &fid) -> void {
  replacer_->Evict(&fid);
  auto pid = pages_[fid].GetPageId();
//  std::cout << "pop page " << pid << " dirty = " << pages_[fid].IsDirty() << '\n';
  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(pid, pages_[fid].GetData());
//    std::cout << "From frame " << fid << " and page " << pid << " write " << pages_[fid].GetData() << '\n';
  }
  pages_[fid].is_dirty_ = false;
  pages_[fid].ResetMemory();
  pages_[fid].page_id_ = 0;
  BUSTUB_ASSERT(page_table_->Remove(pid), "Remove from pages table error!");
}

}  // namespace bustub
