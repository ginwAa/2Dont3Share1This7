//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : bucket_size_(bucket_size) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::lock_guard<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::lock_guard<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::lock_guard<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto hi = IndexOf(key);
  return dir_[hi]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto hi = IndexOf(key);
  return dir_[hi]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(latch_);
  auto hi = IndexOf(key);
  V old_value;
  if (dir_[hi]->Find(key, old_value)) {
    dir_[hi]->Remove(key);
  }
  while (dir_[hi]->IsFull()) {
    dir_[hi]->IncrementDepth();
    if (GetGlobalDepthInternal() < GetLocalDepthInternal(hi)) {
      ++global_depth_;
      for (size_t i = 0; i < 1 << (GetGlobalDepthInternal() - 1); ++i) {
        dir_.emplace_back(dir_[i]);
      }
    }
    RedistributeBucket(dir_[hi], hi);
    hi = IndexOf(key);
  }
  dir_[hi]->Insert(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, const size_t &p) -> void {
  auto pp = p & ((1 << (bucket->GetDepth() - 1)) - 1);
  auto q = pp | 1 << (bucket->GetDepth() - 1);
  dir_[q] = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  ++num_buckets_;
  for (size_t i = 3; i < 1 << (GetGlobalDepthInternal() - bucket->GetDepth() + 1); i += 2) {
    dir_[pp | i << (bucket->GetDepth() - 1)] = dir_[q];
  }
  auto iter = bucket->GetItems().begin();
  while (iter != bucket->GetItems().end()) {
    if ((std::hash<K>()(iter->first) & q) == q) {
      dir_[q]->Insert(iter->first, iter->second);
      iter = bucket->GetItems().erase(iter);
    } else {
      ++iter;
    }
  }
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
    iter++;
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto iter = list_.begin();
  while (iter != list_.end()) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
    iter++;
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
