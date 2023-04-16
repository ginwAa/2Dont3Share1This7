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
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1), dir_(1, std::make_shared<Bucket>(bucket_size_, 0)) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto hi = IndexOf(key);
  return dir_[hi]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto hi = IndexOf(key);
  return dir_[hi]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto hi = IndexOf(key);
  while (dir_[hi]->IsFull()) {
    dir_[hi]->IncrementDepth();
    if (GetGlobalDepthInternal() < GetLocalDepthInternal(hi)) {
      ++global_depth_;
      dir_.reserve(dir_.size() << 1);
      dir_.insert(dir_.end(), dir_.begin(), dir_.end());
    }
    RedistributeBucket(dir_[hi], hi);
    hi = IndexOf(key);
  }
  dir_[hi]->Insert(key, value);

//  std::cout << "$ " << GetNumBucketsInternal() << '\n';
//  for (int i = 0; i < int(dir_.size()); ++i) {
//    std::cout << "# ";
//    std::string s = "";
//    for (int j = 0; j < global_depth_; ++j) {
//      s += char((i >> j & 1) + '0');
//    }
//    std::cout << std::string (s.rbegin(), s.rend()) << ':';
//    for (auto [k, v] : dir_[i]->GetItems()) {
//      std::cout << "[" << k << "] ";
//    }
//    std::cout << '\n';
//  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, const uint64_t &p) -> void {
  auto q = p | 1U << (GetGlobalDepthInternal() - 1);
  dir_[q] = std::make_shared<Bucket>(bucket_size_, GetGlobalDepthInternal());
  ++num_buckets_;
  auto iter = bucket->GetItems().begin();
  while (iter != bucket->GetItems().end()) {
    if (IndexOf(iter->first) == q) {
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
