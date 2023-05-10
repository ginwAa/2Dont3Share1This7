//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_sequential_scale_test.cpp
//
// Identification: test/storage/b_plus_tree_sequential_scale_test.cpp
//
// Copyright (c) 2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

/**
 * This test should be passing with your Checkpoint 1 submission.
 */
TEST(BPlusTreeTests, ScaleTest) {  // NOLINT
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManagerInstance(100, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  int64_t scale = 5000;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key < scale; key++) {
    keys.push_back(key);
  }

  // randomized the insertion order
  auto rng = std::default_random_engine{};
  std::shuffle(keys.begin(), keys.end(), rng);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);

    assert(tree.Insert(index_key, rid, transaction));
  }
  std::vector<RID> rids;
  for (size_t i = 0; i < keys.size() / 2; ++i) {
    index_key.SetFromInteger(keys[i]);
    tree.Remove(index_key, transaction);
  }

  for (size_t i = 0; i < keys.size() / 2; ++i) {
    int64_t value = keys[i] & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(keys[i] >> 32), value);
    index_key.SetFromInteger(keys[i]);

    assert(tree.Insert(index_key, rid, transaction));
  }
  for (size_t i = 0; i < keys.size() / 2; ++i) {
    index_key.SetFromInteger(keys[i]);
    tree.Remove(index_key, transaction);
  }
  for (size_t i = 0; i < keys.size(); ++i) {
    rids.clear();
    index_key.SetFromInteger(keys[i]);
    tree.GetValue(index_key, &rids);
    if (i >= keys.size() / 2) {
      ASSERT_EQ(rids.size(), 1);

      int64_t value = keys[i] & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    } else {
      ASSERT_EQ(rids.size(), 0);
    }
  }
  auto insert_keys = std::vector(keys.begin() + keys.size() / 2, keys.end());
  std::sort(insert_keys.begin(), insert_keys.end());
  int64_t i = 0;
  for (auto it = tree.Begin(); it != tree.End(); ++it) {
    ASSERT_EQ((*it).second.GetSlotNum(), insert_keys[i] & 0xFFFFFFFF);
    ++i;
  }
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
}  // namespace bustub
