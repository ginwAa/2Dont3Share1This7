//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(const page_id_t &page_id, const page_id_t &parent_id = INVALID_PAGE_ID,
            const int &max_size = LEAF_PAGE_SIZE, BufferPoolManager *buffer_pool_manager_ = nullptr);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(const page_id_t &next_page_id);
  auto KeyAt(const int &index) const -> KeyType;
  void SetKeyAt(const int &index, const KeyType &key);
  auto ValueAt(const int &index) const -> ValueType;
  void SetValueAt(const int &index, const ValueType &val);
  auto Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator) -> bool;
  auto UpperBound(const KeyType &key, const KeyComparator &comparator) -> int;
  void MoveAllToLeft(BPlusTreeLeafPage *dst_page);
  void MoveHalfTo(BPlusTreeLeafPage *dst_page, bool side);
  void MoveDataFrom(MappingType *items, int size, bool side);
  auto Remove(const KeyType &key, const KeyComparator &comparator) -> bool;
  auto At(const int &index) -> const MappingType &;

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];
};

}  // namespace bustub
