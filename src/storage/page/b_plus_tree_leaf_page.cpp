//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(const page_id_t &page_id, const page_id_t &parent_id, const int &max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(const page_id_t &next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(const int &index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(const int &index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator)
    -> bool {
  auto i = UpperBound(key, comparator);
  if (i != 0 && comparator(KeyAt(i - 1), key) == 0) {
    return false;
  }
  std::move_backward(array_ + i, array_ + GetSize(), array_ + GetSize() + 1);
  array_[i] = {key, val};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &comparator) -> int {
  return std::upper_bound(array_, array_ + GetSize(), key,
                          [&comparator](auto key, const auto &pair1) { return comparator(pair1.first, key) > 0; }) -
         array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllToLeft(BPlusTreeLeafPage *dst_page) -> void {
  dst_page->MoveDataFrom(array_, GetSize(), 1);
  dst_page->SetNextPageId(GetNextPageId());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *dst_page, bool side) -> void {
  int new_size = GetMaxSize() >> 1;
  if (side) {
    dst_page->MoveDataFrom(array_ + new_size, GetSize() - new_size, 0);
  } else {
    dst_page->MoveDataFrom(array_, GetSize() - new_size, 1);
    std::move(array_ + GetSize() - new_size, array_ + GetSize(), array_);
  }
  SetSize(new_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveDataFrom(MappingType *items, int size, bool side) -> void {
  if (side) {
    std::move(items, items + size, array_ + GetSize());
  } else {
    std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + size);
    std::move(items, items + size, array_);
  }
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  auto i = UpperBound(key, comparator) - 1;
  if (i < 0) {
    return false;
  }
  if (comparator(KeyAt(i), key) != 0) {
    return false;
  }
  std::move(array_ + i + 1, array_ + GetSize(), array_ + i);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::At(const int &index) -> const MappingType & { return array_[index]; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
