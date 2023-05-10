//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(const page_id_t &page_id, page_id_t parent_id, const int &max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(const int &index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(const int &index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(const int &index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(const int &index, const ValueType &val) { array_[index].second = val; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &comparator) -> int {
  return std::upper_bound(array_ + 1, array_ + GetSize(), key,
                          [&comparator](auto key, const auto &pair1) { return comparator(pair1.first, key) > 0; }) -
         array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, BufferPoolManager *bpm, const KeyComparator &comparator)
    -> bool {
  auto i = UpperBound(key, comparator) - 1;
  std::move(array_ + i + 1, array_ + GetSize(), array_ + i);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator,
                                            BufferPoolManager *bpm) -> bool {
  int i = UpperBound(key, comparator);
  if (comparator(KeyAt(i - 1), key) == 0) {
    return false;
  }
  std::move_backward(array_ + i, array_ + GetSize(), array_ + GetSize() + 1);
  array_[i] = {key, val};
  auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(bpm->FetchPage(val)->GetData());
  page->SetParentPageId(GetPageId());
  bpm->UnpinPage(val, true);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllToLeft(BPlusTreeInternalPage *dst_page, BufferPoolManager *bpm,
                                                   const KeyComparator &comparator) -> void {
  dst_page->MoveDataFrom(array_, GetSize(), 1, bpm, comparator);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *dst_page, bool side, BufferPoolManager *bpm,
                                                const KeyComparator &comparator) -> void {
  int new_size = ((GetMaxSize() + 1) >> 1);
  if (side) {
    dst_page->MoveDataFrom(array_ + new_size, GetSize() - new_size, 0, bpm, comparator);
  } else {
    if (GetParentPageId() != INVALID_PAGE_ID) {
      auto raw = bpm->FetchPage(GetParentPageId());
      auto par = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(raw->GetData());
      auto x = par->UpperBound(KeyAt(0), comparator) - 1;
      par->SetKeyAt(x, KeyAt(GetSize() - new_size));
      bpm->UnpinPage(GetParentPageId(), true);
    }
    dst_page->MoveDataFrom(array_, GetSize() - new_size, 1, bpm, comparator);
    std::move(array_ + GetSize() - new_size, array_ + GetSize(), array_);
  }
  SetSize(new_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveDataFrom(MappingType *items, int size, bool side, BufferPoolManager *bpm,
                                                  const KeyComparator &comparator) -> void {
  int j = side ? GetSize() : 0;
  if (side) {
    std::move(items, items + size, array_ + GetSize());
  } else {
    KeyType key = KeyAt(0);
    std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + size);
    std::move(items, items + size, array_);
    if (GetParentPageId() != INVALID_PAGE_ID) {
      auto raw = bpm->FetchPage(GetParentPageId());
      auto par = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(raw->GetData());
      auto x = par->UpperBound(key, comparator) - 1;
      par->SetKeyAt(x, KeyAt(0));
      bpm->UnpinPage(GetParentPageId(), true);
    }
  }
  for (int i = 0; i < size; ++i) {
    auto raw_page = bpm->FetchPage(ValueAt(i + j));
    auto page = reinterpret_cast<BPlusTreePage *>(raw_page->GetData());
    page->SetParentPageId(GetPageId());
    bpm->UnpinPage(ValueAt(i + j), true);
  }
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Generate(const ValueType &left, const KeyType &rhs, const ValueType &right,
                                              BufferPoolManager *bpm) -> void {
  IncreaseSize(2);
  SetKeyAt(1, rhs);
  SetValueAt(0, left);
  SetValueAt(1, right);
  auto lp = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(left)->GetData());
  lp->SetParentPageId(GetPageId());
  bpm->UnpinPage(left, true);
  auto rp = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(right)->GetData());
  rp->SetParentPageId(GetPageId());
  bpm->UnpinPage(right, true);
}

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
