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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(const page_id_t &page_id, const page_id_t &parent_id, const int &max_size,
                                          BufferPoolManager *buffer_pool_manager_) {
  BUSTUB_ASSERT(max_size <= static_cast<int>(INTERNAL_PAGE_SIZE), "Internal page size too large.");
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size, buffer_pool_manager_);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(const int &index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "Wrong index in Internal->KeyAt()");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(const int &index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "Wrong index in Internal->SetKeyAt()");
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(const int &index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "Wrong index in Internal->ValueAt()");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(const int &index, const ValueType &val) {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "Wrong index in Internal->SetKeyAt()");
  array_[index].second = val;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(const KeyType &key, const KeyComparator &comparator) -> int {
  return std::lower_bound(array_, array_ + GetSize(), key,
                          [&comparator](const auto &pair1, auto key) { return comparator(pair1.first, key) < 0; }) -
         array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(const KeyType &key, const KeyComparator &comparator) -> int {
  return std::upper_bound(array_, array_ + GetSize(), key,
                          [&comparator](auto key, const auto &pair1) { return comparator(pair1.first, key) > 0; }) -
         array_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  auto i = UpperBound(key, comparator) - 1;
  if (i < 0) {
    return false;
  }
  std::move(array_ + i + 1, array_ + GetSize(), array_ + i);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &val, const KeyComparator &comparator,
                                            BufferPoolManager *bpm) -> bool {
  auto i = UpperBound(key, comparator);
  if (i != 0 && comparator(KeyAt(i - 1), key) == 0) {
    return false;
  }
  std::move_backward(array_ + i, array_ + GetSize(), array_ + GetSize() + 1);
  array_[i] = {key, val};
  auto page = bpm->FetchPage(val);
  auto ppage = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  ppage->SetParentPageId(GetPageId());
  bpm->UnpinPage(val, true);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllToLeft(BPlusTreeInternalPage *dst_page, BufferPoolManager *bpm) -> void {
  dst_page->MoveDataFrom(array_, GetSize(), 1, bpm);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *dst_page, bool side, BufferPoolManager *bpm)
    -> void {
  int new_size = GetMinSize();
  if (side) {
    dst_page->MoveDataFrom(array_ + new_size, GetSize() - new_size, 0, bpm);
  } else {
    dst_page->MoveDataFrom(array_, GetSize() - new_size, 1, bpm);
    std::move(array_ + GetSize() - new_size, array_ + GetSize(), array_);
  }
  SetSize(new_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveDataFrom(MappingType *items, int size, bool side, BufferPoolManager *bpm)
    -> void {
  int j = side ? GetSize() : 0;
  if (side) {
    std::move(items, items + size, array_ + GetSize());
  } else {
    std::move_backward(items, items + GetSize(), items + GetSize() + size);
    std::move(items, items + size, array_);
  }
  for (int i = 0; i < size; ++i) {
    auto raw_page = bpm->FetchPage(ValueAt(i + j));
    auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(raw_page->GetData());
    page->SetParentPageId(GetPageId());
    bpm->UnpinPage(ValueAt(i), true);
  }
  IncreaseSize(size);
}

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const int &l, const int &r, BufferPoolManager *buffer_pool_manager_) ->
// bool {
//   auto lhs = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(ValueAt(l))->GetData());
//   auto rhs = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(ValueAt(r))->GetData());
//   bool result = false;
//   if (lhs->GetSize() + rhs->GetSize() > lhs->GetMaxSize()) {
//     result = false;
//     buffer_pool_manager_->UnpinPage(ValueAt(r), false);
//   } else if (lhs->IsLeafPage()) {
//     // TODO
//   } else {
//     auto lhs_page = static_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(lhs);
//     auto rhs_page = static_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(rhs);
//     for (int i = 0; i < rhs_page->GetSize(); ++i) {
//       lhs_page->PushBack(rhs_page->array_[i], buffer_pool_manager_);
//     }
//     rhs_page->Destroy(buffer_pool_manager_);
//     result = true;
//   }
//   buffer_pool_manager_->UnpinPage(ValueAt(l), false);
//   return result;
// }
//
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Redistribute(const int &index, BufferPoolManager *buffer_pool_manager_) -> bool
// {
//   auto page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(ValueAt(index))->GetData());
//   bool result = false;
//   bool given = page->GetSize() == page->GetMaxSize();
//   if (page->IsLeafPage()) {
//     // TODO
//   } else {
//     auto lhs = static_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(page);
//     if (index != 0) {
//       auto rhs = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(buffer_pool_manager_->FetchPage(ValueAt(index -
//       1))->GetData()); if (given) {
//         if (rhs->GetSize() + 1 < rhs->GetMaxSize()) {
//           rhs->PushBack(lhs->array_[lhs->GetSize() - 1], buffer_pool_manager_);
//           lhs->PopFront(buffer_pool_manager_);
//           result = true;
//         }
//       } else {
//         if (rhs->GetSize() > rhs->GetMinSize()) {
//           lhs->PushFront(rhs->array_[0], buffer_pool_manager_);
//           rhs->PopBack(buffer_pool_manager_);
//           result = true;
//         }
//       }
//       buffer_pool_manager_->UnpinPage(ValueAt(index - 1), false);
//     }
//     if (!result && (index != GetSize() - 1)) {
//       auto rhs = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(buffer_pool_manager_->FetchPage(ValueAt(index +
//       1))->GetData()); if (given) {
//         if (rhs->GetSize() + 1 < rhs->GetMaxSize()) {
//           rhs->PushFront(lhs->array_[lhs->GetSize() - 1], buffer_pool_manager_);
//           lhs->PopBack(buffer_pool_manager_);
//           result = true;
//         }
//       } else {
//         if (rhs->GetSize() > rhs->GetMinSize()) {
//           lhs->PushBack(rhs->array_[0], buffer_pool_manager_);
//           rhs->PopFront(buffer_pool_manager_);
//           result = true;
//         }
//       }
//       buffer_pool_manager_->UnpinPage(ValueAt(index + 1), false);
//     }
//   }
//   buffer_pool_manager_->UnpinPage(ValueAt(index), false);
//   return result;
// }
//
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Destroy(BufferPoolManager *buffer_pool_manager_) {
//   while (!buffer_pool_manager_->DeletePage(GetPageId())) {
//     buffer_pool_manager_->UnpinPage(GetPageId(), false);
//   }
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetNeighbor(const ValueType &p) -> std::pair<page_id_t, page_id_t> {
//   auto left = INVALID_PAGE_ID;
//   auto right = INVALID_PAGE_ID;
//   for (int i = 0; i < GetSize(); ++i) {
//     if (ValueAt(i) == p) {
//       if (i != 0) {
//         left = ValueAt(i - 1);
//         right = ValueAt(i + 1);
//       }
//       break;
//     }
//   }
//   return {left, right};
// }
//
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyComparator &comparator, BufferPoolManager *buffer_pool_manager_)
// {
//   if (GetSize() != GetMaxSize()) {
//     return;
//   }
//   auto new_page_id = INVALID_PAGE_ID;
//   auto rhs =
//   reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
//   rhs->Init(new_page_id, GetParentPageId(), GetMaxSize());
//   auto new_key = KeyAt(GetMinSize() + 1);
//   for (int i = 0; GetSize() > GetMinSize(); ++i) {
//     rhs->PushBack(array_[GetMinSize() - 1], buffer_pool_manager_);
//     PopBack();
//   }
//   buffer_pool_manager_->UnpinPage(new_page_id, true);
//   IncreaseSize(0, buffer_pool_manager_);
//
//   if (IsRootPage()) {
//     auto new_root_id = INVALID_PAGE_ID;
//     rhs = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(buffer_pool_manager_->NewPage(&new_root_id)->GetData());
//     rhs->Init(new_page_id, GetParentPageId(), GetMaxSize());
//     rhs->PushFront(array_[0], buffer_pool_manager_);
//     rhs->Insert({new_key, new_page_id}, comparator, buffer_pool_manager_);
//   } else {
//     auto par =
//     reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(buffer_pool_manager_->FetchPage(GetParentPageId())->GetData());
//     par->Insert({new_key, new_page_id}, comparator, buffer_pool_manager_);
//   }
// }
//  valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
