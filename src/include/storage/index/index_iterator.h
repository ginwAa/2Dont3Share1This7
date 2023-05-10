//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *bpm, B_PLUS_TREE_LEAF_PAGE_TYPE *page, int i);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (index_ < 0) {
      return index_ == itr.index_;
    }
    return page_->GetPageId() == itr.page_->GetPageId() && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

  // private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *page_;
  int index_{-1};
};

}  // namespace bustub
