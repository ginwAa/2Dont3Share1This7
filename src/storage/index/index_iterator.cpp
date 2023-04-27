/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, B_PLUS_TREE_LEAF_PAGE_TYPE* page, int i) : bpm_(bpm), page_(page), index_(i) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  bpm_->UnpinPage(page_->GetPageId(), true);
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  bool result = (page_->GetNextPageId() == INVALID_PAGE_ID) && (page_->GetSize() == index_ + 1);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return page_->At(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if ((page_->GetSize() == index_ + 1) && (page_->GetNextPageId() != INVALID_PAGE_ID)) {
    auto nxt = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(bpm_->FetchPage(page_->GetNextPageId())->GetData());
    bpm_->UnpinPage(page_->GetPageId(), true);
    index_ = 0;
    page_ = nxt;
  } else {
    ++index_;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
