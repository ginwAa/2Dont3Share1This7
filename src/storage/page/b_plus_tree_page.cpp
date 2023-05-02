//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
auto BPlusTreePage::IsRootPage() const -> bool { return GetParentPageId() == INVALID_PAGE_ID; }
void BPlusTreePage::SetPageType(const IndexPageType &page_type, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  page_type_ = page_type;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(const int &size, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  size_ = size;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}
void BPlusTreePage::IncreaseSize(int amount, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  size_ += amount;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(const int &size, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  max_size_ = size;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsLeafPage()) {
    if (IsRootPage()) {
      return 1;
    }
    return max_size_ >> 1;
  }
  if (IsRootPage()) {
    return std::min(2, (max_size_ + 1) >> 1);
  }
  return (max_size_ + 1) >> 1;
}

/*
 * Helper methods to get/set parent page id
 */
auto BPlusTreePage::GetParentPageId() const -> page_id_t { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  parent_page_id_ = parent_page_id;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}

/*
 * Helper methods to get/set self page id
 */
auto BPlusTreePage::GetPageId() const -> page_id_t { return page_id_; }
void BPlusTreePage::SetPageId(const page_id_t &page_id, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  page_id_ = page_id;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(const lsn_t &lsn, BufferPoolManager *buffer_pool_manager_) {
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->FetchPage(GetPageId());
  }
  lsn_ = lsn;
  if (buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(GetPageId(), true);
  }
}

}  // namespace bustub
