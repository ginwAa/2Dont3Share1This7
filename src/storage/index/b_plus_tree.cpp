#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  BUSTUB_ASSERT(leaf_max_size > 1, "Leaf max size too small.");
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  bool res = false;
  root_latch_.WLock();
  if (IsEmpty()) {
    root_latch_.WUnlock();
    return false;
  }
  if (transaction != nullptr) {
    auto [node, root_locked] = FindLeaf(key, OPT::READ, transaction);
    auto page = reinterpret_cast<LeafPage *>(node->GetData());
    auto x = page->UpperBound(key, comparator_) - 1;
    if (comparator_(page->KeyAt(x), key) == 0) {
      result->emplace_back(page->ValueAt(x));
      res = true;
    }
    UnLockAll(transaction, OPT::READ, root_locked);
  } else {
    auto *t = new Transaction(1);
    auto [node, root_locked] = FindLeaf(key, OPT::READ, t);
    auto page = reinterpret_cast<LeafPage *>(node->GetData());
    auto x = page->UpperBound(key, comparator_) - 1;
    if (comparator_(page->KeyAt(x), key) == 0) {
      result->emplace_back(page->ValueAt(x));
      res = true;
    }
    UnLockAll(t, OPT::READ, root_locked);
    delete t;
  }
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  bool result = false;
  root_latch_.WLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto raw = buffer_pool_manager_->NewPage(&root_page_id_);
    if (raw == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    auto page = reinterpret_cast<LeafPage *>(raw->GetData());
    page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    page->SetNextPageId(INVALID_PAGE_ID);
    page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(true);
    root_latch_.WUnlock();
    return true;
  }
  if (transaction == nullptr) {
    auto *t = new Transaction(1);
    auto [raw, root_locked] = FindLeaf(key, OPT::INSERT, t);
    auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
    result = reinterpret_cast<LeafPage *>(node)->Insert(key, value, comparator_);
    if (node->GetSize() == node->GetMaxSize()) {
      auto [rhs, u] = Split(node, t);
      InsertToParent(rhs, u, t);
    }
    UnLockAll(t, OPT::INSERT, root_locked);
    delete t;
  } else {
    auto [raw, root_locked] = FindLeaf(key, OPT::INSERT, transaction);
    auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
    result = reinterpret_cast<LeafPage *>(node)->Insert(key, value, comparator_);
    if (node->GetSize() == node->GetMaxSize()) {
      auto [rhs, u] = Split(node, transaction);
      InsertToParent(rhs, u, transaction);
    }
    UnLockAll(transaction, OPT::INSERT, root_locked);
  }
  return result;
}

/*
 * the new page hasn't unpin
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *raw_old, Transaction *t) -> std::pair<BPlusTreePage *, KeyType> {
  page_id_t rhs_id = INVALID_PAGE_ID;
  KeyType key;
  auto raw = buffer_pool_manager_->NewPage(&rhs_id);
  if (raw == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }
  raw->WLatch();
  t->AddIntoPageSet(raw);
  if (raw_old->IsLeafPage()) {
    auto old = reinterpret_cast<LeafPage *>(raw_old);
    auto page = reinterpret_cast<LeafPage *>(raw->GetData());
    page->Init(rhs_id, -1, leaf_max_size_);
    old->MoveHalfTo(page, 1);
    page->SetParentPageId(old->GetParentPageId());
    page->SetNextPageId(old->GetNextPageId());
    old->SetNextPageId(page->GetPageId());
    key = page->KeyAt(0);
  } else {
    auto old = reinterpret_cast<InternalPage *>(raw_old);
    auto page = reinterpret_cast<InternalPage *>(raw->GetData());
    page->Init(rhs_id, -1, internal_max_size_);
    old->MoveHalfTo(page, 1, buffer_pool_manager_, comparator_);
    page->SetParentPageId(old->GetParentPageId());
    key = page->KeyAt(0);
  }
  return {reinterpret_cast<BPlusTreePage *>(raw->GetData()), key};
}

/*
 * the son page hasn't unpin
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertToParent(BPlusTreePage *raw_page, const KeyType &key, Transaction *t) -> void {
  if (raw_page->GetParentPageId() == INVALID_PAGE_ID) {
    auto old_root = root_page_id_;
    auto par_raw = buffer_pool_manager_->NewPage(&root_page_id_);
    if (par_raw == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
    }
    par_raw->WLatch();
    t->AddIntoPageSet(par_raw);
    auto par = reinterpret_cast<InternalPage *>(par_raw->GetData());
    par->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    par->Generate(old_root, key, raw_page->GetPageId(), buffer_pool_manager_);
    return;
  }
  auto par = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(raw_page->GetParentPageId())->GetData());
  if (par->GetSize() == par->GetMaxSize()) {
    auto [rhs, u] = Split(par, t);
    InsertToParent(rhs, u, t);
    if (comparator_(key, u) != -1) {
      buffer_pool_manager_->UnpinPage(par->GetPageId(), true);
      par = reinterpret_cast<InternalPage *>(rhs);
      par->Insert(key, raw_page->GetPageId(), comparator_, buffer_pool_manager_);
    } else {
      par->Insert(key, raw_page->GetPageId(), comparator_, buffer_pool_manager_);
      buffer_pool_manager_->UnpinPage(par->GetPageId(), true);
    }
  } else {
    par->Insert(key, raw_page->GetPageId(), comparator_, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(par->GetPageId(), true);
  }
}

/*
 * the leaf hasn't unpin
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, OPT opt, Transaction *t) -> std::pair<Page *, bool> {
  auto cur = root_page_id_;
  auto raw = buffer_pool_manager_->FetchPage(cur);
  bool root_locked = true;
  if (LockAndSafe(raw, opt)) {
    UnLockAll(t, opt, root_locked);
  }
  t->AddIntoPageSet(raw);
  auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  while (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    int x = page->UpperBound(key, comparator_) - 1;
    cur = page->ValueAt(x);
    auto son_raw = buffer_pool_manager_->FetchPage(cur);
    if (LockAndSafe(son_raw, opt)) {
      UnLockAll(t, opt, root_locked);
    }
    t->AddIntoPageSet(son_raw);
    raw = son_raw;
    node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  }
  return {raw, root_locked};
}

/*
 * old hasn't unpin
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeAndMerge(BPlusTreePage *old, Transaction *t) -> bool {
  if (old->GetParentPageId() == INVALID_PAGE_ID) {
    if (old->IsLeafPage()) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
    }
    return false;
  }
  bool result = false;
  page_id_t bro_id = INVALID_PAGE_ID;
  auto par = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(old->GetParentPageId())->GetData());

  // Redistribute
  if (old->IsLeafPage()) {
    auto page = reinterpret_cast<LeafPage *>(old);
    int pos = par->UpperBound(page->KeyAt(0), comparator_) - 1;
    if (pos > 0) {
      bro_id = par->ValueAt(pos - 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      bro_raw->WLatch();
      t->AddIntoPageSet(bro_raw);
      auto bro = reinterpret_cast<LeafPage *>(bro_raw->GetData());
      if (bro->GetSize() + old->GetSize() >= 2 * bro->GetMinSize()) {
        bro->MoveHalfTo(page, 1);
        par->SetKeyAt(pos, page->KeyAt(0));
        result = true;
      }
    }
    if (!result && pos < par->GetSize() - 1) {
      bro_id = par->ValueAt(pos + 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      bro_raw->WLatch();
      t->AddIntoPageSet(bro_raw);
      auto bro = reinterpret_cast<LeafPage *>(bro_raw->GetData());
      if (bro->GetSize() + old->GetSize() >= 2 * bro->GetMinSize()) {
        bro->MoveHalfTo(page, 0);
        par->SetKeyAt(pos + 1, bro->KeyAt(0));
        result = true;
      }
    }
  } else {
    auto page = reinterpret_cast<InternalPage *>(old);
    int pos = par->UpperBound(page->KeyAt(0), comparator_) - 1;
    if (pos > 0) {
      bro_id = par->ValueAt(pos - 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      bro_raw->WLatch();
      t->AddIntoPageSet(bro_raw);
      auto bro = reinterpret_cast<InternalPage *>(bro_raw->GetData());
      if (bro->GetSize() + old->GetSize() >= 2 * bro->GetMinSize()) {
        bro->MoveHalfTo(page, 1, buffer_pool_manager_, comparator_);
        par->SetKeyAt(pos, page->KeyAt(0));
        result = true;
      }
    }
    if (!result && pos < par->GetSize() - 1) {
      bro_id = par->ValueAt(pos + 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      bro_raw->WLatch();
      t->AddIntoPageSet(bro_raw);
      auto bro = reinterpret_cast<InternalPage *>(bro_raw->GetData());
      if (bro->GetSize() + old->GetSize() >= 2 * bro->GetMinSize()) {
        bro->MoveHalfTo(page, 0, buffer_pool_manager_, comparator_);
        par->SetKeyAt(pos + 1, bro->KeyAt(0));
        result = true;
      }
    }
  }
  if (result) {
    buffer_pool_manager_->UnpinPage(par->GetPageId(), true);
    return result;
  }

  // Merge
  if (old->IsLeafPage()) {
    auto page = reinterpret_cast<LeafPage *>(old);
    int pos = par->UpperBound(page->KeyAt(0), comparator_) - 1;
    if (pos > 0) {
      bro_id = par->ValueAt(pos - 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      auto bro = reinterpret_cast<LeafPage *>(bro_raw->GetData());
      if (bro->GetSize() + page->GetSize() <= bro->GetMaxSize()) {
        par->Remove(page->KeyAt(0), buffer_pool_manager_, comparator_);
        page->MoveAllToLeft(bro);
        if (par->GetSize() < par->GetMinSize()) {
          if (!RedistributeAndMerge(reinterpret_cast<BPlusTreePage *>(par), t)) {
            root_page_id_ = bro->GetPageId();
            bro->SetParentPageId(INVALID_PAGE_ID);
            UpdateRootPageId();
          }
        }
        result = true;
      }
      buffer_pool_manager_->UnpinPage(bro_id, result);
    }
    if (!result && pos < par->GetSize() - 1) {
      bro_id = par->ValueAt(pos + 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      auto bro = reinterpret_cast<LeafPage *>(bro_raw->GetData());
      if (bro->GetSize() + page->GetSize() <= bro->GetMaxSize()) {
        par->Remove(bro->KeyAt(0), buffer_pool_manager_, comparator_);
        bro->MoveAllToLeft(page);
        if (par->GetSize() < par->GetMinSize()) {
          if (!RedistributeAndMerge(reinterpret_cast<BPlusTreePage *>(par), t)) {
            root_page_id_ = page->GetPageId();
            page->SetParentPageId(INVALID_PAGE_ID);
            UpdateRootPageId();
          }
        }
        result = true;
      }
      buffer_pool_manager_->UnpinPage(bro_id, result);
    }
  } else {
    auto page = reinterpret_cast<InternalPage *>(old);
    int pos = par->UpperBound(page->KeyAt(0), comparator_) - 1;
    if (pos > 0) {
      bro_id = par->ValueAt(pos - 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      auto bro = reinterpret_cast<InternalPage *>(bro_raw->GetData());
      if (bro->GetSize() + page->GetSize() <= bro->GetMaxSize()) {
        par->Remove(page->KeyAt(0), buffer_pool_manager_, comparator_);
        page->MoveAllToLeft(bro, buffer_pool_manager_, comparator_);
        if (par->GetSize() < par->GetMinSize()) {
          if (!RedistributeAndMerge(reinterpret_cast<BPlusTreePage *>(par), t)) {
            root_page_id_ = bro->GetPageId();
            bro->SetParentPageId(INVALID_PAGE_ID);
            UpdateRootPageId();
          }
        }
        result = true;
      }
      buffer_pool_manager_->UnpinPage(bro_id, result);
    }
    if (!result && pos < par->GetSize() - 1) {
      bro_id = par->ValueAt(pos + 1);
      auto bro_raw = buffer_pool_manager_->FetchPage(bro_id);
      auto bro = reinterpret_cast<InternalPage *>(bro_raw->GetData());
      if (bro->GetSize() + page->GetSize() <= bro->GetMaxSize()) {
        par->Remove(bro->KeyAt(0), buffer_pool_manager_, comparator_);
        bro->MoveAllToLeft(page, buffer_pool_manager_, comparator_);
        if (par->GetSize() < par->GetMinSize()) {
          if (!RedistributeAndMerge(reinterpret_cast<BPlusTreePage *>(par), t)) {
            root_page_id_ = page->GetPageId();
            page->SetParentPageId(INVALID_PAGE_ID);
            UpdateRootPageId();
          }
        }
        result = true;
      }
      buffer_pool_manager_->UnpinPage(bro_id, result);
    }
  }
  buffer_pool_manager_->UnpinPage(par->GetPageId(), result);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  if (IsEmpty()) {
    root_latch_.WUnlock();
    return;
  }
  //  std::cout << "r " << key << std::endl;
  if (transaction == nullptr) {
    auto *t = new Transaction(1);
    auto [raw, root_locked] = FindLeaf(key, OPT::REMOVE, t);
    auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
    reinterpret_cast<LeafPage *>(node)->Remove(key, comparator_);
    if (node->GetSize() < node->GetMinSize()) {
      RedistributeAndMerge(node, t);
    }
    UnLockAll(t, OPT::REMOVE, root_locked);
    delete t;
  } else {
    auto [raw, root_locked] = FindLeaf(key, OPT::REMOVE, transaction);
    auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
    reinterpret_cast<LeafPage *>(node)->Remove(key, comparator_);
    if (node->GetSize() < node->GetMinSize()) {
      RedistributeAndMerge(node, transaction);
    }
    UnLockAll(transaction, OPT::REMOVE, root_locked);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LockAndSafe(Page *page, OPT opt) -> bool {
  if (opt == OPT::READ) {
    page->RLatch();
    return true;
  }
  page->WLatch();
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (opt == OPT::INSERT) {
    return node->GetSize() < (node->GetMaxSize() - static_cast<int>(node->IsLeafPage()));
  }
  return node->GetSize() > node->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UnLockAll(Transaction *t, OPT opt, bool &root) -> void {
  UpdateRootPageId();
  while (!t->GetPageSet()->empty()) {
    auto page = t->GetPageSet()->front();
    t->GetPageSet()->pop_front();
    if (opt == OPT::READ) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  if (root) {
    root_latch_.WUnlock();
    root = false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }
  auto cur = root_page_id_;
  Page *raw = buffer_pool_manager_->FetchPage(cur);
  raw->RLatch();
  root_latch_.RUnlock();
  auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  while (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    cur = page->ValueAt(0);
    auto son_raw = buffer_pool_manager_->FetchPage(cur);
    son_raw->RLatch();
    raw->RUnlatch();
    raw = son_raw;
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  }
  return IndexIterator(buffer_pool_manager_, reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  auto cur = root_page_id_;
  auto raw = buffer_pool_manager_->FetchPage(cur);
  raw->RLatch();
  root_latch_.RUnlock();
  auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  int x = 0;
  while (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    x = page->UpperBound(key, comparator_) - 1;
    if (x < 0) {
      x = 0;
    }
    cur = page->ValueAt(x);
    auto son_raw = buffer_pool_manager_->FetchPage(cur);
    son_raw->RLatch();
    raw->RUnlatch();
    raw = son_raw;
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  }

  x = reinterpret_cast<LeafPage *>(node)->UpperBound(key, comparator_) - 1;
  if (comparator_(reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node)->KeyAt(x), key) != 0) {
    BUSTUB_ASSERT(0, "Begin key not found");
  }
  return IndexIterator(buffer_pool_manager_, reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node), x);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }
  auto cur = root_page_id_;
  auto raw = buffer_pool_manager_->FetchPage(cur);
  raw->RLatch();
  root_latch_.RUnlock();
  auto node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  while (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    cur = page->ValueAt(page->GetSize() - 1);
    auto son_raw = buffer_pool_manager_->FetchPage(cur);
    son_raw->RLatch();
    raw->RUnlatch();
    raw = son_raw;
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(raw->GetData());
  }
  return IndexIterator(buffer_pool_manager_, reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node), node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId(bool create) -> page_id_t {
  root_latch_.RLock();
  root_latch_.RUnlock();
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << " size " << leaf->GetSize() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << " size "
              << internal->GetSize() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
