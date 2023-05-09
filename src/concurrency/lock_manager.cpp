//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockCheck(Transaction *txn, LockMode lock_mode) -> void {
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
}

auto LockManager::UpgradeCheck(Transaction *txn, LockMode lock_mode, LockMode pre_mode) -> bool {
  if (pre_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::EXCLUSIVE) {
    return true;
  }
  if (pre_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || pre_mode == LockMode::EXCLUSIVE) {
    return false;
  }
  return lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
}

auto LockManager::UpdateTable(Transaction *txn, const std::shared_ptr<LockRequest> &req, bool insert) -> void {
  std::shared_ptr<std::unordered_set<bustub::table_oid_t>> s;
  switch (req->lock_mode_) {
    case LockMode::SHARED:
      s = txn->GetSharedTableLockSet();
      break;
    case LockMode::EXCLUSIVE:
      s = txn->GetExclusiveTableLockSet();
      break;
    case LockMode::INTENTION_SHARED:
      s = txn->GetIntentionSharedTableLockSet();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      s = txn->GetIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      s = txn->GetSharedIntentionExclusiveTableLockSet();
      break;
  }
  if (insert) {
    s->insert(req->oid_);
  } else {
    s->erase(req->oid_);
  }
}

auto LockManager::UpdateRow(Transaction *txn, const std::shared_ptr<LockRequest> &req, bool insert) -> void {
  std::shared_ptr<std::unordered_map<bustub::table_oid_t, std::unordered_set<RID>>> s;
  switch (req->lock_mode_) {
    case LockMode::SHARED:
      s = txn->GetSharedRowLockSet();
      break;
    case LockMode::EXCLUSIVE:
      s = txn->GetExclusiveRowLockSet();
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
  if (insert) {
    auto ss = s->find(req->oid_);
    if (ss == s->end()) {
      s->emplace(req->oid_, std::unordered_set<RID>());
      ss = s->find(req->oid_);
    }
    ss->second.emplace(req->rid_);
  } else {
    s->find(req->oid_)->second.erase(req->rid_);
  }
}

auto LockManager::Compatible(const std::shared_ptr<LockRequest> &req, const std::shared_ptr<LockRequestQueue> &que)
    -> bool {
  for (const auto &has : que->request_queue_) {
    if (has.get() == req.get()) {
      return true;
    }
    bool ok = true;
    switch (req->lock_mode_) {
      case LockMode::SHARED:
        if (has->lock_mode_ != LockMode::SHARED && has->lock_mode_ != LockMode::INTENTION_SHARED) {
          ok = false;
        }
        break;
      case LockMode::EXCLUSIVE:
        ok = false;
        break;
      case LockMode::INTENTION_SHARED:
        if (has->lock_mode_ == LockMode::EXCLUSIVE) {
          ok = false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (has->lock_mode_ != LockMode::INTENTION_EXCLUSIVE && has->lock_mode_ != LockMode::INTENTION_SHARED) {
          ok = false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (has->lock_mode_ != LockMode::INTENTION_SHARED) {
          ok = false;
        }
        break;
    }
    if (!ok) {
      return false;
    }
  }
  BUSTUB_ASSERT(0, "UNREACHABLE IN DESIGNED!");
}

auto LockManager::ShrinkDetect(Transaction *txn, const std::shared_ptr<LockRequest> &req) -> void {
  bool shrink = false;
  if (req->lock_mode_ == LockMode::SHARED) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      shrink = true;
    }
  } else if (req->lock_mode_ == LockMode::EXCLUSIVE) {
    shrink = true;
  }
  if (shrink && txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    txn->SetState(TransactionState::SHRINKING);
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LockCheck(txn, lock_mode);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  bool upgrade = false;
  for (auto req : lock_request_queue->request_queue_) {
    if (req->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (req->lock_mode_ == lock_mode) {
      lock_request_queue->latch_.unlock();
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (!UpgradeCheck(txn, lock_mode, req->lock_mode_)) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
    lock_request_queue->request_queue_.remove(req);
    UpdateTable(txn, req, false);
    upgrade = true;
    break;
  }
  auto new_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  if (upgrade && !lock_request_queue->request_queue_.empty()) {
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
      if (!(*it)->granted_) {
        lock_request_queue->request_queue_.insert(it, new_req);
      }
    }
    lock_request_queue->upgrading_ = txn->GetTransactionId();
  } else {
    lock_request_queue->request_queue_.emplace_back(new_req);
  }

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!Compatible(new_req, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_req);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  new_req->granted_ = true;
  UpdateTable(txn, new_req, true);

  if (upgrade) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto x_row = txn->GetExclusiveRowLockSet();
  auto s_row = txn->GetSharedRowLockSet();
  if ((x_row->find(oid) != x_row->end() && !x_row->at(oid).empty()) ||
      (s_row->find(oid) != s_row->end() && !s_row->at(oid).empty())) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  for (auto req : lock_request_queue->request_queue_) {
    if (!req->granted_ || req->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    lock_request_queue->request_queue_.remove(req);
    ShrinkDetect(txn, req);
    UpdateTable(txn, req, false);
    lock_request_queue->cv_.notify_all();
    lock_request_queue->latch_.unlock();
    return true;
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  LockCheck(txn, lock_mode);
  if (lock_mode == LockMode::EXCLUSIVE && !txn->IsTableExclusiveLocked(oid) &&
      !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_.find(rid)->second;
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  bool upgrade = false;
  for (auto req : lock_request_queue->request_queue_) {
    if (req->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    if (req->lock_mode_ == lock_mode) {
      lock_request_queue->latch_.unlock();
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (!UpgradeCheck(txn, lock_mode, req->lock_mode_)) {
      lock_request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }

    lock_request_queue->request_queue_.remove(req);
    UpdateRow(txn, req, false);
    upgrade = true;
    break;
  }

  auto new_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  if (upgrade && !lock_request_queue->request_queue_.empty()) {
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
      if (!(*it)->granted_) {
        lock_request_queue->request_queue_.insert(it, new_req);
      }
    }
    lock_request_queue->upgrading_ = txn->GetTransactionId();
  } else {
    lock_request_queue->request_queue_.emplace_back(new_req);
  }

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!Compatible(new_req, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_req);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  new_req->granted_ = true;
  UpdateRow(txn, new_req, true);

  if (upgrade) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw bustub::TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  for (auto req : lock_request_queue->request_queue_) {
    if (!req->granted_ || req->txn_id_ != txn->GetTransactionId()) {
      continue;
    }
    lock_request_queue->request_queue_.remove(req);
    ShrinkDetect(txn, req);
    UpdateRow(txn, req, false);
    lock_request_queue->cv_.notify_all();
    lock_request_queue->latch_.unlock();

    return true;
  }
  lock_request_queue->latch_.unlock();
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  nodes_.emplace(t1);
  nodes_.emplace(t2);
  auto it = std::lower_bound(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it == waits_for_[t1].end() || *it != t2) {
    waits_for_[t1].insert(it, t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);
  if (it != waits_for_.end()) {
    auto iter = std::lower_bound(it->second.begin(), it->second.end(), t2);
    if (iter != it->second.end() && *iter == t2) {
      it->second.erase(iter);
    }
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  txn_id_t t;
  *txn_id = t = INVALID_TXN_ID;
  for (const auto& u : nodes_) {
    if (acyclic_.find(u) != acyclic_.end()) {
      continue;
    }
    Dfs(u, t, *txn_id);
    mark_.clear();
    if (*txn_id != INVALID_TXN_ID) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto& [u, g] : waits_for_) {
    for (const auto& v : g) {
      edges.emplace_back(u, v);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unordered_map<txn_id_t, table_oid_t> txn_oid;
      std::unordered_map<txn_id_t, RID> txn_rid;
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for (auto &[oid, que] : table_lock_map_) {
        std::vector<txn_id_t> granted;
        que->latch_.lock();
        for (auto const &lock_request : que->request_queue_) {
          if (lock_request->granted_) {
            granted.emplace_back(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted) {
              txn_oid.emplace(lock_request->txn_id_, lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        que->latch_.unlock();
      }
      for (auto &[rid, que] : row_lock_map_) {
        std::vector<txn_id_t> granted;
        que->latch_.lock();
        for (auto const &lock_request : que->request_queue_) {
          if (lock_request->granted_) {
            granted.emplace_back(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted) {
              txn_rid.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        que->latch_.unlock();
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        waits_for_.erase(txn_id);
        nodes_.erase(txn_id);
        for (auto u : nodes_) {
          if (u != txn_id) {
            RemoveEdge(u, txn_id);
          }
        }

        if (txn_oid.find(txn_id) != txn_oid.end()) {
          table_lock_map_[txn_oid[txn_id]]->latch_.lock();
          table_lock_map_[txn_oid[txn_id]]->cv_.notify_all();
          table_lock_map_[txn_oid[txn_id]]->latch_.unlock();
        }
        if (txn_rid.find(txn_id) != txn_rid.end()) {
          row_lock_map_[txn_rid[txn_id]]->latch_.lock();
          row_lock_map_[txn_rid[txn_id]]->cv_.notify_all();
          row_lock_map_[txn_rid[txn_id]]->latch_.unlock();
        }
      }

      waits_for_.clear();
      txn_oid.clear();
      txn_rid.clear();
      acyclic_.clear();
    }
  }
}

void LockManager::Dfs(txn_id_t u, txn_id_t &t, txn_id_t &mx) {
  mark_.emplace(u);
  for (const auto& v : waits_for_[u]) {
    if (acyclic_.find(v) != acyclic_.end()) {
      continue;
    }
    if (mark_.find(v) != mark_.end()) {
      t = v;
      mx = u;
      return;
    }
    Dfs(v, t, mx);
    if (mx != INVALID_TXN_ID) {
      if (t != INVALID_TXN_ID) {
        mx = std::max(mx, u);
        if (t == u) {
          t = INVALID_TXN_ID;
        }
      }
      return;
    }
  }
  mark_.erase(u);
  acyclic_.emplace(u);
}

}  // namespace bustub
