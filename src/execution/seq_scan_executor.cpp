//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool locked = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                           LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == table_info_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
      table_oid_t oid = table_info_->oid_;
      for (auto x : locked_row_set) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, x);
      }
      exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
    }
    return false;
  }
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                         table_info_->oid_, iter_->GetRid());
      if (!locked) {
        throw ExecutionException("SeqScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor Get Row Lock Failed");
    }
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

}  // namespace bustub
