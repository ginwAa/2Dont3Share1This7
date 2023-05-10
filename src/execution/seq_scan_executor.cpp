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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_)) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                  table_info_->oid_)) {
        throw ExecutionException("seqscan table lock failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("seqscan table lock failed" + e.GetInfo());
    }
  }
  iter_ = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  try {
    if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->GetLockManager()->UnlockRow(
              exec_ctx_->GetTransaction(), exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_, *rid)) {
        throw ExecutionException("unlock row share failed");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
  if (iter_ != table_info_->table_->End()) {
    try {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_,
                                                (*iter_).GetRid())) {
        throw ExecutionException("lock row  intention share failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }
    *tuple = *(iter_++);
    *rid = tuple->GetRid();

    return true;
  }
  return false;
}

}  // namespace bustub
