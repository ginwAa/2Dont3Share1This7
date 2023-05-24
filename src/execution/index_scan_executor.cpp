//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      index_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      iter_(index_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool locked = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                           LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!locked) {
        throw ExecutionException("IndexScan Executor Get Table Lock Failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("IndexScan Executor Get Table Lock Failed" + e.GetInfo());
    }
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != index_->GetEndIterator()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        bool locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                           table_info_->oid_, (*iter_).second);
        if (!locked) {
          throw ExecutionException("IndexScan Executor Get Table Lock Failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("IndexScan Executor Get Row Lock Failed");
      }
    }
    *rid = (*iter_).second;
    auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    ++iter_;
    return result;
  }
  return false;
}

}  // namespace bustub
