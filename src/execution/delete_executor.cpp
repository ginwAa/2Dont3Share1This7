//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("delete table lock failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("delete table lock failed" + e.GetInfo());
  }
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  Tuple tup;
  RID r;
  int cnt = 0;
  while (child_executor_->Next(&tup, &r)) {
    try {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                table_info_->oid_, r)) {
        throw ExecutionException("delete row lock failed");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("delete row lock failed" + e.GetInfo());
    }
    if (table_info_->table_->MarkDelete(r, this->GetExecutorContext()->GetTransaction())) {
      for (auto &index : indexes_) {
        auto key = tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key, r, this->GetExecutorContext()->GetTransaction());
      }
      ++cnt;
    }
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, cnt);
  *tuple = Tuple{values, &GetOutputSchema()};
  finished_ = true;
  return true;
}

}  // namespace bustub
