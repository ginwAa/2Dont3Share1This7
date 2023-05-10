//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("insert table lock failed");
    }
  } catch (TransactionAbortException e) {
    throw ExecutionException("insert table lock failed" + e.GetInfo());
  }
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  Tuple tup;
  RID emit_rid;
  int cnt = 0;
  while (child_executor_->Next(&tup, &emit_rid)) {
    if (table_info_->table_->InsertTuple(tup, rid, exec_ctx_->GetTransaction())) {
      try {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  table_info_->oid_, *rid)) {
          throw ExecutionException("insert row lock failed");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException("insert row lock failed" + e.GetInfo());
      }

      for (auto &index : indexes_) {
        auto key = tup.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, *rid, this->GetExecutorContext()->GetTransaction());
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
