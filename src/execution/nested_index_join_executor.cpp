//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_->Init();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple lhs;
  std::vector<Value> res;
  while (child_->Next(&lhs, rid)) {
    auto u = plan_->KeyPredicate()->Evaluate(&lhs, child_->GetOutputSchema());
    std::vector<RID> rids;
    auto key = Tuple({u}, index_info_->index_->GetKeySchema());
    tree_->ScanKey(key, &rids, exec_ctx_->GetTransaction());

    Tuple rhs;
    if (!rids.empty()) {
      table_info_->table_->GetTuple(rids[0], &rhs, exec_ctx_->GetTransaction());
      for (size_t i = 0; i < child_->GetOutputSchema().GetColumnCount(); i++) {
        res.push_back(lhs.GetValue(&child_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        res.push_back(rhs.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple(res, &GetOutputSchema());
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (size_t i = 0; i < child_->GetOutputSchema().GetColumnCount(); i++) {
        res.push_back(lhs.GetValue(&child_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); i++) {
        res.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(res, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
