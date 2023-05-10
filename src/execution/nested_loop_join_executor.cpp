//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  left_exe_ = std::move(left_executor);
  right_exe_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_exe_->Init();
  right_exe_->Init();
  Tuple tup;
  RID rid;
  while (right_exe_->Next(&tup, &rid)) {
    rhs_data_.emplace_back(tup);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID emit_rid;
  while (k_ >= 0 || left_exe_->Next(&lhs_, &emit_rid)) {
    std::vector<Value> res;
    for (size_t i = (k_ < 0 ? 0 : k_); i < rhs_data_.size(); i++) {
      auto &rhs = rhs_data_[i];
      if (Matched(&lhs_, &rhs)) {
        for (size_t j = 0; j < left_exe_->GetOutputSchema().GetColumnCount(); j++) {
          res.emplace_back(lhs_.GetValue(&left_exe_->GetOutputSchema(), j));
        }
        for (size_t j = 0; j < right_exe_->GetOutputSchema().GetColumnCount(); j++) {
          res.emplace_back(rhs.GetValue(&right_exe_->GetOutputSchema(), j));
        }
        *tuple = Tuple(res, &GetOutputSchema());
        k_ = i + 1;
        return true;
      }
    }
    if (k_ < 0 && plan_->GetJoinType() == JoinType::LEFT) {
      for (size_t j = 0; j < left_exe_->GetOutputSchema().GetColumnCount(); j++) {
        res.emplace_back(lhs_.GetValue(&left_exe_->GetOutputSchema(), j));
      }
      for (size_t j = 0; j < right_exe_->GetOutputSchema().GetColumnCount(); j++) {
        res.emplace_back(ValueFactory::GetNullValueByType(right_exe_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple(res, &GetOutputSchema());
      return true;
    }
    k_ = -1;
  }
  return false;
}
auto NestedLoopJoinExecutor::Matched(Tuple *lhs, Tuple *rhs) const -> bool {
  auto res = plan_->Predicate().EvaluateJoin(lhs, left_exe_->GetOutputSchema(), rhs, right_exe_->GetOutputSchema());
  return !res.IsNull() && res.GetAs<bool>();
}

}  // namespace bustub
