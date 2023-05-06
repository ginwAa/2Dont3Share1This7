//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tup;
  RID rid;
  while (child_->Next(&tup, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tup), MakeAggregateValue(&tup));
  }
  InitCombine();
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> res{aht_iterator_.Key().group_bys_};
  res.insert(res.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple(res, &GetOutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
