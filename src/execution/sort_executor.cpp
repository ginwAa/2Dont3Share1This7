#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  Tuple tup;
  RID rid;
  while (child_->Next(&tup, &rid)) {
    data_.emplace_back(tup);
  }

  std::sort(data_.begin(), data_.end(),
            [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &lhs, const Tuple &rhs) {
              for (const auto &order_key : order_bys) {
                auto u = order_key.second->Evaluate(&lhs, schema);
                auto v = order_key.second->Evaluate(&rhs, schema);
                if (static_cast<bool>(u.CompareEquals(v))) {
                  continue;
                }
                bool less = static_cast<bool>(u.CompareLessThan(v));
                return order_key.first == OrderByType::DESC ? !less : less;
              }
              return false;
            });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (j_ >= data_.size()) {
    return false;
  }
  *tuple = data_[j_++];
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
