#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_->Init();
  auto cmp = [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &lhs, const Tuple &rhs) {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> q(cmp);
  Tuple tup;
  RID rid;
  while (child_->Next(&tup, &rid)) {
    q.emplace(tup);
    if (q.size() > plan_->GetN()) {
      q.pop();
    }
  }

  while (!q.empty()) {
    stk_.emplace(q.top());
    q.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (stk_.empty()) {
    return false;
  }
  *tuple = stk_.top();
  stk_.pop();
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
