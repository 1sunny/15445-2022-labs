#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  curr_ = 0;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [&](Tuple &t1, Tuple &t2) {
    for (auto &p : plan_->GetOrderBy()) {
      const Value &v1 = p.second->Evaluate(&t1, child_executor_->GetOutputSchema());
      const Value &v2 = p.second->Evaluate(&t2, child_executor_->GetOutputSchema());
      CmpBool cmp = v1.CompareEquals(v2);
      if (cmp == CmpBool::CmpTrue || p.first == OrderByType::INVALID) {
        continue;
      }
      // NULL默认放前面
      if (cmp == CmpBool::CmpNull) {
        return v1.IsNull() ? p.first != OrderByType::DESC : p.first == OrderByType::DESC;
      }
      return v1.CompareLessThan(v2) == CmpBool::CmpTrue ? p.first != OrderByType::DESC : p.first == OrderByType::DESC;
    }
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (curr_ < tuples_.size()) {
    *tuple = tuples_[curr_];
    curr_++;
    return true;
  }
  return false;
}

}  // namespace bustub
