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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      exec_ctx_(exec_ctx),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  left_.clear();
  right_.clear();
  while (left_executor_->Next(&tuple, &rid)) {
    left_.emplace_back(tuple);
  }
  while (right_executor_->Next(&tuple, &rid)) {
    right_.emplace_back(tuple);
  }
  left_idx_ = 0;
  right_idx_ = 0;
  left_match_.clear();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; left_idx_ < left_.size(); left_idx_++) {
    std::vector<Value> values;
    size_t count = left_executor_->GetOutputSchema().GetColumnCount();
    for (size_t k = 0; k < count; k++) {
      values.emplace_back(left_[left_idx_].GetValue(&left_executor_->GetOutputSchema(), k));
    }
    for (; right_idx_ < right_.size(); right_idx_++) {
      auto match = plan_->Predicate().EvaluateJoin(&left_[left_idx_], left_executor_->GetOutputSchema(),
                                                   &right_[right_idx_], right_executor_->GetOutputSchema());
      if (!match.IsNull() && match.GetAs<bool>()) {
        std::vector<Value> values_copy = values;
        count = right_executor_->GetOutputSchema().GetColumnCount();
        for (size_t k = 0; k < count; k++) {
          values_copy.emplace_back(right_[right_idx_].GetValue(&right_executor_->GetOutputSchema(), k));
        }
        *tuple = Tuple(values_copy, &plan_->OutputSchema());
        left_match_.insert({left_idx_, true});
        right_idx_++;
        return true;
      }
    }
    right_idx_ = 0;
    if (plan_->GetJoinType() == JoinType::LEFT && left_match_.count(left_idx_) == 0) {
      left_match_.insert({left_idx_, true});
      for (auto &col : right_executor_->GetOutputSchema().GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      left_idx_++;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
