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
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  const std::vector<AbstractExpressionRef> &group_bys = plan_->GetGroupBys();
  const std::vector<AbstractExpressionRef> &aggregates = plan_->GetAggregates();
  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    std::vector<Value> agg_key;
    std::vector<Value> agg_value;
    agg_key.reserve(group_bys.size());
    agg_value.reserve(aggregates.size());
    for (auto &expression : group_bys) {
      agg_key.push_back(expression->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    for (auto &expression : aggregates) {
      agg_value.push_back(expression->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    aht_.InsertCombine(AggregateKey{agg_key}, AggregateValue{agg_value});
  }
  if (aht_.Begin() == aht_.End() && group_bys.empty()) {
    aht_.InsertCombine(AggregateKey{}, aht_.GenerateInitialAggregateValue());
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &key = aht_iterator_.Key();
  const AggregateValue &value = aht_iterator_.Val();
  std::vector<Value> values;
  values.reserve(key.group_bys_.size() + value.aggregates_.size());
  for (const auto &v : key.group_bys_) {
    values.push_back(v);
  }
  for (const auto &v : value.aggregates_) {
    values.push_back(v);
  }
  *tuple = Tuple(values, &plan_->OutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
