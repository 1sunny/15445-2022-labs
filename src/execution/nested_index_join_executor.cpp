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
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      exec_ctx_(exec_ctx),
      left_executor_(std::move(child_executor)),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->inner_table_oid_);

  left_executor_->Init();
  Tuple tuple;
  RID rid;
  left_.clear();
  while (left_executor_->Next(&tuple, &rid)) {
    left_.emplace_back(tuple);
  }
  left_idx_ = 0;

  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  for (; left_idx_ < left_.size(); left_idx_++) {
    std::vector<Value> values;
    size_t count = left_executor_->GetOutputSchema().GetColumnCount();
    for (size_t k = 0; k < count; k++) {
      values.emplace_back(left_[left_idx_].GetValue(&left_executor_->GetOutputSchema(), k));
    }

    auto value = plan_->key_predicate_->Evaluate(&left_[left_idx_], left_executor_->GetOutputSchema());
    if (value.IsNull() && plan_->GetJoinType() != JoinType::LEFT) {
      continue;
    }

    const Schema &schema = Schema({Column("my", plan_->key_predicate_->GetReturnType())});
    std::vector<RID> result;
    index_->ScanKey(Tuple({value}, &schema), &result, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(result.size() <= 1, "result.size() <= 1");

    if (!result.empty()) {
      Tuple inner_tuple;
      bool get = table_info_->table_->GetTuple(result[0], &inner_tuple, exec_ctx_->GetTransaction());
      BUSTUB_ASSERT(get, "get");
      count = plan_->inner_table_schema_->GetColumnCount();
      for (size_t k = 0; k < count; k++) {
        values.emplace_back(inner_tuple.GetValue(plan_->inner_table_schema_.get(), k));
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      for (auto &col : plan_->InnerTableSchema().GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
    } else {
      continue;
    }
    *tuple = Tuple(values, &plan_->OutputSchema());
    left_idx_++;
    return true;
  }
  return false;
}

}  // namespace bustub
