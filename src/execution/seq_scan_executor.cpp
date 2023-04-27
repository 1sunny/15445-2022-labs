//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      exec_ctx_(exec_ctx),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      cur_(table_info_->table_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() { cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ == table_info_->table_->End()) {
    return false;
  }
  *tuple = (*cur_);
  *rid = (*cur_).GetRid();
  ++cur_;
  return true;
}

}  // namespace bustub
