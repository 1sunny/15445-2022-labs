//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      exec_ctx_(exec_ctx),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)) {}

void IndexScanExecutor::Init() {
  auto *index = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  cur_ = index->GetBeginIterator();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index->GetMetadata()->GetTableName());
  end_ = index->GetEndIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ == end_) {
    return false;
  }
  *rid = (*cur_).second;
  bool get = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  BUSTUB_ASSERT(get, "get");
  ++cur_;
  return true;
}

}  // namespace bustub
