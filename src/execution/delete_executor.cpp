//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      exec_ctx_(exec_ctx),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  deleted_ = 0;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (deleted_ > 0) {
    return false;
  }
  while (true) {
    // Get the next tuple
    Tuple child_tuple;
    RID child_rid;
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      const Schema schema = Schema({Column("deleted", TypeId::INTEGER)});
      *tuple = Tuple({Value(TypeId::INTEGER, deleted_)}, &schema);
      deleted_++;  // fix BUG
      return true;
    }
    deleted_++;
    // 通过 Catalog 获取表对应的 indexes
    const std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    table_info_->table_->MarkDelete(child_rid, txn);
    // 向该表的所有索引中删除该 tuple
    for (auto index_info : indexes) {
      std::unique_ptr<Index> &index = index_info->index_;
      index->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()),
                         child_rid, txn);
    }
  }
}

}  // namespace bustub
