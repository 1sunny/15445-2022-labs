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
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)),
      txn_(exec_ctx->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  if (!txn_->IsTableExclusiveLocked(table_info_->oid_) &&
      !txn_->IsTableSharedIntentionExclusiveLocked(table_info_->oid_)) {
    try {
      bool locked = lock_manager_->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
      if (!locked) {
        throw ExecutionException("INTENTION_EXCLUSIVE LockTable Fail");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException(e.GetInfo());
    }
  }
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
    try {
      bool locked = lock_manager_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, child_rid);
      if (!locked) {
        throw ExecutionException("EXCLUSIVE LockRow Fail");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException(e.GetInfo());
    }
    if (table_info_->table_->MarkDelete(child_rid, txn)) {
      deleted_++;
      // 通过 Catalog 获取表对应的 indexes
      const std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      // 向该表的所有索引中删除该 tuple
      for (auto index_info : indexes) {
        std::unique_ptr<Index> &index = index_info->index_;
        index->DeleteEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()), child_rid,
            txn);
        // txn_->GetIndexWriteSet()->emplace_back(child_rid, table_info_->oid_, WType::DELETE, child_tuple,
        // index_info->index_oid_, exec_ctx_->GetCatalog());
      }
    }
  }
}

}  // namespace bustub
