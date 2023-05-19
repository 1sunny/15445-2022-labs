//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      exec_ctx_(exec_ctx),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void InsertExecutor::Init() {
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
  inserted_ = 0;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (inserted_ > 0) {
    return false;
  }
  while (true) {
    // Get the next tuple
    Tuple child_tuple;
    RID child_rid;
    const auto status = child_executor_->Next(&child_tuple, &child_rid);

    if (!status) {
      const Schema schema = Schema({Column("inserted", TypeId::INTEGER)});
      *tuple = Tuple({Value(TypeId::INTEGER, inserted_)}, &schema);
      inserted_++;  // fix BUG
      return true;
    }
    if (table_info_->table_->InsertTuple(child_tuple, &child_rid, txn)) {
      inserted_++;
      try {
        bool locked = lock_manager_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, child_rid);
        if (!locked) {
          throw ExecutionException("EXCLUSIVE LockRow Fail");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo());
      }
      // 通过 Catalog 获取表对应的 indexes
      const std::vector<IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      // 向该表的所有索引中加入该 tuple
      for (auto index_info : indexes) {
        std::unique_ptr<Index> &index = index_info->index_;
        index->InsertEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()), child_rid,
            txn);
        // txn_->GetIndexWriteSet()->emplace_back(child_rid, table_info_->oid_, WType::INSERT, child_tuple,
        // index_info->index_oid_, exec_ctx_->GetCatalog());
      }
    }
  }
}

}  // namespace bustub
