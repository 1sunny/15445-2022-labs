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
      cur_(table_info_->table_->Begin(exec_ctx->GetTransaction())),
      txn_(exec_ctx->GetTransaction()),
      lock_manager_(exec_ctx_->GetLockManager()) {}

void SeqScanExecutor::Init() {
  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn_->IsTableSharedLocked(table_info_->oid_) && !txn_->IsTableExclusiveLocked(table_info_->oid_) &&
        !txn_->IsTableIntentionExclusiveLocked(table_info_->oid_) &&
        !txn_->IsTableSharedIntentionExclusiveLocked(table_info_->oid_)) {
      try {
        bool locked = lock_manager_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
        if (!locked) {
          throw ExecutionException("INTENTION_SHARED LockTable Fail");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo());
      }
    }
  }
  cur_ = table_info_->table_->Begin(txn_);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ == table_info_->table_->End()) {
    if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        // 先将要删除的提取出来,不要边遍历迭代器边删除!!!
        std::vector<RID> row_lock_set;
        for (const auto locked_rid : txn_->GetSharedRowLockSet()->at(table_info_->oid_)) {
          row_lock_set.push_back(locked_rid);
        }
        for (const auto locked_rid : row_lock_set) {
          lock_manager_->UnlockRow(txn_, table_info_->oid_, locked_rid);
        }
        if (txn_->IsTableSharedLocked(table_info_->oid_)) {
          lock_manager_->UnlockTable(txn_, table_info_->oid_);
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo());
      }
    }
    return false;
  }
  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn_->IsRowExclusiveLocked(table_info_->oid_, (*cur_).GetRid())) {
      try {
        bool locked = lock_manager_->LockRow(txn_, LockManager::LockMode::SHARED, table_info_->oid_, (*cur_).GetRid());
        if (!locked) {
          throw ExecutionException("SHARED LockRow Fail");
        }
      } catch (TransactionAbortException e) {
        throw ExecutionException(e.GetInfo());
      }
    }
  }
  *tuple = (*cur_);
  *rid = (*cur_).GetRid();
  ++cur_;
  return true;
}

}  // namespace bustub
