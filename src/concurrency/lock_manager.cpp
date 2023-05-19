//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LM2S(LockMode mode) -> const char * {
  switch (mode) {
    case LockMode::SHARED:
      return "SHARED";
    case LockMode::EXCLUSIVE:
      return "EXCLUSIVE";
    case LockMode::INTENTION_SHARED:
      return "INTENTION_SHARED";
    case LockMode::INTENTION_EXCLUSIVE:
      return "INTENTION_EXCLUSIVE";
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return "SHARED_INTENTION_EXCLUSIVE";
    default:
      return "Unknown lock mode";
  }
}

auto LockManager::LT2S(LockType mode) -> const char * {
  switch (mode) {
    case LockType::TABLE:
      return "TABLE";
    case LockType::ROW:
      return "ROW";
  }
}

auto ST2S(TransactionState st) -> const char * {
  switch (st) {
    case TransactionState::GROWING:
      return "GROWING";
    case TransactionState::SHRINKING:
      return "SHRINKING";
    case TransactionState::COMMITTED:
      return "COMMITTED";
    case TransactionState::ABORTED:
      return "ABORTED";
  }
}

void AbortTransaction(Transaction *txn, AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

auto LockManager::CheckIsolationLevel(Transaction *txn, LockMode lock_mode) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED: {
      // S/IS/SIX locks are not required under READ_UNCOMMITTED
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // X, IX locks are allowed in the GROWING state.
      if (txn->GetState() == TransactionState::SHRINKING) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
    case IsolationLevel::READ_COMMITTED: {
      // All locks are allowed in the GROWING state
      // Only IS, S locks are allowed in the SHRINKING state
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
          lock_mode != LockMode::INTENTION_SHARED) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
    case IsolationLevel::REPEATABLE_READ: {
      // All locks are allowed in the GROWING state
      // No locks are allowed in the SHRINKING state
      if (txn->GetState() == TransactionState::SHRINKING) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    }
    default:
      UNREACHABLE("Unknown IsolationLevel");
  }
}

void LockManager::RemoveLockFromTransaction(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                            const RID &rid, LockType lock_type, bool change_state) {
  LOG_DEBUG("[%d:%s] 失去锁: %s %s [%d:%s", txn->GetTransactionId(), ST2S(txn->GetState()), LT2S(lock_type),
            LM2S(lock_mode), oid, rid.ToString().c_str());
  switch (lock_mode) {
    case LockMode::SHARED:
      if (lock_type == LockType::TABLE) {
        txn->GetSharedTableLockSet()->erase(oid);
      } else {
        (*txn->GetSharedRowLockSet())[oid].erase(rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (lock_type == LockType::TABLE) {
        txn->GetExclusiveTableLockSet()->erase(oid);
      } else {
        (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
      }
      break;
    case LockMode::INTENTION_SHARED:
      BUSTUB_ENSURE(lock_type == LockType::TABLE, "lock_type != LockType::TABLE");
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      BUSTUB_ENSURE(lock_type == LockType::TABLE, "lock_type != LockType::TABLE");
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      BUSTUB_ENSURE(lock_type == LockType::TABLE, "lock_type != LockType::TABLE");
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    default:
      UNREACHABLE("Unknown LockMode");
  }
  if (change_state && txn->GetState() == TransactionState::GROWING) {
    ChangeTransactionState(txn, lock_mode);
  }
}

void LockManager::AddLockToTransaction(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                                       LockType lock_type) {
  LOG_DEBUG("[%d:%s] 得到锁: %s %s [%d:%s", txn->GetTransactionId(), ST2S(txn->GetState()), LT2S(lock_type),
            LM2S(lock_mode), oid, rid.ToString().c_str());
  switch (lock_mode) {
    case LockMode::SHARED:
      if (lock_type == LockType::TABLE) {
        txn->GetSharedTableLockSet()->insert(oid);
      } else {
        (*txn->GetSharedRowLockSet())[oid].insert(rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (lock_type == LockType::TABLE) {
        txn->GetExclusiveTableLockSet()->insert(oid);
      } else {
        (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
      }
      break;
    case LockMode::INTENTION_SHARED:
      BUSTUB_ENSURE(lock_type == LockType::TABLE, "lock_type != LockType::TABLE");
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      BUSTUB_ENSURE(lock_type == LockType::TABLE, "lock_type != LockType::TABLE");
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      BUSTUB_ENSURE(lock_type == LockType::TABLE, "lock_type != LockType::TABLE");
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    default:
      UNREACHABLE("Unknown LockMode");
  }
}

auto LockManager::CheckCompatibility(Transaction *txn, const std::list<std::shared_ptr<LockRequest>> &list,
                                     LockMode lock_mode, LockType lock_type) -> bool {
  LOG_DEBUG("[%d:%s] CheckCompatibility Start", txn->GetTransactionId(), ST2S(txn->GetState()));
  // 检查兼容性:
  // 如果已经有事务获取了锁，检查锁的兼容性，如果兼容并且先前锁请求都被授予，才能获取锁，否则只能等待。这里保证了锁的请求不会饥饿。
  for (auto it1 = list.begin(); it1 != list.end(); it1++) {  // NOLINT
    const auto &lock_request = *it1;
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      LOG_DEBUG("[%d:%s] CheckCompatibility true", txn->GetTransactionId(), ST2S(txn->GetState()));
      return true;
    }
    if (!lock_request->granted_) {
      // 虽然当前request没有被 granted_, 但可能它和前面的锁兼容,是可以被 granted_的,只是他还没被唤醒
      // for (auto it2 = list.begin(); it2 != it1; it2++) {
      //   if (!compatible_[lock_request->lock_mode_][(*it2)->lock_mode_]) {
      //     LOG_DEBUG("[%d:%s] CheckCompatibility false1", txn->GetTransactionId(), ST2S(txn->GetState()));
      //     return false;
      //   }
      // }
      return false;
    }
    if (!compatible_[lock_request->lock_mode_][lock_mode]) {
      LOG_DEBUG("[%d:%s] CheckCompatibility false2", txn->GetTransactionId(), ST2S(txn->GetState()));
      return false;
    }
  }
  // 自己请求可能被删除,自己被 ABORT
  return false;
}

void LockManager::ChangeTransactionState(Transaction *txn, LockMode unlock_mode) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      BUSTUB_ENSURE(unlock_mode != LockMode::SHARED, "unlock_mode == LockMode::SHARED in READ_UNCOMMITTED");
      if (unlock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (unlock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (unlock_mode == LockMode::SHARED || unlock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    default:
      UNREACHABLE("Unknown IsolationLevel");
  }
}
auto LockManager::HandleLockRequest(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                                    const std::shared_ptr<LockRequest> &lock_request,
                                    const std::shared_ptr<LockRequestQueue> &request_queue, LockType lock_type,
                                    std::mutex *mu) -> bool {
  std::unique_lock<std::mutex> lk(*mu, std::adopt_lock);
  bool to_upgrade = false;
  std::list<std::shared_ptr<LockRequest>> &queue = request_queue->request_queue_;
  // 如果有相应的请求队列
  // 如果要进行锁升级
  for (auto iter = queue.begin(); iter != queue.end(); iter++) {
    if ((*iter)->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ENSURE((*iter)->granted_, "");
      // 是否已经持有该锁
      if ((*iter)->lock_mode_ == lock_mode) {
        return true;
      }
      // 检查锁升级是否冲突，冲突，abort, throw
      if (!upgrade_[lock_mode][(*iter)->lock_mode_]) {
        AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // why ? only one transaction should be allowed to upgrade its lock on a given resource.
      if (request_queue->upgrading_ != INVALID_TXN_ID) {
        AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
      }
      request_queue->upgrading_ = txn->GetTransactionId();
      to_upgrade = true;
      // 删除旧的记录，同时删除事务中的记录
      RemoveLockFromTransaction(txn, (*iter)->lock_mode_, oid, rid, lock_type, false);
      queue.erase(iter);
      break;
    }
  }
  // 锁不用升级，将新的记录添加在请求队列末尾
  if (!to_upgrade) {
    queue.emplace_back(lock_request);
  } else {
    // 要进行锁升级，将一条新的记录插入在队列最前面一条未授予的记录之前
    std::list<std::shared_ptr<LockRequest>>::iterator iter;
    for (iter = queue.begin(); iter != queue.end(); iter++) {
      if (!(*iter)->granted_) {
        break;
      }
    }
    queue.insert(iter, lock_request);
  }
  // 等待在条件变量上
  while (!CheckCompatibility(txn, queue, lock_mode, lock_type)) {
    request_queue->cv_.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      LOG_DEBUG("[%d:%s] ABORTED %d:[%s", txn->GetTransactionId(), ST2S(txn->GetState()), oid, rid.ToString().c_str());
      // 删除加锁记录
      RemoveLockRequest(txn, &queue);
      request_queue->cv_.notify_all();
      return false;
    }
  }
  if (to_upgrade) {  // 成功升级后立刻将upgrading_重置
    if (request_queue->upgrading_ == txn->GetTransactionId()) {
      request_queue->upgrading_ = INVALID_TXN_ID;  // fix bug
    }
  }
  lock_request->granted_ = true;
  // 更新事务的加锁集合
  AddLockToTransaction(txn, lock_mode, oid, rid, lock_type);
  LOG_DEBUG("[%d:%s] 成功 %s %s : [%d : %s", txn->GetTransactionId(), ST2S(txn->GetState()), LT2S(lock_type),
            to_upgrade ? "upgrade" : "lock", oid, rid.ToString().c_str());
  request_queue->cv_.notify_all();  // fix bug
  return true;
}

void LockManager::RemoveLockRequest(Transaction *txn, std::list<std::shared_ptr<LockRequest>> *rq) {
  // 删除加锁记录
  int removed = 0;
  for (auto it = rq->begin(); it != rq->end();) {
    auto nxt = it;
    nxt++;
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ENSURE((*it)->granted_ || txn->GetState() == TransactionState::ABORTED, "RemoveLockRequest");
      LOG_DEBUG("[%d:%s] 加锁记录: [%s] 被删除", txn->GetTransactionId(), ST2S(txn->GetState()),
                LM2S((*it)->lock_mode_));
      rq->erase(it);
      removed++;
    }
    it = nxt;
  }
  BUSTUB_ENSURE(removed == 1, "removed != 1");
}

auto LockManager::GetLocked(Transaction *txn, const table_oid_t &oid, const RID &rid, LockType lock_type)
    -> std::vector<LockManager::LockMode> {
  std::vector<LockMode> locks;
  if (lock_type == LockType::TABLE) {
    if (txn->IsTableSharedLocked(oid)) {
      locks.emplace_back(LockMode::SHARED);
    }
    if (txn->IsTableExclusiveLocked(oid)) {
      locks.emplace_back(LockMode::EXCLUSIVE);
    }
    if (txn->IsTableIntentionSharedLocked(oid)) {
      locks.emplace_back(LockMode::INTENTION_SHARED);
    }
    if (txn->IsTableIntentionExclusiveLocked(oid)) {
      locks.emplace_back(LockMode::INTENTION_EXCLUSIVE);
    }
    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      locks.emplace_back(LockMode::SHARED_INTENTION_EXCLUSIVE);
    }
  } else if (lock_type == LockType::ROW) {
    if (txn->IsRowSharedLocked(oid, rid)) {
      locks.emplace_back(LockMode::SHARED);
    }
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      locks.emplace_back(LockMode::EXCLUSIVE);
    }
  } else {
    UNREACHABLE("Unknown LockType");
  }
  return locks;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LOG_DEBUG("in LockTable");
  // 1.根据隔离级别，判断锁的请求是否合理
  CheckIsolationLevel(txn, lock_mode);
  // 2.检查锁是否要升级，以及升级是否合理，如果不合理，abort，throw。如果重复申请相同的锁，直接返回
  std::shared_ptr<LockRequest> lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  // 3.lock_table_map如果没有对应的请求队列，那么添加一条请求队列，添加记录，授予锁
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.insert({oid, std::make_shared<LockRequestQueue>()});
  }
  auto request_queue = table_lock_map_.find(oid)->second;
  request_queue->latch_.lock();
  table_lock_map_latch_.unlock();
  bool status = HandleLockRequest(txn, lock_mode, oid, RID{}, lock_request, request_queue, LockType::TABLE,
                                  &request_queue->latch_);
  LOG_DEBUG("out LockTable");
  return status;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_DEBUG("in UnlockTable");
  // 检查是否还持有相应表的行锁
  if (!(*txn->GetSharedRowLockSet())[oid].empty() || !(*txn->GetExclusiveRowLockSet())[oid].empty()) {
    LOG_DEBUG("out UnlockTable");
    AbortTransaction(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  const std::vector<LockMode> &locked = GetLocked(txn, oid, RID{}, LockType::TABLE);
  if (!locked.empty()) {
    BUSTUB_ENSURE(locked.size() == 1, "");
    // 找到对应请求队列，加锁request_queue，解锁 lock_table_map
    table_lock_map_latch_.lock();
    std::shared_ptr<LockRequestQueue> &request_queue = table_lock_map_.find(oid)->second;
    request_queue->latch_.lock();
    table_lock_map_latch_.unlock();
    // 删除加锁记录
    RemoveLockRequest(txn, &request_queue->request_queue_);
    // 更新事务锁集合，事务状态
    RemoveLockFromTransaction(txn, locked[0], oid, RID{}, LockType::TABLE, true);
    LOG_DEBUG("[%d:%s] 成功 UnlockTable: %d", txn->GetTransactionId(), ST2S(txn->GetState()), oid);
    // 在条件变量上 notify_all() 唤醒所有等待的线程
    request_queue->cv_.notify_all();
    request_queue->latch_.unlock();
    LOG_DEBUG("out UnlockTable");
    return true;
  }
  LOG_DEBUG("out UnlockTable");
  AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  UNREACHABLE("");
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_DEBUG("in LockRow");
  CheckIsolationLevel(txn, lock_mode);
  // check supported lock modes, Row locking should not support Intention locks
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    LOG_DEBUG("out LockRow");
    AbortTransaction(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    // if an exclusive lock is attempted on a row, the transaction must hold either X, IX, or SIX on the table.
    bool table_exclusive_locked = txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
                                  txn->IsTableSharedIntentionExclusiveLocked(oid);
    if (!table_exclusive_locked) {
      LOG_DEBUG("out LockRow");
      AbortTransaction(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  std::shared_ptr<LockRequest> lock_request =
      std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  // 为什么 SHARED 不需要父节点带有 S,IS,SIX锁?
  row_lock_map_latch_.lock();
  auto it = row_lock_map_.find(rid);
  if (it == row_lock_map_.end()) {
    row_lock_map_.insert({rid, std::make_shared<LockRequestQueue>()});
  }
  auto request_queue = row_lock_map_.find(rid)->second;
  request_queue->latch_.lock();
  row_lock_map_latch_.unlock();
  bool status =
      HandleLockRequest(txn, lock_mode, oid, rid, lock_request, request_queue, LockType::ROW, &request_queue->latch_);
  LOG_DEBUG("out LockRow");
  return status;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_DEBUG("in UnlockRow");
  const std::vector<LockMode> &locked = GetLocked(txn, oid, rid, LockType::ROW);
  if (!locked.empty()) {
    BUSTUB_ENSURE(locked.size() == 1, "");
    row_lock_map_latch_.lock();
    std::shared_ptr<LockRequestQueue> &request_queue = row_lock_map_[rid];
    request_queue->latch_.lock();
    row_lock_map_latch_.unlock();
    // 删除加锁记录
    RemoveLockRequest(txn, &request_queue->request_queue_);
    // 更新事务锁集合，事务状态
    RemoveLockFromTransaction(txn, locked[0], oid, rid, LockType::ROW, true);
    LOG_DEBUG("[%d:%s] 成功 UnlockRow: [%d : %s", txn->GetTransactionId(), ST2S(txn->GetState()), oid,
              rid.ToString().c_str());
    // 在条件变量上 notify_all() 唤醒所有等待的线程
    request_queue->cv_.notify_all();
    request_queue->latch_.unlock();
    LOG_DEBUG("out UnlockRow1");
    return true;
  }
  LOG_DEBUG("out UnlockRow2");
  AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_[t2].find(t1) == waits_for_[t2].end()) {
    LOG_DEBUG("添加 %d -> %d", t1, t2);
    waits_for_[t2].insert(t1);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_[t2].find(t1) != waits_for_[t2].end()) {
    LOG_DEBUG("删除 %d -> %d", t1, t2);
    waits_for_[t2].erase(t1);
  }
}

auto LockManager::Dfs(txn_id_t txn_id) -> txn_id_t {
  vis_[txn_id] = true;
  st_.insert(txn_id);
  for (auto v : waits_for_[txn_id]) {
    if (st_.find(v) != st_.end()) {
      txn_id_t res = 0;
      for (txn_id_t i = txn_id; i != parent_[v]; i = parent_[i]) {
        res = std::max(res, i);
      }
      return res;
    }
    parent_[v] = txn_id;
    txn_id_t id = Dfs(v);
    if (id != -1) {
      return id;
    }
  }
  st_.erase(txn_id);
  return -1;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  vis_.clear();
  for (const auto &p : waits_for_) {  // NOLINT
    txn_id_t u = p.first;
    if (!vis_[u]) {
      st_.clear();
      parent_.clear();
      txn_id_t id = Dfs(u);
      if (id != -1) {
        *txn_id = id;
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (const auto &p : waits_for_) {
    txn_id_t v = p.first;
    for (auto u : p.second) {
      edges.emplace_back(u, v);
    }
  }
  return edges;
}

template <typename T>
void LockManager::BuildGraph(std::unordered_map<T, std::shared_ptr<LockRequestQueue>> &map) {
  for (const auto &it : map) {
    const std::shared_ptr<LockRequestQueue> &request_queue = it.second;
    request_queue->latch_.lock();
    std::vector<txn_id_t> owners;
    for (const auto &request : request_queue->request_queue_) {
      // && TransactionManager::GetTransaction(request->txn_id_)->GetState() != TransactionState::ABORTED
      if (request->granted_) {
        owners.emplace_back(request->txn_id_);
      }
    }

    for (const auto &request : request_queue->request_queue_) {
      if (!request->granted_) {
        for (auto owner : owners) {
          AddEdge(owner, request->txn_id_);
        }
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    LOG_DEBUG("weak up !");
    table_lock_map_latch_.lock();
    row_lock_map_latch_.lock();
    while (true) {
      waits_for_.clear();
      BuildGraph(table_lock_map_);
      BuildGraph(row_lock_map_);
      txn_id_t txn_id;
      bool has_cycle = HasCycle(&txn_id);
      if (has_cycle) {
        LOG_DEBUG("ABORT %d", txn_id);
        TransactionManager::GetTransaction(txn_id)->SetState(TransactionState::ABORTED);
      }
      for (const auto &request_queue : table_lock_map_) {
        if (has_cycle) {
          for (const auto &request : request_queue.second->request_queue_) {
            if (request->txn_id_ == txn_id) {
              request_queue.second->cv_.notify_all();
              break;
            }
          }
        }
        request_queue.second->latch_.unlock();
      }
      for (const auto &request_queue : row_lock_map_) {
        if (has_cycle) {
          for (const auto &request : request_queue.second->request_queue_) {
            if (request->txn_id_ == txn_id) {
              request_queue.second->cv_.notify_all();
              break;
            }
          }
        }
        request_queue.second->latch_.unlock();
      }
      if (!has_cycle) {
        break;
      }
    }
    LOG_DEBUG("leave weak up !");
    row_lock_map_latch_.unlock();
    table_lock_map_latch_.unlock();
  }
}

}  // namespace bustub
