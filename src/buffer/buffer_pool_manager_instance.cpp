//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

// pick the replacement frame from either the free list or the replacer (always find from the free list first)
auto BufferPoolManagerInstance::GetFreeFrame() -> frame_id_t {
  frame_id_t res;
  if (!free_list_.empty()) {
    res = *(free_list_.begin());
    free_list_.pop_front();
  } else if (!replacer_->Evict(&res)) {
    return -1;
  }
  return res;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // nullptr if all frames are currently in use and not evictable
  size_t i;
  for (i = 0; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {
      break;
    }
  }
  if (i == pool_size_) {
    return nullptr;
  }
  // pick a victim page
  frame_id_t frame_id = GetFreeFrame();
  if (frame_id == -1) {
    return nullptr;
  }
  // get a new page id
  page_id_t new_page_id = AllocatePage();
  Page *page = &pages_[frame_id];
  // write it back to the disk first
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  // 先删除原有的表项,如果后面获取原来的表项,需要从磁盘重新读取
  page_table_->Remove(page->page_id_);
  // add to the page table.
  page_table_->Insert(new_page_id, frame_id);
  // update metadata, zero out memory
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->page_id_ = new_page_id;
  page->ResetMemory();
  // Remember to record the access history of the frame
  replacer_->RecordAccess(frame_id);
  // Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  replacer_->SetEvictable(frame_id, false);
  // set the page ID output parameter.
  *page_id = new_page_id;
  //  Return a pointer to P.
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // 1. Search the page table for the requested page (P).
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    // 1.1 If P exists, pin it and return it immediately.
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }
  // 1.2 If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //     Note that pages are always found from the free list first.
  frame_id = GetFreeFrame();
  if (frame_id == -1) {
    return nullptr;
  }
  Page *victim_page = &pages_[frame_id];
  // 2. If R is dirty, write it back to the disk.
  if (victim_page->is_dirty_) {
    disk_manager_->WritePage(victim_page->page_id_, victim_page->data_);
  }
  // 3. Delete R from the page table and insert P.
  page_table_->Remove(victim_page->page_id_);
  page_table_->Insert(page_id, frame_id);
  // 4. Update P's metadata,
  victim_page->page_id_ = page_id;
  victim_page->is_dirty_ = false;
  victim_page->pin_count_ = 1;
  // need to Pin !!!
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  // read in the page content from disk
  disk_manager_->ReadPage(page_id, victim_page->data_);
  // and then return a pointer to P.
  return victim_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // If page_id is not in the buffer pool or its pin count is already 0, return false
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // BUSTUB_ENSURE(pages_[frame_id].GetPinCount() > 0, "page id: " + std::to_string(pages_[frame_id].GetPageId()));
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    // the frame should be evictable by the replacer
    replacer_->SetEvictable(frame_id, true);
    // 这里刷入磁盘? no !!! see the tests 应该在一个page(frame)被置换出去时(new,fetch)再刷盘
  }
  // set the dirty flag on the page to indicate if the page was modified
  // is_dirty为true表示它脏了,但是为false不代表它就是干净的,比如原本就是脏的.所以不能简单的赋值
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_id != INVALID_PAGE_ID && page_table_->Find(page_id, frame_id)) {
    // REGARDLESS of the dirty flag
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // If page_id is not in the buffer pool, do nothing and return true.
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // pinned and cannot be deleted, return false immediately
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    // UNREACHABLE("page->pin_count_ > 0"); 单线程时不会到这,多线程有可能
    return false;
  }
  // 不检查 dirty ? 需要!!!
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  // deleting the page from the page table
  page_table_->Remove(page_id);
  // stop tracking the frame in the replacer
  replacer_->Remove(frame_id);
  // add the frame back to the free list
  free_list_.emplace_back(frame_id);
  // reset the page's memory and metadata
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  // call DeallocatePage() to imitate freeing the page on the disk.
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

// DEBUG
auto BufferPoolManagerInstance::CheckAllUnPined() -> bool {
  bool res = true;
  for (size_t i = 1; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      std::cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << std::endl;
    }
  }
  return res;
}
}  // namespace bustub
