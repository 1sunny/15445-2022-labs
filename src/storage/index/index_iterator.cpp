/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *leaf_page, int index, BufferPoolManager *bpm) {
  leaf_page_ = leaf_page;
  index_ = index;
  bpm_ = bpm;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  assert(index_ < leaf_page_->GetSize());
  if (++index_ == leaf_page_->GetSize()) {
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    // fix bug: unpin here
    bpm_->UnpinPage(leaf_page_->GetPageId(), false);
    if (next_page_id == INVALID_PAGE_ID) {
      leaf_page_ = nullptr;
    } else {
      // bpm_->UnpinPage(leaf_page_->GetPageId(), false);
      index_ = 0;
      Page *next_page = bpm_->FetchPage(next_page_id);
      leaf_page_ = reinterpret_cast<LeafPage *>(next_page->GetData());
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
