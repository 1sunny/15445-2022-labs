//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  // fix bug
  assert(max_size >= 3);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array_ offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 1 && index < GetSize());
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  int size = GetSize();
  for (int i = 0; i < size; ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return INVALID_PAGE_ID;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array_
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0 && index < GetSize());
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  int size = GetSize();
  // should use binary
  // start with index 1, since index 0 has no key(just a pointer)
  int i;
  for (i = 1; i < size; ++i) {
    // fix bug: < -> >
    if (comparator(array_[i].first, key) > 0) {
      return array_[i - 1].second;
    }
  }
  // 当前 key 比现存的所有key值都大
  return array_[i - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1] = {new_key, new_value};
  // fix bug: add set size
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  // start from 0, !!!!!!
  int i = 0;
  while (array_[i].second != old_value) {
    i++;
  }
  int size = GetSize();
  // fix bug: j>i -> j>=i -> j>=i+2
  for (int j = size; j >= i + 2; --j) {
    array_[j] = array_[j - 1];
  }
  // fix bug: array_[i] -> array_[i+1]
  array_[i + 1] = {new_key, new_value};
  SetSize(size + 1);
  return size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  //  for (int i = size / 2, j = 0; i < size; ++i, ++j) {
  //    recipient->array_[j] = array_[i];
  //  }
  // eg. size=5, [k0,p0],[k1,p1],[k2,p2],[k3,p3],[k4,p4] -> [k0,p0],[k1,p1] & [k2,p2],[k3,p3],[k4,p4]
  /** size=5, [k0,p0],[k1,p1],[k2,p2],[k3,p3],[k4,p4] -> [k0,p0],[k1,p1],[k2,p2] & [k3,p3],[k4,p4]
   * https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html
   * 这里我先改成和网站一样的移动方式
   * **/
  //     [k2,p2] 会被填充到 0的位置,所以其实在 recipient里,它只有 p2有用,但会插入它们共同的父节点中去
  recipient->CopyNFrom(&array_[(size + 1) / 2], size - (size + 1) / 2, buffer_pool_manager);
  SetSize((size + 1) / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int my_size = GetSize();
  assert(my_size == 0);
  int new_size = my_size + size;
  assert(new_size <= GetMaxSize());
  // 所以其实第一个元素的 key会被忽略,只有 value有用
  for (int i = my_size; i < new_size; ++i) {
    array_[i] = items[i];
    Page *child_page = buffer_pool_manager->FetchPage(items[i].second);
    assert(child_page);
    auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_tree_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  SetSize(new_size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array_ offset)
 * NOTE: store key&value pair continuously after deletion
 * 因为移除的都是两个子节点的分界点,所以 index >= 1
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  assert(index >= 1 && index < size);
  // fix bug
  //  for (int i = size; i > index; --i) {
  //    array_[i] = array_[i-1];
  //  }
  for (int i = index; i < size - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  SetSize(size - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  SetSize(0);
  return array_[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  MappingType pair = {middle_key, array_[0].second};
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  for (int i = 1; i < size; ++i) {
    // fix bug: add recipient->
    recipient->CopyLastFrom(array_[i], buffer_pool_manager);
  }
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int re_size = recipient->GetSize();
  // fix bug: resize-1 -> resize
  recipient->array_[re_size].first = middle_key;
  recipient->array_[re_size].second = array_[0].second;
  // adopt child page
  Page *page = buffer_pool_manager->FetchPage(array_[0].second);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page);
  tree_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(array_[0].second, true);

  for (int i = 0; i < size - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  recipient->SetSize(re_size + 1);
  SetSize(size - 1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array_[size] = pair;
  Page *page = buffer_pool_manager->FetchPage(pair.second);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page);
  tree_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
  SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array_ to position the middle_key at
 * the right place. You also need to use BufferPoolManager to persist changes to the parent page id for those pages that
 * are moved to the recipient 更新移到接收者的元素的 parent page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int re_size = recipient->GetSize();
  // 腾出空间
  for (int i = re_size; i >= 0 + 1; --i) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  // set the first pointer
  recipient->array_[0].second = array_[size - 1].second;
  // adopt child page
  Page *page = buffer_pool_manager->FetchPage(recipient->array_[0].second);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page);
  tree_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(recipient->array_[0].second, true);
  // set shared parent key
  recipient->array_[1].first = middle_key;
  recipient->SetSize(recipient->GetSize() + 1);
  SetSize(size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  // 腾出空间
  for (int i = size; i >= 1 + 1; --i) {
    array_[i] = array_[i - 1];
  }
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
