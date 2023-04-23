#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  assert(Check());
  // fix bug: add empty check
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool found = leaf_page->Lookup(key, &value, comparator_);
  if (found) {
    result->push_back(value);
  }
  // FindLeafPage中没有对 Page Unpin,因为还要使用
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // !!!
  assert(Check());
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  assert(Check());
  if (IsEmpty()) {
    StartNewTree(key, value);
    assert(Check());
    return true;  // !!!
  }
  bool success = InsertIntoLeaf(key, value, transaction);
  assert(Check());
  return success;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  assert(page);
  // update new root info
  auto *root_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  root_leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);  // !!!
  UpdateRootPageId(true);                                                // !!!
  // insert key & value
  root_leaf_page->Insert(key, value, comparator_);
  // unpin page !!!
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // first find the right leaf page as insertion target
  Page *target_page = FindLeafPage(key);
  auto *leaf_page = reinterpret_cast<LeafPage *>(target_page->GetData());
  ValueType tmp_value;
  // then look through leaf page to see whether insert key exist or not
  bool exist = leaf_page->Lookup(key, &tmp_value, comparator_);
  if (exist) {
    // exist, return
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // !!!
    return false;
  }
  // insert entry
  leaf_page->Insert(key, value, comparator_);
  // deal with split if necessary
  if (leaf_page->GetSize() >= leaf_page->GetMaxSize()) {  // change == to >=
    LeafPage *new_leaf_page = Split(leaf_page);           // !!!
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  // !!!
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // first ask for new page from buffer pool manager
  page_id_t page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&page_id);
  assert(new_page);

  auto *node_page = reinterpret_cast<BPlusTreePage *>(node);
  // move half to new page and insert key to parent
  if (node_page->IsLeafPage()) {
    auto *leaf_page = reinterpret_cast<LeafPage *>(node_page);
    auto *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf_page->Init(new_page->GetPageId(), node_page->GetPageId(), leaf_max_size_);
    leaf_page->MoveHalfTo(new_leaf_page);
    // return new_leaf_page;
  } else {
    auto *inter_page = reinterpret_cast<InternalPage *>(node_page);
    auto *new_inter_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_inter_page->Init(page_id, node_page->GetPageId(), internal_max_size_);
    inter_page->MoveHalfTo(new_inter_page, buffer_pool_manager_);
    // return new_inter_page; error
  }
  // static_cast -> error
  return reinterpret_cast<N *>(new_page);  // !!!
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    // page_id_t new_root_page_id; 直接用 root_page_id
    Page *new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(new_root_page);
    // update new root info
    auto *new_root_inter_page = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_inter_page->Init(new_root_page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);  // !!!
    UpdateRootPageId();                                                                          // !!!
    // populate new root element
    new_root_inter_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // set parent id
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    // unpin page: Split中没有 Unpin new_node对应的 page
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // !!!
    buffer_pool_manager_->UnpinPage(root_page_id_, true);          // !!!
    return;
  }
  // first find the parent page of old_node
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(parent_page);
  auto *parent_inter_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // set parent page id
  new_node->SetParentPageId(parent_page_id);  // !!!
  // !!! 是根据 value 进行比较(populate new root 时初始化过 0位置的 value)
  parent_inter_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // Split中没有 Unpin new_node对应的 page
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // !!!

  // recursion, 和叶子节点判断不一样,这里是大于才 Split
  if (parent_inter_page->GetSize() > parent_inter_page->GetMaxSize()) {
    InternalPage *new_parent_inter_page = Split(parent_inter_page);
    /** insert key at 0 !!! not 1
     * 因为新的 page的第一个元素是从它左边节点得来的,但 0位置以后**会被忽略**,所以将其插入其父节点去 **/
    InsertIntoParent(parent_inter_page, new_parent_inter_page->KeyAt(0), new_parent_inter_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  assert(Check());
  if (IsEmpty()) {
    return;
  }
  Page *leaf_page = FindLeafPage(key);  // unpin it
  auto *leaf_tree_page = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // just delete, the func can handle the case that key is not exist
  int new_leaf_size = leaf_tree_page->RemoveAndDeleteRecord(key, comparator_);
  //  bool unpin_leaf = false;
  if (new_leaf_size < leaf_tree_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_tree_page);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  assert(Check());
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
  // 是根节点并且不满足个数要求
  if (node->IsRootPage()) {
    // 这里直接上转错了
    AdjustRoot(node);
    // 在里面被 UpdateRootPageId unpin了
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return false;
  }
  N *sibling;
  bool left_sibling = FindSibling(node, &sibling);
  int max_size = node->IsLeafPage() ? leaf_max_size_ : internal_max_size_ + 1;
  if (node->GetSize() + sibling->GetSize() >= max_size) {
    Redistribute(sibling, node, left_sibling);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    return false;
  }
  InternalPage *parent;
  GetParent(node, &parent);  // unpin parent
  Coalesce(&sibling, &node, &parent, left_sibling, transaction);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return false;  // Coalesce 中已经把node unpin了
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. GetParent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 * -------------
 * If index == 0, sibling page on right
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int left_sibling,
                              Transaction *transaction) -> bool {
  if (left_sibling == 0) {
    std::swap(*node, *neighbor_node);
  }
  // page id
  int middle_key_idx = (*parent)->ValueIndex((*node)->GetPageId());
  KeyType middle_key = (*parent)->KeyAt(middle_key_idx);
  (*node)->MoveAllTo(*neighbor_node, middle_key, buffer_pool_manager_);  // will set next
  (*parent)->Remove(middle_key_idx);
  // unpin
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);
  // recursion
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    /* 因为最初调用 CoalesceOrRedistribute时传入的 node就是已经 Fetch过的
     * 所以为了符合定义(CoalesceOrRedistribute可以完美的将 node page及过程中的 page unpin)
     * 我们需要对(*parent) Fetch一次*/
    buffer_pool_manager_->FetchPage((*parent)->GetPageId());  // !!!
    CoalesceOrRedistribute((*parent), transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *sibling, N *node, int left_sibling) {
  // get parent page
  InternalPage *parent;
  GetParent(node, &parent);  // unpin parent
  page_id_t page_id = node->GetPageId();
  page_id_t sibling_id = sibling->GetPageId();
  int middle_key_idx = left_sibling ? (parent)->ValueIndex(page_id) : (parent)->ValueIndex(sibling_id);
  KeyType middle_key = parent->KeyAt(middle_key_idx);

  if (left_sibling) {
    // set parent key first, since next line inter's size will change
    // fix bug: node -> sibling
    parent->SetKeyAt(middle_key_idx, sibling->KeyAt(sibling->GetSize() - 1));
    sibling->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
  } else {
    parent->SetKeyAt(middle_key_idx, sibling->KeyAt(1));
    sibling->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  // case 1 old_root_node (internal node) has only one size
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto *old_root_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_page_id = old_root_page->RemoveAndReturnOnlyChild();
    root_page_id_ = new_root_page_id;
    UpdateRootPageId();

    Page *new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
    auto *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);

    //    return true;
  }
  // case 2 : all elements deleted from the B+ tree
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    //    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // fix bug: add empty check
  if (IsEmpty()) {
    return End();
  }
  KeyType key{};
  Page *left_most_page = FindLeafPage(key, true);
  auto *left_most_tree_page = reinterpret_cast<LeafPage *>(left_most_page);
  return IndexIterator(left_most_tree_page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // fix bug: add empty check
  if (IsEmpty()) {
    return End();
  }
  Page *page = FindLeafPage(key, false);
  auto *tree_page = reinterpret_cast<LeafPage *>(page);
  int index = tree_page->KeyIndex(key, comparator_);  // TODO(ahardway): 没有这个 key 的情况?
  return IndexIterator(tree_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  LeafPage *leaf_page = nullptr;
  return IndexIterator(leaf_page, 0, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
  assert(page_id != INVALID_PAGE_ID);
  auto page = buffer_pool_manager_->FetchPage(page_id);
  assert(page);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

/**
 * find a sibling page, the left sibling is the first choice
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @tparam N
 * @param node
 * @param sibling
 * @return true if found left sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, N **sibling) -> bool {
  auto page = FetchPage(node->GetParentPageId());
  assert(page);
  auto *parent = reinterpret_cast<InternalPage *>(page);
  int index = parent->ValueIndex(node->GetPageId());
  int sibling_index = index ? index - 1 : index + 1;
  *sibling = reinterpret_cast<N *>(FetchPage(parent->ValueAt(sibling_index)));
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  return index != 0;
}

/**
 * remember to unpin the left_page after use this function
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param parent_tree_page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetParent(BPlusTreePage *page, InternalPage **parent_tree_page) {
  *parent_tree_page = reinterpret_cast<InternalPage *>(FetchPage(page->GetParentPageId()));
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 *
 * leftMost为 false,找可以包含这个 key的叶子
 * leftMost为 true, 找最左边的叶子
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> Page * {
  assert(root_page_id_ != INVALID_PAGE_ID);
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  assert(page);
  auto *root = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (true) {
    if (root->IsLeafPage()) {
      break;
    }
    auto *inter_page = reinterpret_cast<InternalPage *>(root);
    page_id_t page_id = leftMost ? inter_page->ValueAt(0) : inter_page->Lookup(key, comparator_);
    assert(root->GetPageId() != INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(page_id);
    assert(page);
    root = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  // now, root is leaf page
  return reinterpret_cast<Page *>(root);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsBalanced(page_id_t pid) -> int {
  if (IsEmpty()) {
    return 1;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception("all page are pinned while isBalanced");
  }
  int ret = 0;
  if (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    int last = -2;
    for (int i = 0; i < page->GetSize(); i++) {
      int cur = IsBalanced(page->ValueAt(i));
      if (cur >= 0 && last == -2) {
        last = cur;
        ret = last + 1;
      } else if (last != cur) {
        ret = -1;
        break;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool {
  if (IsEmpty()) {
    return true;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw Exception("all page are pinned while isPageCorr");
  }
  bool ret = true;
  if (node->IsLeafPage()) {
    auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    int size = page->GetSize();
    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    for (int i = 1; i < size; i++) {
      if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
        ret = false;
        break;
      }
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  } else {
    auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    int size = page->GetSize();
    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    std::pair<KeyType, KeyType> left;
    std::pair<KeyType, KeyType> right;
    for (int i = 1; i < size; i++) {
      if (i == 1) {
        ret = ret && IsPageCorr(page->ValueAt(0), left);
      }
      ret = ret && IsPageCorr(page->ValueAt(i), right);
      ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
      ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
      if (!ret) {
        break;
      }
      left = right;
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Check(bool forceCheck) -> bool {
  if (!forceCheck && !open_check_) {
    return true;
  }
  std::pair<KeyType, KeyType> in;
  bool is_page_in_order_and_size_corr = IsPageCorr(root_page_id_, in);
  bool is_bal = (IsBalanced(root_page_id_) >= 0);
  buffer_pool_manager_instance_ = dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_);
  bool is_all_unpin = buffer_pool_manager_instance_->CheckAllUnPined();
  if (!is_page_in_order_and_size_corr) {
    std::cout << "problem in page order or page size" << std::endl;
  }
  if (!is_bal) {
    std::cout << "problem in balance" << std::endl;
  }
  if (!is_all_unpin) {
    std::cout << "problem in page unpin" << std::endl;
  }
  return is_page_in_order_and_size_corr && is_bal && is_all_unpin;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
