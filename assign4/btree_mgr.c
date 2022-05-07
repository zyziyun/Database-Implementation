#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "string.h"


#include <string.h>
#include <stdlib.h>
#include <math.h>

// init and shutdown index manager
RC initIndexManager (void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

RC shutdownIndexManager () {
    shutdownStorageManager();
    return RC_OK;
}

/**
 * @brief serialize Btree header
 * memory layout: n  |  keyType  | nodes | entries
 * 
 * @param mgmtData 
 * @return char* 
 */
char *
serializeBtreeHeader(BTreeMtdt *mgmtData) {
    char *header = calloc(1, PAGE_SIZE);
    int offset = 0;
    *(int *) (header + offset) = mgmtData->n;
    offset += sizeof(int);
    *(int *) (header + offset) = mgmtData->keyType;
    offset += sizeof(int);
    *(int *) (header + offset) = mgmtData->nodes;
    offset += sizeof(int);
    *(int *) (header + offset) = mgmtData->entries;
    offset += sizeof(int);
    return header;
}

/**
 * @brief deserialize Btree header from file to metadata
 * 
 * @param header 
 * @return BTreeMtdt* 
 */
BTreeMtdt *
deserializeBtreeHeader(char *header) {
    int offset = 0;
    BTreeMtdt *mgmtData = MAKE_TREE_MTDT();
    mgmtData->n = *(int *) (header + offset);
    offset += sizeof(int);
    mgmtData->keyType = *(int *) (header + offset);
    offset += sizeof(int);
    mgmtData->nodes = *(int *) (header + offset);
    offset += sizeof(int);
    mgmtData->entries = *(int *) (header + offset);
    offset += sizeof(int);
    return mgmtData;
}

/**
 * @brief Create a Btree index file
 * 
 * @param idxId 
 * @param keyType 
 * @param n 
 * @return RC 
 */
RC createBtree (char *idxId, DataType keyType, int n) {
	RC result;
    BTreeMtdt *mgmtData = MAKE_TREE_MTDT();
    mgmtData->keyType = keyType;
    mgmtData->n = n;
    mgmtData->entries = 0;
    mgmtData->nodes = 0;
    char *header = serializeBtreeHeader(mgmtData);
    result = writeStrToPage(idxId, 0, header);
    free(mgmtData);
    free(header);
    return result;
}

/**
 * @brief Open a Btree index file
 * 
 * @param tree 
 * @param idxId 
 * @return RC 
 */
RC openBtree (BTreeHandle **tree, char *idxId) {
    int offset = 0;
    *tree = MAKE_TREE_HANDLE();
    BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	BM_PageHandle *phHeader = MAKE_PAGE_HANDLE();
  	initBufferPool(bm, idxId, 10, RS_LRU, NULL);
	pinPage(bm, phHeader, 0);  
    BTreeMtdt *mgmtData = deserializeBtreeHeader(phHeader->data);

    if (mgmtData->nodes == 0) {
        mgmtData->root = NULL;
    }
    mgmtData->minLeaf = (mgmtData->n + 1) / 2;
    mgmtData->minNonLeaf = (mgmtData->n + 2) / 2 - 1;
    mgmtData->ph = ph;
    mgmtData->bm = bm;
    (*tree)->keyType = mgmtData->keyType;
    (*tree)->mgmtData = mgmtData;
    (*tree)->idxId = idxId;
    free(phHeader);
    return RC_OK;
}

/**
 * @brief Close a Btree index file
 * 
 * @param tree 
 * @return RC 
 */
RC closeBtree (BTreeHandle *tree) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
	// write back
    // char *header = serializeBtreeHeader(mgmtData);
    // writeStrToPage(tree->idxId, 0, header);
    // free(header);
	shutdownBufferPool(mgmtData->bm);
	free(mgmtData->ph);
    free(mgmtData);
    free(tree);
    return RC_OK;
}

/**
 * @brief Delete a Btree index file
 * 
 * @param idxId 
 * @return RC 
 */
RC deleteBtree (char *idxId) {
    return destroyPageFile(idxId);
}

/**
 * @brief Get the Num Nodes object
 * 
 * @param tree 
 * @param result 
 * @return RC 
 */
RC getNumNodes (BTreeHandle *tree, int *result) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    *result = mgmtData->nodes;
    return RC_OK;
}

/**
 * @brief Get the Num Entries object
 * 
 * @param tree 
 * @param result 
 * @return RC 
 */
RC getNumEntries (BTreeHandle *tree, int *result) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    *result = mgmtData->entries;
    return RC_OK;
}

/**
 * @brief Get the Key Type object
 * 
 * @param tree 
 * @param result 
 * @return RC 
 */
RC getKeyType (BTreeHandle *tree, DataType *result) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    *result = mgmtData->keyType;
    return RC_OK;
}

/**
 * @brief   if key > sign: 1; 
 *          if key < sign: -1; 
 *          if key == sign: 0;
 *          if key != sign: -1; // bool
 * 
 * @param key 
 * @param realkey 
 * @return int 
 */
int
compareValue(Value *key, Value *sign) {
    int result;
    switch (key->dt)
    {
        case DT_INT:
            if (key->v.intV == sign->v.intV) {
                result = 0;
            } else {
                result = (key->v.intV > sign->v.intV) ? 1 : -1;
            }
            break;
        case DT_FLOAT:
            if (key->v.floatV == sign->v.floatV) {
                result = 0;
            } else {
                result = (key->v.floatV > sign->v.floatV) ? 1 : -1;
            }
            break;
        case DT_STRING:
            result = strcmp(key->v.stringV, sign->v.stringV);
            break;
        case DT_BOOL:
            // Todo: not confirm
            result = (key->v.boolV == sign->v.boolV) ? 0 : -1;
        default:
            break;
    }
    return result;
}

/**
 * @brief find leaf node
 * 
 * @param node 
 * @param key 
 * @return BTreeNode* 
 */
BTreeNode* findLeafNode(BTreeNode *node, Value *key) {
    if (node->type == LEAF_NODE) {
        return node;
    }
    for (int i = 0; i < node->keyNums; i++) {
    
        if (compareValue(key, node->keys[i]) < 0) {
            return findLeafNode((BTreeNode *) node->ptrs[i], key);
        }
    }
    return findLeafNode((BTreeNode *) node->ptrs[node->keyNums], key);
}

/**
 * @brief find entry in node
 * 
 * @param node 
 * @param key 
 * @return RID* 
 */
RID *
findEntryInNode(BTreeNode *node, Value *key) {
    for (int i = 0; i < node->keyNums; i++) {
        if (compareValue(key, node->keys[i]) == 0) {
            return (RID *) node->ptrs[i];
        }
    }
    return NULL;
}

/**
 * @brief find key
 * 
 * @param tree 
 * @param key 
 * @param result 
 * @return RC 
 */
RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    // 1. Find correct leaf node L for k
    BTreeNode* leafNode = findLeafNode(mgmtData->root, key);
    if (leafNode == NULL) {
        return RC_IM_KEY_NOT_FOUND;
    }
    // 2. Find the position of entry in a node, binary search
    RID *r = findEntryInNode(leafNode, key);
    if (r == NULL) {
        return RC_IM_KEY_NOT_FOUND;
    }
    (*result) = (*r);
    return RC_OK;
}

/**
 * @brief Create a Node object block
 * 
 * @param mgmtData 
 * @return BTreeNode* 
 */
BTreeNode *
createNode(BTreeMtdt *mgmtData) {
    mgmtData->nodes += 1;
    BTreeNode * node = MAKE_TREE_NODE();
    // insert first and then split
    node->keys = malloc((mgmtData->n+1) * sizeof(Value *));
    node->keyNums = 0;
    node->next = NULL;
    node->parent = NULL;
    return node;
}

/**
 * @brief Create a Leaf Node object Block
 * 
 * @param mgmtData 
 * @return BTreeNode* 
 */
BTreeNode *
createLeafNode(BTreeMtdt *mgmtData) {
    BTreeNode * node = createNode(mgmtData);
    node->type = LEAF_NODE;
    node->ptrs = (void *)malloc((mgmtData->n+1) * sizeof(void *));
    return node;
}

/**
 * @brief Create a Non Leaf Node object
 * 
 * @param mgmtData 
 * @return BTreeNode* 
 */
BTreeNode *
createNonLeafNode(BTreeMtdt *mgmtData) {
    BTreeNode * node = createNode(mgmtData);
    node->type = Inner_NODE;
    node->ptrs = (void *)malloc((mgmtData->n+2) * sizeof(void *));
    return node;
}

/**
 * @brief Get the Insert Pos object
 * 
 * @param node 
 * @param key 
 * @return int 
 */
int getInsertPos(BTreeNode* node, Value *key) {
    int insert_pos = node->keyNums;
    for (int i = 0; i < node->keyNums; i++) {
        if (compareValue(node->keys[i], key) >= 0) {
            insert_pos = i;
            break;
        }
    }
    return insert_pos;
}

/**
 * @brief build rid
 * 
 * @param rid 
 * @return RID* 
 */
RID*
buildRID(RID *rid) {
    RID *r = (RID *)malloc(sizeof(RID));
    r->page = rid->page;
    r->slot = rid->slot;
    return r;
}

/**
 * @brief insert entry into leaf node
 * 
 * @param node 
 * @param key 
 * @param rid 
 * @param mgmtData 
 */
void 
insertIntoLeafNode(BTreeNode* node,  Value *key, RID *rid, BTreeMtdt *mgmtData) {
    int insert_pos = getInsertPos(node, key);
    for (int i = node->keyNums; i >= insert_pos; i--) {
        node->keys[i] = node->keys[i-1];
        node->ptrs[i] = node->ptrs[i-1];
    }
    node->keys[insert_pos] = key;
    node->ptrs[insert_pos] = buildRID(rid);
    node->keyNums += 1;
    mgmtData->entries += 1;
}

/**
 * @brief split leaf node
 * 
 * @param node 
 * @param mgmtData 
 */
BTreeNode *
splitLeafNode(BTreeNode* node, BTreeMtdt *mgmtData) {
    BTreeNode * new_node = createLeafNode(mgmtData);
    // split right index
    int rpoint = (node->keyNums + 1) / 2;
    for (int i = rpoint; i < node->keyNums; i++) {
        int index = node->keyNums - rpoint - 1;
        new_node->keys[index] = node->keys[i];
        new_node->ptrs[index] = node->ptrs[i];
        node->keys[i] = NULL;
        node->ptrs[i] = NULL;
        new_node->keyNums += 1;
        node->keyNums -= 1;
    }
    node->next = new_node;
    new_node->parent = node->parent;
    return new_node;
}

/**
 * @brief split non leaf node
 * 
 * @param node 
 * @param mgmtData 
 */
void 
splitNonLeafNode(BTreeNode* node, BTreeMtdt *mgmtData) {
    bool isPushUp = node->keyNums % 2 == 0;
    BTreeNode * sibling = createNonLeafNode(mgmtData);
    // split right index
    int rpoint = (node->keyNums + 1) / 2;
    // 5,  rpoint 3   [1.2.3] [4.5]
    // 4,  rpoint 3   [1.2] 3 [4]
    if (isPushUp) {
        rpoint += 1;
    }
    sibling->ptrs[0] = node->ptrs[rpoint];
    for (int i = rpoint; i < node->keyNums; i++) {
        int index = node->keyNums - rpoint - 1;
        sibling->keys[index] = node->keys[i];
        sibling->ptrs[index+1] = node->ptrs[i+1];
        node->keys[i] = NULL;
        node->ptrs[i+1] = NULL;
        sibling->keyNums += 1;
        node->keyNums -= 1;
    }
    sibling->parent = node->parent;
    node->next = sibling;
    
    if (isPushUp) {
        insertIntoParentNode(node, node->keys[rpoint-1], mgmtData);
        node->keys[rpoint-1] = NULL;
        node->keyNums -= 1;
    }
}

/**
 * @brief insert into parent node
 * 
 * @param lnode 
 * @param key 
 * @param mgmtData 
 */
void
insertIntoParentNode(BTreeNode* lnode, Value *key, BTreeMtdt *mgmtData) {
    BTreeNode* rnode = lnode->next;
    BTreeNode *parent = lnode->parent;
    if (parent == NULL) {
        parent = createNonLeafNode(mgmtData);
        mgmtData->root = lnode->parent = rnode->parent = parent;
        rnode->parent->ptrs[0] = lnode;
    }
    int insert_pos = getInsertPos(parent, key);

    for (int i = parent->keyNums; i > insert_pos; i--) {
        parent->keys[i] = parent->keys[i-1];
        parent->ptrs[i+1] = parent->ptrs[i];
    }
    parent->keys[insert_pos] = key;
    parent->ptrs[insert_pos + 1] = rnode;
    parent->keyNums += 1;

    if (parent->keyNums > mgmtData->n) {
        splitNonLeafNode(parent, mgmtData);
    }
}

Value *
copyKey(Value *key) {
    Value *new_key = (Value *) malloc(sizeof(Value));
    memcpy(new_key, key, sizeof(*new_key));
    return new_key;
}

/**
 * @brief insert key to B+Tree
 *        bottom-up strategy
 * @param tree 
 * @param key 
 * @param rid 
 * @return RC 
 */
RC insertKey (BTreeHandle *tree, Value *key, RID rid)  {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    if (mgmtData->root == NULL) {
        mgmtData->root = createLeafNode(mgmtData);
    }
    // 1. Find correct leaf node L for k
    BTreeNode* leafNode = findLeafNode(mgmtData->root, key);
    // this key is already stored in the b-tree
    if (findEntryInNode(leafNode, key)) {
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    // 2. Add new entry into L in sorted order
    insertIntoLeafNode(leafNode, key, &rid, mgmtData);
    // If L has enough space, the operation done
    if (leafNode->keyNums <= mgmtData->n) {
        return RC_OK;
    }
    // Otherwise
    // Split L into two nodes L and L2
    BTreeNode * rnode = splitLeafNode(leafNode, mgmtData);
    // Redistribute entries evenly and copy up middle key (new leaf's smallest key)
    Value *new_key = copyKey(rnode->keys[0]);
    // Insert index entry pointing to L2 into parent of L
    // To split an inner node, redistrubute entries evenly, but push up the middle key Resursive function
    insertIntoParentNode(leafNode, new_key, mgmtData);
    return RC_OK;
}

/**
 * @brief delete from leaf node
 * 
 * @param node 
 * @param key 
 * @param mgmtData 
 */
void
deleteFromLeafNode(BTreeNode *node, Value *key, BTreeMtdt *mgmtData) {
    int insert_pos = getInsertPos(node, key);
    for (int i = insert_pos; i < node->keyNums; i++) {
        node->keys[i] = node->keys[i+1];
        node->ptrs[i] = node->ptrs[i+1];
    }
    node->keyNums -= 1;
    mgmtData->entries -= 1;
}

/**
 * @brief checks for enough space
 * 
 * @param keyNums 
 * @param node 
 * @param mgmtData 
 * @return true 
 * @return false 
 */
bool
isEnoughSpace(int keyNums, BTreeNode *node, BTreeMtdt *mgmtData) {
    int min = node->type == LEAF_NODE ? mgmtData->minLeaf : mgmtData->minNonLeaf;
    if (keyNums >= mgmtData->minLeaf && keyNums <= mgmtData->n) {
        return true;
    }
    return false;
}

/**
 * @brief Deletes the parent entry
 * 
 * @param node  
 */
void deleteParentEntry(BTreeNode *node) {
    BTreeNode *parent = node->parent;
    for (int i = 0; i <= parent->keyNums; i++) {
        if (parent->ptrs[i] == node) {
            int keyIndex = i-1 < 0 ? 0 : i-1;
            parent->keys[keyIndex] = NULL;
            parent->ptrs[i] = NULL;
            for (int j = keyIndex; j < parent->keyNums - 1; j++) {
                parent->keys[j] = parent->keys[j+1];
            }
            for (int j = i; j < parent->keyNums; j++) {
                parent->ptrs[j] = parent->ptrs[j+1];
            }
            parent->keyNums -= 1;
            break;
        }
    }
}
/**
 * @brief Updates parent entry
 * 
 * @param node Inner_Node
 * @param key 
 * @param newkey 
 */
void
updateParentEntry(BTreeNode *node, Value *key, Value *newkey) {
    // equal = no update
    if (compareValue(key, newkey) == 0) {
        return;
    }
    
    bool isDelete = false;
    BTreeNode *parent = node->parent;
    // 1. delete key
    for (int i = 0; i < parent->keyNums; i++) {
        if (compareValue(key, parent->keys[i]) == 0) {
            for (int j = i; j < parent->keyNums - 1; j++) {
                parent->keys[j] = parent->keys[j+1];
            }
            parent->keys[parent->keyNums - 1] = NULL;
            parent->keyNums -= 1;
            isDelete = true;
            break;
        }
    }
    if (isDelete == false) {
        return;
    }
    // 2. add new key
    int insert_pos = getInsertPos(parent, newkey);
    for (int i = parent->keyNums; i >= insert_pos; i--) {
        parent->keys[i] = parent->keys[i-1];
    }
    parent->keys[insert_pos] = newkey;
    parent->keyNums += 1;
}



/**
 * @brief try to redistribute borrow key from sibling
 * 
 * @param node 
 * @param key 
 * @param mgmtData 
 * @return true 
 * @return false 
 */
bool
redistributeFromSibling(BTreeNode *node, Value *key, BTreeMtdt *mgmtData) {
    BTreeNode *parent = node->parent;
    BTreeNode *sibling = NULL;
    int index;
    
    for (int i = 0; i <= parent->keyNums; i++) {
        if (parent->ptrs[i] != node) {
            continue;
        }
        // perfer left sibling
        sibling = (BTreeNode *) parent->ptrs[i-1];
        if (sibling && isEnoughSpace(sibling->keyNums - 1, sibling, mgmtData) == true) {
            index = sibling->keyNums - 1;
            break;
        }
        sibling = (BTreeNode *) parent->ptrs[i+1];
        if (sibling && isEnoughSpace(sibling->keyNums - 1, sibling, mgmtData) == true) {
            index = 0;
            break;
        }
        sibling = NULL;
        break;
    }
    if (sibling) {
        insertIntoLeafNode(node, copyKey(sibling->keys[index]), sibling->ptrs[index], mgmtData);
        updateParentEntry(node, key, copyKey(sibling->keys[index]));
        sibling->keyNums -= 1;
        free(sibling->keys[index]);
        free(sibling->ptrs[index + 1]);
        sibling->keys[index] = NULL;
        sibling->ptrs[index + 1] = NULL;
        return true;
    }
    // redistribute fail
    return false;
}

/**
 * @brief Checks capacity of sibling
 * 
 * @param node 
 * @param mgmtData 
 * @return BTreeNode* 
 */
BTreeNode *
checkSiblingCapacity(BTreeNode *node, BTreeMtdt *mgmtData) {
    BTreeNode *sibling = NULL;
    BTreeNode * parent = node->parent;
    for (int i = 0; i <= parent->keyNums; i++) {
        if (parent->ptrs[i] != node) {
            continue;
        }
        if (i - 1 >= 0) {
            // perfer left sibling
            sibling = (BTreeNode *) parent->ptrs[i-1];
            if (isEnoughSpace(sibling->keyNums + node->keyNums, sibling, mgmtData) == true) {
                break;
            }
        }
        if (i + 1 <= parent->keyNums + 1) {
            sibling = (BTreeNode *) parent->ptrs[i+1];
            if (isEnoughSpace(sibling->keyNums + node->keyNums, sibling, mgmtData) == true) {
                break;
            }
        }
        sibling = NULL;
        break;
    }
    return sibling;
}

/**
 * @brief Merges siblings
 * 
 * @param node 
 * @param sibling 
 * @param mgmtData 
 */
void
mergeSibling(BTreeNode *node, BTreeNode *sibling, BTreeMtdt *mgmtData) {
    BTreeNode *parent = node->parent;
    
    int key_count = sibling->keyNums + node->keyNums; 
    if (key_count > sibling->keyNums) {
        int i = 0, j = 0, curr = 0;
        Value **newkeys = malloc((key_count) * sizeof(Value *));
        void **newptrs = malloc((key_count + 1) * sizeof(void *));
        for (curr = 0; curr < key_count; curr++) {
            if (j >= node->keyNums || compareValue(sibling->keys[i], node->keys[j]) <= 0) {
                newkeys[curr] = sibling->keys[i];
                newptrs[curr] = sibling->ptrs[i];
                i += 1;
            } else {
                newkeys[curr] = sibling->keys[j];
                newptrs[curr] = sibling->ptrs[j];
            }
        }
        free(sibling->keys);
        free(sibling->ptrs);
        sibling->keys = newkeys;
        sibling->ptrs = newptrs;
    }
    // update parent
    deleteParentEntry(node);
    mgmtData->nodes -= 1;
    free(node->ptrs);
    free(node->keys);
    free(node);
    // Todo: recurisve
    if (sibling->keyNums < mgmtData->minNonLeaf) {}
}

/**
 * @brief delete key
 * 
 * @param tree 
 * @param key 
 * @return RC 
 */
RC deleteKey (BTreeHandle *tree, Value *key) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    // 1. Find correct leaf node L for k
    BTreeNode* leafNode = findLeafNode(mgmtData->root, key);
    // check: key not in tree
    if (findEntryInNode(leafNode, key) == NULL) {
        return RC_IM_KEY_NOT_FOUND;
    }
    // 2.Remove the entry
    deleteFromLeafNode(leafNode, key, mgmtData);
    // If L is at least half full, the operation is done
    if (leafNode->keyNums >= mgmtData->minLeaf) {
        Value *newkey = copyKey(leafNode->keys[0]);
        updateParentEntry(leafNode, key, newkey);
        return RC_OK;
    }
    // Otherwise
    // try to redistribute, borrowing from sibling
    // if redistribution fails, merge L and sibling.
    // if merge occured, delete entry in parent pointing to L.
    BTreeNode *sibling = checkSiblingCapacity(leafNode, mgmtData);
    if (sibling) {
        mergeSibling(leafNode, sibling, mgmtData);
    } else {
        redistributeFromSibling(leafNode, key, mgmtData);
    }
    return RC_OK;
}

/**
 * @brief initialize the scan which is used to scan the entries in the B+ tree
 * 
 * @param tree 
 * @param handle 
 * @return RC 
 */

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    Scankey *keydata = NULL;

    Btree_stat *treeStat;
    BTreeNode *node;

    treeStat = tree->mgmtData;
    node = treeStat->mgmtData;

    while (node->ptrs[0] != NULL) {

        node = node->ptrs[0];

    }

    (*handle) = (BT_ScanHandle *) malloc(sizeof (BT_ScanHandle));
    if (*handle == NULL)
        return RC_IM_Empty_Tree;

    keydata = (Scankey *) malloc(sizeof (Scankey));

    keydata->currentNode = node;
    keydata->recnumber = 0;

    (*handle)->tree = tree;
    (*handle)->mgmtData = (void *) keydata;
    return RC_OK;
}

/**
 * @brief next entry
 * 
 * @param handle 
 * @param result 
 * @return RC 
 */
RC nextEntry (BT_ScanHandle *handle, RID *result) {
    //  no more entries to be reyirmed
    // return RC_IM_NO_MORE_ENTRIES;
    BTreeNode *node;
    int numrec;
    Scankey *keydata = NULL;

    keydata = handle->mgmtData;
    node = keydata->currentNode;
    numrec = keydata->recnumber;

    if (node != NULL) {
        if (node->keyNums > numrec) {

            *result = node->records[numrec];
            numrec++;

        }
        if (numrec == node->keyNums) {

            node = node->next;
            numrec = 0;

        }

        keydata->currentNode = node;
        keydata->recnumber = numrec;

        if (result == NULL) {

            return RC_IM_NO_MORE_ENTRIES;

        }

        return RC_OK;
    } else {

        return RC_IM_NO_MORE_ENTRIES;

    }
}

/**
 * @brief close tree scan
 * 
 * @param handle 
 * @return RC 
 */
RC closeTreeScan (BT_ScanHandle *handle) {
    free(handle);
    handle->mgmtData = NULL;
	
    return RC_OK;
}

/**
 * @brief print tree (debug and test functions)
 *        depth-first pre-order sequence
 * @param tree 
 * @return char* 
 */
char *printTree (BTreeHandle *tree) {
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    BTreeNode *root = mgmtData->root;
    if (root == NULL) {
        return NULL;
    }
    BTreeNode **queue = malloc((mgmtData->nodes) * sizeof(BTreeNode *));
    int level = 0;
    int count = 1;
    int curr = 0;

    queue[0] = root;
    while(curr < mgmtData->nodes) {
        BTreeNode * node = queue[curr];
        printf("(%i)[", level);

        for (int i = 0; i < node->keyNums; i++) {
            if (node->type == LEAF_NODE) {
                RID *rid = (RID *)node->ptrs[i];
                printf("%i.%i,", rid->page, rid->slot);
            } else {
                printf("%i,", count);
                queue[count] = (BTreeNode *)node->ptrs[i];
                count += 1;
            }
            if (node->type == LEAF_NODE && i == node->keyNums - 1) {
                printf("%s", serializeValue(node->keys[i]));
            } else {
                printf("%s,", serializeValue(node->keys[i]));
            }
        }
        if (node->type == Inner_NODE) {
            printf("%i", count);
            queue[count] = (BTreeNode *)node->ptrs[node->keyNums];
            count += 1;
        }
        level += 1;
        curr += 1;
        printf("]\n");
    }
    return RC_OK;
}
