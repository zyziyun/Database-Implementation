#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

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
    char *header = serializeBtreeHeader(mgmtData);
    writeStrToPage(tree->idxId, 0, header);
    free(header);
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
            }
            result = (key->v.intV > sign->v.intV) ? 1 : -1;
            break;
        case DT_FLOAT:
            if (key->v.floatV == sign->v.floatV) {
                result = 0;
            }
            result = (key->v.floatV > sign->v.floatV) ? 1 : -1;
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
    return NULL;
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
    result = findEntryInNode(leafNode, key);
    if (result == NULL) {
        return RC_IM_KEY_NOT_FOUND;
    }
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

BTreeNode *
createNonLeafNode(BTreeMtdt *mgmtData) {
    BTreeNode * node = createNode(mgmtData);
    node->type = Inner_NODE;
    node->ptrs = (void *)malloc((mgmtData->n+2) * sizeof(void *));
    return node;
}

int 
getInsertPos(BTreeNode* node, Value *key) {
    int insert_pos = node->keyNums;
    for (int i = 0; i < node->keyNums; i++) {
        if (compareValue(node->keys[i], key) == 1) {
            insert_pos = i;
            break;
        }
    }
    return insert_pos;
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
    node->ptrs[insert_pos] = rid;
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
    int rpoint = ceil(node->keyNums / 2);
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

// insert key to parent
BTreeNode *
insertIntoNonLeafNode(BTreeNode* lnode, Value *key, BTreeMtdt *mgmtData) {
    BTreeNode* rnode = lnode->next;
    if (rnode->parent == NULL) {
        rnode->parent = createNonLeafNode(mgmtData);
        rnode->ptrs[0] = lnode;
    }
    BTreeNode *parent = rnode->parent;
    int insert_pos = getInsertPos(parent, key);
    for (int i = parent->keyNums; i >= insert_pos; i--) {
        parent->keys[i] = parent->keys[i-1];
        parent->ptrs[i+1] = parent->ptrs[i];
    }
    parent->keys[insert_pos] = key;
    parent->ptrs[insert_pos + 1] = rnode;
    parent->keyNums += 1;

    // if ()
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
    Value *middlekey = rnode->keys[0];
    Value *new_key = (Value *) malloc(sizeof(Value));
    memcpy(new_key, middlekey, sizeof(*new_key));
    // Insert index entry pointing to L2 into parent of L
    insertIntoNonLeafNode(leafNode, new_key, mgmtData);

    // To split an inner node, redistrubute entries evenly, but push up the middle key
    // insertIntoNonLeafNode(); // Resursive function
    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key) {
    // key not in tree
    // return RC_IM_KEY_NOT_FOUND;
    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    *handle = MAKE_TREE_SCAN();
    (*handle)->tree = tree;
    
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    //  no more entries to be reyirmed
    // return RC_IM_NO_MORE_ENTRIES;
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle) {
    return RC_OK;
}

// debug and test functions
// depth-first pre-order sequence
char *printTree (BTreeHandle *tree) {
    return RC_OK;
}
