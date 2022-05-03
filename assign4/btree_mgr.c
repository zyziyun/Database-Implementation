#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

#include <string.h>
#include <stdlib.h>

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

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    // key not in tree
    // return RC_IM_KEY_NOT_FOUND;

    // 1. Find correct leaf node L for k
    // findLeafNode();
    // 2. Find the position of entry in a node, binary search
    // findEntryInNode();
    
    return RC_OK;
}

BTreeNode *
createLeafNode(Value **key, RID *rid) {
    BTreeNode * node = MAKE_TREE_NODE();
    node->key = key;
    node->ptr = (void *) rid;
    node->type = LEAF_NODE;
    return node;
}

// bottom-up strategy
RC insertKey (BTreeHandle *tree, Value *key, RID rid)  {
    // create new key node
    // BTreeNode *node = createLeafNode(&key, &rid);

    // this key is already stored in the b-tree
    // return RC_IM_KEY_ALREADY_EXISTS;

    // 1. Find correct leaf node L for k
    // findLeafNode();
    // 2. Add new entry into L in sorted order
    // insertIntoLeafNode();
    // If L has enough space, the operation done
    // if (...) {
        // return RC_OK;
    // }
    // Otherwise
    // Split L into two nodes L and L2
    // Redistribute entries evenly and copy up middle key (new leaf's smallest key)
    // splitLeafNode();
    // 3. Insert index entry pointing to L2 into parent of L
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
    BTreeMtdt *mgmtData = (BTreeMtdt *) tree->mgmtData;
    *handle = MAKE_TREE_SCAN();
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
