#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"

// init and shutdown index manager
RC initIndexManager (void *mgmtData) {
    return RC_OK;
}

RC shutdownIndexManager () {
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n) {
    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId) {
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree) {
    return RC_OK;
}

RC deleteBtree (char *idxId) {
    return RC_OK;
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result) {
    return RC_OK;
}

RC getNumEntries (BTreeHandle *tree, int *result) {
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result) {
    return RC_OK;
}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result) {
    return RC_OK;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid)  {
    return RC_OK;
}

RC deleteKey (BTreeHandle *tree, Value *key) {
    return RC_OK;
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle) {
    return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree) {
    return RC_OK;
}
