#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "tables.h"

typedef enum NodeType {
	Inner_NODE = 1,
	LEAF_NODE = 0
} NodeType;
typedef struct BTreeNode {
  NodeType type;
  Value **keys; // array[0...n]
  // leaf-node => RID
  // non-leaf-node => next level node
  void **ptrs; // array[0...n]
  int keyNums; // the count of key
  RID *records;
  struct BTreeNode *next; // sibling
  struct BTreeNode *parent; // parent
} BTreeNode;
// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
  BTreeNode *root;
} BTreeHandle;

typedef struct Scankey {
    struct BTreeNode *currentNode;
    int recnumber;
} Scankey;

typedef struct Btree_stat {
    void *mgmtData;
    void *fileInfo;
    int num_nodes;
    int num_inserts;
    int order;
} Btree_stat;

typedef struct BTreeMtdt {
  int n; // maximum keys in each block
  int minLeaf;
  int minNonLeaf;
  int nodes; // the count of node
  int entries; // the count of entries
  DataType keyType;

  BTreeNode *root;
  
  BM_PageHandle *ph;
  BM_BufferPool *bm;
} BTreeMtdt;


typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;



#define MAKE_TREE_HANDLE()				\
		((BTreeHandle *) malloc (sizeof(BTreeHandle)))

#define MAKE_TREE_MTDT()				\
		((BTreeMtdt *) malloc (sizeof(BTreeMtdt)))

#define MAKE_TREE_NODE()				\
		((BTreeNode *) malloc (sizeof(BTreeNode)))

#define MAKE_TREE_SCAN()				\
		((BT_ScanHandle *) malloc (sizeof(BT_ScanHandle)))

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// b+tree
extern void insertIntoParentNode(BTreeNode*, Value *, BTreeMtdt *);
extern RID* buildRID(RID *rid);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

#endif // BTREE_MGR_H
