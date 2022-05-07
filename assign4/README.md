# Assignment4 - CS525_S22_GROUP24
- Yun Zi
- MingXi Xia
- Mansoor Syed

# Test Results:
- Environment: 'Ubuntu 20.0.4 LTS'(gcc version 9.3.0) and 'mac OS 12.1'(gcc version clang-1300.0.29.30)
- Pass all tests(100%), function coverage(100%)
- Include `test_assign4_1`, `test_expr`, we also change `test` to avoid memory leak.

# key question
- how to identify keys and nodes?
- how to store parent and child information?
- how to split leaf?
- how to merge leaf?
- how to delete key?
- how to store data and retreive data of tree?


# data structure
- typedef struct Btree_stat: stores status of btree
- typedef struct BTreeMtdt: stores metadata of tree
- typedef struct BT_ScanHandle: store metadata of scan operation

# compile and run
1. cd assign4/
2. make clean && make 
3. ./test_assign4_1 && ./test_expr


# API

## common function
1. serializeBtreeHeader(): serializes Btree header
2. deserializeBtreeHeader(): deserializes Btree header
3. compareValue(): compares key with sign
4. findLeafNode(): finds leaf node
5. findEntryInNode(): finds entry in node
6. createNode(): creates a node object block
7. createLeafNode(): creates a leaf node object block
8. createNonLeafNode(): creats a non leaf node object
9. getInsertPos(): Gets the insert position object
10. insertIntoLeafNode(): Inserts entry into leaf node
11. splitLeafNode(): Splits leaf node
12. splitNonLeafNode(): Splits non lead node
13. copyKey(): Copies a provided key
14. deleteFromLeafNode(): Deletes key from leaf node
15. isEnoughSpace(): checks for enough space
16. deleteParentEntry(): Deletes the parent entry
17. updateParentEntry(): Updates parent entry
18. redistributeFromSibling(): Tries to redistribute borrow key from sibling
19. checkSiblingCapacity(): Checks capacity of sibling
20. mergeSibling(): Merges siblings

## main function
1. initIndexManager(): Initializes the index manager
2. shutdownIndexManager(): Shutsdowns a index manager
3. createBtree(): Creates B Tree
4. openBtree(): Opens provided B Tree
5. closeBtree(): Closes provided B Tree
6. deleteBtree(): Deletes provided B Tree
7. getNumNodes(): Gets the number of nodes in the tree
8. getNumEntries(): Gets the number of enteries in the tree
9. getKeyType(): Gets the type of key in the tree
10. findKey(): Finds a provided key
11. insertKey(): Inserts a provided key
12. deleteKey(): Deletes a provided key
13. openTreeScan(): Initializes a new scan
14. nextEntry(): Gets the next entry
15. closeTreeScan(): Closes the initialized scan
16. insertIntoParentNode(): Inserts given key into the parent node
17. buildRID(): Builds RID
18. printTree(): Prints Tree