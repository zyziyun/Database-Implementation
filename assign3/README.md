# Assignment3 - CS525_S22_GROUP24
- Yun Zi
- MingXi Xia
- Mansoor Syed

# Test Results:
- Environment: 'Ubuntu 20.0.4 LTS'(gcc version 9.3.0) and 'mac OS 12.1'(gcc version clang-1300.0.29.30)
- Pass all tests(100%), function coverage(100%)
- Include `test_assign3_1`, `test_expr`, we also change `test` to avoid memory leak.

# key question
- how to index each table? name -> file (1 vs 1)
- how to store datas of table? tuple(base on slot index)
- how to know table structure? schema(fixed) -> metadata
- how to know free slot? page offset(id.page) + slot offset(id.slot)
- how to store data to file and how to get data from file? serialize the data to string and deserializ datas to memory

# data structure
```
file(table) -> page(block, start from page 1) -> slot(tuple)
            -> metadata(page 0) -> schema and other message
```
- typedef struct RM_RecordMtdt: store metadata of table
- typedef struct RM_ScanMtdt: store metadata of scan operation

# compile and run
1. cd assign3/
2. make clean && make 
3. ./test_assign3_1 && ./test_expr


# API

## common function
1. calcSlotLen(): Calculates the length of slots in a given schema
2. writeStrToPage(): Writes string to page
3. writeTableHeader(): Updates the table header
4. updateRecordOffset(): Updates the available offset of in the record
5. getRecordDataFromSerialize(): Gets the data from serialize

## serialize/deserialize function
- we also implement more serializer and deserializer function
1. deserializeSchema(): deserialize schema from page 0
2. deserializeSchemaAttr(): deserialize schema attribute
3. deserializeSchemaTypeLength(): deserialize schema string length
4. deserializeSchemaKeys(): deserialize schema keys
5. deserializeRecordMtdt(): deserialize record metadata
6. serializeRecordMtdt(): serialize record metadata
7. strtoi(): split string and get int
8. strtochar(): cut down string and copy to new string
9. trim(): remove blank from string


## main function
1. initRecordManager(): Initializes the record manager
2. shutdownRecordManager(): Shutsdowns a record manager
3. createTable(): Creates table file with name and schema
4. openTable(): Opens the table with the provided name
5. closeTable(): Closes the table provided
6. deleteTable(): Deletes the table with given name
7. getNumTuples(): Gets the amount of records in the table
8. insertRecord(): Inserts a record to the table
9. deleteRecord(): Deletes a record from the table
10. updateRecord(): Updates a table with the given record
11. getRecord(): Gets a record from a provided table and RID 
12. startScan(): Initializes a new scan
13. next(): Gets the next record in a table
14. closeScan(): Closes the initalizes scan
15. getRecordSize(): Gets the size of each record provided a schema
16. *createSchema(): Creates a memory space and the schema of the table
17. freeSchema(): Frees a given schema
18. createRecord(): Creates a blank record in a given schema 
19. freeRecord(): Frees a given record
20. getAttr(): Gets attribute from record
21. setAttr(): Sets / updates attribute for given record