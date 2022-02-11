# Assignment1 - CS525_S22_GROUP24
- Yun Zi
- MingXi Xia
- NoodleV4

# Test Results:
- Environment: 'Ubuntu 20.0.4 LTS' and 'mac OS 12.1'(gcc version clang-1300.0.29.30)
- Pass all tests(100%), function coverage(100%)

# compile and run
1. cd assign/
2. make clean && make && ./test_assign1

# API

## common function
1. fsize(): 
2. fexist():
3. ffill(): 

## interface function
1. initStorageManager(): init the storage manager
2. createPageFile(): create a new page file fileName. The initial file size should be one page.
3. openPageFile(): 
4. closePageFile(): 
5. destroyPageFile():
6. readBlock(): 
7. getBlockPos():
8. readFirstBlock():
9. readPrevisousBlock(): 
10. readCurrentBlock():
11. readNextBlock():
12. readLastBlock(): 
13. writeBlock():
14. writeCurrentBlock(): 
15. appendEmptyBlock():
16. ensureCapacity():
