# Assignment1 - CS525_S22_GROUP24
- Yun Zi
- MingXi Xia
- Mansoor Syed

# Test Results:
- Environment: 'Ubuntu 20.0.4 LTS'(gcc version 9.3.0) and 'mac OS 12.1'(gcc version clang-1300.0.29.30)
- Pass all tests(100%), function coverage(100%)

# compile and run
1. cd assign/
2. make clean && make && ./test_assign1

# API

## common function
1. fsize():  get the size of page
2. fexist(): check the file whether exists or not
3. ffill(): fill single page with '0'

## interface function
1. initStorageManager(): init the storage manager.
2. createPageFile(): create a new page file fileName. The initial file size should be one page.
3. openPageFile(): Open the exist page file and initialize the information about fHandle.
4. closePageFile(): close an open page file and destory fHandle.
5. destroyPageFile(): remove the page file.
6. readBlock():  read the blocks in fHandle and store to memPage.
7. getBlockPos(): Get the current position in the fHandle.
8. readFirstBlock(): read the first block in fHandle and store in memPage
9. readPrevisousBlock(): read the previous block in fHandle and store in memPage
10. readCurrentBlock():read the current block in fHandle and store in memPage
11. readNextBlock():read the next block in fHandle and store in memPage
12. readLastBlock(): read the last block in fHandle and store in memPage
13. writeBlock(): write to the selected page in fHandle from memPage
14. writeCurrentBlock(): write to the current page in fHandle from memPage
15. appendEmptyBlock(): appends an empty page to the end of the file
16. ensureCapacity(): checks if provided page count equals file page count and appends needed pages
