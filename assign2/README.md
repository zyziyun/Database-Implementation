# Assignment2 - CS525_S22_GROUP24
- Yun Zi
- MingXi Xia
- Mansoor Syed

# Test Results:
- Environment: 'Ubuntu 20.0.4 LTS'(gcc version 9.3.0) and 'mac OS 12.1'(gcc version clang-1300.0.29.30)
- Pass all tests(100%), function coverage(100%)
- Include `test_assign2_1`, `test_assign2_2`, we also add `more test` to guarantee quality.

# Summary
- We have `modularized the code` in the logical way. 
- We use `macros`, `opaque pointer`, and `callback function` to avoid repeating code(`DRY` principle)
- Except all requirement lab, we also implement `all of the replacement`.(FIFO/LRU/LFU/CLOCK/LRUK), and passed tests.

# compile and run
1. cd assign2/
2. make clean && make 
3. ./test_assign2_1 && ./test_assign2_2

# API

## common function
1. getTimeStamp(): gets the microsecond time - for LRU
2. traverseFrameList(): goes through the frame list
3. initFrame(): sets default values for a frame
4. destroyFrameList(): destroys frame list
5. initFrameList(): sets frames in frame list
6. forceWriteSingle(): force write to single frame
7. forceFlushSingle(): force flush of buffer pool
8. getFrameByNum(): gets the frame requested by its page number
9. getAndCheckEmptyPage(): gets frame and checks if its page number is empty
10. pinFindFrame(): finds the frame with page to pin
11. pinPageFIFO(): pin replacement strategy FIFO implementation
12. pinPageLRU(): pin replacement strategy LRU implementation
13. pinPageLRUK(): pin replacement strategy LRUK  implementation
14. pinPageClock():pin replacement strategy CLOCK implmentation
15. checkFixCount(): check if the page to be replaced is referenced, if it is referenced it will be automatically replaced with the next page.

## main function
1. initBufferPool(): creates a new buffer pool with page frames using the page replacement strategy. 
2. shutdownBufferPool(): destroys a buffer pool
3. forceFlushPool(): causes all dirty pages with fix count 0 from the buffer pool to be written to disk
4. markDirty(): marks a page as dirty
5. unpinPage(): unpins the page
6. forcePage(): writes the current content of the page back to the page file on disk
7. pinPage(): pins the page with page number
8. getFrameContents(): returns an array of frame contents with contents of each page
9. getDirtyFlags(): returns an array with information on whether the flags are dirty or not
10. getFixCounts(): returns an array of ints with the fix count of the page
11. getNumReadIO(): returns number of pages that have been read from disk since buffer pool has been initialized
12. getNumWriteIO(): returns number of pages written to the page file sinze buffer pool has been initialized
