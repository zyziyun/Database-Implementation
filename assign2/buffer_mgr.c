#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "dt.h"

static handlers_t handlers[5] = {
    pinPageFIFO,
    pinPageLRU,
    pinPageLFU,
    pinPageCLOCK,
    pinPageLRUK,
};

/**
 * @brief gets the time
 * 
 * @return int 
 * @author Yun Zi
 */
int getTimeStamp() {
    time_t now;
    now = time(NULL);
    return time(&now);
}

/**
 * @brief goes through the frame list
 * 
 * @param frameList
 * @param mgmt
 * @param callback
 * @return void 
 * @author Yun Zi
 */
void *traverseFrameList(BM_FrameList *frameList, BM_MgmtData *mgmt, RC (*callback)(BM_Frame*, BM_MgmtData*)) {
    BM_Frame *curr = frameList->head;
    while (curr) {
        int status = callback(curr, mgmt);
        if (status == RC_RETURN) {
            return curr;
        }
        curr = curr->next;
    }
    return NULL;
}

/**
 * @brief sets default values for a frame
 * 
 * @return BM_Frame 
 * @author Yun Zi
 */
BM_Frame *initFrame(int frameNum) {
    BM_Frame *frame = MAKE_FRAME();
    frame->dirtyflag = FALSE;
    frame->fixCount = 0;
    frame->refCount = 0;
    frame->timestamp = 0;
    frame->data = NULL;
    frame->frameNum = frameNum;
    frame->pageNum = NO_PAGE;
    frame->next = NULL;
    frame->prev = NULL;
    return frame;
}

/**
 * @brief destroys frame list
 * 
 * @param frameList
 * @return void 
 * @author Yun Zi
 */
void destoryFrameList(BM_FrameList *frameList) {
    BM_Frame *curr = frameList->head;
    while (curr) {
        curr->prev = NULL;
        free(curr->data);
        free(curr);
        curr = curr->next;
        frameList->head = curr;
    }
    frameList->head = frameList->tail = NULL;
    free(frameList);
    frameList = NULL;
}

/**
 * @brief sets frames in frame list
 * 
 * @param num
 * @return BM_FrameList 
 * @author Yun Zi
 */
BM_FrameList *initFrameList(int num) {
    int i;
    BM_Frame *curr;
    BM_FrameList *frameList = MAKE_FRAME_LIST();
    frameList->head = curr = frameList->tail = initFrame(0);

    for (i = 1; i < num; i++) {
        curr->next = initFrame(i);
        curr->next->prev = curr;
        frameList->tail = curr = curr->next;
    }
    return frameList;
}

// Buffer Manager Interface Pool Handling
/**
 * @brief creates a new buffer pool with page frames using the page replacement strategy
 * 
 * @param bm
 * @param pageFileName
 * @param numPages
 * @param strategy
 * @param stratData
 * @return RC 
 * @author Yun Zi
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData) {

    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    BM_MgmtData * mgmt = MAKE_MGMT_DATA();

    mgmt->fh = MAKE_FH_HANDLE();
    openPageFile(bm->pageFile, mgmt->fh);

    mgmt->frameList = initFrameList(numPages);
    mgmt->readCount = 0;
    mgmt->writeCount = 0;
    mgmt->totalSize = numPages;
    mgmt->stratData = stratData;
    pthread_mutex_init(&(mgmt->mutexlock), NULL);
    bm->mgmtData = mgmt;
    return RC_OK;
}

/**
 * @brief destroys a buffer pool
 * 
 * @param bm
 * @return RC
 * @author Yun Zi
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = mgmt->frameList->head;
    while (curr) {
        curr->fixCount = 0;
        curr = curr->next;
    }
    forceFlushPool(bm);
    // free memory in linkedlist
    destoryFrameList(mgmt->frameList);
    closePageFile (mgmt->fh);
    free(mgmt->fh);
    free(mgmt);
    mgmt->stratData = NULL;
    mgmt->fh = NULL;
    mgmt->frameList = NULL;
    bm->mgmtData = NULL;
    bm->numPages = 0;
    return RC_OK;
}

/**
 * @brief force write to single frame
 * 
 * @param frame
 * @param mgmt
 * @return void 
 * @author Yun Zi
 */
void forceWriteSingle(BM_Frame * frame, BM_MgmtData *mgmt) {
    pthread_mutex_lock(&mgmt->mutexlock);
    writeBlock(frame->pageNum, mgmt->fh, frame->data);
    frame->dirtyflag = FALSE;
    mgmt->writeCount += 1;
    pthread_mutex_unlock(&mgmt->mutexlock);
}

/**
 * @brief force flush of buffer pool
 * 
 * @param frame
 * @param mgmt
 * @return int
 * @author Yun Zi
 */
int forceFlushSingle(BM_Frame * frame, BM_MgmtData *mgmt) {
    if (frame->fixCount == 0 && frame->dirtyflag) {
        forceWriteSingle(frame, mgmt);
    }
    return 0;
}

/**
 * @brief causes all dirty pages with fix count 0 from the buffer pool to be written to disk
 * 
 * @param bm
 * @return RC 
 * @author Yun Zi
 */
RC forceFlushPool(BM_BufferPool *const bm) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    traverseFrameList(mgmt->frameList, mgmt, forceFlushSingle);
    return RC_OK;
}

/**
 * @brief gets the frame requested by its page number
 * 
 * @param frameList
 * @param pageNum
 * @return BM_Frame 
 * @author Yun Zi
 */
BM_Frame *getFrameByNum(BM_FrameList *frameList,PageNumber pageNum) {
    BM_Frame *curr = frameList->head;
    while (curr) {
        if (pageNum == curr->pageNum) {
            return curr;
        }
        curr = curr->next;
    }
    return NULL;
}

// Buffer Manager Interface Access Pages
/**
 * @brief marks a page as dirty
 * 
 * @param bm
 * @param page
 * @return RC 
 * @author Yun Zi
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = getFrameByNum(mgmt->frameList, page->pageNum);
    if (!curr) {
        return RC_FAIL;
    }
    curr->dirtyflag = TRUE;
    return RC_OK;    
}

/**
 * @brief unpins the page
 * 
 * @param bm
 * @param page
 * @return RC
 * @author Yun Zi
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = getFrameByNum(mgmt->frameList, page->pageNum);
    if (!curr) {
        return RC_FAIL;
    }
    curr->fixCount -= 1;
    return RC_OK;
}

/**
 * @brief writes the current content of the page back to the page file on disk
 * 
 * @param bm
 * @param page
 * @return RC 
 * @author Yun Zi
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *frame = getFrameByNum(mgmt->frameList, page->pageNum);
    if (frame) {
        forceWriteSingle(frame, mgmt);
    }
    return RC_OK;
}

/**
 * @brief gets frame and checks if its page number is empty
 * 
 * @param frame
 * @param mgmt
 * @return RC 
 * @author Yun Zi
 */
RC getAndCheckEmptyPage(BM_Frame *frame, BM_MgmtData *mgmt) {
    if (frame->pageNum == NO_PAGE) {
        return RC_RETURN;
    }
    return RC_OK;
}

/**
 * @brief finds the frame with page to pin
 * 
 * @param bm
 * @param pageNum
 * @return BM_PINPAGE 
 * @author Yun Zi
 */
BM_PINPAGE* pinFindFrame(BM_BufferPool *const bm,PageNumber pageNum) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *frame = NULL;
    BM_PINPAGE *bm_pinpage = (BM_PINPAGE *) malloc(sizeof(BM_PINPAGE));
    // find pageNum frame
    frame = getFrameByNum(mgmt->frameList, pageNum);
    if (frame) {
        bm_pinpage->status = PIN_EXIST;
        bm_pinpage->frame = frame;
        return bm_pinpage;
    }
    // find empty frame
    frame = (BM_Frame *)traverseFrameList(mgmt->frameList, mgmt, getAndCheckEmptyPage);
    if (frame) {
        bm_pinpage->status = PIN_EMPTY;
        bm_pinpage->frame = frame;
        return bm_pinpage;
    }
    // replace algorithm
    frame = handlers[bm->strategy](mgmt->frameList, mgmt);
    if (frame) {
        bm_pinpage->status = PIN_REPLACE;
        bm_pinpage->frame = frame;
        return bm_pinpage;
    }
    free(bm_pinpage);
    bm_pinpage = NULL;
    return NULL;
}

/**
 * @brief pins the page with page number
 * 
 * @param bm
 * @param page
 * @param pageNum
 * @return RC 
 * @author Yun Zi
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum) {
    // printf("pageNum-%i/n", pageNum);
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_PINPAGE *bm_pinpage = pinFindFrame(bm, pageNum);
    if (!bm_pinpage) {
        return RC_FAIL;
    }
    BM_Frame *frame = bm_pinpage->frame;
    // printf("\nstatus: %i, data: %s, framePageNum: %i", bm_pinpage->status, frame->data, frame->pageNum);
    if (bm_pinpage->status == PIN_REPLACE) {
        BM_Frame *newFrame = initFrame(bm_pinpage->frame->frameNum);
        newFrame->data = (SM_PageHandle) malloc(PAGE_SIZE);
        if (!mgmt->fh) {
            mgmt->fh = MAKE_FH_HANDLE();
            openPageFile(bm->pageFile, mgmt->fh);
        }
        ensureCapacity(pageNum, mgmt->fh);
        readBlock(pageNum, mgmt->fh, newFrame->data);
        newFrame->pageNum = pageNum;
        mgmt->readCount += 1;
        // printf("\nnewdata: %s, newpageNum: %i, framedata: %s, framepageNum: %i\n", 
        //     newFrame->data, 
        //     newFrame->pageNum, 
        //     frame->data, 
        //     frame->pageNum
        // );

        if (frame->dirtyflag) {
            forceWriteSingle(frame, mgmt);
        }
        if (frame->prev) {
            frame->prev->next = newFrame;
        } else {
            mgmt->frameList->head = newFrame;
        }
        if (frame->next) {
            frame->next->prev = newFrame;
        } else {
            mgmt->frameList->tail = newFrame;
        }
        newFrame->prev = frame->prev;
        newFrame->next = frame->next;
        free(frame->data);
        frame->data = NULL;
        free(frame);
        frame = newFrame;
    }
    if (bm_pinpage->status == PIN_EMPTY) {
        frame->data = (SM_PageHandle) malloc(PAGE_SIZE);
        ensureCapacity(pageNum, mgmt->fh);
        readBlock(pageNum, mgmt->fh, frame->data);
        frame->pageNum = pageNum;
        mgmt->readCount += 1;
    }
    frame->timestamp = getTimeStamp();
    frame->fixCount += 1;
    frame->refCount += 1;
    
    if (page) {
        page->data = (char *)frame->data;
        page->pageNum = pageNum;
    }
    free(bm_pinpage);
    bm_pinpage = NULL;
    return RC_OK;    
}

//Replacement Strategy
//FIFO
/**
 * @brief pin replacement strategy FIFO implementation
 * 
 * @param frameList
 * @param mgmt
 * @return BM_Frame 
 * @author MingXi Xia
 */
BM_Frame *pinPageFIFO(BM_FrameList *frameList, BM_MgmtData *mgmt){
    BM_Frame *curr = frameList->head;
    BM_Frame *ret = curr;
    int front = mgmt->readCount % mgmt->totalSize;
    while (curr){
        if (curr->frameNum == front) {
            ret = curr;
            break;
        }
        curr = curr->next;
    }
    if (ret->fixCount == 0) {
        return ret;
    }
    // special case => have referrence
    int currIndex = front;
    int i = 0;
    while (i < mgmt->totalSize) {
        currIndex = (currIndex + 1) % mgmt->totalSize;
        ret = ret->next ? ret->next : frameList->head;
        if (ret->fixCount == 0) {
            break;
        }
        i += 1;
    }
    // TODO: ALL of page have referrence?
    return ret;
}

//LRU implementation
/**
 * @brief pin replacement strategy LRU implementation
 * 
 * @param frameList
 * @param mgmt
 * @return BM_Frame 
 * @author MingXi Xia
 */
BM_Frame *pinPageLRU(BM_FrameList *frameList, BM_MgmtData *mgmt){

    BM_Frame *curr = frameList->head;
    BM_Frame *ret = curr;
    BM_Frame *ptr = curr->prev;
    curr->timestamp = getTimeStamp();
    int min = curr->timestamp;
    while (curr){
        if(curr->data == ptr->data){
                ptr->timestamp = getTimeStamp();
        }else{
            curr->timestamp = getTimeStamp();
        }
        if (curr->timestamp < min) {
            min = curr->timestamp;
            ret = curr;
        }
        curr = curr->next;
          
    }
    
    return ret;
}

/**
 * @brief pin replacement strategy CLOCK implmentation
 * 
 * @param frameList
 * @param mgmt
 * @return BM_Frame 
 * @author MingXi Xia
 */
BM_Frame *pinPageCLOCK(BM_FrameList *frameList, BM_MgmtData *mgmt){
    BM_Frame *curr = frameList->head;



    return frameList->head;
}

/**
 * @brief pin replacement strategy LRUK implementation
 * 
 * @param frameList
 * @param mgmt
 * @return BM_Frame 
 * @author MingXi Xia
 */
BM_Frame *pinPageLRUK(BM_FrameList *frameList, BM_MgmtData *mgmt){
    BM_Frame *curr = frameList->head;
    BM_Frame *ret = curr;
    BM_Frame *ptr = curr->prev;
    curr->timestamp = getTimeStamp();
    int min = curr->timestamp;
    curr->k_count = 0;
    while (curr){
        if(curr->data == ptr->data){
                curr->k_count = ptr->k_count+1;
        }else{
            curr->k_count++;
        }
        if (curr->k_count==2&&curr->timestamp < min) {
            min = curr->timestamp;
            ret = curr;
    }
        curr = curr->next;
          
    }
    return ret;
}

/**
 * @brief pin replacement strategy LFU implmentation
 * 
 * @param frameList
 * @param mgmt
 * @return BM_Frame 
 * @author MingXi Xia
 */
BM_Frame *pinPageLFU(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}



// Statistics Interface
/**
 * @brief returns an array of frame contents with contents of each page
 * 
 * @param bm
 * @return PageNumber 
 * @author Mansoor Syed
 */
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    PageNumber *arrayPageNumbers = (PageNumber*)malloc(bm->numPages * sizeof(PageNumber));
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = mgmt->frameList->head;
    while (curr) {
        arrayPageNumbers[curr->frameNum] = curr->pageNum;
        curr = curr->next;
    }
    return arrayPageNumbers;
}

/**
 * @brief returns an array with information on whether the flags are dirty or not
 * 
 * @param bm
 * @return bool 
 * @author Mansoor Syed
 */
bool *getDirtyFlags (BM_BufferPool *const bm) {
    bool *arrayOfBools = (bool*)malloc(bm->numPages * sizeof(bool));
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = mgmt->frameList->head;
    while (curr) {
        arrayOfBools[curr->frameNum] = (bool)curr->dirtyflag;
        curr = curr->next;
    }
    return arrayOfBools;
}

/**
 * @brief returns an array of ints with the fix count of the page
 * 
 * @param bm
 * @return int 
 * @author Mansoor Syed
 */
int *getFixCounts (BM_BufferPool *const bm) {
    int *arrayOfInts = (int*)malloc(bm->numPages * sizeof(int));
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = mgmt->frameList->head;
    while (curr) {
        arrayOfInts[curr->frameNum] = (bool)curr->fixCount;
        curr = curr->next;
    }
    return arrayOfInts;
}

/**
 * @brief returns number of pages that have been read from disk since buffer pool has been initialized
 * 
 * @param bm
 * @return int 
 * @author Mansoor Syed
 */
int getNumReadIO (BM_BufferPool *const bm) {
    BM_MgmtData *data = (BM_MgmtData *) bm->mgmtData;
    return (data->readCount);
}

/**
 * @brief returns number of pages written to the page file sinze buffer pool has been initialized
 * 
 * @param bm
 * @return int 
 * @author Mansoor Syed
 */
int getNumWriteIO (BM_BufferPool *const bm) {
    BM_MgmtData *data = (BM_MgmtData *) bm->mgmtData;
    return (data->writeCount);
}
