#include <stdio.h>
#include <stdlib.h>
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

int getTimeStamp() {
    time_t now;
    now = time(NULL);
    return time(&now);
}

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

BM_Frame *initFrame() {
    BM_Frame *frame = MAKE_FRAME();
    frame->dirtyflag = FALSE;
    frame->fixCount = 0;
    frame->refCount = 0;
    frame->timestamp = 0;
    frame->data = NULL;
    frame->pageNum = NO_PAGE;
    frame->next = NULL;
    frame->prev = NULL;
    return frame;
}

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
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData) {

    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    BM_MgmtData * mgmt = MAKE_MGMT_DATA();

    mgmt->fh = MAKE_FH_HANDLE();
    RC status = openPageFile(bm->pageFile, mgmt->fh);
    if (status != RC_OK) {
        return status;
    }

    mgmt->frameList = initFrameList(numPages);
    mgmt->readCount = 0;
    mgmt->writeCount = 0;
    mgmt->totalSize = numPages;
    mgmt->stratData = stratData;
    bm->mgmtData = mgmt;
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
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

void forceWriteSingle(BM_Frame * frame, BM_MgmtData *mgmt) {
    writeBlock(frame->pageNum, mgmt->fh, frame->data);
    frame->dirtyflag = FALSE;
    mgmt->writeCount += 1;
}
int forceFlushSingle(BM_Frame * frame, BM_MgmtData *mgmt) {
    if (frame->fixCount == 0 && frame->dirtyflag) {
        forceWriteSingle(frame, mgmt);
    }
    return 0;
}
RC forceFlushPool(BM_BufferPool *const bm) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    traverseFrameList(mgmt->frameList, mgmt, forceFlushSingle);
    return RC_OK;    
}


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
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = getFrameByNum(mgmt->frameList, page->pageNum);
    if (!curr) {
        return RC_FAIL;
    }
    curr->dirtyflag = TRUE;
    return RC_OK;    
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *curr = getFrameByNum(mgmt->frameList, page->pageNum);
    if (!curr) {
        return RC_FAIL;
    }
    curr->fixCount -= 1;
    return RC_OK;    
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *frame = getFrameByNum(mgmt->frameList, page->pageNum);
    if (frame) {
        forceWriteSingle(frame, mgmt);
    }
    return RC_OK;
}

RC getAndCheckEmptyPage(BM_Frame *frame, BM_MgmtData *mgmt) {
    if (frame->pageNum == NO_PAGE) {
        return RC_RETURN;
    }
    return RC_OK;
}

BM_PINPAGE* pinFindFrame(BM_BufferPool *const bm,PageNumber pageNum) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    BM_Frame *frame = NULL;
    BM_PINPAGE *bm_pinpage;
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
    return NULL;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum) {
    int status = 0;

    BM_PINPAGE *bm_pinpage;
    BM_Frame *frame = NULL;
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    
    bm_pinpage = pinFindFrame(bm, pageNum);
    if (!bm_pinpage) {
        return RC_FAIL;
    }
    frame = bm_pinpage->frame;
    if (bm_pinpage->status == PIN_EMPTY) {
        bm_pinpage->frame = MAKE_FRAME();
    }
    if (bm_pinpage->status == PIN_REPLACE) {
        frame->dirtyflag = FALSE;
        frame->fixCount = 0;
        frame->refCount = 0;
    }
    ensureCapacity(pageNum, mgmt->fh);
    readBlock(pageNum, mgmt->fh, frame->data);
    frame->pageNum = pageNum;
    frame->timestamp = getTimeStamp();
    frame->fixCount += 1;
    frame->refCount += 1;
    mgmt->readCount += 1;
    if (page) {
        page->data = frame->data;
        page->pageNum = pageNum;
    }
    return RC_OK;    
}
//Replacement Strategy
//LRU implementation
BM_Frame *pinPageLRU(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}

BM_Frame *pinPageLRUK(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}
BM_Frame *pinPageLFU(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}
BM_Frame *pinPageFIFO(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}
BM_Frame *pinPageCLOCK(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    return 0;
}

bool *getDirtyFlags (BM_BufferPool *const bm) {
    return (short)0;
}

int *getFixCounts (BM_BufferPool *const bm) {
    return 0;
}

int getNumReadIO (BM_BufferPool *const bm) {
    return 0;
}

int getNumWriteIO (BM_BufferPool *const bm) {
    return 0;
}
