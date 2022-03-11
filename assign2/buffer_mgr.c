#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "dt.h"


int getTimeStamp() {
    time_t now;
    now = time(NULL);
    return time(&now);
}

void traverseFrameList(BM_FrameList *frameList, BM_MgmtData *mgmt, int (*callback)(BM_Frame*, BM_MgmtData*)) {
    BM_Frame *curr = frameList->head;
    while (curr) {
        callback(curr, mgmt);
        curr = curr->next;
    }
}

BM_Frame *initFrame(int num) {
    BM_Frame *frame = MAKE_FRAME();
    frame->dirtyflag = FALSE;
    frame->fixCount = 0;
    frame->frameNum = num;
    frame->refCount = 0;
    frame->timestamp = getTimeStamp();
    frame->page = MAKE_PAGE_HANDLE();
    frame->next = NULL;
    frame->prev = NULL;
    return frame;
}

void destoryFrameList(BM_FrameList *frameList) {
    BM_Frame *curr = frameList->head;
    while (curr) {
        curr->page = NULL;
        curr->prev = NULL;
        free(curr->page);
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


int forceFlushSingle(BM_Frame * frame, BM_MgmtData *mgmt) {
    if (frame->fixCount == 0 && frame->dirtyflag) {
        writeBlock(frame->frameNum, mgmt->fh, (SM_PageHandle)frame->page->data);
        frame->dirtyflag = true;
    }
    return 0;
}
RC forceFlushPool(BM_BufferPool *const bm) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    traverseFrameList(mgmt->frameList, mgmt, forceFlushSingle);
    return RC_OK;    
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;

    return RC_OK;    
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;    
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;    
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum) {

    return RC_OK;    
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
