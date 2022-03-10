#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"


// getTimeStamp

BM_Frame *initFrame(int num) {
    BM_Frame *frame = MAKE_FRAME();
    frame->dirtyflag = FALSE;
    frame->fixCount = 0;
    frame->frameNum = num;
    frame->refCount = 0;
    frame->timestamp = 0;
    return frame;
}

BM_FrameList *initFrameList() {
    BM_FrameList *frameList = MAKE_FRAME_LIST();
    frameList->head = initFrame(0);
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

    // mgmt->fh = MAKE_FH_HANDLE();
    // openPageFile(pageFileName, mgmt->fh);
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

    mgmt->stratData = NULL;
    
    // free(mgmt->fh);
    free(mgmt);
    // mgmt->fh = NULL;
    bm->mgmtData = NULL;
    bm->numPages = 0;
    return RC_OK;    
}

RC forceFlushPool(BM_BufferPool *const bm) {
    return RC_OK;    
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
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
