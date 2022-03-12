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
    frameList->head = curr = frameList->tail = initFrame();

    for (i = 1; i < num; i++) {
        curr->next = initFrame();
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

void forceWriteSingle(BM_Frame * frame, BM_MgmtData *mgmt) {
    pthread_mutex_lock(&mgmt->mutexlock);
    writeBlock(frame->pageNum, mgmt->fh, frame->data);
    frame->dirtyflag = FALSE;
    mgmt->writeCount += 1;
    pthread_mutex_unlock(&mgmt->mutexlock);
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

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum) {
    
    BM_MgmtData * mgmt = (BM_MgmtData*)bm->mgmtData;
    pthread_mutex_lock(&mgmt->mutexlock);
    BM_PINPAGE *bm_pinpage = pinFindFrame(bm, pageNum);
    if (!bm_pinpage) {
        return RC_FAIL;
    }
    BM_Frame *frame = bm_pinpage->frame;
    // printf("\nstatus: %i, data: %s, framePageNum: %i", bm_pinpage->status, frame->data, frame->pageNum);
    if (bm_pinpage->status == PIN_REPLACE) {
        BM_Frame *newFrame = initFrame();
        newFrame->data = (SM_PageHandle) malloc(PAGE_SIZE);
        if (!mgmt->fh) {
            mgmt->fh = MAKE_FH_HANDLE();
            openPageFile(bm->pageFile, mgmt->fh);
        }
        ensureCapacity(pageNum, mgmt->fh);
        readBlock(pageNum, mgmt->fh, newFrame->data);
        newFrame->pageNum = pageNum;

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
    }
    frame->timestamp = getTimeStamp();
    frame->fixCount += 1;
    frame->refCount += 1;
    mgmt->readCount += 1;
    if (page) {
        page->data = (char *)frame->data;
        page->pageNum = pageNum;
    }
    free(bm_pinpage);
    bm_pinpage = NULL;
    pthread_mutex_unlock(&mgmt->mutexlock);
    return RC_OK;    
}
//Replacement Strategy
//FIFO
BM_Frame *pinPageFIFO(BM_FrameList *frameList, BM_MgmtData *mgmt){
    BM_Frame *curr = frameList->head;
    BM_Frame *ret = curr;
    curr->timestamp = getTimeStamp();
    int min = curr->timestamp;
    while (curr) {
        if (curr->timestamp < min) {
            min = curr->timestamp;
            ret = curr;
        }
        curr = curr->next;
    }
    return ret;
}
//LRU implementation
BM_Frame *pinPageLRU(BM_FrameList *frameList, BM_MgmtData *mgmt){
    BM_Frame *curr = frameList->head;
    BM_Frame *ret = curr;
    curr->timestamp = getTimeStamp();
    int max = curr->timestamp;
    while (curr) {
        if (curr->timestamp > max) {
            max = curr->timestamp;
            ret = curr;
        }
        curr = curr->next;
    }
    return ret;
}
BM_Frame *pinPageCLOCK(BM_FrameList *frameList, BM_MgmtData *mgmt){
    BM_Frame *curr = frameList->head;



    return frameList->head;
}
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
BM_Frame *pinPageLFU(BM_FrameList *frameList, BM_MgmtData *mgmt){
    return frameList->head;
}



// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    
    PageNumber *arrayPageNumbers = (PageNumber*)malloc(bm->numPages * sizeof(PageNumber)); 
    BM_Frame *frameData = bm->mgmtData;

    for(int i = 0; i < (bm->numPages); i++){
        if((frameData[i].data) == NULL){
            arrayPageNumbers[i] = NO_PAGE;
        }
        else {
            arrayPageNumbers[i] = frameData[i].pageNum;
        }
    }
    return arrayPageNumbers;
    
}

bool *getDirtyFlags (BM_BufferPool *const bm) {
    bool *arrayOfBools = (bool*)malloc(bm->numPages * sizeof(bool));
    BM_Frame *frameData = bm->mgmtData;
    for(int i = 0; i < (bm->numPages); i++){
        arrayOfBools[i] = (bool)frameData->dirtyflag;
    }
    return arrayOfBools;
}

int *getFixCounts (BM_BufferPool *const bm) {
    int *arrayOfInts = (int*)malloc(bm->numPages * sizeof(int));
    BM_Frame *frameData = bm->mgmtData;

    for(int i = 0; i < (bm->numPages); i++){
        arrayOfInts[i] = (int)frameData->fixCount;
    }

    return arrayOfInts;
}

int getNumReadIO (BM_BufferPool *const bm) {
    
    BM_MgmtData *data = (BM_MgmtData *) bm->mgmtData;
    return (data->readCount);

}

int getNumWriteIO (BM_BufferPool *const bm) {

    BM_MgmtData *data = (BM_MgmtData *) bm->mgmtData;
    return (data->writeCount);

}
