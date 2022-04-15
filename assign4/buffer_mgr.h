#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <pthread.h>
// Include return codes and methods for logging errors
#include "dberror.h"
// Include bool DT
#include "dt.h"
#include "storage_mgr.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

typedef struct BM_Frame {
	bool dirtyflag;
	PageNumber pageNum; //  current frame page size in list
	int frameNum;       //  current frame number
	SM_PageHandle data;

	long timestamp;     //  for LRU replacement strategy
	int fixCount;       //  pin counter
	int refCount;       //  for LFU replacement strategy
	int k_count;        //  for LRUK replacement strategy
	int pointer;		// for CLOCK replacement strategy
	struct BM_Frame *prev;
	struct BM_Frame *next;
} BM_Frame;

typedef struct BM_FrameList {
	BM_Frame *head;
	BM_Frame *tail;
} BM_FrameList;


typedef struct BM_MgmtData {
	int totalSize; // buffer pool size
	int readCount;
	int writeCount;
	SM_FileHandle *fh;
	BM_FrameList *frameList;
	int k;
	pthread_mutex_t mutexlock;// make the buffer pool thread safe
} BM_MgmtData;

typedef struct BM_PINPAGE {
    int status;
    BM_Frame *frame;
} BM_PINPAGE;

typedef BM_Frame *(*handlers_t)(BM_FrameList *frameList, BM_MgmtData *mgmt);

#define PIN_EMPTY 0
#define PIN_REPLACE 1
#define PIN_EXIST 2

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

#define MAKE_MGMT_DATA()					\
		((BM_MgmtData *) malloc(sizeof(BM_MgmtData)))

#define MAKE_FH_HANDLE() \
		((SM_FileHandle *) malloc(sizeof(SM_FileHandle)))

#define MAKE_FRAME_LIST() \
		((BM_FrameList *) malloc(sizeof(BM_FrameList)))

#define MAKE_FRAME() \
		((BM_Frame *) malloc(sizeof(BM_Frame)))

#define MAKE_MEMPAGE() \
		((SM_PageHandle *) malloc(sizeof(SM_PageHandle)))

//Replacement Strategy
BM_Frame *pinPageLRU(BM_FrameList *, BM_MgmtData *);
BM_Frame *pinPageLRUK(BM_FrameList *, BM_MgmtData *);
BM_Frame *pinPageLFU(BM_FrameList *, BM_MgmtData *);
BM_Frame *pinPageFIFO(BM_FrameList *, BM_MgmtData *);
BM_Frame *pinPageCLOCK(BM_FrameList *, BM_MgmtData *);


// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
