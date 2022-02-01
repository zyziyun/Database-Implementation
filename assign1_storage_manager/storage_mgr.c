#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include "dberror.h"


/* manipulating page files */
void initStorageManager (void) {
    printf("storage manager is initializing...\n");
}

RC createPageFile (char *fileName) {
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle) {
    return RC_OK;
}

RC destroyPageFile (char *fileName) {
    return RC_OK;
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {

}

int getBlockPos (SM_FileHandle *fHandle) {
    return 0;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return RC_OK;
}

RC appendEmptyBlock (SM_FileHandle *fHandle) {
    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    return RC_OK;
}

