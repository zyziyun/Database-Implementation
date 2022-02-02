#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "storage_mgr.h"
#include "dberror.h"

unsigned long fsize(FILE *fp) {
    fseek(fp, 0, SEEK_END);
    unsigned long size = ftell(fp);
    rewind(fp);
    return size;
}

int fexist(char *fileName) {
    return (access(fileName, 0) == 0);
}

/* manipulating page files */
void initStorageManager (void) {
    printf("storage manager is initializing...\n");
    printf("storage manager is ready...\n");
}

int ffill(FILE *fp) {
    // initial file size one page
    char *buffer = (char*)malloc(PAGE_SIZE * sizeof(char));
    // fill single page with '0'
    memset(buffer, 0, PAGE_SIZE * sizeof(char));
    int curr = fwrite(buffer, sizeof(char), PAGE_SIZE, fp);
    // free buffer
    free(buffer);
    return curr;
}

/**
 * @brief Create a new page File
 * 
 * @param fileName 
 * @return RC
 */
RC createPageFile (char *fileName) {
    // use w+ mode to create file
    FILE *fp = fopen(fileName, "w+");
    if (fp == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    ffill(fp);
    fclose(fp);
    return RC_OK;
}

/**
 * @brief Open the exist page file and initialize the information about fHandle
 * 
 * @param fileName 
 * @param fHandle 
 * @return RC 
 */
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    // check file exist
    if (!fexist(fileName)) {
        return RC_FILE_NOT_FOUND;
    }
    FILE *fp = fopen(fileName, "r+");
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // get file size and calc the number of page
    unsigned long size = fsize(fp);
    fHandle->totalNumPages = (int)ceil(size / PAGE_SIZE);
    fHandle->fileName = fileName;
    fHandle->mgmtInfo = fp;
    fHandle->curPagePos = 0;
    return RC_OK;
}

/**
 * @brief close an open page file and destory fHandle
 * 
 * @param fHandle 
 * @return RC 
 */
RC closePageFile (SM_FileHandle *fHandle) {
    FILE *fp = (FILE*)fHandle->mgmtInfo;
    if (fp == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    fclose(fp);
    fHandle->mgmtInfo = NULL;
    fHandle->fileName = "";
    fHandle->curPagePos = -1;
    fHandle->totalNumPages = 0;
    fHandle = NULL;
    return RC_OK;
}

/**
 * @brief remove the page file
 * 
 * @param fileName 
 * @return RC 
 */
RC destroyPageFile (char *fileName) {
    if (remove(fileName) != 0) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

/**
 * @brief read the blocks in fHandle and store to memPage
 * 
 * @param pageNum 
 * @param fHandle 
 * @param memPage 
 * @return RC 
 */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // check the file has less than pageNum pages.
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    FILE *fp = fHandle->mgmtInfo;
    unsigned long pos = pageNum * PAGE_SIZE;
    fseek(fp, pos, SEEK_SET);
    int curr = fread(memPage, sizeof(char), PAGE_SIZE, fp);
    if (curr < PAGE_SIZE) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

/**
 * @brief Get the current position in the fHandle
 * 
 * @param fHandle 
 * @return int 
 */
int getBlockPos (SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int prev = getBlockPos(fHandle) - 1;
    return readBlock(prev, fHandle, memPage);
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(getBlockPos(fHandle), fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int next = getBlockPos(fHandle) + 1;
    return readBlock(next, fHandle, memPage);
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int last = fHandle->totalNumPages - 1;
    return readBlock(last, fHandle, memPage);
}

/**
 * @brief writing blocks to a page file
 * 
 * @param pageNum 
 * @param fHandle 
 * @param memPage 
 * @return RC 
 */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // check the file has less than pageNum pages.
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_WRITE_NON_EXISTING_PAGE;
    }
    FILE *fp = fHandle->mgmtInfo;
    int fs = fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
    int curr = fwrite(memPage, sizeof(char), PAGE_SIZE, fp);
    if (curr < PAGE_SIZE) {
        return RC_WRITE_NON_EXISTING_PAGE;
    }
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(getBlockPos(fHandle), fHandle, memPage);
}


/**
 * @brief append empty block to page, and fill in 0
 * 
 * @param fHandle 
 * @return RC 
 */
RC appendEmptyBlock (SM_FileHandle *fHandle) {
    FILE *fp = fHandle->mgmtInfo;
    fseek(fp, 0, SEEK_END);
    if (ffill(fHandle->mgmtInfo) != PAGE_SIZE) {
        return RC_WRITE_NON_EXISTING_PAGE;
    }
    fHandle->totalNumPages += 1;
    return RC_OK;
}

/**
 * @brief extend total number page to numberOfPages
 * 
 * @param numberOfPages 
 * @param fHandle 
 * @return RC 
 */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    int total = fHandle->totalNumPages;
    if (numberOfPages <= total) {
        return RC_OK;
    }
    // calc the number of pages to append;
    int newPageNum = numberOfPages - total;
    int i = 0;
    while(i < newPageNum) {
        RC value = appendEmptyBlock(fHandle);
        // if certain page append fail, the whole function is failed
        if (value != RC_OK){
            return value;
        }
        i += 1;
    }
    return RC_OK;
}

