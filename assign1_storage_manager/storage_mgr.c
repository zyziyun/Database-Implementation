#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

unsigned long fsize(FILE *fp) {
    fseek(fp, 0, SEEK_END);
    return ftell(fp);
}

/* manipulating page files */
void initStorageManager (void) {
    printf("storage manager is initializing...\n");
    printf("storage manager is ready...\n");
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
    // initial file size one page
    char *buffer = (char*)malloc(PAGE_SIZE * sizeof(char));
    // fill single page with '0'
    memset(buffer, 0, PAGE_SIZE * sizeof(char));
    fwrite(buffer, PAGE_SIZE, sizeof(char), fp);
    // free buffer
    free(buffer);
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
    if (!fHandle) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    FILE *fp = fopen(fileName, "r+");
    if (!fp) {
        return RC_FILE_NOT_FOUND; 
    }
    unsigned long size = fsize(fp);
    fHandle->totalNumPages = (int)(size / PAGE_SIZE);
    fHandle->fileName = fileName;
    fHandle->mgmtInfo = fp;
    fHandle->curPagePos = 0;
    fclose(fp);
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

