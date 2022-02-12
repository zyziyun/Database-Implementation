#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "storage_mgr.h"
#include "dberror.h"

/**
 * @brief get the size of page
 * 
 * @param fp
 * @return unsigned long 
 * @author Yun Zi
 */
unsigned long fsize(FILE *fp) {
    fseek(fp, 0, SEEK_END);
    unsigned long size = ftell(fp);
    rewind(fp);
    return size;
}

/**
 * @brief check the file whether exists or not
 * 
 * @param fileName pointer
 * @return int 0 is not exist
 * @author Yun Zi
 */
int fexist(char *fileName) {
    return (access(fileName, 0) == 0);
}

int initialize = 0;


/**
 * @brief write fill single page with '0'
 * 
 * @param fp 
 * @return int real size of writing
 * @author Yun Zi
 */
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
 * @brief init the storage manager
 * @author Yun Zi
 */
void initStorageManager (void) {
    if (initialize == 0) {
        initialize = 1;
        printf("The storage manager have been ready\n");
    } else {
        printf("The storage manager initialized, Please do not init repeatedly.\n");
    }
}

/**
 * @brief Create a new page File
 * 
 * @param fileName 
 * @return RC
 * @author Yun Zi
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
 * @author Yun Zi
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
 * @author Yun Zi
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
 * @author Yun Zi
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
 * @author Yun Zi 
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
 * @author Yun Zi
 */
int getBlockPos (SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}
/**
 * @brief read the first block in fHandle, and store in memPage
 * 
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author MingXi Xia
 */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (RC_OK == readBlock(0, fHandle, memPage))
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}
/**
 * @brief read the previous block in fHandle, and store in memPage
 * @details previous block equals to current position minus 1
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author MingXi Xia
 */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int prev = getBlockPos(fHandle) - 1;
    if (RC_OK == readBlock(prev, fHandle, memPage))
        return RC_OK;
    else    
        return RC_READ_NON_EXISTING_PAGE;
}
/**
 * @brief read the current block in fHandle, and store in memPage
 * 
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author MingXi Xia
 */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(RC_OK==readBlock(getBlockPos(fHandle), fHandle, memPage))
        return RC_OK;
    else    
        return RC_READ_NON_EXISTING_PAGE;
}
/**
 * @brief read the next block in fHandle, and store in memPage
 * @details next block equals to current position plus 1
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author MingXi Xia
 */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int next = getBlockPos(fHandle) + 1;
    if(RC_OK == readBlock(next, fHandle, memPage))
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}
/**
 * @brief read the last block in fHandle, and store in memPage
 * @details lase block equals to totalNumPages minus 1
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author MingXi Xia
 */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int last = fHandle->totalNumPages - 1;
    if(RC_OK == readBlock(last, fHandle, memPage))
        return RC_OK;
    else
        return RC_READ_NON_EXISTING_PAGE;
}

/**
 * @brief writing blocks to a page file
 * @details writes a page to disk at absolute position
 * @param pageNum 
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author Mansoor Syed
 */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // check the file has less than pageNum pages.
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        return RC_WRITE_NON_EXISTING_PAGE;
    }

    ensureCapacity(pageNum, fHandle);
    FILE *fp = fopen(fHandle->fileName, "r+");

    int fs = fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
    if(fs != 0){
        return RC_WRITE_FAILED;
    }
    fwrite(memPage, sizeof(char), PAGE_SIZE, fp);
    fHandle->curPagePos = pageNum;

    fclose(fp);
    return RC_OK;
}

/**
 * @brief base on the current position in the fHandle to write current block
 * 
 * @param fHandle 
 * @param memPage 
 * @return RC 
 * @author Mansoor Syed
 */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(getBlockPos(fHandle), fHandle, memPage);
}


/**
 * @brief append empty block to page, and fill in 0
 * @details increases the amount of pages in the file by a page, this page is filled with zero bytes
 * @param fHandle 
 * @return RC 
 * @author Mansoor Syed
 */
RC appendEmptyBlock (SM_FileHandle *fHandle) {
    if(fHandle == NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }

    FILE *fp = fopen(fHandle->fileName,"a+");
    SM_PageHandle emptyPageAlloc = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    
    fseek(fp, 0, SEEK_END);
    fwrite(emptyPageAlloc, sizeof(char), PAGE_SIZE, fp);
    free(emptyPageAlloc);

    if (ffill(fHandle->mgmtInfo) != PAGE_SIZE) {
        return RC_WRITE_NON_EXISTING_PAGE;
    }

    fHandle->totalNumPages += 1;
    fclose(fp);
    return RC_OK;
}

/**
 * @brief extend total number page to numberOfPages
 * @details checks if the amount if pages in the file is less than numberOfPages if so it appends pages to the file until they are the same amount
 * @related appendEmptyBlock
 * @param numberOfPages 
 * @param fHandle 
 * @return RC 
 * @author Mansoor Syed
 */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {

    FILE *fp = fopen(fHandle->fileName,"a+");

    int total = fHandle->totalNumPages; //Temporariy storing the amount of pages allocated in file
    if (numberOfPages <= total) {       
        return RC_OK;
    }
    
    // calc the number of pages to append;
    //int newPageNum = numberOfPages - total;
    //int i = 0;
    while(total < numberOfPages) {
        RC value = appendEmptyBlock(fHandle);
        // if certain page append fail, the whole function is failed
        if (value != RC_OK){
            return value;
        }
        total += 1;
    }
    fclose(fp);
    return RC_OK;
}

