#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = 'A';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing A block\n");

  //write second block to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = 'B';
  TEST_CHECK(writeBlock (1, &fh, ph));
  printf("writing B block\n");

    //read page 2
  TEST_CHECK(readBlock (1,&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 'B'), "character B in page 2 read from disk is the one we expected.");
  printf("reading page 2\n");

  // read page 1
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 'A'), "character A in page 1 read from disk is the one we expected.");
  printf("reading first page 1\n");

 //read the next page
  TEST_CHECK(readNextBlock (&fh, ph));
  printf("reading page 2\n");

  //read current page
  TEST_CHECK(readCurrentBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 'B'), "character B in page  2 read from disk is the one we expected.");
  printf("reading current page 2 again\n");

  //read the previous block
  TEST_CHECK(readPreviousBlock (&fh, ph));
  printf("reading previous page 1 \n");

   //read the last block
  TEST_CHECK(readLastBlock (&fh, ph));
  printf("reading last page 2 \n");

  // append and test append
  int count = fh.totalNumPages;
  TEST_CHECK(ensureCapacity(4, &fh));
  int updatedCount = fh.totalNumPages;
  ASSERT_TRUE(updatedCount == count+3, "expected appending of 3 pages");                                  
  ASSERT_TRUE((fh.curPagePos == 0), "expected no changes of position:"  );
  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
}
