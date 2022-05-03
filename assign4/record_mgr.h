#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"


// Bookkeeping for scans
typedef struct RM_ScanHandle
{
	RM_TableData *rel;
	void *mgmtData;
} RM_ScanHandle;

typedef struct RM_ScanMtdt
{
	Expr *expr;
	int page;
	int slot;
	
	int slotNum;
	int pageNum;
} RM_ScanMtdt;

typedef struct RM_RecordMtdt{
	int tupleLen; // exist's number of tuple 

	int schemaLen;// schema length
	char *schemaStr;// schema string

	int slotLen;// single slot length
	int slotMax;// the count of slot on one single page
	
	int slotOffset;// free slot offset
	int pageOffset;// page(block) offset
	
	BM_PageHandle *ph;// openTable -> page file
	BM_BufferPool *bm;
	BM_PageHandle *phSchema;

} RM_RecordMtdt;


// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager (void);
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

extern RC writeStrToPage(char *, int , char *);
extern char *serializeRecordMtdt(RM_RecordMtdt *);
extern RM_RecordMtdt *deserializeRecordMtdt(char *);
extern Schema *deserializeSchema(char * str);
extern RC attrOffset (Schema *, int, int *);
extern int strtoi(char *, int);
extern char *strtochar(char *, int);

#endif // RECORD_MGR_H
