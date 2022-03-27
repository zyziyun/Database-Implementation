#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"


int MAX_BUFFER_NUMS = 10;
ReplacementStrategy REPLACE_STRATEGY = RS_LFU;

// table and manager
/**
 * @brief 
 * 
 * @param mgmtData 
 * @return RC 
 */
RC initRecordManager (void *mgmtData) {
	initStorageManager();
	return RC_OK;
}

/**
 * @brief 
 * 
 * @return RC 
 */
RC shutdownRecordManager () {
	shutdownStorageManager();
	return RC_OK;
}

/**
 * @brief Create a Table object
 * 
 * @param name 
 * @param schema 
 * @return RC 
 */
RC createTable (char *name, Schema *schema) {
	BM_BufferPool *bm = MAKE_POOL();

  	initBufferPool(bm, name, MAX_BUFFER_NUMS, REPLACE_STRATEGY, NULL);

	
	return RC_OK;
}

/**
 * @brief 
 * 
 * @param rel 
 * @param name 
 * @return RC 
 */
RC openTable (RM_TableData *rel, char *name) {

	return RC_OK;
}

RC closeTable (RM_TableData *rel) {
	return RC_OK;
}

RC deleteTable (char *name) {
	return RC_OK;
}

int getNumTuples (RM_TableData *rel) {
	return 1;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record) {
	return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {
	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
	return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {
	return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record) {
	return RC_OK;
}

RC closeScan (RM_ScanHandle *scan) {
	return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema) {
	return 1;
}

/**
 * @brief Create a memory space, and build a Schema object
 * 
 * @param numAttr 
 * @param attrNames 
 * @param dataTypes 
 * @param typeLength 
 * @param keySize 
 * @param keys 
 * @return Schema* 
 */
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;
	return schema;
}

RC freeSchema (Schema *schema) {
	free(schema);
	return RC_OK;
}

// dealing with records and attribute values
/**
 * @brief Create a Record object
 * 
 * @param record 
 * @param schema 
 * @return RC 
 */
RC createRecord (Record **record, Schema *schema) {
	*record = (Record *) malloc(sizeof(Record));
	// *record
	return RC_OK;
}

/**
 * @brief 
 * 
 * @param record 
 * @return RC 
 */
RC freeRecord (Record *record) {
	free(record->data);
	free(record);
	return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
	return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
	return RC_OK;
}
