#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"

#include <math.h>
#include <string.h>
#include <stdlib.h>


int MAX_BUFFER_NUMS = 10;
ReplacementStrategy REPLACE_STRATEGY = RS_LFU;
int ATTR_SIZE = 20;

// table and manager
/**
 * @brief initialize record manager
 * 
 * @param mgmtData 
 * @return RC 
 */
RC initRecordManager (void *mgmtData) {
	initStorageManager();
	return RC_OK;
}

/**
 * @brief shutdown record manager
 * 
 * @return RC 
 */
RC shutdownRecordManager () {
	shutdownStorageManager();
	return RC_OK;
}

/**
 * @brief calculate the length of one slot
 * 
 * @param schema 
 * @return int 
 */
int calcSlotLen (Schema *schema) {
	int len = 0, i = 0;
	for (i = 0; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i])
		{
			case DT_INT:
				len += increment;
				break;
			case DT_FLOAT:
				len += sizeof(float);
				break;
			case DT_BOOL:
				len += sizeof(bool);
				break;
			case DT_STRING:
				len += schema->typeLength[i];
				break;
			default:
				break;
		}
	}
	return len;
}

/**
 * @brief write any string to Page File
 * 
 * @param name 
 * @param fh 
 * @param str 
 * @return RC 
 */
RC writeStrToPage(char *name, int pageNum, char *str) {
	SM_FileHandle fh;
	RC result = RC_OK;
	result = createPageFile(name);
	if (result != RC_OK) {
		return result;
	}
	result = openPageFile(name, &fh);
	if (result != RC_OK) {
		return result;
	}
	// Page 0 include schema and relative table message
	result = writeBlock(pageNum, &fh, str);
	if (result != RC_OK) {
		return result;
	}
	return closePageFile(&fh);
}

/**
 * @brief update table header
 * 
 * @param name 
 * @param recordMtdt 
 * @return RC 
 */
RC writeTableHeader(char *name, RM_RecordMtdt *recordMtdt) {
	char * headerStr = serializeRecordMtdt(recordMtdt);
	return writeStrToPage(name, 0, headerStr);
}

/**
 * @brief Create a Table object
 * 
 * @param name 
 * @param schema 
 * @return RC 
 */
RC createTable (char *name, Schema *schema) {
	RC result; //
	RM_RecordMtdt *recordMtdt = (RM_RecordMtdt *) malloc(sizeof(RM_RecordMtdt)); //
	SM_FileHandle fHandle; //
	initBufferPool(&recordMtdt->bm, name, MAX_BUFFER_NUMS, RS_LFU, NULL); //

	int data[PAGE_SIZE]; //
	int increment = sizeof(int);

	recordMtdt->tupleLen = data[PAGE_SIZE];

    recordMtdt->tupleLen = 0;
    recordMtdt->tupleLen = recordMtdt->tupleLen + increment;

    recordMtdt->tupleLen = 1;
    recordMtdt->tupleLen = recordMtdt->tupleLen + increment;

	recordMtdt->tupleLen = schema->numAttr;
    recordMtdt->tupleLen = recordMtdt->tupleLen + increment;

    recordMtdt->tupleLen = schema->keySize;
    recordMtdt->tupleLen = recordMtdt->tupleLen + increment;

	for(int i = 0; i < schema->numAttr; i++){
		char temp = recordMtdt->tupleLen + '0';
        char *temp2 = &temp;
		strncpy(temp2, schema->attrNames[i], ATTR_SIZE);
		recordMtdt->tupleLen = recordMtdt->tupleLen + ATTR_SIZE;

		recordMtdt->tupleLen = (int)schema->dataTypes[i];
        recordMtdt->tupleLen = recordMtdt->tupleLen + increment;
        recordMtdt->tupleLen = (int)schema->typeLength[i];
        recordMtdt->tupleLen = recordMtdt->tupleLen + increment;
	}

	result = createPageFile(name);
	if(result != RC_OK){
		return result;
	}

	result = openPageFile(name, &fHandle);
	if(result != RC_OK){
		return result;
	}

	result = writeBlock(0, &fHandle, (char *)data);
	if(result != RC_OK){
		return result;
	}

	result = closePageFile(&fHandle);
	if(result != RC_OK){
		return result;
	}

	//recordMtdt->slotLen = calcSlotLen(schema);
	//recordMtdt->schemaStr = serializeSchema(schema);
	//recordMtdt->schemaLen = strlen(recordMtdt->schemaStr);
	//recordMtdt->slotOffset = 0;
	//// The storage of tuple starts from the 1th page
	//recordMtdt->pageOffset = 1;
	//recordMtdt->tupleLen = 0;
	
	//// Todo: schema overflow new block
	//recordMtdt->slotMax = (int)(floor(PAGE_SIZE/recordMtdt->slotLen));

	//result = writeTableHeader(name, recordMtdt);
	free(recordMtdt);
	return result;
}

/**
 * @brief 
 * 
 * @param rel 
 * @param name 
 * @return RC 
 */
RC openTable (RM_TableData *rel, char *name) {
	//if (!fexist(name)) {
	//	return RC_RM_TABLE_NOT_EXIST;
	//}
	//BM_BufferPool *bm = MAKE_POOL();
	//BM_PageHandle *ph = MAKE_PAGE_HANDLE();
  	//initBufferPool(bm, name, MAX_BUFFER_NUMS, REPLACE_STRATEGY, NULL);
	//pinPage(bm, ph, 0);

	//RM_RecordMtdt *mgmtData = deserializeRecordMtdt(ph->data);
	//rel->schema = deserializeSchema(mgmtData->schemaStr);
	//mgmtData->bm = bm;
	
	//free(mgmtData->schemaStr);
	//mgmtData->schemaStr = NULL;
	//rel->mgmtData = mgmtData;
	//free(ph->data);
	//free(ph);

	RM_RecordMtdt *recordMtdt = (RM_RecordMtdt *) malloc(sizeof(RM_RecordMtdt));
	int numAttr;
	int increment = sizeof(int);

	rel->mgmtData = recordMtdt;
	rel->name = name;

	BM_PageHandle pageHolder = recordMtdt-> ph;
	pinPage(&recordMtdt->bm, &recordMtdt->ph, 0);
	
	recordMtdt->ph.data = recordMtdt->ph.data + increment;
	
	recordMtdt->slotOffset = *(int*)recordMtdt->ph.data;
	recordMtdt->ph.data = recordMtdt->ph.data + increment;

	numAttr = *(int*)recordMtdt->ph.data;
	recordMtdt->ph.data = recordMtdt->ph.data + increment;

	Schema *schema = (Schema *) malloc(sizeof(Schema));

	schema->numAttr = numAttr;
	schema->attrNames = (char **) malloc(sizeof(char*)*numAttr);
	schema->dataTypes = (DataType *) malloc(sizeof(DataType)*numAttr);
	schema->typeLength = (int *) malloc(sizeof(int)*numAttr);

	for(int i = 0; i<numAttr; i++){
		schema->attrNames[i] = (char*) malloc(ATTR_SIZE);
	}
	for(int i = 0; i < schema->numAttr; i++){
		strncpy(schema->attrNames[i], recordMtdt->ph.data, ATTR_SIZE);
		recordMtdt->ph.data = recordMtdt->ph.data + ATTR_SIZE;

		schema->dataTypes[i] = *(int*)recordMtdt->ph;
		recordMtdt->ph = recordMtdt->ph + increment;
	}

	rel->schema = schema;
	unpinPage(&recordMtdt->bm, &pageHolder);
	forcePage(&recordMtdt->bm, &pageHolder);

	free(recordMtdt);
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	return RC_OK;
}

/**
 * @brief close table and free mgmtData and schema
 * 
 * @param rel 
 * @return RC 
 */
RC closeTable (RM_TableData *rel) {
	RM_RecordMtdt *mgmtData = rel->mgmtData;
	shutdownBufferPool(mgmtData->bm);
	free(rel->mgmtData);
	free(rel->schema->attrNames);
	free(rel->schema->dataTypes);
	free(rel->schema->keyAttrs);
	free(rel->schema->typeLength);
	free(rel->schema);
	return RC_OK;
}

/**
 * @brief 
 * 
 * @param name 
 * @return RC 
 */
RC deleteTable (char *name) {
	return destroyPageFile(name);
}

/**
 * @brief Get the Num Tuples object
 * 
 * @param rel 
 * @return int 
 */
int getNumTuples (RM_TableData *rel) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	return mgmtData->tupleLen;
}


void updateRecordOffset (RM_RecordMtdt *mgmtData, int offset) {
	mgmtData->tupleLen = mgmtData->tupleLen + offset;
	if (mgmtData->slotOffset + offset >= mgmtData->slotMax) {
		mgmtData->slotOffset = 0;
		mgmtData->pageOffset = mgmtData->pageOffset + offset;
	} else {
		mgmtData->slotOffset = mgmtData->slotOffset + offset;
	}
}

// handling records in a table
char *extendCharMemmory(char *oldData, char *newData) {
	int extendLen = strlen(oldData) + strlen(newData);
	oldData = (char *) realloc(oldData, extendLen);
	strcat(oldData, newData);
}

/**
 * @brief 
 * 
 * @param rel 
 * @param record 
 * @return RC 
 */
RC insertRecord (RM_TableData *rel, Record *record) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	BM_BufferPool *bm = &mgmtData->bm;

	record->id.page = mgmtData->pageOffset;
	record->id.slot = mgmtData->slotOffset;
	
	char *newData = serializeRecord(record, rel->schema);
	pinPage(bm, ph, record->id.page);
	ph->data = extendCharMemmory(ph->data, newData);
	
	markDirty(bm, ph);
	unpinPage(bm, ph);
	forcePage(bm, ph);

	updateRecordOffset(mgmtData, 1);
	writeTableHeader(rel->name, mgmtData);

	// free(ph->data);
	// free(ph);
	return RC_OK;
}

/**
 * @brief 
 * 
 * @param rel 
 * @param id 
 * @return RC 
 */
RC deleteRecord (RM_TableData *rel, RID id) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	BM_BufferPool *bm = &mgmtData->bm;
	
	pinPage(bm, ph, id.page);
	// Todo: NOT SURE how to clear parts of memory
	memcpy(ph->data + mgmtData->slotLen, "\0", mgmtData->slotLen);
	markDirty(bm, ph);
	unpinPage(bm, ph);
	forcePage(bm, ph);

	updateRecordOffset(mgmtData, -1);
	writeTableHeader(rel->name, mgmtData);
	free(ph->data);
	free(ph);
	return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	BM_BufferPool *bm = &mgmtData->bm;

	record->id.page = mgmtData->pageOffset;
	record->id.slot = mgmtData->slotOffset;
	
	char *newData = serializeRecord(record, rel->schema);

	pinPage(bm, ph, record->id.page);
	// Todo: NOT SURE how to clear parts of memory
	memcpy(ph->data + mgmtData->slotLen, newData, strlen(newData));
	markDirty(bm, ph);
	unpinPage(bm, ph);
	forcePage(bm, ph);

	free(ph->data);
	free(ph);
	return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	BM_BufferPool *bm = &mgmtData->bm;

	pinPage(bm, ph, record->id.page);
	record->id.page = id.page;
	record->id.slot = id.slot;

	// record->data = deserializeRecordMtdt()
	
	unpinPage(bm, ph);
	free(ph->data);
	free(ph);
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
	return calcSlotLen(schema);
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
	(*record)->data = (char *) malloc(getRecordSize(schema));
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
	*value = (Value *) malloc(sizeof(Value*));
	int offset;
	char *attrData;

	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;
	(*value)->dt = schema->dataTypes[attrNum];
	
	switch(schema->dataTypes[attrNum]) {
		case DT_INT:
			memcpy(&((*value)->v.intV), attrData, sizeof(int));
			break;
		case DT_FLOAT:
			memcpy(&((*value)->v.floatV), attrData, sizeof(float));
			break;
		case DT_BOOL:
			memcpy(&((*value)->v.boolV), attrData, sizeof(bool));
			break;
		case DT_STRING:
			strncpy((*value)->v.stringV, attrData, schema->typeLength[attrNum]);
			break;
	}
	return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
	int offset;
	char *attrData;

	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;

	switch(schema->dataTypes[attrNum]) {
		case DT_INT:
			memcpy(attrData, &(value->v.intV), sizeof(int));
			break;
		case DT_FLOAT:
			memcpy(attrData, &(value->v.floatV), sizeof(float));
			break;
		case DT_BOOL:
			memcpy(attrData, &(value->v.boolV), sizeof(bool));
			break;
		case DT_STRING:
			strncpy(attrData, (value->v.stringV), schema->typeLength[attrNum]);
			break;
	}
	return RC_OK;
}
