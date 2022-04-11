#include "tables.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "expr.h"

#include <math.h>
#include <string.h>
#include <stdlib.h>


int MAX_BUFFER_NUMS = 10;
ReplacementStrategy REPLACE_STRATEGY = RS_LFU;

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
	// [1-0] (a:0,b:aaaa,c:3)
	// initize, 10(int slot.page, slot.id) + 5 [-] ()
	int len = 16, i = 0;
	for (i = 0; i < schema->numAttr; i++) {
		len += strlen(schema->attrNames[i]) + 2;// ,:
		switch (schema->dataTypes[i])
		{
			case DT_INT:
				len += 5;// maximum int size 5
				break;
			case DT_FLOAT:
				len += 10;
				break;
			case DT_BOOL:
				len += 5;
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
	writeStrToPage(name, 0, headerStr);
	free(headerStr);
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
	if (fexist(name)) {
        return RC_RM_TABLE_EXISTENCE;
    }
	RC result;
	RM_RecordMtdt *recordMtdt = (RM_RecordMtdt *) malloc(sizeof(RM_RecordMtdt));
	
	recordMtdt->slotLen = calcSlotLen(schema);
	recordMtdt->schemaStr = serializeSchema(schema);
	recordMtdt->schemaLen = strlen(recordMtdt->schemaStr);
	recordMtdt->slotOffset = 0;
	// The storage of tuple starts from the 1th page
	recordMtdt->pageOffset = 1;
	recordMtdt->tupleLen = 0;
	
	// Todo: schema overflow new block
	recordMtdt->slotMax = (int)(floor(PAGE_SIZE/recordMtdt->slotLen));

	result = writeTableHeader(name, recordMtdt);
	free(recordMtdt->schemaStr);
	free(recordMtdt);
	return result;
}

/**
 * @brief opens the table with the provided name
 * 
 * @param rel 
 * @param name 
 * @return RC 
 */
RC openTable (RM_TableData *rel, char *name) {
	if (!fexist(name)) {
		return RC_RM_TABLE_NOT_EXIST;
	}
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *ph = MAKE_PAGE_HANDLE();
	BM_PageHandle *phSchema = MAKE_PAGE_HANDLE();
  	initBufferPool(bm, name, MAX_BUFFER_NUMS, REPLACE_STRATEGY, NULL);
	
	pinPage(bm, phSchema, 0);
	RM_RecordMtdt *mgmtData = deserializeRecordMtdt(phSchema->data);
	rel->schema = deserializeSchema(mgmtData->schemaStr);
	
	mgmtData->bm = bm;
	mgmtData->ph = ph;
	mgmtData->phSchema = phSchema;
	rel->mgmtData = mgmtData;
	rel->name = name;
	return RC_OK;
}

/**
 * @brief close table and free mgmtData and schema
 * 
 * @param rel 
 * @return RC 
 */
RC closeTable (RM_TableData *rel) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	// write back header file to file system close buffer pool
	char *header = serializeRecordMtdt(mgmtData);
	memcpy(mgmtData->phSchema->data, header, strlen(header));
	markDirty(mgmtData->bm, mgmtData->phSchema);
	free(header);

	shutdownBufferPool(mgmtData->bm);
	free(mgmtData->ph);
	free(mgmtData->phSchema);
	free(mgmtData->schemaStr);
	free(rel->mgmtData);
	free(rel->schema->attrNames);
	free(rel->schema->dataTypes);
	free(rel->schema->keyAttrs);
	free(rel->schema->typeLength);
	free(rel->schema);
	return RC_OK;
}

/**
 * @brief deletes the table with given name
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

/**
 * @brief updates offset of record
 * 
 * @param mgmtData 
 * @param offset
 * @return void
 */
void updateRecordOffset (RM_RecordMtdt *mgmtData, int offset) {
	// offset: 1 | -1
	mgmtData->tupleLen = mgmtData->tupleLen + offset;
	if (mgmtData->slotOffset + offset >= mgmtData->slotMax) {
		mgmtData->slotOffset = 0;
		mgmtData->pageOffset = mgmtData->pageOffset + offset;
	} else if (mgmtData->slotOffset + offset < 0) {
		mgmtData->slotOffset = mgmtData->slotMax;
		mgmtData->pageOffset = mgmtData->pageOffset + offset;
	} else {
		mgmtData->slotOffset = mgmtData->slotOffset + offset;
	}
}

/**
 * @brief inserts a record to the table
 * 
 * @param rel 
 * @param record 
 * @return RC 
 */
RC insertRecord (RM_TableData *rel, Record *record) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = mgmtData->ph;
	BM_BufferPool *bm = mgmtData->bm;

	record->id.page = mgmtData->pageOffset;
	record->id.slot = mgmtData->slotOffset;
	char *newData = serializeRecord(record, rel->schema);

	pinPage(bm, ph, record->id.page);
	// find fit position and insert it
	int offset = record->id.slot * mgmtData->slotLen;
	memcpy(ph->data + offset, newData, strlen(newData));
	
	markDirty(bm, ph);
	unpinPage(bm, ph);

	updateRecordOffset(mgmtData, 1);
	free(newData);
	return RC_OK;
}

/**
 * @brief deletes a record from the table
 * 
 * @param rel 
 * @param id 
 * @return RC 
 */
RC deleteRecord (RM_TableData *rel, RID id) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = mgmtData->ph;
	BM_BufferPool *bm = mgmtData->bm;
	
	pinPage(bm, ph, id.page);
	// Todo: NOT SURE how to clear parts of memory
	int offset = id.slot * mgmtData->slotLen;
	char *str = (char *)calloc(mgmtData->slotLen, 1);

	memcpy(ph->data + offset, str, mgmtData->slotLen);
	markDirty(bm, ph);
	unpinPage(bm, ph);

	updateRecordOffset(mgmtData, -1);
	free(str);
	return RC_OK;
}

/**
 * @brief updates table in a record
 * 
 * @param rel
 * @param record 
 * @return RC 
 */
RC updateRecord (RM_TableData *rel, Record *record) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = mgmtData->ph;
	BM_BufferPool *bm = mgmtData->bm;

	char *newData = serializeRecord(record, rel->schema);
	pinPage(bm, ph, record->id.page);

	int offset = record->id.slot * mgmtData->slotLen;
	memcpy(ph->data + offset, newData, strlen(newData));

	markDirty(bm, ph);
	unpinPage(bm, ph);
	free(newData);
	return RC_OK;
}

/**
 * @brief gets data from serialize
 * 
 * @param mgmtData 
 * @return RC 
 */
RC getRecordDataFromSerialize(char *str, Schema *schema, Record **result) {
	// [1-0] (a:0,b:aaaa,c:3)
	char *temp;
	int i;
	char strcp[strlen(str)];

	// (*result)->data = (char *) malloc(getRecordSize(schema));
	strcpy(strcp, str);
	strtok(strcp, "(");

	for (i = 0; i < schema->numAttr; i++) {
		strtok(NULL, ":");
		char *delim = (i == schema->numAttr - 1) ? ")" : ",";
		Value *value = (Value *) malloc(sizeof(Value));
		value->dt = schema->dataTypes[i];
		if (schema->dataTypes[i] == DT_INT) {
			value->v.intV = strtoi(delim, 1);
		} else if (schema->dataTypes[i] == DT_BOOL) {
			temp = strtok(NULL, delim);
			value->v.boolV = temp[0] == 't' ? TRUE : FALSE;
		} else if (schema->dataTypes[i] == DT_FLOAT) {
			temp = strtok(NULL, delim);
			value->v.floatV = strtof(temp, NULL);
		} else if (schema->dataTypes[i] == DT_STRING) {
			value->v.stringV = strtochar(delim, 1);
		}
		setAttr(*result, schema, i, value);
		freeVal(value);
	}
	return RC_OK;
}

/**
 * @brief gets a record from a provided table and RID 
 * 
 * @param rel
 * @param id
 * @param record 
 * @return RC 
 */
RC getRecord (RM_TableData *rel, RID id, Record *record) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	BM_PageHandle *ph = mgmtData->ph;
	BM_BufferPool *bm = mgmtData->bm;

	int currIndex = (id.page - 1) * (mgmtData->slotMax - 1) + (id.slot + 1);
	if (currIndex > mgmtData->tupleLen) {
		return RC_RM_NO_MORE_TUPLES;
	}
	char str[mgmtData->slotLen];
	record->id.page = id.page;
	record->id.slot = id.slot;

	pinPage(bm, ph, id.page);
	int offset = record->id.slot * mgmtData->slotLen;
	memcpy(str, ph->data + offset, mgmtData->slotLen);

	if (str[0] == 0 && str[1] == 0) {
		unpinPage(bm, ph);
		return RC_RM_DELETED_TUPLES;
	}
	getRecordDataFromSerialize(str, rel->schema, &record);
	unpinPage(bm, ph);
	return RC_OK;
}

//scan
/**
 * @brief initializes a new scan
 * 
 * @param rel
 * @param scan
 * @param cond 
 * @return RC 
 */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	RM_RecordMtdt *mgmtData = (RM_RecordMtdt *) rel->mgmtData;
	RM_ScanMtdt *scanMtdt = (RM_ScanMtdt *) malloc(sizeof(RM_ScanMtdt));
	scanMtdt->expr = cond;
	scanMtdt->page = 1;
	scanMtdt->slot = 0;
	scanMtdt->pageNum = mgmtData->pageOffset;
	scanMtdt->slotNum = mgmtData->slotMax;
	scan->rel = rel;
	scan->mgmtData = scanMtdt;
	return RC_OK;
}

/**
 * @brief gets the next record in a table
 * 
 * @param scan
 * @param record 
 * @return RC 
 */
RC next (RM_ScanHandle *scan, Record *record) {
	RM_ScanMtdt *scanMtdt = (RM_ScanMtdt *)scan->mgmtData;
	Value *value;
	record->id.page = scanMtdt->page;
	record->id.slot = scanMtdt->slot;

	int result = getRecord(scan->rel, record->id, record);
	if (result != RC_OK) {
		return result;
	}

	evalExpr(record, scan->rel->schema, scanMtdt->expr, &value);
	if (scanMtdt->slot + 1 >= scanMtdt->slotNum) {
		scanMtdt->slot = 0;
		scanMtdt->page += 1;
	} else {
		scanMtdt->slot += 1;
	}
	if (value->v.boolV != TRUE) {
		return next(scan, record);
	}
	return RC_OK;
}

/**
 * @brief closes the initalizes scan
 * 
 * @param scan 
 * @return RC 
 */
RC closeScan (RM_ScanHandle *scan) {
	free(scan->mgmtData);
	return RC_OK;
}

// dealing with schemas
/**
 * @brief gets the size of each record provided a schema
 * 
 * @param schema 
 * @return int 
 */
int getRecordSize (Schema *schema) {
	int len = 0, i = 0;
	for (i = 0; i < schema->numAttr; i++) {
		switch (schema->dataTypes[i])
		{
			case DT_INT:
				len += sizeof(int);
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

/**
 * @brief frees a given schema
 * 
 * @param schema 
 * @return RC 
 */
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
	(*record) = (Record *) malloc(sizeof(Record));
	(*record)->data = (char *) malloc(getRecordSize(schema));
	return RC_OK;
}

/**
 * @brief frees a given record
 * 
 * @param record 
 * @return RC 
 */
RC freeRecord (Record *record) {
	free(record->data);
	free(record);
	return RC_OK;
}

/**
 * @brief gets attribute from record
 * 
 * @param record
 * @param schema
 * @param attrNum
 * @param value 
 * @return RC 
 */
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
			{
				char *str = (char*) malloc(schema->typeLength[attrNum] + 1);
				strncpy(str, attrData, schema->typeLength[attrNum]);
				str[schema->typeLength[attrNum]] = '\0';
				(*value)->v.stringV = str;
			}
			break;
	}
	return RC_OK;
}

/**
 * @brief sets attribute for given record
 * 
 * @param record
 * @param schema
 * @param attrNum
 * @param value 
 * @return RC 
 */
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
