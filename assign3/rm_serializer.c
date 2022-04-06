#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"

// dynamic string
typedef struct VarString {
	char *buf;
	int size;
	int bufsize;
} VarString;

//Fixed:Summer 21: calloc(100,0);
#define MAKE_VARSTRING(var)				\
		do {							\
			var = (VarString *) malloc(sizeof(VarString));	\
			var->size = 0;					\
			var->bufsize = 100;					\
			var->buf = calloc(100,1);			\
		} while (0)

#define FREE_VARSTRING(var)			\
		do {						\
			free(var->buf);				\
			free(var);					\
		} while (0)

#define GET_STRING(result, var)			\
		do {						\
			result = malloc((var->size) + 1);		\
			memcpy(result, var->buf, var->size);	\
			result[var->size] = '\0';			\
		} while (0)

#define RETURN_STRING(var)			\
		do {						\
			char *resultStr;				\
			GET_STRING(resultStr, var);			\
			FREE_VARSTRING(var);			\
			return resultStr;				\
		} while (0)

#define ENSURE_SIZE(var,newsize)				\
		do {								\
			if (var->bufsize < newsize)					\
			{								\
				int newbufsize = var->bufsize;				\
				while((newbufsize *= 2) < newsize);			\
				var->buf = realloc(var->buf, newbufsize);			\
			}								\
		} while (0)

#define APPEND_STRING(var,string)					\
		do {									\
			ENSURE_SIZE(var, var->size + strlen(string));			\
			memcpy(var->buf + var->size, string, strlen(string));		\
			var->size += strlen(string);					\
		} while(0)

#define APPEND(var, ...)			\
		do {						\
			char *tmp = malloc(10000);			\
			sprintf(tmp, __VA_ARGS__);			\
			APPEND_STRING(var,tmp);			\
			free(tmp);					\
		} while(0)

// prototypes
static RC attrOffset (Schema *schema, int attrNum, int *result);

// implementations
char *
serializeTableInfo(RM_TableData *rel)
{
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "TABLE <%s> with <%i> tuples:\n", rel->name, getNumTuples(rel));
	APPEND_STRING(result, serializeSchema(rel->schema));

	RETURN_STRING(result);
}

char * 
serializeTableContent(RM_TableData *rel)
{
	int i;
	VarString *result;
	RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
	Record *r = (Record *) malloc(sizeof(Record));
	MAKE_VARSTRING(result);

	for(i = 0; i < rel->schema->numAttr; i++)
		APPEND(result, "%s%s", (i != 0) ? ", " : "", rel->schema->attrNames[i]);

	startScan(rel, sc, NULL);

	while(next(sc, r) != RC_RM_NO_MORE_TUPLES)
	{
		APPEND_STRING(result,serializeRecord(r, rel->schema));
		APPEND_STRING(result,"\n");
	}
	closeScan(sc);

	RETURN_STRING(result);
}

char *
serializeRecordMtdt(RM_RecordMtdt * recordMtdt) {
	int i;
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "tupleLen {%i} schemaLen {%i} slotLen {%i} slotMax {%i} slotOffset {%i} pageOffset {%i} schemaStr {%s}",
		recordMtdt->tupleLen,
		recordMtdt->schemaLen,
		recordMtdt->slotLen,
		recordMtdt->slotMax,
		recordMtdt->slotOffset,
		recordMtdt->pageOffset,
		recordMtdt->schemaStr
	);
	RETURN_STRING(result);
}

int
strtoi(char *delim, int cut_count) {
	char *ptr, *temp;
	int i;
	for (i = 0; i < cut_count; i++) {
		temp = strtok(NULL, delim);
	}
	return (int) strtol(temp, &ptr, 10);
}

char *
strtochar(char *delim, int cut_count) {
	char *ptr, *temp;
	int i;
	for (i = 0; i < cut_count; i++) {
		temp = strtok(NULL, delim);
	}
	ptr = (char *) malloc(sizeof(char) * strlen(temp));
	return strcpy(ptr, temp);
}

RM_RecordMtdt *
deserializeRecordMtdt(char * str) {
	RM_RecordMtdt *recordMtdt = (RM_RecordMtdt *) malloc(sizeof(RM_RecordMtdt));
	char *delim = "{}";
	char strcp[strlen(str)];
	strcpy(strcp, str);
	strtok(strcp, delim);
	recordMtdt->tupleLen = strtoi(delim, 1);
	recordMtdt->schemaLen = strtoi(delim, 2);
	recordMtdt->slotLen = strtoi(delim, 2);
	recordMtdt->slotMax = strtoi(delim, 2);
	recordMtdt->slotOffset = strtoi(delim, 2);
	recordMtdt->pageOffset = strtoi(delim, 2);
	recordMtdt->schemaStr = strtochar(delim, 2);
	return recordMtdt;
}

void setKeyIndex(Schema *schema, char *temp, int index) {
	int i;
	for (i = 0; i < schema->numAttr; i++) {
		if (strcmp(temp, schema->attrNames[i])) {
			schema->keyAttrs[index] = i;
			break;
		}
	}
}

void
deserializeSchemaKeys(char *str, Schema *schema) {
	char *temp;
	char strcp[strlen(str)];
	strcpy(strcp, str);
	temp = strtok(strcp, ",");

	if (strlen(temp) == 0) {
		schema->keySize = 0;
		return;
	}
	int index = 0;
	setKeyIndex(schema, temp, index);
	while(temp = strtok(NULL, ",")) {
		index += 1;
		setKeyIndex(schema, temp, index);
	}
	schema->keySize = index + 1;
	schema->keyAttrs = realloc(schema->keyAttrs, schema->keySize * sizeof(int));
}

Schema * 
deserializeSchema(char * str)
{
	// "Schema with <3> attributes (a: INT, b: STRING[4], c: INT) with keys: (a)\n"
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	char *temp;
	char strcp[strlen(str)];
	strcpy(strcp, str);
	strtok(strcp, "<>");
	schema->numAttr = strtoi("<>", 1);
	schema->attrNames = (char **)malloc(sizeof(char*) * schema->numAttr);
	schema->dataTypes = (DataType *)malloc(sizeof(DataType) * schema->numAttr);
	schema->typeLength = (int *) calloc(schema->numAttr, sizeof(int));
	schema->keyAttrs = (int *) malloc(schema->numAttr * sizeof(int));

	strtok(NULL, "(");
	int i;
	for (i = 0; i < schema->numAttr; i++) {
		schema->attrNames[i] = strtochar(":", 1);
		temp = strtok(NULL, i == schema->numAttr - 1 ? ")" : ",");
		if (strcmp(temp, "INT") == 0) {
			schema->dataTypes[i] = DT_INT;
		} else if (strcmp(temp, "FLOAT")) {
			schema->dataTypes[i] = DT_FLOAT;
		} else if (strcmp(temp, "BOOL")) {
			schema->dataTypes[i] = DT_BOOL;
		} else {
			temp = strtok(NULL, "[");
			if (strcmp(temp, "STRING")) {
				schema->dataTypes[i] = DT_STRING;
			}
			schema->typeLength[i] = strtoi("]", 1);
		}
	}
	if (schema->numAttr == 0) {
		temp = strtok(NULL, ")");
	}
	temp = strtok(NULL, "(");
	temp = strtok(NULL, ")");
	deserializeSchemaKeys(temp, schema);
	return schema;
}


char * 
serializeSchema(Schema *schema)
{
	int i;
	VarString *result;
	MAKE_VARSTRING(result);

	APPEND(result, "Schema with <%i> attributes (", schema->numAttr);

	for(i = 0; i < schema->numAttr; i++)
	{
		APPEND(result,"%s%s: ", (i != 0) ? ", ": "", schema->attrNames[i]);
		switch (schema->dataTypes[i])
		{
		case DT_INT:
			APPEND_STRING(result, "INT");
			break;
		case DT_FLOAT:
			APPEND_STRING(result, "FLOAT");
			break;
		case DT_STRING:
			APPEND(result,"STRING[%i]", schema->typeLength[i]);
			break;
		case DT_BOOL:
			APPEND_STRING(result,"BOOL");
			break;
		}
	}
	APPEND_STRING(result,")");

	APPEND_STRING(result," with keys: (");

	for(i = 0; i < schema->keySize; i++)
		APPEND(result, "%s%s", ((i != 0) ? ", ": ""), schema->attrNames[schema->keyAttrs[i]]);

	APPEND_STRING(result,")\n");

	RETURN_STRING(result);
}

char * 
serializeRecord(Record *record, Schema *schema)
{
	VarString *result;
	MAKE_VARSTRING(result);
	int i;

	APPEND(result, "[%i-%i] (", record->id.page, record->id.slot);

	for(i = 0; i < schema->numAttr; i++)
	{
		APPEND_STRING(result, serializeAttr (record, schema, i));
		APPEND(result, "%s", (i == 0) ? "" : ",");
	}

	APPEND_STRING(result, ")");

	RETURN_STRING(result);
}

char * 
serializeAttr(Record *record, Schema *schema, int attrNum)
{
	int offset;
	char *attrData;
	VarString *result;
	MAKE_VARSTRING(result);

	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;

	switch(schema->dataTypes[attrNum])
	{
	case DT_INT:
	{
		int val = 0;
		memcpy(&val,attrData, sizeof(int));
		APPEND(result, "%s:%i", schema->attrNames[attrNum], val);
	}
	break;
	case DT_STRING:
	{
		char *buf;
		int len = schema->typeLength[attrNum];
		buf = (char *) malloc(len + 1);
		strncpy(buf, attrData, len);
		buf[len] = '\0';

		APPEND(result, "%s:%s", schema->attrNames[attrNum], buf);
		free(buf);
	}
	break;
	case DT_FLOAT:
	{
		float val;
		memcpy(&val,attrData, sizeof(float));
		APPEND(result, "%s:%f", schema->attrNames[attrNum], val);
	}
	break;
	case DT_BOOL:
	{
		bool val;
		memcpy(&val,attrData, sizeof(bool));
		APPEND(result, "%s:%s", schema->attrNames[attrNum], val ? "TRUE" : "FALSE");
	}
	break;
	default:
		return "NO SERIALIZER FOR DATATYPE";
	}

	RETURN_STRING(result);
}

char *
serializeValue(Value *val)
{
	VarString *result;
	MAKE_VARSTRING(result);

	switch(val->dt)
	{
	case DT_INT:
		APPEND(result,"%i",val->v.intV);
		break;
	case DT_FLOAT:
		APPEND(result,"%f", val->v.floatV);
		break;
	case DT_STRING:
		APPEND(result,"%s", val->v.stringV);
		break;
	case DT_BOOL:
		APPEND_STRING(result, ((val->v.boolV) ? "true" : "false"));
		break;
	}

	RETURN_STRING(result);
}

Value *
stringToValue(char *val)
{
	Value *result = (Value *) malloc(sizeof(Value));

	switch(val[0])
	{
	case 'i':
		result->dt = DT_INT;
		result->v.intV = atoi(val + 1);
		break;
	case 'f':
		result->dt = DT_FLOAT;
		result->v.floatV = atof(val + 1);
		break;
	case 's':
		result->dt = DT_STRING;
		result->v.stringV = malloc(strlen(val));
		strcpy(result->v.stringV, val + 1);
		break;
	case 'b':
		result->dt = DT_BOOL;
		result->v.boolV = (val[1] == 't') ? TRUE : FALSE;
		break;
	default:
		result->dt = DT_INT;
		result->v.intV = -1;
		break;
	}

	return result;
}


RC 
attrOffset (Schema *schema, int attrNum, int *result)
{
	int offset = 0;
	int attrPos = 0;

	for(attrPos = 0; attrPos < attrNum; attrPos++)
		switch (schema->dataTypes[attrPos])
		{
		case DT_STRING:
			offset += schema->typeLength[attrPos];
			break;
		case DT_INT:
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			offset += sizeof(float);
			break;
		case DT_BOOL:
			offset += sizeof(bool);
			break;
		}

	*result = offset;
	return RC_OK;
}
