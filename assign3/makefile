.PHONY: all
FILE_LIST = storage_mgr.c buffer_mgr.c buffer_mgr_stat.c rm_serializer.c record_mgr.c dberror.c
TARGET1 = test_assign3_1
TARGET2 = test_expr
SOURCE1 = test_assign3_1.c $(FILE_LIST)
SOURCE2 = test_expr.c $(FILE_LIST)

all: test_assign3_1 test_expr

test_assign3_1: $(SOURCE1)
	gcc -o $(TARGET1) $(SOURCE1) -g 

test_expr: $(SOURCE2)
	gcc -o $(TARGET2) $(SOURCE2) -g 

clean:
	rm -rf *.o $(TARGET1) $(TARGET2)