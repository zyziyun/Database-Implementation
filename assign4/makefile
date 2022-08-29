.PHONY: all
FILE_LIST = storage_mgr.c buffer_mgr.c buffer_mgr_stat.c dberror.c expr.c record_mgr.c rm_serializer.c btree_mgr.c
TARGET1 = test_assign4_1
TARGET2 = test_expr
SOURCE1 = test_assign4_1.c $(FILE_LIST)
SOURCE2 = test_expr.c $(FILE_LIST)

all: test_assign4_1 test_expr

test_assign4_1: $(SOURCE1)
	gcc -o $@ $^ -g -lm

test_expr: $(SOURCE2)
	gcc -o $@ $^ -g -lm

clean:
	rm -rf *.o $(TARGET1) $(TARGET2)