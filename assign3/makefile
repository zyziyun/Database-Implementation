.PHONY: all
FILE_LIST = storage_mgr.c buffer_mgr.c buffer_mgr_stat.c dberror.c
TARGET1 = test_assign2_1
TARGET2 = test_assign2_2
SOURCE1 = test_assign2_1.c $(FILE_LIST)
SOURCE2 = test_assign2_2.c $(FILE_LIST)

all: test_assign2_1 test_assign2_2

cp:
	cp ../assign1/storage_mgr.* ./
	cp ../assign1/dberror.* ./

test_assign2_1: $(SOURCE1)
	gcc -o $(TARGET1) $(SOURCE1) -g -lm

test_assign2_2: $(SOURCE2)
	gcc -o $(TARGET2) $(SOURCE2) -g -lm

clean:
	rm -rf *.o $(TARGET1) $(TARGET2)