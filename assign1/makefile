.PHONY: all
FILE_LIST = test_assign1_1.c storage_mgr.c dberror.c
TARGET = test_assign1

all: test_assign1

test_assign1: $(FILE_LIST)
	gcc -o $(TARGET) $(FILE_LIST) -g

clean:
	rm -rf *.o $(TARGET)