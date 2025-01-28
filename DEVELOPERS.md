# Information for Developers

Information that we want to share with each other so that it's easier to collaborate.

## Usage

1. Run `make` in `src/rt`
2. Use the `./rt_program` executable. There are three options for it, 'create', 'read', and 'add'.

### Command: create

Create a table with specified name and number of columns. Below creates the table `table1.tbl` and it has 5 columns.

```
./rt_program create <table_name> <num_col>
./rt_program create table1.tbl 5
```

### Command: read

Read the table and print out statistics.

```
./rt_program read <table_name>
./rt_program read table1.tbl
```

### Command: add

Add data to the table.

```
./rt_program add <table_name> <float1> <float2> ... <floatN>
./rt_program add table1.tbl 1.1 2.2 3.3 4.4 5.5
```

## Adding new Makefile Test

1. Write the new test in `src/tests/test_*.cpp`
2. Navigate to the Makefile and add a target for the executable and for the object file. Usually, it's just 
```Makefile
test_XXXX: test_XXXX.o rt.o helper.o
	$(CC) $@.o rt.o helper.o -o $@
...
test_XXXX.o: $(TESTS_DIR)/test_XXXX.cpp rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@
```

## Implementation Details / Misc

### rt

Contains code for the relational table storage.

The start of the file begins with:
1. The number of entries (rows) of the table (4 bytes, uint32_t)
2. The number of columns of the table (4 bytes, uint32_t)

The rest of the file is just filled with the entry data, each row takes up 4 * num_col bytes.

### rt_handler

Main file.

### helper

Helper functions.

