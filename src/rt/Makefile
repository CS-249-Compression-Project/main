# Include the makefile.inc file
include ../makefile.inc

# Define the sources and the output executable
TESTS = test_1 test_2 test_3
TESTS_DIR = ../tests

all: rt_program $(TESTS)

rt_program: rt_handler.o rt.o helper.o
	$(CC) rt_handler.o rt.o helper.o -o rt_program

rt.o: rt.cpp rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

helper.o: helper.cpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

# test programs
test_1: test_1.o rt.o helper.o
	$(CC) $@.o rt.o helper.o -o $@

test_2: test_2.o rt.o helper.o
	$(CC) $@.o rt.o helper.o -o $@

test_3: test_3.o rt.o helper.o
	$(CC) $@.o rt.o helper.o -o $@

# test object files
test_1.o: $(TESTS_DIR)/test_1.cpp rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

test_2.o: $(TESTS_DIR)/test_2.cpp rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

test_3.o: $(TESTS_DIR)/test_3.cpp rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up the generated files
clean:
	rm -f $(OBJ) $(EXEC) $(TESTS) $(TESTS_OBJ) test_* rt_program

superclean:
	rm -f $(OBJ) $(EXEC) $(TESTS) $(TESTS_OBJ) *~ *.tbl *.o test_* rt_program

# Phony targets (these aren't real files, just commands)
.PHONY: clean superclean all
