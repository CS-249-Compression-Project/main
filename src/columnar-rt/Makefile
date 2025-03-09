# Include the makefile.inc file
include ../makefile.inc

# Define the sources and the output executable
TESTS = test_1 test_2 test_3 test_4
TESTS_DIR = ../column_tests

all: columnar_rt_program $(TESTS)

columnar_rt_program: columnar_rt_handler.o columnar_rt.o helper.o
	$(CC) columnar_rt_handler.o columnar_rt.o helper.o -o $@

columnar_rt.o: columnar_rt.cpp columnar_rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

helper.o: helper.cpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

# test programs
test_1: test_1.o columnar_rt.o helper.o
	$(CC) $@.o columnar_rt.o helper.o -o $@

test_2: test_2.o columnar_rt.o helper.o
	$(CC) $@.o columnar_rt.o helper.o -o $@

test_3: test_3.o columnar_rt.o helper.o
	$(CC) $@.o columnar_rt.o helper.o -o $@

test_4: test_4.o columnar_rt.o helper.o
	$(CC) $@.o columnar_rt.o helper.o -o $@

# test_5: test_5.o columnar_rt.o helper.o
# 	$(CC) $@.o columnar_rt.o helper.o -o $@

# test_6: test_6.o columnar_rt.o helper.o
# 	$(CC) $@.o columnar_rt.o helper.o -o $@

# test object files
test_1.o: $(TESTS_DIR)/test_1.cpp columnar_rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

test_2.o: $(TESTS_DIR)/test_2.cpp columnar_rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

test_3.o: $(TESTS_DIR)/test_3.cpp columnar_rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

test_4.o: $(TESTS_DIR)/test_4.cpp columnar_rt.hpp helper.hpp
	$(CC) $(CFLAGS) -c $< -o $@

# test_5.o: $(TESTS_DIR)/test_5.cpp columnar_rt.hpp helper.hpp
# 	$(CC) $(CFLAGS) -c $< -o $@

# test_6.o: $(TESTS_DIR)/test_6.cpp columnar_rt.hpp helper.hpp
# 	$(CC) $(CFLAGS) -c $< -o $@

# Clean up the generated files
clean:
	rm -f $(OBJ) $(EXEC) $(TESTS) $(TESTS_OBJ) test_* columnar_rt_program

superclean:
	rm -f $(OBJ) $(EXEC) $(TESTS) $(TESTS_OBJ) *~ *.tbl *.o test_* columnar_rt_program

# Phony targets (these aren't real files, just commands)
.PHONY: clean superclean all
