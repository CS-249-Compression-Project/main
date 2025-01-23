# Include the makefile.inc file
include ../makefile.inc

# Define the sources and the output executable
SRCS = rt.cpp rt_handler.cpp
HEADERS = rt.hpp
OBJ = $(SRCS:.cpp=.o)
EXEC = rt_program
TESTS = test_1
TESTS_OBJ = $(TESTS:%=%.o)

all: $(EXEC) test_1

# Target to build the executable
$(EXEC): $(OBJ)
	$(CC) $(OBJ) -o $(EXEC)

# Rule to compile .cpp files to .o files
%.o: %.cpp $(HEADERS)
	$(CC) $(CPPFLAGS) -c $< -o $@

# binary dependencies for tests
$(TESTS): $(TESTS_OBJ) $(HEADERS)
	$(CC) $(OBJ) -o $(TESTS)

# obj dependencies
$(TESTS).o: ../tests/$(TESTS).cpp $(HEADERS)
	$(CC) $(CPPFLAGS) -c $< -o $@

# Clean up the generated files
clean:
	rm -f $(OBJ) $(EXEC) $(TESTS) ../tests/$(TESTS_OBJ)

# Phony targets (these aren't real files, just commands)
.PHONY: clean
