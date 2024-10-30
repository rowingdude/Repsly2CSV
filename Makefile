# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -O2 -march=native
LDFLAGS = -lcurl -ljson-c -lm

# Project name
TARGET = repsly2csv

# Source files
SRC = repsly2csv.c
OBJ = $(SRC:.c=.o)

# Installation directory
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

# Debug build flags
DEBUGFLAGS = -g -DDEBUG

.PHONY: all clean install uninstall debug

# Default target
all: $(TARGET)

# Main build target
$(TARGET): $(OBJ)
	$(CC) $(OBJ) -o $(TARGET) $(LDFLAGS)

# Object file compilation
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Debug build
debug: CFLAGS += $(DEBUGFLAGS)
debug: clean $(TARGET)

# Clean build files
clean:
	rm -f $(TARGET) $(OBJ)
# rm -f *.csv *.json *.txt

# Install the executable
install: $(TARGET)
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET) $(DESTDIR)$(BINDIR)

# Uninstall the executable
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(TARGET)

# Dependencies check
check-deps:
	@echo "Checking dependencies..."
	@which curl-config >/dev/null || (echo "Error: curl-config not found. Please install libcurl development package." && exit 1)
	@pkg-config --libs json-c >/dev/null || (echo "Error: json-c not found. Please install libjson-c development package." && exit 1)

# Testing target (add your test commands here)
test: $(TARGET)
	@echo "Running tests..."
	./$(TARGET) --help

# Help target
help:
	@echo "Available targets:"
	@echo "  all        - Build the project (default)"
	@echo "  debug      - Build with debug flags"
	@echo "  clean      - Remove built files"
	@echo "  install    - Install to system"
	@echo "  uninstall  - Remove from system"
	@echo "  check-deps - Check required dependencies"
	@echo "  test       - Run tests"
	@echo "  help       - Show this help message"
