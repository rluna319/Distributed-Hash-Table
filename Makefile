CC = gcc
CFLAGS = -Wall -Wextra -O2

TARGET = node

SRC = node.c

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

.PHONY: clean
clean:
	rm -f $(TARGET)