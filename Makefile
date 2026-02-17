CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra

TARGET = server-relay
SRC = server-relay.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET) *.o

.PHONY: all clean
