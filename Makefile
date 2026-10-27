CC = x86_64-w64-mingw32-g++
CFLAGS = -std=c++17 -Wall -Wextra
LDFLAGS = -mwindows -lws2_32 -lole32 -luser32 -luuid -lws2_32 -static-libgcc -static-libstdc++ -static

TARGET = loopback_stream.exe

all: $(TARGET)

$(TARGET): loopback_stream.cpp
	$(CC) $(CFLAGS) loopback_stream.cpp -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)
