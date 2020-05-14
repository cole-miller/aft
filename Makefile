CC = clang
CFLAGS = -Weverything \
	 -Wno-missing-noreturn \
	 -Wno-padded \
	 -Wno-reserved-id-macro \
	 -Wno-disabled-macro-expansion \
	 -g
LDLIBS = -lrt

all: driver driver-flaky

driver: server.o driver.o
	$(CC) $(CFLAGS) -o $@ server.o driver.o $(LDLIBS)

driver-flaky: flaky_server.o driver.o
	$(CC) $(CFLAGS) -o $@ flaky_server.o driver.o $(LDLIBS)

server.o: raft.h server.h server.c

flaky_server.o: raft.h server.h server.c
	$(CC) -D UNRELIABLE $(CFLAGS) -c -o $@ server.c

driver.o: raft.h driver.c

check:
	clang-check -analyze server.c driver.c --

clean:
	rm -f *.o *.plist driver driver-flaky
