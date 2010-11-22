SHELL=/bin/bash

SRC=sds.cpp hiredis.cpp anet.cpp
OBJ=sds.o hiredis.o anet.o

default:
	gcc -fPIC -Wall -Wwrite-strings -O2 -c $(SRC)
	gcc -shared -Wl,-soname,libhiredis.so -o libhiredis.so.0.0.1 $(OBJ)
	/sbin/ldconfig -n .

clean:
	@rm *.o  libhiredis*
