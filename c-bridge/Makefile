sources=	executor_driver.cpp \
		scheduler_driver.cpp

objects=	executor_driver.o \
		scheduler_driver.o

all: link

cflags=-O2 -static -I/usr/local/include -I/usr/local/include/mesos -L/usr/local/lib

%.o : %.cpp
	$(CXX) $(cflags) -c $<

link: $(objects)
	ar -rcs libcbridge.a $(objects)

.PHONY : clean
clean:
	-rm -f $(objects)
	-rm -f libcbridge.a
