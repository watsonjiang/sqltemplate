

OBJS=MySQLDriver.o \
	 MySQLFactory.o \
	 MySQLTemplate.o \

CXXFLAGS=-I/usr/include/mysql

all:main

clean:
	$(RM) $(OBJS) main.o

libmysqltemplate.a: $(OBJS)
	ar rcs $@ $<

main:main.o libmysqltemplate.a
	echo $(CXX) -lmysqlclient -o $@ $^

.PHONY:clean all
