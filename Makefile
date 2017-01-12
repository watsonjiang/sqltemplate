

OBJS=MySQLDriver.o \
	 MySQLFactory.o \
	 MySQLTemplate.o \

CXXFLAGS=-I/usr/include/mysql -g

all:main

clean:
	$(RM) $(OBJS) main.o

libmysqltemplate.a: $(OBJS)
	ar rcs $@ $^

main:main.o libmysqltemplate.a
	$(CXX) -lmysqlclient -lpthread -o $@ $^

.PHONY:clean all
