#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#


#---------------------build config-------------------------

DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=

BIND_LEVELDB ?= 0
BIND_ROCKSDB ?= 0
BIND_LMDB ?= 0
BIND_REDISDB ?= 0
BIND_KVMIGRATION ?= 0
BIND_ROCKSTEADY ?= 0
BIND_FULVA ?= 0
BIND_SOURCE ?= 0

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
endif

ifeq ($(BIND_LMDB), 1)
	LDFLAGS += -llmdb
	SOURCES += $(wildcard lmdb/*.cc)
endif

EXEC = ycsb

ifeq ($(BIND_REDISDB), 1)
	LDFLAGS += -lredis++ -lhiredis -lpthread
	SOURCES += $(wildcard redis/*.cc)
	EXEC = ycsb-redis
endif



ifeq ($(BIND_KVMIGRATION), 1)
	CPPSOURCES += $(wildcard ../utils/*.cpp)
	SOURCES += $(wildcard kv_migration/*.cc)
	EXEC = ycsb-kv_migration
endif

ifeq ($(BIND_ROCKSTEADY), 1)
	CPPSOURCES += $(wildcard ../utils/*.cpp)
	SOURCES += $(wildcard Rocksteady/*.cc)
	EXEC = ycsb-rocksteady
endif

ifeq ($(BIND_FULVA), 1)
	CPPSOURCES += $(wildcard ../utils/*.cpp)
	SOURCES += $(wildcard Fulva/*.cc)
	CSOURCES += $(wildcard Fulva/*.c)
	EXEC = ycsb-fulva
endif

ifeq ($(BIND_SOURCE), 1)
	CPPSOURCES += $(wildcard ../utils/*.cpp)
	SOURCES += $(wildcard Source/*.cc)
	EXEC = ycsb-source
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o) $(CPPSOURCES:.cpp=.o) $(CSOURCES:.c=.o)
DEPS += $(SOURCES:.cc=.d) $(CPPSOURCES:.cpp=.d) $(CSOURCES:.c=.d)


all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

%.o: %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.o: %.c
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.o: %.cpp
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d : %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

%.d : %.c
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.c=.o)' -o $@ $<

%.d : %.cpp
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cpp=.o)' -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
