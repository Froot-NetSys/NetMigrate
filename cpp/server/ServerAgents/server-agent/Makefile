

DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?= 

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./src/include -I./../../../utils/
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread -lredis++ -lhiredis
SOURCES += $(wildcard src/*.cpp)
SOURCES += $(wildcard ../../../utils/*.cpp)
OBJECTS += $(SOURCES:.cpp=.o)
DEPS += $(SOURCES:.cpp=.d)
EXEC = server_agent


all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cpp.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  cpp      " $@

%.d : %.cpp
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cpp=.o)' -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
