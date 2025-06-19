CXX       = mpicxx -std=c++20
OPTFLAGS  = -O3 -ffast-math
CXXFLAGS += -Wall
INCLUDES  = -I. -I./fastflow
LIBS      = -pthread

ifdef RPAYLOAD
  DEFS = -DRPAYLOAD=$(RPAYLOAD)
else
  DEFS =
endif

SOURCES = $(wildcard *.cpp)

TARGETS = $(SOURCES:.cpp=)

.PHONY: all clean cleanall

%: %.cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTFLAGS) $(DEFS) -o $@ $< $(LIBS)

all: $(TARGETS)

clean:
	-rm -f *.o *~

cleanall: clean
	-rm -f $(TARGETS)

