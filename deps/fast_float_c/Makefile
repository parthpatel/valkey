
fast_float_c.o: fast_float_c.cpp
	$(CXX) -c -fPIC -std=c++11 fast_float_c.cpp -o fast_float_c.o
		
test_fast_float_c.o: fast_float_c.o 
	$(CC) test_fast_float_c.c -c -o test_fast_float_c.o

test: test_fast_float_c.o 
	$(CXX) test_fast_float_c.o fast_float_c.o -o test
	./test

.Phony: clean all 

all: fast_float_c.o

clean: 
	rm *.o | true
	rm *.so | true
	rm *.gch | true
	rm test | true	
	
