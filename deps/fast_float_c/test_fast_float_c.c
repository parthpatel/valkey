#include "fast_float_c.h"
#include "assert.h"
#include "string.h"
#include "stdio.h"

void test1()
{
    const char *test_string = "231.2341234";
    double value = 0;
    const char *end = test_string + strlen(test_string);
    from_chars_double(test_string, end, &value);
    assert(value != 0);
    printf("successfully invoked cpp function from c, string %s converted to double %lf", test_string, value);
}

int main()
{
    test1();
    return 0;
}
