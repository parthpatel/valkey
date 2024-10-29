#include "../fast_float/include/fast_float/fast_float.h"

extern "C"
{
    void from_chars_double(const char *first, const char *last, double *value)
    {
        double temp = 0;
        fast_float::from_chars(first, last, temp);
        *value = temp;
    }
}