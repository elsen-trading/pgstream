#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <postgresql/libpq-fe.h>

int extract_int4_array (char *raw_array, int32_t *values, int size);
int extract_float_array (char *raw_array, float *values, int size);
int extract_double_array (char *raw_array, double *values, int size);
