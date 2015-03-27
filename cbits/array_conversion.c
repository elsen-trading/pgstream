#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <math.h>
#include <arpa/inet.h>
#include <postgresql/libpq-fe.h>

#define INT4OID      23
#define FLOAT4OID    700
#define FLOAT8OID    701
#define NUMERICOID   1700

// XXX: Check endianness problems on OS X? Production is invariable Linux so
// probably shouldn't matter.

// int4[]
struct array_int4 {
    int32_t ndim;
    int32_t _ign;
    Oid elemtype;
    
    int32_t size;
    int32_t index;
    int32_t data;
};

// float4[]
struct array_numeric {
    int32_t ndim;
    int32_t _ign;
    Oid elemtype;
    
    int32_t size;
    int32_t index;
    float data;
};


// Postgres binary uses network byte order, we have to swap the bytes for the
// floating point values.
static inline float swap( const float inFloat )
{
    float retVal;
    char *in = ( char * ) & inFloat;
    char *out = ( char * ) & retVal;
    
    out[0] = in[3];
    out[1] = in[2];
    out[2] = in[1];
    out[3] = in[0];
    
    return retVal;
}

#define INT_SEP -1

int extract_int4_array (char *raw_array, int32_t *values, int size)
{
    struct array_int4 *array = (struct array_int4 *) raw_array;
    int32_t *val = &(array->data);
    int32_t ival;
    
    if (ntohl(array->ndim) != 1 || ntohl(array->elemtype) != INT4OID) {
        return -1;
    }
    int array_elements = ntohl(array->size);
    
    int n = 0;
    val = &(array->data);
    for (int i = 0; i < array_elements; ++i) {
        ival = ntohl (*val);
        if (ival != INT_SEP) {
            ++val;
            values[n] = ntohl(*val);
            n += 1;
        }
        
        ++val;
    }
    
    return size;
}

#define FLOAT_SEP 6.0e-45

int extract_float_array(char *raw_array, float *values, int size)
{
    struct array_numeric *array = (struct array_numeric *) raw_array;
    float *val = &(array->data);
    int32_t ival;
    
    if (ntohl(array->ndim) != 1 || ntohl(array->elemtype) != FLOAT4OID) {
        return -1;
    }
    int array_elements = ntohl(array->size);
    
    int n = 0;
    val = &(array->data);
    for (int i = 0; i < array_elements; ++i) {
        ival = swap(*val);
        if (ival != FLOAT_SEP) {
            ++val;
            values[n] = swap(*val);
            n += 1;
        }
        ++val;
    }
    
    return size;
}
