/*
 * Copyright 2016 Capital One Services, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __GIRAFFEZ_CONVERT_H
#define __GIRAFFEZ_CONVERT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#include "common.h"


enum DecimalTypes {
    DECIMAL8   =  1,
    DECIMAL16  =  2,
    DECIMAL32  =  4,
    DECIMAL64  =  8,
    DECIMAL128 = 16
};

enum IntegerTypes {
    INTEGER8   =  1,
    INTEGER16  =  2,
    INTEGER32  =  4,
    INTEGER64  =  8
};

typedef union {
    double d;
    unsigned char b[sizeof(double)];
} dsplit_t;

void pack_int8_t(unsigned char **data, int8_t val);
void pack_int16_t(unsigned char **data, int16_t val);
void pack_uint16_t(unsigned char **data, uint16_t val);
void pack_int32_t(unsigned char **data, int32_t val);
void pack_uint32_t(unsigned char **data, uint32_t val);
void pack_int64_t(unsigned char **data, int64_t val);
void pack_uint64_t(unsigned char **data, uint64_t val);
void pack_float(unsigned char **data, double val);
uint16_t pack_string(unsigned char **data, const char *str, const uint16_t length);
void unpack_int8_t(unsigned char **data, int8_t *dst);
void unpack_int16_t(unsigned char **data, int16_t *dst);
void unpack_uint16_t(unsigned char **data, uint16_t *dst);
void unpack_int32_t(unsigned char **data, int32_t *dst);
void unpack_int64_t(unsigned char **data, int64_t *dst);
void unpack_uint64_t(unsigned char **data, uint64_t *dst);
void unpack_float(unsigned char **data, double *dst);
void unpack_char(unsigned char **data, char **str);
void unpack_uchar(unsigned char **data, unsigned char **str);
uint16_t unpack_string(unsigned char **data, char **str);

// Character types
PyObject* teradata_char_to_pystring(unsigned char **data, const uint64_t column_length);
PyObject* teradata_vchar_to_pystring(unsigned char** data);

// Numeric types
PyObject* teradata_byteint_to_pylong(unsigned char **data);
PyObject* teradata_byteint_to_pystring(unsigned char **data);
PyObject* teradata_smallint_to_pylong(unsigned char **data);
PyObject* teradata_smallint_to_pystring(unsigned char **data);
PyObject* teradata_int_to_pylong(unsigned char **data);
PyObject* teradata_int_to_pystring(unsigned char **data);
PyObject* teradata_bigint_to_pylong(unsigned char **data);
PyObject* teradata_bigint_to_pystring(unsigned char **data);
PyObject* teradata_float_to_pyfloat(unsigned char **data);
PyObject* teradata_float_to_pystring(unsigned char **data);

// Dates
int teradata_date_to_cstring(unsigned char **data, char *buf);
PyObject* teradata_date_to_giraffez_date(unsigned char **data);
PyObject* teradata_date_to_pystring(unsigned char **data);
PyObject* teradata_time_to_giraffez_time(unsigned char **data, const uint64_t column_length);
PyObject* teradata_ts_to_giraffez_ts(unsigned char **data, const uint64_t column_length);

// Decimal
int teradata_decimal_to_cstring(unsigned char **data, const uint64_t column_length,
        const uint16_t column_scale, char *buf);
int teradata_decimal8_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf);
int teradata_decimal16_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf);
int teradata_decimal32_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf);
int teradata_decimal64_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf);
int teradata_decimal128_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf);


// TODO: inline?
inline PyObject* cstring_to_pystring(const char *buf, const int length);
PyObject* cstring_to_giraffez_decimal(const char *buf, const int length);
PyObject* cstring_to_pyfloat(const char *buf, const int length);
PyObject* pystring_from_cformat(const char* fmt, ...);

// PACK
PyObject* teradata_varchar_from_pystring(PyObject *s, unsigned char **buf, uint16_t *len);
PyObject* teradata_char_from_pystring(PyObject *s, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_byte_from_pylong(PyObject *item, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_short_from_pylong(PyObject *item, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_int_from_pylong(PyObject *item, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_long_from_pylong(PyObject *item, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_float_from_pyfloat(PyObject *item, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_int_from_pydate(PyObject *item, const uint16_t column_length,
    unsigned char **buf, uint16_t *len);
PyObject* teradata_decimal_from_pystring(PyObject *item, const uint16_t column_length,
    const uint16_t column_scale, unsigned char **buf, uint16_t *len);

#ifdef __cplusplus
}
#endif

#endif
