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
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif


enum DecimalTypes {
    DECIMAL8   =  1,
    DECIMAL16  =  2,
    DECIMAL32  =  4,
    DECIMAL64  =  8,
    DECIMAL128 = 16
};

PyObject* byte_to_pylong(unsigned char** data);
PyObject* byte_to_pystring(unsigned char** data);
PyObject* char_to_pystring(unsigned char** data, const uint64_t column_length);
PyObject* char_to_time(unsigned char** data, const uint64_t column_length);
PyObject* char_to_timestamp(unsigned char** data, const uint64_t column_length);
PyObject* date_to_pydate(unsigned char** data);
PyObject* date_to_pystring(unsigned char** data);
PyObject* decimal_to_pystring(unsigned char** data, const uint64_t column_length,
    const uint16_t column_scale);
PyObject* decimal_to_pyfloat(unsigned char** data, const uint64_t column_length,
        const uint16_t column_scale);
PyObject* decimal_to_giraffez_decimal(unsigned char** data, const uint64_t column_length,
        const uint16_t column_scale);
PyObject* int_to_pylong(unsigned char** data);
PyObject* int_to_pystring(unsigned char** data);
PyObject* float_to_pyfloat(unsigned char** data);
PyObject* float_to_pystring(unsigned char** data);
PyObject* long_to_pylong(unsigned char** data);
PyObject* long_to_pystring(unsigned char** data);
PyObject* pystring_from_cformat(const char* fmt, ...);
PyObject* short_to_pylong(unsigned char** data);
PyObject* short_to_pystring(unsigned char** data);
PyObject* vchar_to_pystring(unsigned char** data);

PyObject* pystring_to_vchar(PyObject *s, unsigned char **buf, uint16_t *len);
PyObject* pystring_to_char(PyObject *s, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pylong_to_byte(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pylong_to_short(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pylong_to_int(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pylong_to_long(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pyfloat_to_float(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pydate_to_int(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len);
PyObject* pystring_to_decimal(PyObject *item, const uint16_t column_length, const uint16_t column_scale, unsigned char **buf, uint16_t *len);

#ifdef __cplusplus
}
#endif

#endif
