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

#ifndef __ENCODER_CONVERT_H
#define __ENCODER_CONVERT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#ifdef _WIN32
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

PyObject* date_to_pystring(unsigned char** data);

PyObject* decimal_to_pystring(unsigned char** data, const uint64_t column_length,
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

#ifdef __cplusplus
}
#endif

#endif
