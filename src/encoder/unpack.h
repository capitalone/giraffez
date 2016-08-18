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

#ifndef __ENCODER_UNPACK_H
#define __ENCODER_UNPACK_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#ifdef _WIN32
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include "columns.h"


enum GiraffeTypes {
    GD_DEFAULT = 0,
    GD_BYTEINT,
    GD_SMALLINT,
    GD_INTEGER,
    GD_BIGINT,
    GD_FLOAT,
    GD_DECIMAL,
    GD_CHAR,
    GD_VARCHAR,
    GD_DATE
};

uint32_t count_rows(unsigned char* data, const uint32_t length);
void unpack_row(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
    PyObject* row);
void unpack_rows(unsigned char** data, const uint32_t length, GiraffeColumns* columns,
    PyObject* rows);
void unpack_row_dict(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
    PyObject* row);
void unpack_rows_dict(unsigned char** data, const uint32_t length, GiraffeColumns* columns,
    PyObject* rows);
void unpack_row_str(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
    PyObject* row, const char* null, const char* delimiter);
void unpack_rows_str(unsigned char** data, const uint32_t length, GiraffeColumns* columns,
    PyObject* rows, const char* null, const char* delimiter);

#ifdef __cplusplus
}
#endif

#endif
