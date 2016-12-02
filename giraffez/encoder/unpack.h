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
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include "columns.h"


uint32_t count_rows(unsigned char* data, const uint32_t length);
uint16_t unpack_null_length(GiraffeColumn* column);

typedef struct EncoderSettings {
    GiraffeColumns* Columns;
    //const char* Delimiter;
    //const char* NullValue;
    PyObject* Delimiter;
    PyObject* NullValue;

    PyObject* (*Encoder)(struct EncoderSettings*,unsigned char**,const uint16_t);
} EncoderSettings;

typedef enum EncodingType {
    ENCODING_STRING = 0,
    ENCODING_DICT,
} EncodingType;

EncoderSettings* encoder_new(GiraffeColumns* columns);
void encoder_set_encoding(EncoderSettings* e, EncodingType t);
void encoder_set_delimiter(EncoderSettings* e, const char* delimiter);
void encoder_set_null(EncoderSettings* e, const char* null);

PyObject* unpack_rows(EncoderSettings* settings, unsigned char** data, const uint32_t length);

PyObject* unpack_row(EncoderSettings* settings, unsigned char** data, const uint16_t length);
PyObject* unpack_row_s(EncoderSettings* settings, unsigned char** data, const uint16_t length);
PyObject* unpack_row_dict(EncoderSettings* settings, unsigned char** data, const uint16_t length);
PyObject* unpack_row_item(unsigned char** data, GiraffeColumn* column);
PyObject* unpack_row_item_s(unsigned char** data, GiraffeColumn* column);

void unpack_row_x(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
    PyObject* row);

void unpack_stmt_info(unsigned char** data, StatementInfo* s, const uint32_t length);
void unpack_stmt_info_ext(unsigned char** data, StatementInfoColumn* column, const uint16_t length);

#ifdef __cplusplus
}
#endif

#endif
