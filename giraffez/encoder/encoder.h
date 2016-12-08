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

#ifndef __ENCODER_ENCODER_H
#define __ENCODER_ENCODER_H

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


#define DEFAULT_DELIMITER PyUnicode_FromString("|")
#define DEFAULT_NULLVALUE Py_None
#define DEFAULT_NULLVALUE_STR PyUnicode_FromString("NULL")


typedef enum EncodingType {
    ENCODING_STRING = 0,
    ENCODING_DICT,
    ENCODING_GIRAFFE_TYPES,
} EncodingType;

typedef struct EncoderSettings {
    EncodingType Encoding;
    GiraffeColumns* Columns;
    PyObject* Delimiter;
    PyObject* NullValue;

    PyObject* (*UnpackRowsFunc)(const struct EncoderSettings*,unsigned char**,const uint32_t);
    PyObject* (*UnpackRowFunc)(const struct EncoderSettings*,unsigned char**,const uint16_t);
    PyObject* (*UnpackItemFunc)(unsigned char**,const GiraffeColumn*);
} EncoderSettings;

typedef PyObject* (*EncoderFunc)(const EncoderSettings*,unsigned char**,const uint16_t);

EncoderSettings* encoder_new(GiraffeColumns* columns);
void encoder_set_encoding(EncoderSettings* e, EncodingType t);
void encoder_set_delimiter(EncoderSettings* e, PyObject* obj);
void encoder_set_null(EncoderSettings* e, PyObject* obj);

#ifdef __cplusplus
}
#endif

#endif
