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

#ifndef __GIRAFFEZ_ENCODER_H
#define __GIRAFFEZ_ENCODER_H

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

#define ENCODER_SETTINGS_DEFAULT (ROW_ENCODING_DICT|ITEM_ENCODING_BUILTIN_TYPES|DECIMAL_AS_STRING)
#define ENCODER_SETTINGS_STRING  (ROW_ENCODING_STRING|ITEM_ENCODING_STRING|DECIMAL_AS_STRING)

enum RowEncodingType {
    ROW_ENCODING_INVALID  = 0x00,
    ROW_ENCODING_STRING   = 0x01,
    ROW_ENCODING_DICT     = 0x02,
    ROW_ENCODING_LIST     = 0x04,
    ROW_ENCODING_RAW      = 0x08,
    ROW_ENCODING_MASK     = 0xff,
};

enum ItemEncodingType {
    ITEM_ENCODING_INVALID        = 0x0000,
    ITEM_ENCODING_STRING         = 0x0100,
    ITEM_ENCODING_BUILTIN_TYPES  = 0x0200,
    ITEM_ENCODING_GIRAFFE_TYPES  = 0x0400,
    ITEM_ENCODING_MASK           = 0xff00,
};

enum DecimalReturnType {
    DECIMAL_AS_INVALID          = 0x000000,
    DECIMAL_AS_STRING           = 0x010000,
    DECIMAL_AS_FLOAT            = 0x020000,
    DECIMAL_AS_GIRAFFEZ_DECIMAL = 0x040000,
    DECIMAL_RETURN_MASK         = 0xff0000,
};

typedef struct TeradataEncoder {
    GiraffeColumns *Columns;
    PyObject *Delimiter;
    PyObject *NullValue;
    uint32_t Settings;

    GiraffeColumns* (*UnpackStmtInfoFunc)(unsigned char**,const uint32_t);
    PyObject* (*PackRowFunc)(const struct TeradataEncoder*,PyObject*,unsigned char**,uint16_t*);
    PyObject* (*UnpackRowsFunc)(const struct TeradataEncoder*,unsigned char**,const uint32_t);
    PyObject* (*UnpackRowFunc)(const struct TeradataEncoder*,unsigned char**,const uint16_t);
    PyObject* (*UnpackItemFunc)(const struct TeradataEncoder*,unsigned char**,const GiraffeColumn*);
    PyObject* (*UnpackDecimalFunc)(unsigned char**,const uint64_t,const uint16_t);
} TeradataEncoder;

typedef PyObject* (*EncoderFunc)(const TeradataEncoder*,unsigned char**,const uint16_t);

TeradataEncoder* encoder_new(GiraffeColumns *columns, uint32_t settings);
int encoder_set_encoding(TeradataEncoder *e, uint32_t settings);
void encoder_set_delimiter(TeradataEncoder *e, PyObject *obj);
void encoder_set_null(TeradataEncoder *e, PyObject *obj);
void encoder_clear(TeradataEncoder *e);
void encoder_free(TeradataEncoder *e);

#ifdef __cplusplus
}
#endif

#endif
