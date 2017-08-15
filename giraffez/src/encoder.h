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

#include "common.h"
#include "columns.h"
#include "buffer.h"


enum RowEncodingType {
    ROW_ENCODING_INVALID  = 0x00,
    ROW_ENCODING_STRING   = 0x01,
    ROW_ENCODING_DICT     = 0x02,
    ROW_ENCODING_LIST     = 0x04,
    ROW_ENCODING_RAW      = 0x08,
    ROW_RETURN_MASK       = 0xff,
};

enum DateTimeReturnType {
    DATETIME_AS_INVALID        = 0x0000,
    DATETIME_AS_STRING         = 0x0100,
    DATETIME_AS_GIRAFFE_TYPES  = 0x0200,
    DATETIME_RETURN_MASK       = 0xff00,
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
    PyObject       *Delimiter;
    PyObject       *NullValue;
    uint32_t       Settings;
    size_t         DelimiterStrLen;
    char           *DelimiterStr;
    size_t         NullValueStrLen;
    char           *NullValueStr;
    buffer_t       *buffer;

    GiraffeColumns *(*UnpackStmtInfoFunc)  (unsigned char**, const uint32_t);

    PyObject *(*PackRowFunc)  (const struct TeradataEncoder*, PyObject*, unsigned char**, uint16_t*);
    PyObject *(*PackItemFunc) (const struct TeradataEncoder*, const GiraffeColumn*, PyObject*, unsigned char**, uint16_t*);

    PyObject *(*UnpackRowsFunc) (const struct TeradataEncoder*, unsigned char**, const uint32_t);
    PyObject *(*UnpackRowFunc)  (const struct TeradataEncoder*, unsigned char**, const uint16_t);
    PyObject *(*UnpackItemFunc) (const struct TeradataEncoder*, unsigned char**, const GiraffeColumn*);

    PyObject *(*UnpackDecimalFunc)   (const char*, const int);
    PyObject *(*UnpackDateFunc)      (unsigned char**);
    PyObject *(*UnpackTimeFunc)      (unsigned char**, const uint64_t);
    PyObject *(*UnpackTimestampFunc) (unsigned char**, const uint64_t);
} TeradataEncoder;

typedef PyObject *(*EncoderFunc)(const TeradataEncoder*, unsigned char**, const uint16_t);

TeradataEncoder* encoder_new(GiraffeColumns *columns, uint32_t settings);
int              encoder_set_encoding(TeradataEncoder *e, uint32_t settings);
PyObject*        encoder_set_delimiter(TeradataEncoder *e, PyObject *obj);
PyObject*        encoder_set_null(TeradataEncoder *e, PyObject *obj);
void             encoder_clear(TeradataEncoder *e);
void             encoder_free(TeradataEncoder *e);

#ifdef __cplusplus
}
#endif

#endif
