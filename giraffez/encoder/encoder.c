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

#include "unpack.h"
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>
#include <string.h>
#include "_compat.h"
#include "columns.h"
#include "convert.h"
#include "pytypes.h"
#include "types.h"
#include "util.h"


#define DEFAULT_DELIMITER PyUnicode_FromString("|");
#define DEFAULT_NULLVALUE (Py_INCREF(Py_None), Py_None)
#define DEFAULT_NULLVALUE_STR PyUnicode_FromString("NULL")

EncoderSettings* encoder_new(GiraffeColumns* columns) {
    EncoderSettings* e;
    e = (EncoderSettings*)malloc(sizeof(EncoderSettings));
    e->Columns = columns;
    e->Delimiter = DEFAULT_DELIMITER;
    e->NullValue = DEFAULT_NULLVALUE;
    e->UnpackRowsFunc = unpack_rows;
    e->UnpackRowFunc = unpack_row_str;
    e->UnpackItemFunc = unpack_row_item_as_str;
    return e;
}

void encoder_set_encoding(EncoderSettings* e, EncodingType t) {
    switch (t) {
        case ENCODING_STRING:
            e->UnpackRowFunc = unpack_row_str;
            e->UnpackItemFunc = unpack_row_item_as_str;
            break;
        case ENCODING_DICT:
            e->UnpackRowFunc = unpack_row_dict;
            e->UnpackItemFunc = unpack_row_item_with_builtin_types;
            break;
        case ENCODING_GIRAFFE_TYPES:
            e->UnpackRowFunc = unpack_row_list;
            e->UnpackItemFunc = unpack_row_item_with_giraffe_types;
            break;
    }
}

void encoder_set_delimiter(EncoderSettings* e, const char* delimiter) {
    e->Delimiter = PyUnicode_FromString(delimiter);
}

void encoder_set_null(EncoderSettings* e, const char* null) {
    e->NullValue = PyUnicode_FromString(null);
}
