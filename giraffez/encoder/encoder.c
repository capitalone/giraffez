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

#include "encoder.h"

#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif

#include "_compat.h"
#include "encoder/unpack.h"


EncoderSettings* encoder_new(GiraffeColumns* columns) {
    EncoderSettings* e;
    e = (EncoderSettings*)malloc(sizeof(EncoderSettings));
    e->Columns = columns;
    e->Delimiter = DEFAULT_DELIMITER;
    e->NullValue = DEFAULT_NULLVALUE_STR;
    e->UnpackRowsFunc = unpack_rows;
    e->UnpackRowFunc = unpack_row_str;
    e->UnpackItemFunc = unpack_row_item_as_str;
    return e;
}

void encoder_set_encoding(EncoderSettings* e, RowEncodingType row_t, ItemEncodingType item_t) {
    switch (row_t) {
        case ROW_ENCODING_STRING:
            e->UnpackRowFunc = unpack_row_str;
            encoder_set_delimiter(e, DEFAULT_DELIMITER);
            encoder_set_null(e, DEFAULT_NULLVALUE_STR);
            break;
        case ROW_ENCODING_DICT:
            e->UnpackRowFunc = unpack_row_dict;
            encoder_set_null(e, DEFAULT_NULLVALUE);
            break;
        case ROW_ENCODING_LIST:
            e->UnpackRowFunc = unpack_row_list;
            encoder_set_null(e, DEFAULT_NULLVALUE);
            break;
    }
    switch (item_t) {
        case ITEM_ENCODING_STRING:
            e->UnpackItemFunc = unpack_row_item_as_str;
            break;
        case ITEM_ENCODING_BUILTIN_TYPES:
            e->UnpackItemFunc = unpack_row_item_with_builtin_types;
            break;
        case ITEM_ENCODING_GIRAFFE_TYPES:
            e->UnpackItemFunc = unpack_row_item_with_giraffe_types;
            break;
    }
}

void encoder_set_delimiter(EncoderSettings* e, PyObject* obj) {
    Py_XDECREF(e->Delimiter);
    e->Delimiter = obj;
}

void encoder_set_null(EncoderSettings* e, PyObject* obj) {
    Py_XDECREF(e->NullValue);
    e->NullValue = obj;
}
