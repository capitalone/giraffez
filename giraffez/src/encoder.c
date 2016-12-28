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

// Python 2/3 C API and Windows compatibility
#include "_compat.h"

#include "columns.h"
#include "convert.h"
#include "types.h"
#include "unpack.h"


static EncoderSettings default_settings = {
    ROW_ENCODING_LIST,
    ITEM_ENCODING_BUILTIN_TYPES,
    DECIMAL_AS_STRING
};


TeradataEncoder* encoder_new(GiraffeColumns *columns, EncoderSettings *settings) {
    TeradataEncoder *e;
    e = (TeradataEncoder*)malloc(sizeof(TeradataEncoder));
    if (settings == NULL) {
        settings = &default_settings;
    }
    e->Columns = columns;
    e->Settings = *settings;
    e->Delimiter = NULL;
    e->NullValue = NULL;
    e->UnpackStmtInfoFunc = unpack_stmt_info_to_columns;
    e->UnpackRowsFunc = unpack_rows;
    e->UnpackRowFunc = NULL;
    e->UnpackItemFunc = NULL;
    e->UnpackDecimalFunc = NULL;
    encoder_set_delimiter(e, DEFAULT_DELIMITER);
    encoder_set_null(e, DEFAULT_NULLVALUE);
    encoder_set_encoding(e, settings);
    return e;
}

void encoder_set_encoding(TeradataEncoder *e, EncoderSettings *settings) {
    switch (settings->row_encoding_type) {
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
        case ROW_ENCODING_RAW:
            e->UnpackRowsFunc = unpack_rows_raw;
            break;
    }
    switch (settings->item_encoding_type) {
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
    switch (settings->decimal_return_type) {
        case DECIMAL_AS_STRING:
            e->UnpackDecimalFunc = decimal_to_pystring;
            break;
        case DECIMAL_AS_FLOAT:
            e->UnpackDecimalFunc = decimal_to_pyfloat;
            break;
        case DECIMAL_AS_GIRAFFEZ_DECIMAL:
            e->UnpackDecimalFunc = decimal_to_giraffez_decimal;
            break;
    }
}

void encoder_set_delimiter(TeradataEncoder* e, PyObject *obj) {
    Py_XDECREF(e->Delimiter);
    e->Delimiter = obj;
}

void encoder_set_null(TeradataEncoder *e, PyObject *obj) {
    Py_XDECREF(e->NullValue);
    e->NullValue = obj;
}

void encoder_clear(TeradataEncoder *e) {
    if (e->Columns != NULL) {
        columns_free(e->Columns);
        e->Columns = NULL;
    }
}

void encoder_free(TeradataEncoder *e) {
    Py_XDECREF(e->Delimiter);
    Py_XDECREF(e->NullValue);
    encoder_clear(e);
    if (e != NULL) {
        free(e);
    }
}
