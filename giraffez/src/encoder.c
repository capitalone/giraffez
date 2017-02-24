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
#include "pack.h"
#include "types.h"
#include "unpack.h"


TeradataEncoder* encoder_new(GiraffeColumns *columns, uint32_t settings) {
    TeradataEncoder *e;
    e = (TeradataEncoder*)malloc(sizeof(TeradataEncoder));
    if (settings == 0) {
        settings = ENCODER_SETTINGS_DEFAULT;
    }
    e->Columns = columns;
    e->Settings = settings;
    e->Delimiter = NULL;
    e->NullValue = NULL;
    e->DelimiterStr = NULL;
    e->NullValueStr = NULL;
    e->DelimiterStrLen = 0;
    e->NullValueStrLen = 0;
    /*e->buffer = malloc(sizeof(char) * ENCODER_BUFFER_SIZE);*/
    e->buffer = buffer_new(ENCODER_BUFFER_SIZE);
    e->PackRowFunc = pytuple_to_teradata_row;
    e->UnpackStmtInfoFunc = unpack_stmt_info_to_columns;
    e->UnpackRowsFunc = teradata_buffer_to_pylist;
    e->UnpackRowFunc = NULL;
    e->UnpackItemFunc = teradata_item_to_pyobject;
    e->UnpackDecimalFunc = NULL;
    encoder_set_delimiter(e, DEFAULT_DELIMITER);
    encoder_set_null(e, DEFAULT_NULLVALUE);
    if (encoder_set_encoding(e, settings) != 0) {
        return NULL;
    }
    return e;
}

int encoder_set_encoding(TeradataEncoder *e, uint32_t settings) {
    // to switch on the value we just mask the particular byte
    switch (settings & ROW_ENCODING_MASK) {
        case ROW_ENCODING_STRING:
            e->UnpackRowFunc = teradata_row_to_pystring;
            e->PackRowFunc = pystring_to_teradata_row;
            encoder_set_delimiter(e, DEFAULT_DELIMITER);
            encoder_set_null(e, DEFAULT_NULLVALUE_STR);
            break;
        case ROW_ENCODING_DICT:
            e->UnpackRowFunc = teradata_row_to_pydict;
            e->PackRowFunc = pydict_to_teradata_row;
            encoder_set_null(e, DEFAULT_NULLVALUE);
            break;
        case ROW_ENCODING_LIST:
            e->UnpackRowFunc = teradata_row_to_pytuple;
            e->PackRowFunc = pytuple_to_teradata_row;
            encoder_set_null(e, DEFAULT_NULLVALUE);
            break;
        case ROW_ENCODING_RAW:
            e->UnpackRowFunc = teradata_row_to_pybytes;
            e->UnpackRowsFunc = teradata_buffer_to_pybytes;
            /*e->UnpackRowsFunc = unpack_rows;*/
            e->PackRowFunc = pybytes_to_teradata_row;
            break;
        default:
            return -1;
    }
    switch (settings & ITEM_ENCODING_MASK) {
        case ITEM_ENCODING_STRING:
            /*e->UnpackItemFunc = unpack_row_item_as_str;*/
            e->UnpackDateFunc = teradata_date_to_pystring;
            e->UnpackTimeFunc = teradata_char_to_pystring;
            e->UnpackTimestampFunc = teradata_char_to_pystring;
            break;
        case ITEM_ENCODING_BUILTIN_TYPES:
            e->UnpackItemFunc = teradata_item_to_pyobject;
            e->UnpackDateFunc = teradata_date_to_pystring;
            e->UnpackTimeFunc = teradata_char_to_pystring;
            e->UnpackTimestampFunc = teradata_char_to_pystring;
            break;
        case ITEM_ENCODING_GIRAFFE_TYPES:
            e->UnpackDateFunc = teradata_date_to_giraffez_date;
            e->UnpackTimeFunc = teradata_time_to_giraffez_time;
            e->UnpackTimestampFunc = teradata_ts_to_giraffez_ts;
            break;
        default:
            return -1;
    }
    switch (settings & DECIMAL_RETURN_MASK) {
        case DECIMAL_AS_STRING:
            /*e->UnpackDecimalFunc = decimal_to_pystring;*/
            /*e->UnpackDecimalFunc = PyUnicode_FromStringAndSize;*/
            e->UnpackDecimalFunc = cstring_to_pystring;
            break;
        case DECIMAL_AS_FLOAT:
            /*e->UnpackDecimalFunc = cstring_to_pystring;*/
            e->UnpackDecimalFunc = cstring_to_pyfloat;
            break;
        case DECIMAL_AS_GIRAFFEZ_DECIMAL:
            /*e->UnpackDecimalFunc = decimal_to_giraffez_decimal;*/
            e->UnpackDecimalFunc = cstring_to_giraffez_decimal;
            break;
        default:
            return -1;
    }
    return 0;
}

PyObject* encoder_set_delimiter(TeradataEncoder *e, PyObject *obj) {
    Py_XDECREF(e->Delimiter);
    e->Delimiter = obj;
    // TODO: should probably check items here to ensure they are
    // the correct Python type
    // TODO: should also create a helper function for going from
    // utf-8 to bytes, needs to be very correct at the cost
    // of performance
    PyObject *tmp;
    char *delimiter = NULL;
    if (_PyUnicode_Check(obj)) {
        if ((delimiter = PyUnicode_AsUTF8(obj)) == NULL) {
            return NULL;
        }
    } else if (PyBytes_Check(obj)) {
        if ((delimiter = PyBytes_AsString(obj)) == NULL) {
            return NULL;
        }
    } else {
        tmp = PyObject_Str(obj);
        if ((delimiter = PyUnicode_AsUTF8(tmp)) == NULL) {
            return NULL;
        }
    }
    e->DelimiterStr = delimiter;
    e->DelimiterStrLen = strlen(delimiter);
    Py_RETURN_NONE;
}

PyObject* encoder_set_null(TeradataEncoder *e, PyObject *obj) {
    Py_XDECREF(e->NullValue);
    e->NullValue = obj;
    // TODO: should probably check items here to ensure they are
    // the correct Python type
    PyObject *tmp;
    char *null = NULL;
    if (_PyUnicode_Check(obj)) {
        if ((null = PyUnicode_AsUTF8(obj)) == NULL) {
            return NULL;
        }
    } else if (PyBytes_Check(obj)) {
        if ((null = PyBytes_AsString(obj)) == NULL) {
            return NULL;
        }
    } else {
        tmp = PyObject_Str(obj);
        if ((null = PyUnicode_AsUTF8(tmp)) == NULL) {
            return NULL;
        }
    }
    e->NullValueStr = null;
    e->NullValueStrLen = strlen(e->NullValueStr);
    Py_RETURN_NONE;
}

void encoder_clear(TeradataEncoder *e) {
    if (e != NULL && e->Columns != NULL) {
        columns_free(e->Columns);
        e->Columns = NULL;
    }
}

void encoder_free(TeradataEncoder *e) {
    Py_XDECREF(e->Delimiter);
    Py_XDECREF(e->NullValue);
    encoder_clear(e);
    if (e != NULL) {
        /*if (e->buffer != NULL) {*/
            /*free(e->buffer);*/
            /*e->buffer = NULL;*/
        /*}*/
        free(e);
        e = NULL;
    }
}

Buffer* buffer_new(int buffer_size) {
    Buffer *b;
    b = (Buffer*)malloc(sizeof(Buffer));
    b->length = 0;
    b->pos = 0;
    b->data = malloc(sizeof(char) * buffer_size);
    return b;
}

void buffer_write(Buffer *b, char *data, int length) {
    memcpy(b->data+b->pos, data, length);
    b->pos += length;
    b->length += length;
}

void buffer_writef(Buffer *b, const char *fmt, ...) {
    int length;
    va_list vargs;
    va_start(vargs, fmt);
    length = vsprintf(b->data, fmt, vargs);
    va_end(vargs);
    b->pos += length;
    b->length += length;
}

void buffer_reset(Buffer *b, size_t n) {
    b->pos = n;
    b->length = n;
}
