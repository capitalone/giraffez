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

#include "pack.h"

#include <Python.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>

// Python 2/3 C API and Windows compatibility
#include "_compat.h"

#include "columns.h"
#include "convert.h"
#include "encoder.h"
#include "pytypes.h"
#include "types.h"
#include "util.h"


PyObject* pack_rows(const TeradataEncoder *e, unsigned char **data, const uint32_t length) {
    PyObject *row;
    PyObject *rows;
    unsigned char *start = *data;
    rows = PyList_New(0);
    while ((*data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(data, &row_length);
        if ((row = e->UnpackRowFunc(e, data, row_length)) == NULL) {
            return NULL;
        }
        PyList_Append(rows, row);
        Py_DECREF(row);
    }
    return rows;
}


PyObject* pack_row(const TeradataEncoder *e, unsigned char **data, const uint16_t length) {
    PyObject *item;
    PyObject *row;
    GiraffeColumn *column;
    size_t i;
    int nullable;
    row = PyList_New(e->Columns->length);
    indicator_set(e->Columns, data);
    for (i=0; i<e->Columns->length; i++) {
        if (PyObject_RichCompareBool(item, e->NullValue)) {
            indicator_write(e->Columns->buffer, i);
        }
    }
    memcpy(*data, e->Columns->buffer, e->Columns->header_length); 
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        nullable = indicator_read(e->Columns->buffer, i);
        if (nullable) {
            *data += column->NullLength;
            Py_INCREF(e->NullValue);
            PyList_SetItem(row, i, e->NullValue);
            continue;
        }
        if ((item = e->UnpackItemFunc(e, data, column)) == NULL) {
            return NULL;
        }
        PyList_SetItem(row, i, item);
    }
    return row;
}

PyObject* pack_row_item(const TeradataEncoder *e, unsigned char **data, const GiraffeColumn *column) {
    switch (column->GDType) {
        case GD_BYTEINT:
            return byte_to_pylong(data);
        case GD_SMALLINT:
            return short_to_pylong(data);
        case GD_INTEGER:
            return int_to_pylong(data);
        case GD_BIGINT:
            return long_to_pylong(data);
        case GD_FLOAT:
            return float_to_pyfloat(data);
        case GD_DECIMAL:
            return e->UnpackDecimalFunc(data, column->Length, column->Scale);
        case GD_CHAR:
            return char_to_pystring(data, column->Length);
        case GD_VARCHAR:
            return vchar_to_pystring(data);
        case GD_DATE:
            return date_to_pydate(data);
        case GD_TIME:
            return char_to_time(data, column->Length);
        case GD_TIMESTAMP:
            return char_to_timestamp(data, column->Length);
        default:
            return char_to_pystring(data, column->Length);
    }
    return NULL;
}
