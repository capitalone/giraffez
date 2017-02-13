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


PyObject* pack_none(const GiraffeColumn *column, unsigned char **buf, uint16_t *len) {
    switch (column->GDType) {
        case GD_VARCHAR:
            memset(*buf, 0, 2);
            break;
        case GD_CHAR:
        case GD_DATE:
        case GD_TIME:
        case GD_TIMESTAMP:
            memset(*buf, 0x20, column->Length);
            break;
        default:
            memset(*buf, 0, column->Length);
    }
    *buf += column->NullLength;
    *len += column->NullLength;
    Py_RETURN_NONE;
}

PyObject* pack_row(const TeradataEncoder *e, PyObject *row, unsigned char **data, uint16_t *length) {
    PyObject *item;
    GiraffeColumn *column;
    Py_ssize_t i, slength;
    // TODO: check if tuple, list, etc
    slength = PySequence_Size(row);
    if (e->Columns->length != slength) {
        return NULL;
    }
    int nullable;
    unsigned char *ind = *data;
    indicator_clear(&ind, e->Columns->header_length);
    *data += e->Columns->header_length;
    *length += e->Columns->header_length;
    for (i=0; i<slength; i++) {
        column = &e->Columns->array[i];
        if ((item = PySequence_GetItem(row, i)) == NULL) {
            return NULL;
        }
        if ((nullable = PyObject_RichCompareBool(item, e->NullValue, Py_EQ)) == -1) {
            return NULL;
        }
        if (nullable) {
            indicator_write(&ind, i, 1);
            pack_none(column, data, length);
            continue;
        }
        if (pack_row_item(e, column, item, data, length) == NULL) {
            indicator_write(&ind, i, 1);
            pack_none(column, data, length);
            PyErr_Format(PyExc_ValueError, "Unable to unpack column %d '%s'\n", i, column->Name);
            return NULL;
            // TODO: if panic, then return NULL;
        }
    }
    Py_DECREF(item);
    Py_RETURN_NONE;
}

PyObject* pack_row_item(const TeradataEncoder *e, const GiraffeColumn *column, PyObject *item, unsigned char **data, uint16_t *length) {
    switch (column->GDType) {
        case GD_BYTEINT:
            return pylong_to_byte(item, column->Length, data, length);
        case GD_SMALLINT:
            return pylong_to_short(item, column->Length, data, length);
        case GD_INTEGER:
            return pylong_to_int(item, column->Length, data, length);
        case GD_BIGINT:
            return pylong_to_long(item, column->Length, data, length);
        case GD_FLOAT:
            return pyfloat_to_float(item, column->Length, data, length);
        case GD_DECIMAL:
            return pystring_to_decimal(item, column->Length, column->Scale, data, length);
        case GD_CHAR:
            return pystring_to_char(item, column->Length, data, length);
        case GD_VARCHAR:
            return pystring_to_vchar(item, data, length);
        case GD_DATE:
            return pydate_to_int(item, column->Length, data, length);
        case GD_TIME:
            return pystring_to_char(item, column->Length, data, length);
        case GD_TIMESTAMP:
            return pystring_to_char(item, column->Length, data, length);
        default:
            return pystring_to_char(item, column->Length, data, length);
    }
    return NULL;
}
