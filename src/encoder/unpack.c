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
#include "compat.h"
#include "convert.h"
#include "indicator.h"
#include "util.h"

uint32_t count_rows(unsigned char* data, const uint32_t length) {
    unsigned char *start = data;
    uint32_t n = 0;
    while ((data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(&data, &row_length);
        data += row_length;
        n++;
    }
    return n;
}

void unpack_rows(unsigned char** data, const uint32_t length, GiraffeColumns* columns,
        PyObject* rows) {
    PyObject* row;
    unsigned char* start = *data;


    while ((*data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(data, &row_length);
        row = PyList_New(columns->length);
        unpack_row(data, row_length, columns, row);
        PyList_Append(rows, row);
        Py_DECREF(row);
    }
}

void unpack_row(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
        PyObject* row) {
    size_t i;
    int nullable;
    unsigned char* ind;
    indicator_init(&ind, data, columns->header_length);
    for (i=0; i<columns->length; i++) {
        GiraffeColumn* column = &columns->array[i];
        nullable = indicator_read(ind, i);
        if (nullable) {
            if (column->GDType == GD_VARCHAR) {
                *data += 2;
            } else {
                *data += column->Length;
            }
            Py_INCREF(Py_None);
            PyList_SetItem(row, i, Py_None);
            continue;
        }
        switch (column->GDType) {
            case GD_BYTEINT:
                PyList_SetItem(row, i, byte_to_pylong(data));
                break;
            case GD_SMALLINT:
                PyList_SetItem(row, i, short_to_pylong(data));
                break;
            case GD_INTEGER:
                PyList_SetItem(row, i, int_to_pylong(data));
                break;
            case GD_BIGINT:
                PyList_SetItem(row, i, long_to_pylong(data));
                break;
            case GD_FLOAT:
                PyList_SetItem(row, i, float_to_pyfloat(data));
                break;
            case GD_DECIMAL:
                PyList_SetItem(row, i, decimal_to_pystring(data, column->Length, column->Scale));
                break;
            case GD_CHAR:
                PyList_SetItem(row, i, char_to_pystring(data, column->Length));
                break;
            case GD_VARCHAR:
                PyList_SetItem(row, i, vchar_to_pystring(data));
                break;
            case GD_DATE:
                PyList_SetItem(row, i, int_to_pylong(data));
                break;
            default:
                PyList_SetItem(row, i, char_to_pystring(data, column->Length));
        }
    }
    indicator_free(&ind);
}
