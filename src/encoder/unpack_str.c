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


void unpack_rows_str(unsigned char** data, const uint32_t length, GiraffeColumns* columns,
        PyObject* rows, const char* null, const char* delimiter) {
    PyObject* row;
    PyObject* row_str;
    unsigned char* start = *data;

    PyObject* sep = PyUnicode_FromString(delimiter);
    while ((*data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(data, &row_length);
        row = PyList_New(columns->length);
        unpack_row_str(data, row_length, columns, row, null, delimiter);
        row_str = PyUnicode_Join(sep, row);
        PyList_Append(rows, row_str);
        Py_DECREF(row);
        Py_DECREF(row_str);
    }
    Py_DECREF(sep);
}

void unpack_row_str(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
        PyObject* row, const char* null, const char* delimiter) {
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
            PyList_SetItem(row, i, PyUnicode_FromString(null));
            continue;
        }
        switch (column->GDType) {
            case GD_BYTEINT:
                PyList_SetItem(row, i, byte_to_pystring(data));
                break;
            case GD_SMALLINT:
                PyList_SetItem(row, i, short_to_pystring(data));
                break;
            case GD_INTEGER:
                PyList_SetItem(row, i, int_to_pystring(data));
                break;
            case GD_BIGINT:
                PyList_SetItem(row, i, long_to_pystring(data));
                break;
            case GD_FLOAT:
                PyList_SetItem(row, i, float_to_pystring(data));
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
                PyList_SetItem(row, i, date_to_pystring(data));
                break;
            default:
                PyList_SetItem(row, i, char_to_pystring(data, column->Length));
        }
    }
    indicator_free(&ind);
}
