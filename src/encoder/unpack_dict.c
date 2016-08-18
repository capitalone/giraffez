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


void unpack_rows_dict(unsigned char** data, const uint32_t length, GiraffeColumns* columns,
        PyObject* rows) {
    PyObject* row;
    unsigned char* start = *data;

    while ((*data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(data, &row_length);
        row = PyDict_New();
        unpack_row_dict(data, row_length, columns, row);
        PyList_Append(rows, row);
        Py_DECREF(row);
    }
}

void unpack_row_dict(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
        PyObject* row) {
    PyObject* item;
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
            PyDict_SetItemString(row, column->Name, Py_None);
            continue;
        }
        switch (column->GDType) {
            case GD_BYTEINT:
                item = byte_to_pylong(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_SMALLINT:
                item = short_to_pylong(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_INTEGER:
                item = int_to_pylong(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_BIGINT:
                item = long_to_pylong(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_FLOAT:
                item = float_to_pyfloat(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_DECIMAL:
                item = decimal_to_pystring(data, column->Length, column->Scale);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_CHAR:
                item = char_to_pystring(data, column->Length);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_VARCHAR:
                item = vchar_to_pystring(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            case GD_DATE:
                item = date_to_pystring(data);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
                break;
            default:
                item = char_to_pystring(data, column->Length);
                PyDict_SetItemString(row, column->Name, item);
                Py_DECREF(item);
        }
    }
    indicator_free(&ind);
}
