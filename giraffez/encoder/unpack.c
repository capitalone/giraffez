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
#include "convert.h"
#include "types.h"


void unpack_int8_t(unsigned char **data, int8_t *dst) {
    *dst = *((*data)++);
}

void unpack_int16_t(unsigned char **data, int16_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffLL) << 8;
}

void unpack_uint16_t(unsigned char **data, uint16_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffULL) << 8;
}

void unpack_int32_t(unsigned char **data, int32_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffLL) << 8;
    *dst |= (*((*data)++) & 0xffLL) << 16;
    *dst |= (*((*data)++) & 0xffLL) << 24;
}

void unpack_int64_t(unsigned char **data, int64_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffLL) << 8;
    *dst |= (*((*data)++) & 0xffLL) << 16;
    *dst |= (*((*data)++) & 0xffLL) << 24;
    *dst |= (*((*data)++) & 0xffLL) << 32;
    *dst |= (*((*data)++) & 0xffLL) << 40;
    *dst |= (*((*data)++) & 0xffLL) << 48;
    *dst |= (*((*data)++) & 0xffLL) << 56;
}

void unpack_uint64_t(unsigned char **data, uint64_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffULL) << 8;
    *dst |= (*((*data)++) & 0xffULL) << 16;
    *dst |= (*((*data)++) & 0xffULL) << 24;
    *dst |= (*((*data)++) & 0xffULL) << 32;
    *dst |= (*((*data)++) & 0xffULL) << 40;
    *dst |= (*((*data)++) & 0xffULL) << 48;
    *dst |= (*((*data)++) & 0xffULL) << 56;
}

void unpack_float(unsigned char **data, double *dst) {
    dsplit_t u;

    u.b[0] = *((*data)++);
    u.b[1] = *((*data)++);
    u.b[2] = *((*data)++);
    u.b[3] = *((*data)++);
    u.b[4] = *((*data)++);
    u.b[5] = *((*data)++);
    u.b[6] = *((*data)++);
    u.b[7] = *((*data)++);
    *dst = u.d;
}

void unpack_char(unsigned char** data, char **str) {
    char* dest = (char*)malloc(sizeof(char)+1);
    memcpy(dest, (char*)*data, 1);
    dest[1] = '\0';
    *str = dest;
    *data += 1;
}

void unpack_uchar(unsigned char** data, unsigned char **str) {
    unsigned char* dest = (unsigned char*)malloc(sizeof(unsigned char)+1);
    memcpy(dest, (unsigned char*)*data, 1);
    dest[1] = '\0';
    *str = dest;
    *data += 1;
}

uint16_t unpack_string(unsigned char** data, char **str) {
    uint16_t len = 0;
    unpack_uint16_t(data, &len);
    if (len > 0) {
        char* dst = (char*)malloc(len+1);
        memcpy(dst, (char*)*data, len);
        dst[len] = '\0';
        *str = dst;
        *data += len;
    } else {
        *str = (char*)malloc(1);
        (*str)[0] = 0;
    }
    return len;
}

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
