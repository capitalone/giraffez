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
#include "encoder.h"
#include "pytypes.h"
#include "types.h"
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

PyObject* unpack_rows(EncoderSettings* e, unsigned char** data, const uint32_t length) {
    PyObject* row;
    PyObject* rows;
    unsigned char* start = *data;
    rows = PyList_New(0);
    while ((*data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(data, &row_length);
        row = e->UnpackRowFunc(e, data, row_length);
        PyList_Append(rows, row);
        Py_DECREF(row);
    }
    return rows;
}

PyObject* unpack_row_dict(EncoderSettings* e, unsigned char** data, const uint16_t length) {
    PyObject* item;
    PyObject* row;
    GiraffeColumn* column;
    size_t i;
    int nullable;
    row = PyDict_New();
    indicator_set(e->Columns, data);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        nullable = indicator_read(e->Columns->buffer, i);
        if (nullable) {
            *data += column->NullLength;
            PyDict_SetItemString(row, column->Name, e->NullValue);
            continue;
        }
        item = e->UnpackItemFunc(data, column);
        PyDict_SetItemString(row, column->Name, item);
        Py_DECREF(item);
    }
    return row;
}

PyObject* unpack_row_list(EncoderSettings* e, unsigned char** data, const uint16_t length) {
    PyObject* row;
    GiraffeColumn* column;
    size_t i;
    int nullable;
    row = PyList_New(e->Columns->length);
    indicator_set(e->Columns, data);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        nullable = indicator_read(e->Columns->buffer, i);
        if (nullable) {
            *data += column->NullLength;
            PyList_SetItem(row, i, e->NullValue);
            continue;
        }
        PyList_SetItem(row, i, e->UnpackItemFunc(data, column));
    }
    return row;
}

PyObject* unpack_row_str(EncoderSettings* e, unsigned char** data, const uint16_t length) {
    PyObject* row;
    PyObject* row_str;
    row = unpack_row_list(e, data, length);
    row_str = PyUnicode_Join(e->Delimiter, row);
    Py_DECREF(row);
    return row_str;
}

PyObject* unpack_row_item_as_str(unsigned char** data, GiraffeColumn* column) {
    switch (column->GDType) {
        case GD_BYTEINT:
            return byte_to_pystring(data);
        case GD_SMALLINT:
            return short_to_pystring(data);
        case GD_INTEGER:
            return int_to_pystring(data);
        case GD_BIGINT:
            return long_to_pystring(data);
        case GD_FLOAT:
            return float_to_pystring(data);
        case GD_DECIMAL:
            return decimal_to_pystring(data, column->Length, column->Scale);
        case GD_CHAR:
            return char_to_pystring(data, column->Length);
        case GD_VARCHAR:
            return vchar_to_pystring(data);
        case GD_DATE:
            return date_to_pystring(data);
        default:
            return char_to_pystring(data, column->Length);
    }
    return NULL;
}

PyObject* unpack_row_item_with_builtin_types(unsigned char** data, GiraffeColumn* column) {
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
            return decimal_to_pystring(data, column->Length, column->Scale);
        case GD_CHAR:
            return char_to_pystring(data, column->Length);
        case GD_VARCHAR:
            return vchar_to_pystring(data);
        case GD_DATE:
            return date_to_pystring(data);
        default:
            return char_to_pystring(data, column->Length);
    }
    return NULL;
}

PyObject* unpack_row_item_with_giraffe_types(unsigned char** data, GiraffeColumn* column) {
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
            return giraffez_decimal_from_pystring(decimal_to_pystring(data,
                column->Length, column->Scale));
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

GiraffeColumns* unpack_stmt_info_to_columns(unsigned char** data, const uint32_t length) {
    StatementInfoColumn *scolumn;
    StatementInfo *s;
    GiraffeColumn *column;
    GiraffeColumns *columns;
    size_t i;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);
    s = (StatementInfo*)malloc(sizeof(StatementInfo));
    stmt_info_init(s, 1);
    unpack_stmt_info(data, s, length);
    for (i=0; i<s->length; i++) {
        scolumn = &s->array[i];
        column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
        column->Name = strdup(scolumn->Name);
        column->Type = scolumn->Type;
        column->Length = scolumn->Length;
        column->Precision = scolumn->Precision;
        column->Scale = scolumn->Scale;
        column->Alias = scolumn->Alias;
        column->Title = scolumn->Title;
        column->Format = scolumn->Format;
        column->Default = scolumn->Default;
        column->Nullable = scolumn->CanReturnNull;
        columns_append(columns, *column);
    }
    stmt_info_free(s);
    return columns;
}

void unpack_stmt_info(unsigned char** data, StatementInfo* s, const uint32_t length) {
    unsigned char* start = *data;
    StatementInfoColumn* column;
    while ((*data - start) < length) {
        column = (StatementInfoColumn*)malloc(sizeof(StatementInfoColumn));
        unpack_uint16_t(data, &column->ExtensionLayout);
        unpack_uint16_t(data, &column->ExtensionType);
        unpack_uint16_t(data, &column->ExtensionLength);
        if (column->ExtensionLayout != 1) {
            *data += column->ExtensionLength;
            continue;
        }
        unpack_stmt_info_ext(data, column, column->ExtensionLength);
        stmt_info_append(s, *column);
    }
}

void unpack_stmt_info_ext(unsigned char** data, StatementInfoColumn* column, const uint16_t length) {
    unsigned char* start = *data;
    unpack_string(data, &column->Database);
    unpack_string(data, &column->Table);
    unpack_string(data, &column->Name);
    unpack_uint16_t(data, &column->Position);
    unpack_string(data, &column->Alias);
    unpack_string(data, &column->Title);
    unpack_string(data, &column->Format);
    unpack_string(data, &column->Default);
    unpack_char(data, &column->IdentityColumn);
    unpack_char(data, &column->DefinitelyWritable);
    unpack_char(data, &column->NotDefinedNotNull);
    unpack_char(data, &column->CanReturnNull);
    unpack_char(data, &column->PermittedInWhere);
    unpack_char(data, &column->Writable);
    unpack_uint16_t(data, &column->Type);
    unpack_uint16_t(data, &column->UDType);
    unpack_string(data, &column->TypeName);
    unpack_string(data, &column->DataTypeMiscInfo);
    unpack_uint64_t(data, &column->Length);
    unpack_uint16_t(data, &column->Precision);
    unpack_uint16_t(data, &column->Interval);
    unpack_uint16_t(data, &column->Scale);
    unpack_uchar(data, &column->CharacterSetType);
    unpack_uint64_t(data, &column->TotalNumberCharacters);
    unpack_uchar(data, &column->CaseSensitive);
    unpack_uchar(data, &column->NumericItemSigned);
    unpack_uchar(data, &column->UniquelyDescribesRow);
    unpack_uchar(data, &column->OnlyMemberUniqueIndex);
    unpack_uchar(data, &column->IsExpression);
    unpack_uchar(data, &column->PermittedInOrderBy);
    // Sometimes extra data...
    if ((*data-start) < length) {
        *data += (length - (*data-start));
    }
}
