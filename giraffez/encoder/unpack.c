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

#define DEFAULT_DELIMITER PyUnicode_FromString("|");
#define DEFAULT_NULLVALUE PyUnicode_FromString("NULL")

const uint16_t VARCHAR_NULL_LENGTH = 2;

uint16_t unpack_null_length(GiraffeColumn* column){
    if (column->GDType == GD_VARCHAR) {
        return VARCHAR_NULL_LENGTH;
    }
    return column->Length;
}

EncoderSettings* encoder_new(GiraffeColumns* columns) {
    EncoderSettings* e;
    e = (EncoderSettings*)malloc(sizeof(EncoderSettings));
    e->Columns = columns;
    e->Delimiter = DEFAULT_DELIMITER;
    Py_INCREF(Py_None);
    e->NullValue = Py_None;
    e->Encoder = unpack_row_s;
    return e;
}

void encoder_set_encoding(EncoderSettings* e, EncodingType t) {
    switch (t) {
        case ENCODING_STRING:
            e->Encoder = unpack_row_s;
            break;
        case ENCODING_DICT:
            e->Encoder = unpack_row_dict;
            break;
        case ENCODING_CUSTOM_TYPES:
            e->Encoder = unpack_row;
            break;
    }
}

void encoder_set_delimiter(EncoderSettings* e, const char* delimiter) {
    e->Delimiter = PyUnicode_FromString(delimiter);
}

void encoder_set_null(EncoderSettings* e, const char* null) {
    e->NullValue = PyUnicode_FromString(null);
}

PyObject* unpack_rows(EncoderSettings* settings, unsigned char** data, const uint32_t length) {
    PyObject* row;
    PyObject* rows;
    GiraffeColumns* columns;
    PyObject* (*encoder)(EncoderSettings*,unsigned char**,const uint16_t);
    unsigned char* start = *data;
    rows = PyList_New(0);
    columns = settings->Columns;
    encoder = settings->Encoder;

    while ((*data-start) < length) {
        uint16_t row_length = 0;
        unpack_uint16_t(data, &row_length);
        row = encoder(settings, data, row_length);
        PyList_Append(rows, row);
        Py_DECREF(row);
    }
    return rows;
}

PyObject* unpack_row_s(EncoderSettings* settings, unsigned char** data, const uint16_t length) {
    PyObject* row;
    PyObject* row_str;
    GiraffeColumn* column;
    GiraffeColumns* columns;
    size_t i;
    int nullable;
    unsigned char* ind;
    columns = settings->Columns;
    row = PyList_New(columns->length);
    indicator_init(&ind, data, columns->header_length);
    for (i=0; i<columns->length; i++) {
        column = &columns->array[i];
        nullable = indicator_read(ind, i);
        if (nullable) {
            *data += column->NullLength;
            PyList_SetItem(row, i, settings->NullValue);
            continue;
        }
        PyList_SetItem(row, i, unpack_row_item_s(data, column));
    }
    indicator_free(&ind);
    row_str = PyUnicode_Join(settings->Delimiter, row);
    Py_DECREF(row);
    return row_str;
}

PyObject* unpack_row_dict(EncoderSettings* settings, unsigned char** data, const uint16_t length) {
    PyObject* item;
    PyObject* row;
    GiraffeColumn* column;
    GiraffeColumns* columns;
    size_t i;
    int nullable;
    unsigned char* ind;
    columns = settings->Columns;
    row = PyDict_New();
    indicator_init(&ind, data, columns->header_length);
    for (i=0; i<columns->length; i++) {
        column = &columns->array[i];
        nullable = indicator_read(ind, i);
        if (nullable) {
            *data += column->NullLength;
            PyDict_SetItemString(row, column->Name, settings->NullValue);
            continue;
        }
        item = unpack_row_item(data, column);
        PyDict_SetItemString(row, column->Name, item);
        Py_DECREF(item);
    }
    indicator_free(&ind);
    return row;
}

PyObject* unpack_row(EncoderSettings* settings, unsigned char** data, const uint16_t length) {
    PyObject* row;
    GiraffeColumn* column;
    GiraffeColumns* columns;
    size_t i;
    int nullable;
    unsigned char* ind;
    columns = settings->Columns;
    row = PyList_New(columns->length);
    indicator_init(&ind, data, columns->header_length);
    for (i=0; i<columns->length; i++) {
        column = &columns->array[i];
        nullable = indicator_read(ind, i);
        if (nullable) {
            *data += column->NullLength;
            PyList_SetItem(row, i, settings->NullValue);
            continue;
        }
        /*PyList_SetItem(row, i, unpack_row_item(data, column));*/
        PyList_SetItem(row, i, unpack_row_item_x(data, column));
    }
    indicator_free(&ind);
    return row;
}

PyObject* unpack_row_item(unsigned char** data, GiraffeColumn* column) {
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
            // return int_to_pylong(data);
            return date_to_pystring(data);
        default:
            return char_to_pystring(data, column->Length);
    }
    return NULL;
}

PyObject* unpack_row_item_s(unsigned char** data, GiraffeColumn* column) {
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

PyObject* unpack_row_item_x(unsigned char** data, GiraffeColumn* column) {
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
            switch (column->Type) {
                case TIMESTAMP_NN:
                case TIMESTAMP_N:
                case TIMESTAMP_NNZ:
                case TIMESTAMP_NZ:
                    return char_to_timestamp(data, column->Length);
                case TIME_NN:
                case TIME_N:
                case TIME_NNZ:
                case TIME_NZ:
                    return char_to_time(data, column->Length);
                default:
                    return char_to_pystring(data, column->Length);
            }
        case GD_VARCHAR:
            return vchar_to_pystring(data);
        case GD_DATE:
            return date_to_pydate(data);
        default:
            return char_to_pystring(data, column->Length);
    }
    return NULL;
}

void unpack_row_x(unsigned char** data, const uint16_t length, GiraffeColumns* columns,
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
                PyList_SetItem(row, i, giraffez_decimal_from_pystring(decimal_to_pystring(data,
                    column->Length, column->Scale)));
                break;
            case GD_CHAR:
                switch (column->Type) {
                    case TIMESTAMP_NN:
                    case TIMESTAMP_N:
                    case TIMESTAMP_NNZ:
                    case TIMESTAMP_NZ:
                        PyList_SetItem(row, i, char_to_timestamp(data, column->Length));
                        break;
                    case TIME_NN:
                    case TIME_N:
                    case TIME_NNZ:
                    case TIME_NZ:
                        PyList_SetItem(row, i, char_to_time(data, column->Length));
                        break;
                    default:
                        PyList_SetItem(row, i, char_to_pystring(data, column->Length));
                }
                break;
            case GD_VARCHAR:
                PyList_SetItem(row, i, vchar_to_pystring(data));
                break;
            case GD_DATE:
                PyList_SetItem(row, i, date_to_pydate(data));
                break;
            default:
                PyList_SetItem(row, i, char_to_pystring(data, column->Length));
        }
    }
    indicator_free(&ind);
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
