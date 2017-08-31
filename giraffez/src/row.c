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


#include "common.h"
#include "buffer.h"
#include "columns.h"
#include "convert.h"
#include "encoder.h"

#include "row.h"

uint32_t teradata_buffer_count_rows(unsigned char *data, const uint32_t length) {
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

PyObject* teradata_buffer_to_pybytes(const TeradataEncoder *e, unsigned char **data, const uint32_t length) {
    PyObject *result;
    result = PyTuple_New(1);
    PyTuple_SetItem(result, 0, PyBytes_FromStringAndSize((char*)*data, length));
    return result;
}

PyObject* teradata_buffer_to_pylist(const TeradataEncoder *e, unsigned char **data, const uint32_t length) {
    PyObject *row;
    PyObject *rows;
    uint16_t row_length;
    unsigned char *start = *data;
    rows = PyList_New(0);
    while ((*data-start) < length) {
        row_length = 0;
        unpack_uint16_t(data, &row_length);
        Py_RETURN_ERROR(row = e->UnpackRowFunc(e, data, row_length));
        PyList_Append(rows, row);
        Py_DECREF(row);
    }
    return rows;
}

PyObject* teradata_row_to_pydict(const TeradataEncoder *e, unsigned char **data, const uint16_t length) {
    PyObject *item;
    PyObject *row;
    GiraffeColumn *column;
    size_t i;
    row = PyDict_New();
    indicator_set(e->Columns, data);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        if (indicator_read(e->Columns->buffer, i)) {
            *data += column->NullLength;
            PyDict_SetItemString(row, column->Title, e->NullValue);
            continue;
        }
        Py_RETURN_ERROR(item = e->UnpackItemFunc(e, data, column));
        PyDict_SetItemString(row, column->Title, item);
        Py_DECREF(item);
    }
    return row;
}

PyObject* teradata_row_to_pytuple(const TeradataEncoder *e, unsigned char **data, const uint16_t length) {
    PyObject *item;
    PyObject *row;
    GiraffeColumn *column;
    size_t i;
    row = PyTuple_New(e->Columns->length);
    indicator_set(e->Columns, data);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        if (indicator_read(e->Columns->buffer, i)) {
            *data += column->NullLength;
            Py_INCREF(e->NullValue);
            PyTuple_SetItem(row, i, e->NullValue);
            continue;
        }
        Py_RETURN_ERROR(item = e->UnpackItemFunc(e, data, column));
        PyTuple_SetItem(row, i, item);
    }
    return row;
}

PyObject* teradata_row_to_pybytes(const TeradataEncoder *e, unsigned char **data, const uint16_t length) {
    PyObject *s = PyBytes_FromStringAndSize((char*)*data, length);
    *data += length;
    return s;
}

PyObject* teradata_row_to_pystring(const TeradataEncoder *e, unsigned char **data, const uint16_t length) {
    PyObject *row;
    GiraffeColumn *column;
    size_t i;
    int n;
    char item[BUFFER_ITEM_SIZE];
    int8_t b; int16_t h; int32_t l; int64_t q; double d; uint16_t H;
    indicator_set(e->Columns, data);
    buffer_reset(e->buffer, 0);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        if (indicator_read(e->Columns->buffer, i)) {
            *data += column->NullLength;
            buffer_write(e->buffer, e->NullValueStr, e->NullValueStrLen);
            buffer_write(e->buffer, e->DelimiterStr, e->DelimiterStrLen);
            continue;
        }
        switch (column->GDType) {
            case GD_BYTEINT:
                unpack_int8_t(data, &b);
                buffer_writef(e->buffer, "%d", b);
                break;
            case GD_SMALLINT:
                unpack_int16_t(data, &h);
                buffer_writef(e->buffer, "%d", h);
                break;
            case GD_INTEGER:
                unpack_int32_t(data, &l);
                buffer_writef(e->buffer, "%d", l);
                break;
            case GD_BIGINT:
                unpack_int64_t(data, &q);
                buffer_writef(e->buffer, "%lld", q);
                break;
            case GD_FLOAT:
                unpack_float(data, &d);
                buffer_writef(e->buffer, "%f", d);
                break;
            case GD_DECIMAL:
                if ((n = teradata_decimal_to_cstring(data, column->Length, column->Scale, item)) < 0) {
                    PyErr_SetString(EncoderError, "Unexpected error while converting decimal");
                    return NULL;
                }
                buffer_write(e->buffer, item, n);
                break;
            case GD_CHAR:
                buffer_write(e->buffer, (char*)*data, column->Length);
                *data += column->Length;
                break;
            case GD_VARCHAR:
                unpack_uint16_t(data, &H);
                buffer_write(e->buffer, (char*)*data, H);
                *data += H;
                break;
            case GD_DATE:
                if ((n = teradata_date_to_cstring(data, item)) < 0) {
                    PyErr_SetString(EncoderError, "Unexpected error while converting date");
                    return NULL;
                }
                buffer_write(e->buffer, item, n);
                break;
            default:
                buffer_write(e->buffer, (char*)*data, column->Length);
                *data += column->Length;
        }
        if (i != e->Columns->length-1) {
            buffer_write(e->buffer, e->DelimiterStr, e->DelimiterStrLen);
        }
    }
    Py_RETURN_ERROR(row = PyUnicode_FromStringAndSize(e->buffer->data, e->buffer->length));
    return row;
}

PyObject* teradata_item_to_pyobject(const TeradataEncoder *e, unsigned char **data,
        const GiraffeColumn *column) {
    int n;
    char item[BUFFER_ITEM_SIZE];
    switch (column->GDType) {
        case GD_BYTEINT:
            return teradata_byteint_to_pylong(data);
        case GD_SMALLINT:
            return teradata_smallint_to_pylong(data);
        case GD_INTEGER:
            return teradata_int_to_pylong(data);
        case GD_BIGINT:
            return teradata_bigint_to_pylong(data);
        case GD_FLOAT:
            return teradata_float_to_pyfloat(data);
        case GD_DECIMAL:
            if ((n = teradata_decimal_to_cstring(data, column->Length, column->Scale, item)) < 0) {
                return NULL;
            }
            return e->UnpackDecimalFunc(item, n);
        case GD_CHAR:
            return teradata_char_to_pystring_f(data, column->Length, column->FormatLength);
        case GD_VARCHAR:
            return teradata_varchar_to_pystring(data);
        case GD_DATE:
            return e->UnpackDateFunc(data);
        case GD_TIME:
            return e->UnpackTimeFunc(data, column->Length);
        case GD_TIMESTAMP:
            return e->UnpackTimestampFunc(data, column->Length);
        case GD_BYTE:
            return teradata_byte_to_pybytes(data, column->Length);
        case GD_VARBYTE:
            return teradata_varbyte_to_pybytes(data);
        default:
            return teradata_char_to_pystring(data, column->Length);
    }
    return NULL;
}

#ifdef _MSC_VER
static __inline PyObject* pack_none(const GiraffeColumn *column, unsigned char **buf, uint16_t *len) {
#else
static inline PyObject* pack_none(const GiraffeColumn *column, unsigned char **buf, uint16_t *len) {
#endif
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

PyObject* teradata_row_from_unknown(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    if (PyDict_Check(row)) {
        encoder_set_encoding((TeradataEncoder*)e, ROW_ENCODING_DICT);
    } else if (PyUnicode_Check(row) || PyBytes_Check(row)) {
        encoder_set_encoding((TeradataEncoder*)e, ROW_ENCODING_STRING);
    } else if (PyTuple_Check(row) || PyList_Check(row)) {
        encoder_set_encoding((TeradataEncoder*)e, ROW_ENCODING_LIST);
    } else {
        PyErr_Format(EncoderError, "Row type '%s' cannot be serialized.", Py_TYPE(row)->tp_name);
        return NULL;
    }
    return e->PackRowFunc(e, row, data, length);
}

PyObject* teradata_row_from_pybytes(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    char *str;
    int len;
    if (!PyBytes_Check(row)) {
        PyErr_Format(EncoderError, "Must be bytes, received '%s'", Py_TYPE(row)->tp_name);
        return NULL;
    }
    if ((str = PyBytes_AsString(row)) == NULL) {
        return NULL;
    }
    len = PyBytes_Size(row);
    memcpy(*data, str, len);
    *length += len;
    Py_RETURN_NONE;
}

PyObject* teradata_row_from_pystring(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    PyObject *items;
    if (!(PyStr_Check(row) || PyBytes_Check(row))) {
        return teradata_row_from_unknown(e, row, data, length);
    }
    if ((items = PyUnicode_Split(row, e->Delimiter, e->Columns->length-1)) == NULL) {
        return NULL;
    }
    if (teradata_row_from_pytuple(e, items, data, length) == NULL) {
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject* teradata_row_from_pydict(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    PyObject *items;
    PyObject *item;
    size_t i;
    GiraffeColumn *column;
    if (!PyDict_Check(row)) {
        return teradata_row_from_unknown(e, row, data, length);
    }
    items = PyList_New(e->Columns->length);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        if ((item = PyDict_GetItemString(row, column->Name)) == NULL) {
            item = Py_None;
        }
        Py_INCREF(item);
        PyList_SetItem(items, i, item);
    }
    if (teradata_row_from_pytuple(e, items, data, length) == NULL) {
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject* teradata_row_from_pytuple(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    PyObject *item = NULL;
    GiraffeColumn *column;
    Py_ssize_t i, slength;
    int nullable;
    unsigned char *ind;
    if (!(PyTuple_Check(row) || PyList_Check(row))) {
        return teradata_row_from_unknown(e, row, data, length);
    }
    if ((slength = PySequence_Size(row)) == -1) {
        return NULL;
    }
    if (e->Columns->length != (size_t)slength) {
        PyErr_Format(EncoderError, "Wrong number of items in row, expected %lu but got %ld", e->Columns->length, slength);
        return NULL;
    }
    ind = *data;
    indicator_clear(&ind, e->Columns->header_length);
    *data += e->Columns->header_length;
    *length += e->Columns->header_length;
    for (i=0; i<slength; i++) {
        column = &e->Columns->array[i];
        Py_RETURN_ERROR(item = PySequence_GetItem(row, i));
        if ((nullable = PyObject_RichCompareBool(item, e->NullValue, Py_EQ)) == -1) {
            return NULL;
        }
        if (nullable) {
            indicator_write(&ind, i, 1);
            pack_none(column, data, length);
            continue;
        }
        if (e->PackItemFunc(e, column, item, data, length) == NULL) {
            return NULL;
        }
    }
    Py_DECREF(item);
    Py_RETURN_NONE;
}

PyObject* teradata_item_from_pyobject(const TeradataEncoder *e, const GiraffeColumn *column,
        PyObject *item, unsigned char **data, uint16_t *length) {
    switch (column->GDType) {
        case GD_BYTEINT:
            return teradata_byteint_from_pylong(item, column->Length, data, length);
        case GD_SMALLINT:
            return teradata_smallint_from_pylong(item, column->Length, data, length);
        case GD_INTEGER:
            return teradata_int_from_pylong(item, column->Length, data, length);
        case GD_BIGINT:
            return teradata_bigint_from_pylong(item, column->Length, data, length);
        case GD_FLOAT:
            return teradata_float_from_pyfloat(item, column->Length, data, length);
        case GD_DECIMAL:
            return teradata_decimal_from_pystring(item, column->Length, column->Scale, data, length);
        case GD_CHAR:
            return teradata_char_from_pystring(item, column->Length, data, length);
        case GD_VARCHAR:
            return teradata_varchar_from_pystring(item, data, length);
        case GD_DATE:
            return teradata_dateint_from_pystring(item, column->Length, data, length);
        case GD_TIME:
            return teradata_datetime_from_pystring(item, column->Length, data, length);
        case GD_TIMESTAMP:
            return teradata_datetime_from_pystring(item, column->Length, data, length);
        default:
            return teradata_char_from_pystring(item, column->Length, data, length);
    }
    return NULL;
}
