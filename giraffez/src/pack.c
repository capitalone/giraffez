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

#include "giraffez.h"


static inline PyObject* pack_none(const GiraffeColumn *column, unsigned char **buf, uint16_t *len) {
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
    } else if (_PyUnicode_Check(row) || PyBytes_Check(row)) {
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
    *length += pack_string(data, str, len);
    Py_RETURN_NONE;
}

PyObject* teradata_row_from_pystring(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    PyObject *items;
    if (!(_PyUnicode_Check(row) || PyBytes_Check(row))) {
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
    if (!(PyTuple_Check(row) || PyList_Check(row))) {
        return teradata_row_from_unknown(e, row, data, length);
    }
    if ((slength = PySequence_Size(row)) == -1) {
        return NULL;
    }
    if (e->Columns->length != (size_t)slength) {
        // TODO: make sure this works
        PyErr_Format(EncoderError, "Wrong number of items in row, expected %d but got %d", e->Columns->length, slength);
        return NULL;
    }
    int nullable;
    unsigned char *ind = *data;
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
            return teradata_char_from_pystring(item, column->Length, data, length);
        case GD_TIMESTAMP:
            return teradata_char_from_pystring(item, column->Length, data, length);
        default:
            return teradata_char_from_pystring(item, column->Length, data, length);
    }
    return NULL;
}
