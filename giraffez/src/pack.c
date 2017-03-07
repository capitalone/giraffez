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

PyObject* teradata_row_from_pybytes(const TeradataEncoder *e, PyObject *row, unsigned char **data,
        uint16_t *length) {
    char *str;
    int len;
    if (!PyBytes_Check(row)) {
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

    // TODO: any other types acceptable here?
    if (!(_PyUnicode_Check(row) || PyBytes_Check(row))) {
        // TODO: better error given py2/3 types
        PyErr_Format(EncoderError, "Expected row as a str or bytes, not '%s'.", row->ob_type->tp_name);
        return NULL;
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
        // TODO: verify error text is accurate
        PyErr_Format(EncoderError, "Expected row as a dict object, not '%s'.", row->ob_type->tp_name);
        return NULL;
    }
    items = PyList_New(e->Columns->length);
    for (i=0; i<e->Columns->length; i++) {
        column = &e->Columns->array[i];
        if ((item = PyDict_GetItemString(row, column->Name)) == NULL) {
            return NULL;
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

    // TODO: any other types acceptable here?
    if (!(PyList_Check(row) || PyTuple_Check(row))) {
        PyErr_Format(EncoderError, "Expected row as a tuple or list object, not '%s'.", row->ob_type->tp_name);
        return NULL;
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
        if (teradata_item_from_pyobject(e, column, item, data, length) == NULL) {
            indicator_write(&ind, i, 1);
            pack_none(column, data, length);
            /*PyErr_Format(PyExc_ValueError, "Unable to unpack column %d '%s'\n", i, column->Name);*/
            return NULL;
            // TODO: if panic, then return NULL;
        }
    }
    Py_DECREF(item);
    Py_RETURN_NONE;
}

PyObject* teradata_item_from_pyobject(const TeradataEncoder *e, const GiraffeColumn *column,
        PyObject *item, unsigned char **data, uint16_t *length) {
    switch (column->GDType) {
        case GD_BYTEINT:
            return teradata_byte_from_pylong(item, column->Length, data, length);
        case GD_SMALLINT:
            return teradata_short_from_pylong(item, column->Length, data, length);
        case GD_INTEGER:
            return teradata_int_from_pylong(item, column->Length, data, length);
        case GD_BIGINT:
            return teradata_long_from_pylong(item, column->Length, data, length);
        case GD_FLOAT:
            return teradata_float_from_pyfloat(item, column->Length, data, length);
        case GD_DECIMAL:
            return teradata_decimal_from_pystring(item, column->Length, column->Scale, data, length);
        case GD_CHAR:
            return teradata_char_from_pystring(item, column->Length, data, length);
        case GD_VARCHAR:
            return teradata_varchar_from_pystring(item, data, length);
        case GD_DATE:
            return teradata_int_from_pydate(item, column->Length, data, length);
        case GD_TIME:
            return teradata_char_from_pystring(item, column->Length, data, length);
        case GD_TIMESTAMP:
            return teradata_char_from_pystring(item, column->Length, data, length);
        default:
            return teradata_char_from_pystring(item, column->Length, data, length);
    }
    return NULL;
}
