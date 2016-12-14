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

#include "pytypes.h"

#include <Python.h>

#include "_compat.h"

#include "encoder/columns.h"


static PyObject* ColumnsType;

static PyObject* DecimalType;

static PyObject* DateType;

static PyObject* ResultType;

static PyObject* RowType;

static PyObject* TimeType;

static PyObject* TimestampType;


int giraffez_columns_import() {
    PyObject* mod;
    mod = PyImport_ImportModule("giraffez.types");
    if (!mod) {
        PyErr_SetString(PyExc_ImportError, "Unable to import module giraffez.types");
        return -1;
    }
    ColumnsType = PyObject_GetAttrString(mod, "Columns");
    ResultType = PyObject_GetAttrString(mod, "Result");
    RowType = PyObject_GetAttrString(mod, "Row");
    Py_DECREF(mod);
    return 0;
}

int giraffez_datetime_import() {
    PyObject* mod;
    mod = PyImport_ImportModule("giraffez.types");
    if (!mod) {
        PyErr_SetString(PyExc_ImportError, "Unable to import module giraffez.types");
        return -1;
    }
    DateType = PyObject_GetAttrString(mod, "Date");
    TimeType = PyObject_GetAttrString(mod, "Time");
    TimestampType = PyObject_GetAttrString(mod, "Timestamp");
    Py_DECREF(mod);
    return 0;
}

int giraffez_decimal_import() {
    PyObject* decimalmod;
    decimalmod = PyImport_ImportModule("giraffez.types");
    if (!decimalmod) {
        PyErr_SetString(PyExc_ImportError, "Unable to import decimal");
        return -1;
    }
    DecimalType = PyObject_GetAttrString(decimalmod, "Decimal");
    Py_DECREF(decimalmod);
    if (DecimalType == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import decimal.Decimal");
        return -1;
    }
    return 0;
}

// TODO: determine how best GiraffeColumn members should be initialized
// to ensure that GiraffeColumns can be passed to this function with
// only some of the members and not cause undefined behavior. Some things
// like export only ever set name, type, length, precision and scale for 
// example.
PyObject* giraffez_columns_from_columns(GiraffeColumns* c) {
    size_t i;
    GiraffeColumn* column;
    PyObject* d;
    PyObject* obj;
    PyObject* columns;
    PyObject* item;
    columns = PyList_New(c->length);
    for (i=0; i<c->length; i++) {
        column = &c->array[i];
        d = PyDict_New();
        item = PyUnicode_FromString(column->Name);
        PyDict_SetItemString(d, "name", item);
        Py_DECREF(item);
        item = PyUnicode_FromString(column->Title);
        PyDict_SetItemString(d, "title", item);
        Py_DECREF(item);
        item = PyUnicode_FromString(column->Alias);
        PyDict_SetItemString(d, "alias", item);
        Py_DECREF(item);
        item = PyLong_FromLong((long)column->Type);
        PyDict_SetItemString(d, "type", item);
        Py_DECREF(item);
        item = PyLong_FromLong((long)column->Length);
        PyDict_SetItemString(d, "length", item);
        Py_DECREF(item);
        item = PyLong_FromLong((long)column->Precision);
        PyDict_SetItemString(d, "precision", item);
        Py_DECREF(item);
        item = PyLong_FromLong((long)column->Scale);
        PyDict_SetItemString(d, "scale", item);
        Py_DECREF(item);
        item = PyUnicode_FromString(column->Nullable);
        PyDict_SetItemString(d, "nullable", item);
        Py_DECREF(item);
        item = PyUnicode_FromString(column->Default);
        PyDict_SetItemString(d, "default", item);
        Py_DECREF(item);
        item = PyUnicode_FromString(column->Format);
        PyDict_SetItemString(d, "format", item);
        Py_DECREF(item);
        PyList_SetItem(columns, i, d);
    }
    obj =  PyObject_CallFunction(ColumnsType, "O", columns);
    Py_DECREF(columns);
    return obj;
}

// TODO: Add PyUnicode_Check and other checks just in case
GiraffeColumns* columns_to_giraffez_columns(PyObject* columns_obj) {
    PyObject *item;
    PyObject *column_obj;
    PyObject *iterator;
    GiraffeColumn* column;
    GiraffeColumns *columns;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);
    iterator = PyObject_GetIter(columns_obj);
    if (iterator == NULL) {
        return NULL;
    }
    while ((column_obj = PyIter_Next(iterator))) {
        column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
        item = PyObject_GetAttrString(column_obj, "name");
        if (item != NULL && item != Py_None) {
            column->Name = strdup(PyUnicode_AsUTF8(item));
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "title");
        if (item != NULL && item != Py_None) {
            column->Title = strdup(PyUnicode_AsUTF8(item));
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "alias");
        if (item != NULL && item != Py_None) {
            column->Alias = strdup(PyUnicode_AsUTF8(item));
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "type");
        if (item != NULL && item != Py_None) {
            column->Type = (uint16_t)PyLong_AsLong(item);
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "length");
        if (item != NULL && item != Py_None) {
            column->Length = (uint16_t)PyLong_AsLong(item);
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "precision");
        if (item != NULL && item != Py_None) {
            column->Precision = (uint16_t)PyLong_AsLong(item);
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "scale");
        if (item != NULL && item != Py_None) {
            column->Scale = (uint16_t)PyLong_AsLong(item);
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "_nullable");
        if (item != NULL && item != Py_None) {
            column->Nullable = strdup(PyUnicode_AsUTF8(item));
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "_default");
        if (item != NULL && item != Py_None) {
            column->Default = strdup(PyUnicode_AsUTF8(item));
            Py_DECREF(item);
        }
        item = PyObject_GetAttrString(column_obj, "_format");
        if (item != NULL && item != Py_None) {
            column->Format = strdup(PyUnicode_AsUTF8(item));
            Py_DECREF(item);
        }
        columns_append(columns, *column);
        Py_DECREF(column_obj);
    }
    Py_DECREF(iterator);
    return columns;
}

PyObject* giraffez_date_from_datetime(int year, int month, int day, int hour, int minute,
        int second, int microsecond) {
    return PyObject_CallFunction(DateType, "iiiiiii", year, month, day, hour, minute, second,
        microsecond);
}

PyObject* giraffez_time_from_time(int hour, int minute, int second, int microsecond) {
    return PyObject_CallFunction(TimeType, "iiii", hour, minute, second, microsecond);
}

PyObject* giraffez_ts_from_datetime(int year, int month, int day, int hour, int minute, int second,
        int microsecond) {
    return PyObject_CallFunction(TimestampType, "iiiiiii", year, month, day, hour, minute, second,
        microsecond);
}

PyObject* giraffez_decimal_from_pystring(PyObject* s) {
    PyObject* obj;
    obj = PyObject_CallFunction(DecimalType, "O", s);
    Py_DECREF(s);
    return obj;
}

PyObject* giraffez_result_from_rows(PyObject* columns, PyObject* rows) {
    PyObject* d;
    PyObject* obj;
    d = PyDict_New();
    PyDict_SetItemString(d, "columns", columns);
    PyDict_SetItemString(d, "rows", rows);
    obj = PyObject_CallFunction(ResultType, "O", d);
    Py_DECREF(rows);
    Py_DECREF(d);
    return obj;
}

PyObject* giraffez_row_from_list(PyObject* columns, PyObject* row) {
    PyObject* obj;
    obj = PyObject_CallFunction(RowType, "OO", columns, row);
    Py_DECREF(row);
    return obj;
}
