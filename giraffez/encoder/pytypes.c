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

#include <Python.h>
#include "types.h"
#include <stddef.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>
#include "unpack.h"


static PyObject* ColumnsType;

static PyObject* DecimalType;

static PyObject* DateType;

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

PyObject* giraffez_columns_from_columns(GiraffeColumns* c) {
    size_t i;
    GiraffeColumn* column;
    PyObject* d;
    PyObject* columns;
    columns = PyList_New(c->length);
    for (i=0; i<c->length; i++) {
        column = &c->array[i];
        d = PyDict_New();
        PyDict_SetItemString(d, "name", PyUnicode_FromString(column->Name));
        PyDict_SetItemString(d, "title", PyUnicode_FromString(column->Title));
        PyDict_SetItemString(d, "alias", PyUnicode_FromString(column->Alias));
        PyDict_SetItemString(d, "type", PyLong_FromLong((long)column->Type));
        PyDict_SetItemString(d, "length", PyLong_FromLong((long)column->Length));
        PyDict_SetItemString(d, "precision", PyLong_FromLong((long)column->Precision));
        PyDict_SetItemString(d, "scale", PyLong_FromLong((long)column->Scale));
        PyDict_SetItemString(d, "nullable", PyUnicode_FromString(column->Nullable));
        PyDict_SetItemString(d, "default", PyUnicode_FromString(column->Default));
        PyDict_SetItemString(d, "format", PyUnicode_FromString(column->Format));
        PyList_SetItem(columns, i, d);
    }
    return PyObject_CallFunction(ColumnsType, "O", columns);
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

PyObject* giraffez_row_from_list(PyObject* columns, PyObject* row) {
    PyObject* obj;
    obj = PyObject_CallFunction(RowType, "OO", columns, row);
    Py_DECREF(row);
    return obj;
}
