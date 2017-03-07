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


static PyObject *ColumnsType;
static PyObject *DecimalType;
static PyObject *DateType;
static PyObject *TimeType;
static PyObject *TimestampType;


int giraffez_types_import() {
    PyObject *mod;
    mod = PyImport_ImportModule("giraffez.types");
    if (!mod) {
        PyErr_SetString(PyExc_ImportError, "Unable to import module giraffez.types");
        return -1;
    }
    if ((ColumnsType = PyObject_GetAttrString(mod, "Columns")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Columns");
        return -1;

    }
    if ((DateType = PyObject_GetAttrString(mod, "Date")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Date");
        return -1;

    };
    if ((TimeType = PyObject_GetAttrString(mod, "Time")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Time");
        return -1;

    }
    if ((TimestampType = PyObject_GetAttrString(mod, "Timestamp")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Timestamp");
        return -1;

    }
    if ((DecimalType = PyObject_GetAttrString(mod, "Decimal")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Decimal");
        return -1;
    }
    Py_DECREF(mod);
    return 0;
}

static PyObject* giraffez_column_to_pydict(GiraffeColumn *column) {
    PyObject *column_dict, *item;
    column_dict = PyDict_New();
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Name));
    PyDict_SetItemString(column_dict, "name", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Title));
    PyDict_SetItemString(column_dict, "title", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Alias));
    PyDict_SetItemString(column_dict, "alias", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromLong(column->Type));
    PyDict_SetItemString(column_dict, "type", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromUnsignedLongLong(column->Length));
    PyDict_SetItemString(column_dict, "length", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromLong(column->Precision));
    PyDict_SetItemString(column_dict, "precision", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromLong(column->Scale));
    PyDict_SetItemString(column_dict, "scale", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Nullable));
    PyDict_SetItemString(column_dict, "nullable", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Default));
    PyDict_SetItemString(column_dict, "default", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Format));
    PyDict_SetItemString(column_dict, "format", item);
    Py_DECREF(item);
    return column_dict;
}

PyObject* giraffez_columns_to_pyobject(GiraffeColumns *columns) {
    size_t i;
    GiraffeColumn *column;
    PyObject *column_dict, *columns_list, *obj;
    columns_list = PyList_New(columns->length);
    for (i=0; i<columns->length; i++) {
        column = &columns->array[i];
        if ((column_dict = giraffez_column_to_pydict(column)) == NULL) {
            return NULL;
        }
        PyList_SetItem(columns_list, i, column_dict);
    }
    obj =  PyObject_CallFunction(ColumnsType, "O", columns_list);
    Py_DECREF(columns_list);
    return obj;
}

// TODO: Add PyUnicode_Check and other checks just in case
GiraffeColumns* giraffez_columns_from_pyobject(PyObject *columns_obj) {
    PyObject *item, *column_obj, *iterator;
    GiraffeColumn *column;
    GiraffeColumns *columns;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);
    if ((iterator = PyObject_GetIter(columns_obj)) == NULL) {
        return NULL;
    }
    while ((column_obj = PyIter_Next(iterator))) {
        column = column_new();
        item = PyObject_GetAttrString(column_obj, "name");
        if (item != NULL && item != Py_None) {
            column->Name = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "title");
        if (item != NULL && item != Py_None) {
            column->Title = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "alias");
        if (item != NULL && item != Py_None) {
            column->Alias = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "type");
        if (item != NULL && item != Py_None) {
            column->Type = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "length");
        if (item != NULL && item != Py_None) {
            column->Length = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "precision");
        if (item != NULL && item != Py_None) {
            column->Precision = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "scale");
        if (item != NULL && item != Py_None) {
            column->Scale = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "_nullable");
        if (item != NULL && item != Py_None) {
            column->Nullable = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "_default");
        if (item != NULL && item != Py_None) {
            column->Default = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "_format");
        if (item != NULL && item != Py_None) {
            column->Format = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
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

PyObject* giraffez_decimal_from_pystring(PyObject *s) {
    PyObject *obj;
    obj = PyObject_CallFunction(DecimalType, "O", s);
    Py_DECREF(s);
    return obj;
}
