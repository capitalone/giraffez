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

#include "rowobject.h"

#include "_compat.h"


static void Row_dealloc(Row *self) {
    Py_XDECREF(self->column_map);
    Py_XDECREF(self->values);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject* Row_new(PyTypeObject *type, PyObject *column_map, PyObject *values) {
    Row *self;
    self = (Row*)type->tp_alloc(type, 0);
    self->column_map = column_map;
    self->values = values;
    return (PyObject*)self;
}

static int Row_init(Row *self, PyObject *args, PyObject *kwargs) {
    PyObject *column_map;
    PyObject *values;
    if (!PyArg_ParseTuple(args, "OO", &column_map, &values)) {
        return -1;
    }
    Py_INCREF(column_map);
    self->column_map = column_map;
    Py_INCREF(values);
    self->values = values;
    return 0;
}

static Py_ssize_t Row_Len(PyObject *self) {
    Row *row = (Row*)self;
    return PyList_Size(row->values);
}

static PyObject* Row_GetItem(PyObject *self, Py_ssize_t i) {
    Row *row = (Row*)self;
    return PyList_GetItem(row->values, i);
}

static int Row_SetItem(PyObject *self, Py_ssize_t index, PyObject *value) {
    Row *row = (Row*)self;
    return PyList_SetItem(row->values, index, value);
}

static int Row_Contains(PyObject *self, PyObject *element) {
    Row *row = (Row*)self;
    PyObject *item;
    size_t i;
    size_t length;
    int cmp;
    cmp = 0;
    length = PyList_Size(row->values);
    for (i=0; i<length && cmp == 0; i++) {
        item = PyList_GetItem(row->values, i);
        cmp = PyObject_RichCompareBool(element, item, Py_EQ);
    }
    return cmp;
}

static PyObject* Row_GetAttr(PyObject *self, PyObject *name) {
    PyObject *index;
    Row *row = (Row*)self;
    index = PyDict_GetItem(row->column_map, name);
    if (index) {
        return PyList_GetItem(row->values, PyNumber_AsSsize_t(index, 0));
    }
    return PyObject_GenericGetAttr(self, name);
}

static int Row_SetAttr(PyObject *self, PyObject *name, PyObject *value) {
    PyObject *index;
    Row *row = (Row*)self;
    index = PyDict_GetItem(row->column_map, name);
    if (index == NULL) {
        return -1;
    }
    return PyList_SetItem(row->values, PyLong_AsSsize_t(index), value);
}

static PyObject* Row_repr(PyObject *self) {
    Row *row = (Row*)self;
    return PyObject_Repr(row->values);
}

static PyObject* Row_items(Row *self) {
    PyObject *d, *k, *v, *keys;
    size_t i, length;
    d = PyDict_New();
    keys = PyDict_Keys(self->column_map);
    length = PyList_Size(self->values);
    for (i=0; i<length; i++) {
        k = PyList_GetItem(keys, i);
        v = PyList_GetItem(self->values, i);
        PyDict_SetItem(d, k, v);
    }
    Py_DECREF(keys);
    return d;
}

static PySequenceMethods Row_as_sequence = {
    Row_Len,       /* sq_length    __len__      */
    0,             /* sq_concat    __add__      */
    0,             /* sq_repeat    __mul__      */
    Row_GetItem,   /* sq_item      __getitem__  */
    0,             /* sq_slice     __getslice__ */
    Row_SetItem,   /* sq_ass_item  __setitem__  */
    0,             /* sq_ass_slice __setslice__ */
    Row_Contains,  /* sq_contains  __contains__ */
};

static PyMappingMethods Row_as_mapping = {
    Row_Len,         /* mp_length        __len__     */
    Row_GetAttr,     /* mp_subscript     __getitem__ */
    0,               /* mp_ass_subscript __setitem__ */
};

static PyMethodDef Row_methods[] = {
    {"items", (PyCFunction)Row_items, METH_NOARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject RowType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "giraffez.Row",                                 /* tp_name */
    sizeof(Row),                                    /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Row_dealloc,                        /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_compare */
    Row_repr,                                       /* tp_repr */
    0,                                              /* tp_as_number */
    &Row_as_sequence,                               /* tp_as_sequence */
    &Row_as_mapping,                                /* tp_as_mapping */
    0,                                              /* tp_hash */
    0,                                              /* tp_call */
    0,                                              /* tp_str */
    Row_GetAttr,                                    /* tp_getattro */
    Row_SetAttr,                                    /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                             /* tp_flags */
    "",                                             /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Row_methods,                                    /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)Row_init,                             /* tp_init */
    0,                                              /* tp_alloc */
    Row_new,                                        /* tp_new */
    0,                                              /* tp_free */
    0,                                              /* tp_is_gc */
    0,                                              /* tp_bases */
    0,                                              /* tp_mro */
    0,                                              /* tp_cache */
    0,                                              /* tp_subclasses */
    0,                                              /* tp_weaklist */
};
