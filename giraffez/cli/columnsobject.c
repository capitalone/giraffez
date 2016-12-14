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

#include "columnsobject.h"

#include "_compat.h"
#include "encoder/pytypes.h"


static PyObject* column_to_pydict(GiraffeColumn *column) {
    PyObject *column_dict, *item;
    column_dict = PyDict_New();
    item = PyUnicode_FromString(column->Name);
    PyDict_SetItemString(column_dict, "name", item);
    Py_DECREF(item);
    item = PyUnicode_FromString(column->Title);
    PyDict_SetItemString(column_dict, "title", item);
    Py_DECREF(item);
    item = PyUnicode_FromString(column->Alias);
    PyDict_SetItemString(column_dict, "alias", item);
    Py_DECREF(item);
    item = PyLong_FromLong((long)column->Type);
    PyDict_SetItemString(column_dict, "type", item);
    Py_DECREF(item);
    item = PyLong_FromUnsignedLongLong(column->Length);
    PyDict_SetItemString(column_dict, "length", item);
    Py_DECREF(item);
    item = PyLong_FromLong((long)column->Precision);
    PyDict_SetItemString(column_dict, "precision", item);
    Py_DECREF(item);
    item = PyLong_FromLong((long)column->Scale);
    PyDict_SetItemString(column_dict, "scale", item);
    Py_DECREF(item);
    item = PyUnicode_FromString(column->Nullable);
    PyDict_SetItemString(column_dict, "nullable", item);
    Py_DECREF(item);
    item = PyUnicode_FromString(column->Default);
    PyDict_SetItemString(column_dict, "default", item);
    Py_DECREF(item);
    item = PyUnicode_FromString(column->Format);
    PyDict_SetItemString(column_dict, "format", item);
    Py_DECREF(item);
    return column_dict;
}

static PyObject* columns_to_pylist(GiraffeColumns *columns) {
    size_t i;
    GiraffeColumn *column;
    PyObject *column_dict;
    PyObject *columns_list;
    columns_list = PyList_New(columns->length);
    for (i=0; i<columns->length; i++) {
        column = &columns->array[i];
        if ((column_dict = column_to_pydict(column)) == NULL) {
            return NULL;
        }
        PyList_SetItem(columns_list, i, column_dict);
    }
    return columns_list;
}

static PyObject* pyobject_getitem(PyObject *obj, const char *name) {
    PyObject *item;
    if (PyDict_Check(obj)) {
        item = PyDict_GetItemString(obj, name);
    } else {
        item = PyObject_GetAttrString(obj, name);
    }
    return item;
}

static GiraffeColumn* pyobject_to_column(PyObject *column_obj) {
    PyObject *item;
    GiraffeColumn *column;
    column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
    if ((item = pyobject_getitem(column_obj, "name")) != NULL && item != Py_None) {
        column->Name = strdup(PyUnicode_AsUTF8(item));
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "title")) != NULL && item != Py_None) {
        column->Title = strdup(PyUnicode_AsUTF8(item));
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "alias")) != NULL && item != Py_None) {
        column->Alias = strdup(PyUnicode_AsUTF8(item));
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "type")) != NULL && item != Py_None) {
        column->Type = (uint16_t)PyLong_AsLong(item);
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "length")) != NULL && item != Py_None) {
        // TODO: causes weird error with unsignedlonglong
        /*column->Length = (uint64_t)PyLong_AsUnsignedLongLong(item);*/
        column->Length = (uint64_t)PyLong_AsLong(item);
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "precision")) != NULL && item != Py_None) {
        column->Precision = (uint16_t)PyLong_AsLong(item);
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "scale")) != NULL && item != Py_None) {
        column->Scale = (uint16_t)PyLong_AsLong(item);
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "nullable")) != NULL && item != Py_None) {
        column->Nullable = strdup(PyUnicode_AsUTF8(item));
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "default")) != NULL && item != Py_None) {
        column->Default = strdup(PyUnicode_AsUTF8(item));
        Py_DECREF(item);
    }
    if ((item = pyobject_getitem(column_obj, "format")) != NULL && item != Py_None) {
        column->Format = strdup(PyUnicode_AsUTF8(item));
        Py_DECREF(item);
    }
    return column;
}

/*static GiraffeColumn* pyobject_to_column(PyObject *column_obj) {*/
    /*GiraffeColumn *column;*/
    /*column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));*/
    /*item = PyObject_GetAttrString(column_obj, "name");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Name = strdup(PyUnicode_AsUTF8(item));*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "title");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Title = strdup(PyUnicode_AsUTF8(item));*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "alias");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Alias = strdup(PyUnicode_AsUTF8(item));*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "type");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Type = (uint16_t)PyLong_AsLong(item);*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "length");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Length = (uint16_t)PyLong_AsLong(item);*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "precision");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Precision = (uint16_t)PyLong_AsLong(item);*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "scale");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Scale = (uint16_t)PyLong_AsLong(item);*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "nullable");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Nullable = strdup(PyUnicode_AsUTF8(item));*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "default");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Default = strdup(PyUnicode_AsUTF8(item));*/
        /*Py_DECREF(item);*/
    /*}*/
    /*item = PyObject_GetAttrString(column_obj, "format");*/
    /*if (item != NULL && item != Py_None) {*/
        /*column->Format = strdup(PyUnicode_AsUTF8(item));*/
        /*Py_DECREF(item);*/
    /*}*/
    /*return column;*/
/*}*/

static GiraffeColumns* pylist_to_columns(PyObject *columns_list) {
    PyObject *column_obj;
    PyObject *iterator;
    GiraffeColumn* column;
    GiraffeColumns *columns;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);
    iterator = PyObject_GetIter(columns_list);
    if (iterator == NULL) {
        return NULL;
    }
    while ((column_obj = PyIter_Next(iterator))) {
        column = pyobject_to_column(column_obj);
        columns_append(columns, *column);
        Py_DECREF(column_obj);
    }
    Py_DECREF(iterator);
    return columns;
}

static void Columns_dealloc(Columns *self) {
    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject* Columns_new(PyTypeObject *type, PyObject *columns_list, PyObject *whatever) {
    Columns *self;
    self = (Columns*)type->tp_alloc(type, 0);
    /*if ((self->columns = pylist_to_columns(columns_list)) == NULL) {;*/
        /*PyErr_SetString(PyExc_ValueError, "No columns found");*/
        /*return NULL;*/
    /*}*/
    return (PyObject*)self;
}

PyObject* Columns_FromC(PyTypeObject *type, GiraffeColumns *columns) {
    Columns *self;
    self = (Columns*)type->tp_alloc(type, 0);
    self->columns = columns;
    return (PyObject*)self;
}

static int Columns_init(Columns *self, PyObject *args, PyObject *kwargs) {
    PyObject *columns_list;
    if (!PyArg_ParseTuple(args, "O", &columns_list)) {
        return -1;
    }
    if ((self->columns = pylist_to_columns(columns_list)) == NULL) {;
        PyErr_SetString(PyExc_ValueError, "No columns found");
        return -1;
    }
    return 0;
}

static Py_ssize_t Columns_Len(PyObject *obj) {
    Columns *self = (Columns*)obj;
    PyObject *l;
    l = PyLong_FromSsize_t(self->columns->length);
    return PyLong_AsSsize_t(l);
}

static PyObject* Columns_GetItem(PyObject *obj, Py_ssize_t i) {
    Columns *self = (Columns*)obj;
    if (i > self->columns->length) {
        PyErr_SetString(PyExc_AttributeError, "Index not found");
        return NULL;
    }
    return column_to_pydict(&self->columns->array[i]);
}

static int Columns_SetItem(PyObject *obj, Py_ssize_t index, PyObject *value) {
    Columns *self = (Columns*)obj;
    GiraffeColumn *column;
    if (index > self->columns->length) {
        PyErr_SetString(PyExc_AttributeError, "Index not found");
        return -1;
    }
    if ((column = pyobject_to_column(value)) == NULL) {
        // TODO: Error
        return -1;
    }
    self->columns->array[index] = *column;
    return 0;
}

static int Columns_Contains(PyObject *obj, PyObject *element) {
    Columns *self = (Columns*)obj;
    PyObject *item;
    size_t i;
    int cmp;
    cmp = 0;
    for (i=0; i<self->columns->length && cmp == 0; i++) {
        item = PyUnicode_FromString(self->columns->array[i].Name);
        cmp = PyObject_RichCompareBool(element, item, Py_EQ);
    }
    return cmp;
}

static PyObject* Columns_GetAttr(PyObject *obj, PyObject *name) {
    Columns *self = (Columns*)obj;
    PyObject *item;
    int cmp;
    size_t i;
    if (self->columns != NULL) {
        /*names = PyList_New(self->columns->length);*/
        for (i=0; i<self->columns->length; i++) {
            item = PyUnicode_FromString(self->columns->array[i].Name);
            cmp = PyObject_RichCompareBool(name, item, Py_EQ);
            if (cmp) {
                return item;
            }
            /*if (*/
            /*PyList_SetItem(names, i, PyUnicode_FromString(self->columns->array[i].Name));*/
        }
        /*return names;*/
    }
    return PyObject_GenericGetAttr(obj, name);
}

static int Columns_SetAttr(PyObject *obj, PyObject *name, PyObject *value) {
    return -1;
    /*PyObject *index;*/
    /*Columns *self = (Columns*)obj;*/
    /*index = PyDict_GetItem(columns->column_map, name);*/
    /*if (index == NULL) {*/
        /*return -1;*/
    /*}*/
    /*return PyList_SetItem(columns->values, PyLong_AsSsize_t(index), value);*/
}

static PyObject* Columns_repr(PyObject *obj) {
    return PyUnicode_FromString("Columns<>");
    /*Columns *self = (Columns*)obj;*/
    /*return PyObject_Repr(self->columns);*/
}

/*static PyObject* Columns_items(Columns *self) {*/
    /*PyObject *d, *k, *v, *keys;*/
    /*size_t i, length;*/
    /*d = PyDict_New();*/
    /*keys = PyDict_Keys(self->column_map);*/
    /*length = PyList_Size(self->values);*/
    /*for (i=0; i<length; i++) {*/
        /*k = PyList_GetItem(keys, i);*/
        /*v = PyList_GetItem(self->values, i);*/
        /*PyDict_SetItem(d, k, v);*/
    /*}*/
    /*Py_DECREF(keys);*/
    /*return d;*/
/*}*/

static PySequenceMethods Columns_as_sequence = {
    Columns_Len,       /* sq_length    __len__      */
    0,             /* sq_concat    __add__      */
    0,             /* sq_repeat    __mul__      */
    Columns_GetItem,   /* sq_item      __getitem__  */
    0,             /* sq_slice     __getslice__ */
    Columns_SetItem,   /* sq_ass_item  __setitem__  */
    0,             /* sq_ass_slice __setslice__ */
    Columns_Contains,  /* sq_contains  __contains__ */
};

static PyMappingMethods Columns_as_mapping = {
    Columns_Len,         /* mp_length        __len__     */
    Columns_GetAttr,     /* mp_subscript     __getitem__ */
    0,               /* mp_ass_subscript __setitem__ */
};

static PyMethodDef Columns_methods[] = {
    /*{"items", (PyCFunction)Columns_items, METH_NOARGS, ""},*/
    {NULL}  /* Sentinel */
};

PyTypeObject ColumnsType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "giraffez.Columns",                             /* tp_name */
    sizeof(Columns),                                /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Columns_dealloc,                    /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_compare */
    Columns_repr,                                   /* tp_repr */
    0,                                              /* tp_as_number */
    &Columns_as_sequence,                           /* tp_as_sequence */
    &Columns_as_mapping,                            /* tp_as_mapping */
    0,                                              /* tp_hash */
    0,                                              /* tp_call */
    0,                                              /* tp_str */
    Columns_GetAttr,                                /* tp_getattro */
    Columns_SetAttr,                                /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                             /* tp_flags */
    "",                                             /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Columns_methods,                                /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)Columns_init,                         /* tp_init */
    0,                                              /* tp_alloc */
    Columns_new,                                    /* tp_new */
};
