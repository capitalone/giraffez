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

#include "encoderobject.h"

#include "_compat.h"
#include "encoder/encoder.h"
#include "encoder/pytypes.h"
#include "encoder/unpack.h"


static void Encoder_dealloc(Encoder* self) {
    if (self->encoder != NULL) {
        free(self->encoder);
        self->encoder = NULL;
    }
    if (self->columns != NULL) {
        columns_free(self->columns);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Encoder_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    Encoder* self;
    self = (Encoder*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int Encoder_init(Encoder* self, PyObject* args, PyObject* kwds) {
    PyObject* columns_obj;
    PyObject* column_obj;
    PyObject* iterator;
    if (self->columns == NULL) {
        self->columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
        self->columns->array = NULL;
    }
    if (!PyArg_ParseTuple(args, "O", &columns_obj)) {
        return 0;
    }
    columns_init(self->columns, 1);
    iterator = PyObject_GetIter(columns_obj);
    if (iterator == NULL) {
        PyErr_SetString(PyExc_ValueError, "No columns found.");
        return 0;
    }
    while ((column_obj = PyIter_Next(iterator))) {
        PyObject* column_name = PyObject_GetAttrString(column_obj, "name");
        PyObject* column_type = PyObject_GetAttrString(column_obj, "type");
        PyObject* column_length = PyObject_GetAttrString(column_obj, "length");
        PyObject* column_precision = PyObject_GetAttrString(column_obj, "precision");
        PyObject* column_scale = PyObject_GetAttrString(column_obj, "scale");
        GiraffeColumn* column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
        column->Name = strdup(PyUnicode_AsUTF8(column_name));
        column->Type = (uint16_t)PyLong_AsLong(column_type);
        column->Length = (uint16_t)PyLong_AsLong(column_length);
        column->Precision = (uint16_t)PyLong_AsLong(column_precision);
        column->Scale = (uint16_t)PyLong_AsLong(column_scale);
        Py_DECREF(column_name);
        Py_DECREF(column_type);
        Py_DECREF(column_length);
        Py_DECREF(column_precision);
        Py_DECREF(column_scale);
        columns_append(self->columns, *column);
        Py_DECREF(column_obj);
    }
    Py_DECREF(iterator);
    Py_DECREF(columns_obj);
    self->encoder = encoder_new(self->columns);
    encoder_set_encoding(self->encoder, ENCODING_GIRAFFE_TYPES);
    return 0;
}

static PyObject* Encoder_count_rows(PyObject* self, PyObject* args) {
    Py_buffer buffer;
    uint32_t n;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    n = count_rows((unsigned char*)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return PyLong_FromLong(n);
}

static PyObject* Encoder_set_encoding(Encoder* self, PyObject* args) {
    int encoding;
    if (!PyArg_ParseTuple(args, "i", &encoding)) {
        return NULL;
    }
    encoder_set_encoding(self->encoder, encoding);
    Py_RETURN_NONE;
}

static PyObject* Encoder_unpack_row(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    PyObject* row;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    row = self->encoder->UnpackRowFunc(self->encoder, (unsigned char**)&buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return row;
}

static PyObject* Encoder_unpack_rows(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    PyObject* rows;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    rows = self->encoder->UnpackRowsFunc(self->encoder, (unsigned char**)&buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return rows;
}

static PyObject* Encoder_unpack_stmt_info(PyObject* self, PyObject* args) {
    Py_buffer buffer;
    GiraffeColumns* columns;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    columns = unpack_stmt_info_to_columns((unsigned char**)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return giraffez_columns_from_columns(columns);
}

static PyMethodDef Encoder_methods[] = {
    {"count_rows", (PyCFunction)Encoder_count_rows, METH_STATIC|METH_VARARGS, ""},
    {"set_encoding", (PyCFunction)Encoder_set_encoding, METH_VARARGS, ""},
    {"unpack_row", (PyCFunction)Encoder_unpack_row, METH_VARARGS, ""},
    {"unpack_rows", (PyCFunction)Encoder_unpack_rows, METH_VARARGS, ""},
    {"unpack_stmt_info", (PyCFunction)Encoder_unpack_stmt_info, METH_STATIC|METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject EncoderType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_encoder.Encoder",                             /* tp_name */
    sizeof(Encoder),                                /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Encoder_dealloc,                    /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_compare */
    0,                                              /* tp_repr */
    0,                                              /* tp_as_number */
    0,                                              /* tp_as_sequence */
    0,                                              /* tp_as_mapping */
    0,                                              /* tp_hash */
    0,                                              /* tp_call */
    0,                                              /* tp_str */
    0,                                              /* tp_getattro */
    0,                                              /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,       /* tp_flags */
    "Encoder objects",                              /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Encoder_methods,                                /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)Encoder_init,                         /* tp_init */
    0,                                              /* tp_alloc */
    Encoder_new,                                    /* tp_new */
};
