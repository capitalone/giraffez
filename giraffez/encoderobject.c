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
#include "compat.h"
#include "encoder/columns.h"
#include "encoder/unpack.h"
#include "teradata/stmt_info.h"


static void Encoder_dealloc(Encoder* self) {
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
        PyObject* gd_type = PyObject_GetAttrString(column_obj, "gd_type");
        GiraffeColumn* column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
        column->Name = strdup(PyUnicode_AsUTF8(column_name));
        column->Type = (int16_t)PyLong_AsLong(column_type);
        column->Length = (int16_t)PyLong_AsLong(column_length);
        column->Precision = (int16_t)PyLong_AsLong(column_precision);
        column->Scale = (int16_t)PyLong_AsLong(column_scale);
        column->GDType = (int16_t)PyLong_AsLong(gd_type);
        Py_DECREF(column_name);
        Py_DECREF(column_type);
        Py_DECREF(column_length);
        Py_DECREF(column_precision);
        Py_DECREF(column_scale);
        Py_DECREF(gd_type);

        columns_append(self->columns, *column);
    }
    self->columns->header_length = (int)ceil(self->columns->length/8.0);
    
    Py_DECREF(iterator);

    return 0;
}

static PyObject* Encoder_count_rows(PyObject* self, PyObject* args) {
    Py_buffer buffer;
    uint32_t n;

    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        Py_RETURN_NONE;
    }
    n = count_rows((unsigned char*) buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return PyLong_FromLong(n);
}

static PyObject* Encoder_unpack_row(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    PyObject* row;
    unsigned char* data;

    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    row = PyList_New(self->columns->length);
    unpack_row(&data, buffer.len, self->columns, row);
    PyBuffer_Release(&buffer);
    return row;
}

static PyObject* Encoder_unpack_rows(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    unsigned char* data;
    PyObject* rows;

    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    rows = PyList_New(0);
    unpack_rows(&data, buffer.len, self->columns, rows);
    PyBuffer_Release(&buffer);
    return rows;
}

static PyObject* Encoder_unpack_row_dict(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    unsigned char* data;
    PyObject* row;

    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    row = PyDict_New();
    unpack_row_dict(&data, buffer.len, self->columns, row);
    PyBuffer_Release(&buffer);
    return row;
}

static PyObject* Encoder_unpack_rows_dict(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    unsigned char* data;
    PyObject* rows;

    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    rows = PyList_New(0);
    unpack_rows_dict(&data, buffer.len, self->columns, rows);
    PyBuffer_Release(&buffer);
    return rows;
}

static PyObject* Encoder_unpack_row_str(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    char* null;
    char* delimiter;
    unsigned char* data;
    PyObject* row;

    if (!PyArg_ParseTuple(args, "s*ss", &buffer, &null, &delimiter)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    row = PyList_New(self->columns->length);
    unpack_row_str(&data, buffer.len, self->columns, row, null, delimiter);
    PyBuffer_Release(&buffer);
    return row;
}

static PyObject* Encoder_unpack_rows_str(Encoder* self, PyObject* args) {
    Py_buffer buffer;
    char* null;
    char* delimiter;
    unsigned char* data;
    PyObject* rows;

    if (!PyArg_ParseTuple(args, "s*ss", &buffer, &null, &delimiter)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    rows = PyList_New(0);
    unpack_rows_str(&data, buffer.len, self->columns, rows, null, delimiter);
    PyBuffer_Release(&buffer);
    return rows;
}

static PyObject* Encoder_unpack_stmt_info(PyObject* self, PyObject* args) {
    Py_buffer buffer;
    unsigned char* data;
    StatementInfo* s;
    PyObject* columns;
    StatementInfoColumn* column;
    PyObject* d;
    size_t i;

    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        Py_RETURN_NONE;
    }
    data = (unsigned char*) buffer.buf;

    s = (StatementInfo*)malloc(sizeof(StatementInfo));
    stmt_info_init(s, 1);
    parse_stmt_info(&data, s, buffer.len);
    columns = PyList_New(0);
    for (i=0; i<s->length; i++) {
        column = &s->array[i];
        d = PyDict_New();
        PyDict_SetItemString(d, "name", PyUnicode_FromString(column->Name));
        PyDict_SetItemString(d, "title", PyUnicode_FromString(column->Title));
        PyDict_SetItemString(d, "alias", PyUnicode_FromString(column->Alias));
        PyDict_SetItemString(d, "type", PyLong_FromLong((long)column->Type));
        PyDict_SetItemString(d, "length", PyLong_FromLong((long)column->Length));
        PyDict_SetItemString(d, "precision", PyLong_FromLong((long)column->Precision));
        PyDict_SetItemString(d, "scale", PyLong_FromLong((long)column->Scale));
        PyDict_SetItemString(d, "nullable", PyUnicode_FromString(column->CanReturnNull));
        PyDict_SetItemString(d, "default", PyUnicode_FromString(column->Default));
        PyDict_SetItemString(d, "format", PyUnicode_FromString(column->Format));
        PyList_Append(columns, d);
        Py_DECREF(d);
    }
    stmt_info_free(s);
    PyBuffer_Release(&buffer);
    return columns;
}

static PyMethodDef Encoder_methods[] = {
    {"count_rows", (PyCFunction)Encoder_count_rows, METH_STATIC|METH_VARARGS, ""},
    {"unpack_row", (PyCFunction)Encoder_unpack_row, METH_VARARGS, ""},
    {"unpack_rows", (PyCFunction)Encoder_unpack_rows, METH_VARARGS, ""},
    {"unpack_row_dict", (PyCFunction)Encoder_unpack_row_dict, METH_VARARGS, ""},
    {"unpack_rows_dict", (PyCFunction)Encoder_unpack_rows_dict, METH_VARARGS, ""},
    {"unpack_row_str", (PyCFunction)Encoder_unpack_row_str, METH_VARARGS, ""},
    {"unpack_rows_str", (PyCFunction)Encoder_unpack_rows_str, METH_VARARGS, ""},
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
