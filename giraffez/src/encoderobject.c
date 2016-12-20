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

// Python 2/3 C API and Windows compatibility
#include "_compat.h"

#include "encoder.h"
#include "pytypes.h"
#include "unpack.h"


static void Encoder_dealloc(Encoder *self) {
    if (self->encoder != NULL) {
        encoder_free(self->encoder);
        self->encoder = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Encoder_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    Encoder *self;
    self = (Encoder*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int Encoder_init(Encoder *self, PyObject *args, PyObject *kwargs) {
    PyObject *columns_obj;
    GiraffeColumns *columns;
    static EncoderSettings settings = {
        ROW_ENCODING_LIST,
        ITEM_ENCODING_BUILTIN_TYPES,
        DECIMAL_AS_STRING
    };
    char *row_encoding = "";
    char *item_encoding = "";
    char *decimal_return_type = "";
    static char *kwlist[] = {"columns_obj", "row_encoding", "item_encoding", "decimal_return_type", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|sss", kwlist, &columns_obj, &row_encoding,
            &item_encoding, &decimal_return_type)) {
        return -1;
    }
    if (strcmp(row_encoding, "str") == 0) {
        settings.row_encoding_type = ROW_ENCODING_STRING;
    } else if (strcmp(row_encoding, "dict") == 0) {
        settings.row_encoding_type = ROW_ENCODING_DICT;
    } else if (strcmp(row_encoding, "list") == 0) {
        settings.row_encoding_type = ROW_ENCODING_LIST;
    }
    if (strcmp(item_encoding, "str") == 0) {
        settings.item_encoding_type = ITEM_ENCODING_STRING;
    } else if (strcmp(item_encoding, "builtin") == 0) {
        settings.item_encoding_type = ITEM_ENCODING_BUILTIN_TYPES;
    } else if (strcmp(item_encoding, "giraffez") == 0) {
        settings.item_encoding_type = ITEM_ENCODING_GIRAFFE_TYPES;
    }
    if (strcmp(decimal_return_type, "str") == 0) {
        settings.decimal_return_type = DECIMAL_AS_STRING;
    } else if (strcmp(decimal_return_type, "float") == 0) {
        settings.decimal_return_type = DECIMAL_AS_FLOAT;
    } else if (strcmp(decimal_return_type, "decimal") == 0) {
        settings.decimal_return_type = DECIMAL_AS_GIRAFFEZ_DECIMAL;
    }
    columns = columns_to_giraffez_columns(columns_obj);
    if (columns == NULL) {
        PyErr_SetString(PyExc_ValueError, "No columns found.");
        return -1;
    }
    self->encoder = encoder_new(columns, &settings);
    return 0;
}

static PyObject* Encoder_count_rows(PyObject *self, PyObject *args) {
    Py_buffer buffer;
    uint32_t n;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    n = count_rows((unsigned char*)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return PyLong_FromLong(n);
}

static PyObject* Encoder_set_encoding(Encoder *self, PyObject *args) {
    int row_encoding;
    int item_encoding;
    int decimal_return_type;
    EncoderSettings settings;
    if (!PyArg_ParseTuple(args, "iii", &row_encoding, &item_encoding, &decimal_return_type)) {
        return NULL;
    }
    settings = (EncoderSettings){row_encoding, item_encoding, decimal_return_type};
    encoder_set_encoding(self->encoder, &settings);
    Py_RETURN_NONE;
}

static PyObject* Encoder_set_delimiter(Encoder *self, PyObject *args) {
    PyObject *obj;
    if (!PyArg_ParseTuple(args, "O", &obj)) {
        return NULL;
    }
    encoder_set_delimiter(self->encoder, obj);
    Py_RETURN_NONE;
}

static PyObject* Encoder_set_null(Encoder *self, PyObject *args) {
    PyObject *obj;
    if (!PyArg_ParseTuple(args, "O", &obj)) {
        return NULL;
    }
    encoder_set_null(self->encoder, obj);
    Py_RETURN_NONE;
}

static PyObject* Encoder_unpack_row(Encoder *self, PyObject *args) {
    Py_buffer buffer;
    PyObject *row;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    row = self->encoder->UnpackRowFunc(self->encoder, (unsigned char**)&buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return row;
}

static PyObject* Encoder_unpack_rows(Encoder *self, PyObject *args) {
    Py_buffer buffer;
    PyObject *rows;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    rows = self->encoder->UnpackRowsFunc(self->encoder, (unsigned char**)&buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return rows;
}

static PyObject* Encoder_unpack_stmt_info(PyObject *self, PyObject *args) {
    Py_buffer buffer;
    GiraffeColumns *columns;
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
    {"set_delimiter", (PyCFunction)Encoder_set_delimiter, METH_VARARGS, ""},
    {"set_null", (PyCFunction)Encoder_set_null, METH_VARARGS, ""},
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
