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

#include "common.h"


PyObject *TeradataError;
PyObject *GiraffezError;
PyObject *EncoderError;
PyObject *InvalidCredentialsError;

typedef struct {
    PyException_HEAD
    PyObject *message;
    PyObject *code;
} TeradataErrorObject;

static int TeradataError_init(TeradataErrorObject *self, PyObject *args, PyObject *kwds) {
    PyObject *msg = NULL;
    PyObject *code = NULL;
    PyObject *item = NULL;
    PyObject *parts = NULL;
    PyObject *colon = NULL;
    PyObject *utmp = NULL;
    Py_ssize_t size;
    if (!PyArg_ParseTuple(args, "O", &msg)) {
        return -1;
    }
    Py_INCREF(args);
    Py_XSETREF(self->args, args);
    colon = PyUnicode_FromString(":");
    if (PyBytes_Check(msg)) {
        if ((utmp = PyUnicode_FromEncodedObject(msg, "UTF-8", NULL)) == NULL) {
            goto error;
        }
        msg = utmp;
    }
    if ((parts = PyUnicode_Split(msg, colon, 1)) == NULL) {
        goto error;
    }
    size = PyList_Size(parts);
    if (size == 2) {
        PyObject *cro = PyList_GET_ITEM(parts, 0);
        if ((code = PyLong_FromUnicodeObject(cro, 10)) == NULL) {
            goto error;
        }
        Py_XSETREF(self->code, code);
        if ((item = PyList_GetItem(parts, 1)) == NULL) {
            goto error;
        }
        Py_INCREF(PyList_GET_ITEM(parts, 1));
        Py_XSETREF(self->message, PyList_GET_ITEM(parts, 1));
    }
    Py_XDECREF(utmp);
    Py_XDECREF(colon);
    return 0;
error:
    Py_XDECREF(utmp);
    Py_XDECREF(colon);
    return -1;
}

static int TeradataError_clear(TeradataErrorObject *self) {
    Py_CLEAR(self->message);
    Py_CLEAR(self->code);
    Py_CLEAR(self->dict);
    Py_CLEAR(self->args);
    Py_CLEAR(self->traceback);
    Py_CLEAR(self->cause);
    Py_CLEAR(self->context);
    return 0;
}

static void TeradataError_dealloc(TeradataErrorObject *self) {
    _PyObject_GC_UNTRACK(self);
    TeradataError_clear(self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static int TeradataError_traverse(TeradataErrorObject *self, visitproc visit, void *arg) {
    Py_VISIT(self->message);
    Py_VISIT(self->code);
    Py_VISIT(self->dict);
    Py_VISIT(self->args);
    Py_VISIT(self->traceback);
    Py_VISIT(self->cause);
    Py_VISIT(self->context);
    return 0;
}

static PyObject* TeradataError_repr(PyObject *obj) {
    PyObject *message, *rep, *repr, *sub;
    TeradataErrorObject *self = (TeradataErrorObject*)obj;
    sub = PyUnicode_FromString("\x01");
    rep = PyUnicode_FromString("");
    if ((message = PyUnicode_Replace(self->message, sub, rep, -1)) == NULL) {
        return NULL;
    }
    repr = PyUnicode_FromFormat("TeradataError(%S:%S)", self->code, message);
    Py_DECREF(message);
    Py_DECREF(sub);
    Py_DECREF(rep);
    return repr;
}

static PyMemberDef TeradataError_members[] = {
    {"message", T_OBJECT, offsetof(TeradataErrorObject, message), 0, PyDoc_STR("exception message")},
    {"code", T_OBJECT, offsetof(TeradataErrorObject, code), 0, PyDoc_STR("code name")},
    {NULL}  /* Sentinel */
};

PyTypeObject TeradataErrorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "giraffez.TeradataErrorz",                      /* tp_name */
    sizeof(TeradataErrorObject),                    /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)TeradataError_dealloc,              /* tp_dealloc */
    0,                                              /* tp_print */
    0,                                              /* tp_getattr */
    0,                                              /* tp_setattr */
    0,                                              /* tp_compare */
    TeradataError_repr,                             /* tp_repr */
    0,                                              /* tp_as_number */
    0,                                              /* tp_as_sequence */
    0,                                              /* tp_as_mapping */
    0,                                              /* tp_hash */
    0,                                              /* tp_call */
    0,                                              /* tp_str */
    0,                                              /* tp_getattro */
    0,                                              /* tp_setattro */
    0,                                              /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE
         | Py_TPFLAGS_HAVE_GC,                      /* tp_flags */
    0,                                              /* tp_doc */
    (traverseproc)TeradataError_traverse,           /* tp_traverse */
    (inquiry)TeradataError_clear,                   /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    0,                                              /* tp_methods */
    TeradataError_members,                          /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    offsetof(TeradataErrorObject, dict),            /* tp_dictoffset */
    (initproc)TeradataError_init,                   /* tp_init */
    0,                                              /* tp_alloc */
    0,                                              // tp_new
};

PyObject* define_exceptions(PyObject *module) {
    GiraffezError = PyErr_NewException("giraffez.Error", NULL, NULL);
    PyModule_AddObject(module, "Error", GiraffezError);

    EncoderError = PyErr_NewException("giraffez.EncoderError", NULL, NULL);
    PyModule_AddObject(module, "EncoderError", EncoderError);

    TeradataErrorType.tp_base = (PyTypeObject*)PyExc_Exception;
    if (PyType_Ready(&TeradataErrorType) < 0) {
        return NULL;
    }
    if ((TeradataError = PyErr_NewException("giraffez.TeradataError",  (PyObject*)&TeradataErrorType, NULL)) == NULL) {
        return NULL;
    }
    if (PyModule_AddObject(module, "TeradataError", TeradataError) == -1) {
        return NULL;
    }

    InvalidCredentialsError = PyErr_NewException("giraffez.InvalidCredentialsError", TeradataError, NULL);
    PyModule_AddObject(module, "InvalidCredentialsError", InvalidCredentialsError);
    Py_RETURN_NONE;
}

void debug_printf(const char *fmt, ...) {
    PyObject *s;
    va_list vargs;
    va_start(vargs, fmt);
    s = PyUnicode_FromFormatV(fmt, vargs);
    va_end(vargs);
    fprintf(stderr, "DEBUG: %s\n", PyUnicode_AsUTF8(s));
    Py_XDECREF(s);
}
