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

#include "src/common.h"
#include "src/convert.h"
#include "src/encoder.h"
#include "src/row.h"
#include "src/teradata.h"

#include <signal.h>

#ifdef __cplusplus
extern "C" {
#endif 

typedef struct {
    PyObject_HEAD
    TeradataConnection *conn;
    TeradataEncoder    *encoder;
} Cmd;

static void Cmd_dealloc(Cmd *self) {
    if (self->conn != NULL) {
        __teradata_free(self->conn);
        self->conn = NULL;
    }
    if (self->encoder != NULL) {
        encoder_free(self->encoder);
        self->encoder = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Cmd_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    Cmd *self;
    self = (Cmd*)type->tp_alloc(type, 0);
    self->conn = NULL;
    self->encoder = NULL;
    return (PyObject*)self;
}

static int Cmd_init(Cmd *self, PyObject *args, PyObject *kwargs) {
    char *host=NULL, *username=NULL, *password=NULL;
    uint32_t settings = 0;

    static char *kwlist[] = {"host", "username", "password", "encoder_settings", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sss|i", kwlist, &host, &username, &password,
            &settings)) {
        return -1;
    }
    if ((self->conn = teradata_connect(host, username, password)) == NULL) {
        return -1;
    }

    self->encoder = encoder_new(NULL, settings);
    if (self->encoder == NULL) {
        PyErr_Format(PyExc_ValueError, "Could not create encoder, settings value 0x%06x is invalid.", settings);
        return -1;
    }
    return 0;
}

static PyObject* Cmd_close(Cmd *self) {
    __teradata_close(self->conn);
    self->conn->connected = NOT_CONNECTED;
    Py_RETURN_NONE;
}

static PyObject* Cmd_columns(Cmd *self, PyObject *args, PyObject *kwargs) {
    PyObject *debug = NULL;
    static char *kwlist[] = {"debug", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &debug)) {
        return NULL;
    }
    if (self->encoder == NULL || self->encoder->Columns == NULL) {
        Py_RETURN_NONE;
    }
    if (debug != NULL && PyBool_Check(debug) && debug == Py_True) {
        RawStatementInfo *raw = self->encoder->Columns->raw;
        return PyBytes_FromStringAndSize(raw->data, raw->length);
    }
    return giraffez_columns_to_pyobject(self->encoder->Columns);
}

static PyObject* Cmd_execute(Cmd *self, PyObject *args, PyObject *kwargs) {
    char *command = NULL;
    int prepare_only = 0;
    static char *kwlist[] = {"command", "prepare_only", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|i", kwlist, &command, &prepare_only)) {
        return NULL;
    }
    if (self->conn->connected == NOT_CONNECTED) {
        PyErr_SetString(TeradataError, "1: Connection not established.");
        return NULL;
    }
    encoder_clear(self->encoder);
    if (prepare_only) {
        self->conn->dbc->req_proc_opt = 'P';
    } else {
        self->conn->dbc->req_proc_opt = 'B';
    }
    if (teradata_execute(self->conn, self->encoder, command) == NULL) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* Cmd_fetchone(Cmd *self) {
    PyObject* row = NULL;
    while (__teradata_fetch_record(self->conn) == OK) {
        if ((row = teradata_handle_record(self->encoder, self->conn->dbc->fet_parcel_flavor,
                (unsigned char**)&self->conn->dbc->fet_data_ptr,
                self->conn->dbc->fet_ret_data_len)) == NULL) {
            return NULL;
        } else if (row != Py_None) {
            return row;
        }
    }
    return teradata_check_error(self->conn);
}

static PyObject* Cmd_set_encoding(Cmd *self, PyObject *args) {
    uint32_t new_settings = 0;
    uint32_t settings;
    PyObject *null = NULL, *delimiter = NULL;
    if (!PyArg_ParseTuple(args, "i|OO", &settings, &null, &delimiter)) {
        return NULL;
    }
    if (settings & ROW_RETURN_MASK) {
        new_settings = (self->encoder->Settings & ~ROW_RETURN_MASK) | (settings & ROW_RETURN_MASK);
    }
    if (settings & DATETIME_RETURN_MASK) {
        new_settings = (self->encoder->Settings & ~DATETIME_RETURN_MASK) | (settings & DATETIME_RETURN_MASK);
    }
    if (settings & DECIMAL_RETURN_MASK) {
        new_settings = (self->encoder->Settings & ~DECIMAL_RETURN_MASK) | (settings & DECIMAL_RETURN_MASK);
    }
    if (encoder_set_encoding(self->encoder, new_settings) != 0) {
        PyErr_Format(PyExc_ValueError, "Encoder set_encoding failed, bad encoding '0x%06x'.", settings);
        return NULL;
    }
    Py_RETURN_ERROR(encoder_set_null(self->encoder, null));
    Py_RETURN_ERROR(encoder_set_delimiter(self->encoder, delimiter));
    Py_RETURN_NONE;
}

static PyMethodDef Cmd_methods[] = {
    {"close", (PyCFunction)Cmd_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Cmd_columns, METH_VARARGS|METH_KEYWORDS, ""},
    {"execute", (PyCFunction)Cmd_execute, METH_VARARGS|METH_KEYWORDS, ""},
    {"fetchone", (PyCFunction)Cmd_fetchone, METH_NOARGS, ""},
    {"set_encoding", (PyCFunction)Cmd_set_encoding, METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject CmdType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_teradata.Cmd",                                /* tp_name */
    sizeof(Cmd),                                    /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Cmd_dealloc,                        /* tp_dealloc */
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
    "GiraffeCmd objects",                           /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Cmd_methods,                                    /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)Cmd_init,                             /* tp_init */
    0,                                              /* tp_alloc */
    Cmd_new,                                        /* tp_new */
};

typedef struct {
    PyObject_HEAD
    TeradataEncoder *encoder;
} Encoder;

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
    uint32_t settings = ENCODER_SETTINGS_DEFAULT;
    static char *kwlist[] = {"columns_obj", "settings", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|i", kwlist, &columns_obj, &settings)) {
        return -1;
    }
    columns = giraffez_columns_from_pyobject(columns_obj);
    if (columns == NULL) {
        PyErr_SetString(PyExc_ValueError, "No columns found.");
        return -1;
    }
    self->encoder = encoder_new(columns, settings);
    if (self->encoder == NULL) {
        PyErr_SetString(PyExc_ValueError, "Could not create encoder. Bad settings. Bad person.");
        return -1;
    }
    return 0;
}

static PyObject* Encoder_count_rows(PyObject *self, PyObject *args) {
    Py_buffer buffer;
    uint32_t n;
    if (!PyArg_ParseTuple(args, "s*", &buffer)) {
        return NULL;
    }
    n = teradata_buffer_count_rows((unsigned char*)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return PyLong_FromLong(n);
}

static PyObject* Encoder_pack_row(Encoder *self, PyObject *args) {
    PyObject *items;
    unsigned char *data;
    uint16_t length = 0;
    if (!PyArg_ParseTuple(args, "O", &items)) {
        return NULL;
    }
    buffer_reset(self->encoder->buffer, 0);
    data = (unsigned char*)self->encoder->buffer->data;
    Py_RETURN_ERROR(self->encoder->PackRowFunc(self->encoder, items, &data, &length));
    return PyBytes_FromStringAndSize((char*)self->encoder->buffer->data, length);
}

static PyObject* Encoder_set_encoding(Encoder *self, PyObject *args) {
    uint32_t settings;
    if (!PyArg_ParseTuple(args, "i", &settings)) {
        return NULL;
    }
    if (encoder_set_encoding(self->encoder, settings) != 0) {
        PyErr_Format(PyExc_ValueError, "Encoder set_encoding failed, bad encoding '0x%06x'.", settings);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* Encoder_set_columns(Encoder *self, PyObject *args) {
    PyObject *obj;
    GiraffeColumns *columns;
    if (!PyArg_ParseTuple(args, "O", &obj)) {
        return NULL;
    }
    columns = giraffez_columns_from_pyobject(obj);
    if (columns == NULL) {
        PyErr_SetString(PyExc_ValueError, "No columns found.");
        return NULL;
    }
    self->encoder->Columns = columns;
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
    columns = columns_from_stmtinfo((unsigned char**)&buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);
    return giraffez_columns_to_pyobject(columns);
}

static PyMethodDef Encoder_methods[] = {
    {"count_rows", (PyCFunction)Encoder_count_rows, METH_STATIC|METH_VARARGS, ""},
    {"pack_row", (PyCFunction)Encoder_pack_row, METH_VARARGS, ""},
    {"set_columns", (PyCFunction)Encoder_set_columns, METH_VARARGS, ""},
    {"set_delimiter", (PyCFunction)Encoder_set_delimiter, METH_VARARGS, ""},
    {"set_encoding", (PyCFunction)Encoder_set_encoding, METH_VARARGS, ""},
    {"set_null", (PyCFunction)Encoder_set_null, METH_VARARGS, ""},
    {"unpack_row", (PyCFunction)Encoder_unpack_row, METH_VARARGS, ""},
    {"unpack_rows", (PyCFunction)Encoder_unpack_rows, METH_VARARGS, ""},
    {"unpack_stmt_info", (PyCFunction)Encoder_unpack_stmt_info, METH_STATIC|METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject EncoderType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_teradata.Encoder",                            /* tp_name */
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

static int py_quit(void* n) {
    Py_Exit(1);
    return -1;
}

static void cold_shutdown(int signum) {
    PyGILState_STATE state;
    fprintf(stderr, "*** Received SIGINT. Closing ...\n");
    // Py_Exit() is added as a pending call just in case the Python main
    // thread has any chance of finalizing. This will not allow for any
    // __exit__ code to run if it is successful in completing Py_Exit.
    // If unsuccessful, the standard C exit() function is called since
    // this code is reached when the user wants the process to end
    // regardless of the state of any open connections.
    state = PyGILState_Ensure();
    Py_AddPendingCall(&py_quit, NULL);
    PyGILState_Release(state);
    PyErr_SetInterrupt();
    exit(1);
}

static void warm_shutdown(int signum) {
    PyGILState_STATE state;
    fprintf(stderr, "\n*** Received SIGINT. Shutting down (clean) ...\n");
    // GIL must be acquired before setting interrupt
    state = PyGILState_Ensure();
    // Interrupt sent to Python main thread
    PyErr_SetInterrupt();
    PyGILState_Release(state);

    // Set secondary interrupt to allow for improper shutdown
    signal(SIGINT, cold_shutdown);
}

static void shutdown(int signum) {
    // Should imitate normal Ctrl+C behavior for giraffez code that has
    // not acquired the GIL
    PyGILState_STATE state;
    state = PyGILState_Ensure();
    PyErr_SetInterrupt();
    PyGILState_Release(state);
}

static PyObject* register_graceful_shutdown(PyObject* self) {
    signal(SIGINT, &warm_shutdown);
    signal(SIGTERM, &warm_shutdown);
    Py_RETURN_NONE;
}

static PyObject* register_shutdown(PyObject* self) {
    signal(SIGINT, &shutdown);
    signal(SIGTERM, &shutdown);
    Py_RETURN_NONE;
}

// TODO(chris): create a signal unregister
static PyMethodDef module_methods[] = {
    {"register_shutdown_signal", (PyCFunction)register_shutdown, METH_NOARGS, NULL},
    {"register_graceful_shutdown_signal", (PyCFunction)register_graceful_shutdown, METH_NOARGS, NULL},
    {NULL}  /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_teradata", "", -1, module_methods
};
#endif


MOD_INIT(_teradata)
{
    PyObject* m;

    if (PyType_Ready(&CmdType) < 0) {
        return MOD_ERROR_VAL;
    }

    if (PyType_Ready(&EncoderType) < 0) {
        return MOD_ERROR_VAL;
    }

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_teradata", module_methods);
#endif

    giraffez_types_import();

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    EndStatementError = PyErr_NewException("_teradata.StatementEnded", NULL, NULL);
    PyModule_AddObject(m, "StatementEnded", EndStatementError);
    EndStatementInfoError = PyErr_NewException("_teradata.StatementInfoEnded", NULL, NULL);
    PyModule_AddObject(m, "StatementInfoEnded", EndStatementInfoError);
    EndRequestError = PyErr_NewException("_teradata.RequestEnded", NULL, NULL);
    PyModule_AddObject(m, "RequestEnded", EndRequestError);

    if (define_exceptions(m) == NULL) {
        return MOD_ERROR_VAL;
    }

    Py_INCREF(&CmdType);
    PyModule_AddObject(m, "Cmd", (PyObject*)&CmdType);
    Py_INCREF(&EncoderType);
    PyModule_AddObject(m, "Encoder", (PyObject*)&EncoderType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
