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


#include "cmdobject.h"
#include "compat.h"
#include "teradata/cmd.h"


PyObject* GiraffeError;

static char cnta[4];

static void Cmd_dealloc(Cmd* self) {
    if (self->dbc != NULL) {
        free(self->dbc);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Cmd_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    Cmd* self;

    self = (Cmd*)type->tp_alloc(type, 0);
    self->connected = 0;
    return (PyObject*)self;
}

static int Cmd_init(Cmd* self, PyObject* args, PyObject* kwds) {
    return 0;
}

static PyObject* Cmd_close(Cmd* self) {
    if (self->connected) {
        close_connection(self->dbc, cnta);
        self->connected = 0;
    }
    return Py_BuildValue("i", self->connected);
}

static PyObject* Cmd_connect(Cmd* self, PyObject* args) {
    PyObject *host=NULL, *username=NULL, *password=NULL;
    char connection_string[1024] = "";
    int status;

    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        Py_RETURN_NONE;
    }
    if (self->dbc == NULL) {
        self->dbc = (dbcarea_t*)malloc(sizeof(dbcarea_t));
    }
    sprintf(connection_string, "%s/%s,%s", (char*)host, (char*)username, (char*)password);
    status = 0;
    if ((status = open_connection(self->dbc, cnta, connection_string)) != 0) {
        char err_msg[256] = "";
        if (status == -1 || self->dbc->fet_ret_data_len < 1) {
            // msg_text can only be read after initialize_dbcarea
            snprintf(err_msg, 256, "%d: Connection to host '%s' failed.", status, (char*)host);
        } else {
            struct CliFailureType *Error_Fail = (struct CliFailureType *) self->dbc->fet_data_ptr;
            snprintf(err_msg, 256, "%d: %s", Error_Fail->Code, Error_Fail->Msg);
        }
        PyErr_SetString(GiraffeError, err_msg);
        return NULL;
    } 
    self->connected = 1;
    return Py_BuildValue("i", 0);
}

static int safe_handle_record(dbcarea_t* dbc, char cnta[]) {
    int status;
    Py_BEGIN_ALLOW_THREADS
    status = handle_record(dbc, cnta);
    Py_END_ALLOW_THREADS
    return status;
}

static PyObject* Cmd_execute(Cmd* self, PyObject* args) {
    PyObject* command = NULL;
    PyObject* statementinfo = PyBytes_FromStringAndSize(NULL, 0);
    PyObject* rows = PyBytes_FromStringAndSize(NULL, 0);
    PyObject* results = NULL;
    PyObject* data = PyList_New(0);
    PyObject* error = NULL;
    int status;

    if (self == NULL || self->dbc == NULL) {
        PyErr_SetString(GiraffeError, "1: Connection not established");
        return NULL;
    }

    if (!PyArg_ParseTuple(args, "s", &command)) {
        Py_RETURN_NONE;
    }

    open_request(self->dbc, cnta, (char*)command);
    fetch_request(self->dbc, cnta, self->dbc->o_req_id, self->dbc->o_sess_id);

    while ((status = safe_handle_record(self->dbc, cnta)) == OK) {
        size_t length = self->dbc->fet_ret_data_len;
        if (self->dbc->fet_parcel_flavor == PclRECORD) {
            unsigned char v1 = (self->dbc->fet_ret_data_len & 0xff);
            unsigned char v2 = (self->dbc->fet_ret_data_len & 0xff00) >> 8;
            PyObject* s = PyBytes_FromFormat("%c%c", v1, v2);
            PyBytes_Concat(&rows, s);
            Py_DECREF(s);
        }

        if (self->dbc->fet_parcel_flavor == PclSTATEMENTINFO) {
            // TODO: should do _check of types everywhere before assuming encoding
            // while unlikely teradata could ignore the request for UTF-8 or not default
            // to latin-1, etc
            PyObject* s = PyBytes_FromStringAndSize(self->dbc->fet_data_ptr, length);
            PyBytes_Concat(&statementinfo, s);
            Py_DECREF(s);
        } else if (self->dbc->fet_parcel_flavor == PclRECORD) {
            PyObject* s = PyBytes_FromStringAndSize(self->dbc->fet_data_ptr, length);
            PyBytes_Concat(&rows, s);
            Py_DECREF(s);
        } else if (self->dbc->fet_parcel_flavor == PclENDSTATEMENT) {
            PyObject* d = PyDict_New();
            if (statementinfo) {
                PyDict_SetItemString(d, "stmtinfo", statementinfo);
                Py_DECREF(statementinfo);
            } else {
                Py_INCREF(Py_None);
                PyDict_SetItemString(d, "stmtinfo", Py_None);
            }
            PyDict_SetItemString(d, "rows", rows);
            Py_DECREF(rows);
            statementinfo = PyBytes_FromStringAndSize(NULL, 0);
            rows = PyBytes_FromStringAndSize(NULL, 0);
            PyList_Append(data, d);
            Py_DECREF(d);
        }
    }
    if (status == FAILED) {
        error = Py_BuildValue("(is)", status, self->dbc->msg_text);
    } else if (status == PCL_FAIL) {
        struct CliFailureType *ErrorFail = (struct CliFailureType *) self->dbc->fet_data_ptr;
        error = Py_BuildValue("(is)", ErrorFail->Code, ErrorFail->Msg);
    } else if (status == PCL_ERR) {
        struct CliErrorType *ErrorFail = (struct CliErrorType *) self->dbc->fet_data_ptr;
        error = Py_BuildValue("(is)", ErrorFail->Code, ErrorFail->Msg);
    } else {
        Py_INCREF(Py_None);
        error = Py_None;
    }
    close_request(self->dbc, cnta, self->dbc->o_req_id, self->dbc->o_sess_id);
    results = Py_BuildValue("(OO)", data, error);
    Py_DECREF(data);
    Py_XDECREF(error);
    return results;
}

static PyMethodDef Cmd_methods[] = {
    {"close", (PyCFunction)Cmd_close, METH_VARARGS, ""},
    {"connect", (PyCFunction)Cmd_connect, METH_VARARGS, ""},
    {"execute", (PyCFunction)Cmd_execute, METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject CmdType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_cli.Cmd",                                     /* tp_name */
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
