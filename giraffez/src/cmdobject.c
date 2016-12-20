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

// Python 2/3 C API and Windows compatibility
#include "_compat.h"

#include "errors.h"
#include "tdcli.h"
#include "columns.h"
#include "pytypes.h"
#include "unpack.h"


#define MAX_PARCEL_ATTEMPTS 5


static EncoderSettings settings = {
    ROW_ENCODING_LIST,
    ITEM_ENCODING_GIRAFFE_TYPES,
    DECIMAL_AS_STRING
};


PyObject* check_tdcli_error(unsigned char *dataptr) {
    TeradataError *err;
    err = tdcli_read_error((char*)dataptr);
    PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
    return NULL;
}

PyObject* check_tdcli_failure(unsigned char *dataptr) {
    TeradataFailure *err;
    err = tdcli_read_failure((char*)dataptr);
    PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
    return NULL;
}

PyObject* check_parcel_error(TeradataConnection *conn) {
    switch (conn->dbc->fet_parcel_flavor) {
    case PclFAILURE:
        return check_tdcli_failure((unsigned char*)conn->dbc->fet_data_ptr);
    case PclERROR:
        return check_tdcli_error((unsigned char*)&conn->dbc->fet_data_ptr);
    }
    Py_RETURN_NONE;
}

PyObject* check_error(TeradataConnection *conn) {
    if (conn->result == REQEXHAUST) {
        if (tdcli_end_request(conn) != OK) {
            PyErr_Format(GiraffeError, "%d: %s", conn->result, conn->dbc->msg_text);
            return NULL;
        }
    } else if (conn->result != OK) {
        PyErr_Format(GiraffeError, "%d: %s", conn->result, conn->dbc->msg_text);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject* teradata_connect(TeradataConnection *conn, const char *host, const char *username,
        const char *password) {
    int status;
    if (conn->result != OK) {
        PyErr_Format(GiraffeError, "CLIv2: init failed -- %s", conn->dbc->msg_text);
        return NULL;
    }
    if ((status = tdcli_connect(conn, host, username, password)) != OK) {
        PyErr_Format(GiraffeError, "CLIv2: connect failed -- %s", conn->dbc->msg_text);
        return NULL;
    }
    // TODO: Not sure if we are even able to receive a failure parcel
    // if the connection wasn't successful. Might be ok to remove this.
    // I'm also not sure if this fetch is even necessary. It appears
    // to work either way despite being in the CLIv2 documentation
    // as a required part of connecting. Also not sure if need to 
    // check both ->result and ->status at this step also.
    if ((status = tdcli_fetch(conn)) != OK) {
        PyErr_Format(GiraffeError, "CLIv2: fetch failed -- %s", conn->dbc->msg_text);
        return NULL;
    } else {
        if (check_parcel_error(conn) == NULL) {
            return NULL;
        }
    }
    if (conn->dbc->fet_ret_data_len < 1) {
        PyErr_Format(GiraffeError, "%d: Connection to host '%s' failed.", -1, (char*)host);
        return NULL;
    }
    if (tdcli_end_request(conn) != OK) {
        PyErr_Format(GiraffeError, "CLIv2: end request failed -- %s", conn->dbc->msg_text);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject* teradata_execute(TeradataConnection *conn, TeradataEncoder *e, const char *command) {
    PyObject *row = NULL;
    size_t count;
    if (tdcli_execute(conn, command) != OK) {
        PyErr_Format(GiraffeError, "CLIv2: initiate request failed -- %s", conn->dbc->msg_text);
        return NULL;
    }
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFFET;
    count = 0;
    // Find first statement info parcel only
    while (tdcli_fetch_record(conn) == OK && count < MAX_PARCEL_ATTEMPTS) {
        if ((row = handle_record(e, conn->dbc->fet_parcel_flavor,
                (unsigned char**)&conn->dbc->fet_data_ptr,
                conn->dbc->fet_ret_data_len)) == NULL) {
            return NULL;
        }
        if (e != NULL && e->Columns != NULL) {
            Py_RETURN_NONE;
        }
        count++;
    }
    return check_error(conn);
}

PyObject* handle_record(TeradataEncoder *e, const uint32_t parcel_t, unsigned char **data, const uint32_t length) {
    PyGILState_STATE state;
    PyObject *row = NULL;
    switch (parcel_t) {
        case PclRECORD:
            state = PyGILState_Ensure();
            if ((row = e->UnpackRowFunc(e, data, length)) == NULL) {
                return NULL;
            }
            PyGILState_Release(state);
            return row;
        case PclSTATEMENTINFO:
            encoder_clear(e);
            e->Columns = e->UnpackStmtInfoFunc(data, length);
            break;
        case PclSTATEMENTINFOEND:
            break;
        case PclENDSTATEMENT:
            PyErr_SetNone(EndStatementError);
            return NULL;
        case PclENDREQUEST:
            PyErr_SetNone(EndRequestError);
            return NULL;
        case PclFAILURE:
            return check_tdcli_failure(*data);
        case PclERROR:
            return check_tdcli_error(*data);
    }
    Py_RETURN_NONE;
}

static void Cmd_dealloc(Cmd *self) {
    if (self->conn != NULL) {
        tdcli_free(self->conn);
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
    return (PyObject*)self;
}

static int Cmd_init(Cmd *self, PyObject *args, PyObject *kwargs) {
    char *host=NULL, *username=NULL, *password=NULL;
    char *decimal_return_type = "";
    static char *kwlist[] = {"host", "username", "password", "decimal_return_type", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sss|s", kwlist, &host, &username, &password,
            &decimal_return_type)) {
        return -1;
    }
    if (strcmp(decimal_return_type, "str") == 0) {
        settings.decimal_return_type = DECIMAL_AS_STRING;
    } else if (strcmp(decimal_return_type, "float") == 0) {
        settings.decimal_return_type = DECIMAL_AS_FLOAT;
    } else if (strcmp(decimal_return_type, "decimal") == 0) {
        settings.decimal_return_type = DECIMAL_AS_GIRAFFEZ_DECIMAL;
    }
    self->encoder = encoder_new(NULL, &settings);
    self->connected = NOT_CONNECTED;
    self->status = OK;
    self->conn = tdcli_new();
    if (teradata_connect(self->conn, host, username, password) == NULL) {
        return -1;
    }
    self->connected = CONNECTED;
    return 0;
}

static PyObject* Cmd_close(Cmd *self) {
    tdcli_close(self->conn, self->connected);
    self->connected = NOT_CONNECTED;
    return Py_BuildValue("i", self->connected);
}

static PyObject* Cmd_columns(Cmd *self) {
    if (self->encoder == NULL || self->encoder->Columns == NULL) {
        Py_RETURN_NONE;
    }
    return columns_to_pylist(self->encoder->Columns);
}

static PyObject* Cmd_execute(Cmd *self, PyObject *args, PyObject *kwargs) {
    char *command = NULL;
    int prepare_only = 0;
    static char *kwlist[] = {"command", "prepare_only", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|i", kwlist, &command, &prepare_only)) {
        return NULL;
    }
    if (self->connected == NOT_CONNECTED) {
        PyErr_SetString(GiraffeError, "1: Connection not established.");
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
    while (tdcli_fetch_record(self->conn) == OK) {
        if ((row = handle_record(self->encoder, self->conn->dbc->fet_parcel_flavor,
                (unsigned char**)&self->conn->dbc->fet_data_ptr,
                self->conn->dbc->fet_ret_data_len)) == NULL) {
            return NULL;
        } else if (row != Py_None) {
            return row;
        }
    }
    return check_error(self->conn);
}

static PyMethodDef Cmd_methods[] = {
    {"close", (PyCFunction)Cmd_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Cmd_columns, METH_NOARGS, ""},
    {"execute", (PyCFunction)Cmd_execute, METH_VARARGS|METH_KEYWORDS, ""},
    {"fetchone", (PyCFunction)Cmd_fetchone, METH_NOARGS, ""},
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
