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
#include <string.h>
#include <stdio.h>
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>
#include "_compat.h"
#include "encoder/columns.h"
#include "encoder/pytypes.h"
#include "encoder/types.h"
#include "encoder/unpack.h"
#include "tdcli.h"

PyObject* GiraffeError;


static PyObject* Cmd_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    Cmd* self;
    self = (Cmd*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int Cmd_init(Cmd* self, PyObject* args, PyObject* kwds) {
    char *host=NULL, *username=NULL, *password=NULL;
    char logonstr[1024] = "";
    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return -1;
    }
    sprintf(logonstr, "%s/%s,%s", host, username, password);
    self->connected = NOT_CONNECTED;
    self->status = OK;
    self->conn = tdcli_new();
    if (self->conn->result != OK) {
        /*PyErr_SetString(GiraffeError, "CLIv2: init failed");*/
        PyErr_Format(GiraffeError, "CLIv2: init failed -- %s", self->conn->dbc->msg_text);
        return -1;
    }
    if ((self->status = tdcli_connect(self->conn, host, username, password)) != OK) {
        PyErr_Format(GiraffeError, "CLIv2: connect failed -- %s", self->conn->dbc->msg_text);
        return -1;
    }
    if ((self->status = tdcli_fetch(self->conn)) != OK) {
        TeradataFailure* err = tdcli_read_failure(self->conn->dbc->fet_data_ptr);
        PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
        return -1;
    }
    if (self->conn->dbc->fet_ret_data_len < 1) {
        PyErr_Format(GiraffeError, "%d: Connection to host '%s' failed.", -1, (char*)host);
        return -1;
    }
    if (tdcli_end_request(self->conn) != OK) {
        PyErr_Format(GiraffeError, "CLIv2: end request failed -- %s", self->conn->dbc->msg_text);
        return -1;
    }
    self->connected = CONNECTED;
    return 0;
}

static PyObject* Cmd_close(Cmd* self) {
    tdcli_close(self->conn, self->connected);
    self->connected = NOT_CONNECTED;
    return Py_BuildValue("i", self->connected);
}

static void Cmd_dealloc(Cmd* self) {
    if (self->conn != NULL) {
        tdcli_free(self->conn);
        self->conn = NULL;
    }
    if (self->encoder != NULL) {
        free(self->encoder);
        self->encoder = NULL;
    }
    if (self->columns != NULL) {
        columns_free(self->columns);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Cmd_execute(Cmd* self, PyObject* args) {
    char* command = NULL;
    if (self->connected == NOT_CONNECTED) {
        PyErr_SetString(GiraffeError, "1: Connection not established.");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "s", &command)) {
        return NULL;
    }
    if (tdcli_execute(self->conn, command) != OK) {
        Cmd_close(self);
        PyErr_Format(GiraffeError, "CLIv2: initiate request failed -- %s", self->conn->dbc->msg_text);
        return NULL;
    }
    self->conn->dbc->i_sess_id = self->conn->dbc->o_sess_id;
    self->conn->dbc->i_req_id = self->conn->dbc->o_req_id;
    self->conn->dbc->func = DBFFET;
    if (self->encoder != NULL) {
        free(self->encoder);
        self->encoder = NULL;
    }
    if (self->columns != NULL) {
        columns_free(self->columns);
        self->columns = NULL;
    }
    return Py_BuildValue("i", 0);
}

static PyObject* Cmd_fetch_one(Cmd* self) {
    PyObject* result = NULL;
    PyObject* row = NULL;
    while ((self->status = tdcli_fetch_record(self->conn)) == OK) {
        if (self->conn->result == REQEXHAUST) {
            if (tdcli_end_request(self->conn) != OK) {
                PyErr_Format(GiraffeError, "%d: %s", self->conn->result, self->conn->dbc->msg_text);
                return NULL;
            }
            PyErr_Format(PyExc_StopIteration, "%d: %s", self->conn->result, self->conn->dbc->msg_text);
            return NULL;
        } else if (self->conn->result != OK) {
            PyErr_Format(GiraffeError, "%d: %s", self->conn->result, self->conn->dbc->msg_text);
            return NULL;
        }
        if (self->status == PCL_FAIL) {
            TeradataFailure* err = tdcli_read_failure(self->conn->dbc->fet_data_ptr);
            PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
            return NULL;
        } else if (self->status == PCL_ERR) {
            TeradataError* err = tdcli_read_error(self->conn->dbc->fet_data_ptr);
            PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
            return NULL;
        }
        switch (self->conn->dbc->fet_parcel_flavor) {
            case PclRECORD:
                /*row = PyList_New(0);*/
                row = self->encoder->UnpackRowFunc(self->encoder,
                    (unsigned char**)&self->conn->dbc->fet_data_ptr, self->conn->dbc->fet_ret_data_len);
                /*Py_INCREF(self->columns_obj);*/
                return giraffez_row_from_list(self->columns_obj, row);
                /*return row;*/
                /*Py_DECREF(row);*/
                /*result = PyList_New(0);*/
                /*return result;*/
                /*result = Py_BuildValue("(OO)", self->columns_obj, row);*/
                /*Py_DECREF(row);*/
                /*return result;*/
            case PclSTATEMENTINFO:
                if (self->encoder != NULL) {
                    free(self->encoder);
                    self->encoder = NULL;
                }
                if (self->columns != NULL) {
                    columns_free(self->columns);
                    self->columns = NULL;
                }
                self->columns = unpack_stmt_info_to_columns((unsigned char**)&self->conn->dbc->fet_data_ptr,
                    self->conn->dbc->fet_ret_data_len);
                self->encoder = encoder_new(self->columns);
                encoder_set_encoding(self->encoder, ENCODING_GIRAFFE_TYPES);
                self->columns_obj = giraffez_columns_from_columns(self->columns);
                break;
            case PclFAILURE:
                self->status = PCL_FAIL;
                break;
            case PclERROR:
                self->status = PCL_ERR;
                break;
            case PclENDSTATEMENT:
                break;
            case PclENDREQUEST:
                break;
            default:
                break;
        }
    }
    Py_RETURN_NONE;
}

static PyObject* Cmd_columns(Cmd* self) {
    PyObject* columns;
    if (self->columns == NULL) {
        Py_RETURN_NONE;
    }
    columns = giraffez_columns_from_columns(self->columns);
    return columns;
}

static PyMethodDef Cmd_methods[] = {
    {"close", (PyCFunction)Cmd_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Cmd_columns, METH_NOARGS, ""},
    {"execute", (PyCFunction)Cmd_execute, METH_VARARGS, ""},
    {"fetch_one", (PyCFunction)Cmd_fetch_one, METH_NOARGS, ""},
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
