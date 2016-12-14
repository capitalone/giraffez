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

#include "_compat.h"
#include "cli/errors.h"
#include "cli/rowobject.h"
#include "cli/tdcli.h"
#include "encoder/columns.h"
#include "encoder/pytypes.h"
#include "encoder/unpack.h"


static EncoderSettings settings = {
    ROW_ENCODING_LIST,
    ITEM_ENCODING_GIRAFFE_TYPES,
    DECIMAL_AS_STRING
};

static PyObject* check_tdcli_error(char* dataptr) {
    TeradataError* err;
    err = tdcli_read_error(dataptr);
    PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
    return NULL;
}

static PyObject* check_tdcli_failure(char* dataptr) {
    TeradataFailure* err;
    err = tdcli_read_failure(dataptr);
    PyErr_Format(GiraffeError, "%d: %s", err->Code, err->Msg);
    return NULL;
}

static PyObject* check_error(Cmd *self) {
    if (self->conn->result == REQEXHAUST) {
        if (tdcli_end_request(self->conn) != OK) {
            PyErr_Format(GiraffeError, "%d: %s", self->conn->result, self->conn->dbc->msg_text);
            return NULL;
        }
    } else if (self->conn->result != OK) {
        PyErr_Format(GiraffeError, "%d: %s", self->conn->result, self->conn->dbc->msg_text);
        return NULL;
    }
    if (self->status == PCL_FAIL) {
        return check_tdcli_failure(self->conn->dbc->fet_data_ptr);
    } else if (self->status == PCL_ERR) {
        return check_tdcli_error(self->conn->dbc->fet_data_ptr);
    }
    Py_RETURN_NONE;
}

static void Cmd_dealloc(Cmd* self) {
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

static PyObject* Cmd_new(PyTypeObject* type, PyObject* args, PyObject* kwargs) {
    Cmd* self;
    self = (Cmd*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int Cmd_init(Cmd* self, PyObject* args, PyObject* kwargs) {
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
    if (self->conn->result != OK) {
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

static PyObject* Cmd_columns(Cmd* self) {
    if (self->columns_obj == NULL) {
        Py_RETURN_NONE;
    }
    return self->columns_obj;
}

static PyObject* Cmd_execute(Cmd* self, PyObject* args) {
    char* command = NULL;
    size_t i;
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
    encoder_clear(self->encoder);
    Py_XDECREF(self->column_map);
    self->column_map = PyDict_New();
    while ((self->status = tdcli_fetch_record(self->conn)) == OK) {
        switch (self->conn->dbc->fet_parcel_flavor) {
            case PclSTATEMENTINFO:
                encoder_clear(self->encoder);
                self->encoder->Columns = self->encoder->UnpackStmtInfoFunc(
                    (unsigned char**)&self->conn->dbc->fet_data_ptr,
                    self->conn->dbc->fet_ret_data_len);
                self->columns_obj = giraffez_columns_from_columns(self->encoder->Columns);
                for (i=0; i<self->encoder->Columns->length; i++) {
                    PyDict_SetItemString(self->column_map, self->encoder->Columns->array[i].SafeName, PyLong_FromLong((long)i));
                }
                goto error;
        }
    }
error:
    return check_error(self);
}

static PyObject* Cmd_fetchone(Cmd *self) {
    PyGILState_STATE state;
    PyObject* result = NULL;
    PyObject* row = NULL;
    while ((self->status = tdcli_fetch_record(self->conn)) == OK) {
        switch (self->conn->dbc->fet_parcel_flavor) {
            case PclRECORD:
                state = PyGILState_Ensure();
                if ((row = self->encoder->UnpackRowFunc(self->encoder,
                    (unsigned char**)&self->conn->dbc->fet_data_ptr, self->conn->dbc->fet_ret_data_len)) == NULL) {
                    return NULL;
                }
                result = GiraffezRow_FromList(self->column_map, row);
                /*result = giraffez_row_from_list(self->columns_obj, row);*/
                PyGILState_Release(state);
                return result;
            case PclSTATEMENTINFO:
                encoder_clear(self->encoder);
                self->encoder->Columns = self->encoder->UnpackStmtInfoFunc(
                    (unsigned char**)&self->conn->dbc->fet_data_ptr,
                    self->conn->dbc->fet_ret_data_len);
                self->columns_obj = giraffez_columns_from_columns(self->encoder->Columns);
                break;
            case PclSTATEMENTINFOEND:
                if (check_error(self) == NULL) {
                    return NULL;
                }
                break;
            case PclENDSTATEMENT:
                if (check_error(self) == NULL) {
                    return NULL;
                }
                PyErr_SetNone(EndStatementError);
                return NULL;
            case PclENDREQUEST:
                if (check_error(self) == NULL) {
                    return NULL;
                }
                PyErr_SetNone(EndRequestError);
                return NULL;
            default:
                break;
        }
    }
    Py_RETURN_NONE;
}

static PyMethodDef Cmd_methods[] = {
    {"close", (PyCFunction)Cmd_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Cmd_columns, METH_NOARGS, ""},
    {"execute", (PyCFunction)Cmd_execute, METH_VARARGS, ""},
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
