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
#include "encoder/types.h"
#include "encoder/unpack.h"

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
    self->result = EM_OK;
    self->dbc = (dbcarea_t*)malloc(sizeof(dbcarea_t));
    self->dbc->total_len = sizeof(dbcarea_t);
    DBCHINI(&self->result, self->cnta, self->dbc);
    if (self->result != EM_OK) {
        PyErr_SetString(GiraffeError, "CLIv2: init failed");
        return -1;
    }
    self->dbc->change_opts = 'Y';
    self->dbc->resp_mode = 'I';
    self->dbc->use_presence_bits = 'N';
    self->dbc->keep_resp = 'N';
    self->dbc->wait_across_crash = 'N';
    self->dbc->tell_about_crash = 'Y';
    self->dbc->loc_mode = 'Y';
    self->dbc->var_len_req = 'N';
    self->dbc->var_len_fetch = 'N';
    self->dbc->save_resp_buf = 'N';
    self->dbc->two_resp_bufs = 'Y';
    self->dbc->ret_time = 'N';
    self->dbc->parcel_mode = 'Y';
    self->dbc->wait_for_resp = 'Y';
    self->dbc->req_proc_opt = 'B';
    self->dbc->return_statement_info = 'Y';
    self->dbc->req_buf_len = 65535;
    self->dbc->maximum_parcel = 'H';
    self->dbc->max_decimal_returned = 38;
    self->dbc->charset_type = 'N';

    snprintf(self->session_charset, 32, "%-30s", "UTF8");
    self->dbc->inter_ptr = self->session_charset;
    self->dbc->logon_ptr = logonstr;
    self->dbc->logon_len = (UInt32) strlen(logonstr);
    self->dbc->func = DBFCON;
    DBCHCL(&self->result, self->cnta, self->dbc);
    if (self->result != EM_OK) {
        PyErr_Format(GiraffeError, "CLIv2: connect failed -- %s", self->dbc->msg_text);
        return -1;
    }
    self->dbc->i_sess_id = self->dbc->o_sess_id;
    self->dbc->i_req_id = self->dbc->o_req_id;
    self->dbc->func = DBFFET;
    DBCHCL(&self->result, self->cnta, self->dbc);
    if (self->dbc->fet_ret_data_len < 1) {
        PyErr_Format(GiraffeError, "%d: Connection to host '%s' failed.", -1, (char*)host);
        return -1;
    }
    if (self->result == EM_OK) {
        switch (self->dbc->fet_parcel_flavor) {
        case PclFAILURE:
            self->status = PCL_FAIL;
            break;
        case PclERROR:
            self->status = PCL_ERR;
            break;
        }
    }
    self->dbc->i_sess_id = self->dbc->o_sess_id;
    self->dbc->i_req_id = self->dbc->o_req_id;
    self->dbc->func = DBFERQ;
    DBCHCL(&self->result, self->cnta, self->dbc);
    if (self->result != EM_OK) {
        PyErr_SetString(GiraffeError, "CLIv2: end request failed");
        return -1;
    }
    if (self->status != OK) {
        struct CliFailureType *ErrorFail = (struct CliFailureType *) self->dbc->fet_data_ptr;
        PyErr_Format(GiraffeError, "%d: %s", ErrorFail->Code, ErrorFail->Msg);
        return -1;
    }
    self->connected = CONNECTED;
    return 0;
}

static PyObject* Cmd_close(Cmd* self) {
    if (self->connected == CONNECTED) {
        self->dbc->func = DBFDSC;
        DBCHCL(&self->result, self->cnta, self->dbc);
    }
    DBCHCLN(&self->result, self->cnta);
    self->connected = NOT_CONNECTED;
    return Py_BuildValue("i", self->connected);
}

static void Cmd_dealloc(Cmd* self) {
    if (self->dbc != NULL) {
        free(self->dbc);
        self->dbc = NULL;
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
    self->dbc->req_ptr = command;
    self->dbc->req_len = (UInt32) strlen(command);
    self->dbc->func = DBFIRQ;
    DBCHCL(&self->result, self->cnta, self->dbc);
    if (self->result != EM_OK) {
        Cmd_close(self);
        PyErr_Format(GiraffeError, "CLIv2: initiate request failed -- %s", self->dbc->msg_text);
        return NULL;
    }
    self->dbc->i_sess_id = self->dbc->o_sess_id;
    self->dbc->i_req_id = self->dbc->o_req_id;
    self->dbc->func = DBFFET;
    if (self->columns != NULL) {
        columns_free(self->columns);
        self->columns = NULL;
    }
    return Py_BuildValue("i", 0);
}

static PyObject* Cmd_columns(Cmd* self) {
    int i;
    PyObject* d;
    PyObject* columns;
    GiraffeColumn* column;
    if (self->columns == NULL) {
        Py_RETURN_NONE;
    }
    columns = PyList_New(self->columns->length);
    for (i=0; i<self->columns->length; i++) {
        column = &self->columns->array[i];
        d = PyDict_New();
        PyDict_SetItemString(d, "name", PyUnicode_FromString(column->Name));
        PyDict_SetItemString(d, "title", PyUnicode_FromString(column->Title));
        PyDict_SetItemString(d, "alias", PyUnicode_FromString(column->Alias));
        PyDict_SetItemString(d, "type", PyLong_FromLong((long)column->Type));
        PyDict_SetItemString(d, "length", PyLong_FromLong((long)column->Length));
        PyDict_SetItemString(d, "precision", PyLong_FromLong((long)column->Precision));
        PyDict_SetItemString(d, "scale", PyLong_FromLong((long)column->Scale));
        PyDict_SetItemString(d, "nullable", PyUnicode_FromString(column->Nullable));
        PyDict_SetItemString(d, "default", PyUnicode_FromString(column->Default));
        PyDict_SetItemString(d, "format", PyUnicode_FromString(column->Format));
        PyList_SetItem(columns, i, d);
    }
    return columns;
}

static PyObject* Cmd_fetch_one(Cmd* self, PyObject* args) {
    PyObject* row = NULL;
    StatementInfoColumn *scolumn;
    StatementInfo *s;
    GiraffeColumn *column;
    int i;
    Py_BEGIN_ALLOW_THREADS
    DBCHCL(&self->result, self->cnta, self->dbc);
    Py_END_ALLOW_THREADS
    if (self->result == REQEXHAUST) {
        self->dbc->i_sess_id = self->dbc->o_sess_id;
        self->dbc->i_req_id = self->dbc->o_req_id;
        self->dbc->func = DBFERQ;
        DBCHCL(&self->result, self->cnta, self->dbc);
        PyErr_Format(PyExc_StopIteration, "%d: %s", self->result, self->dbc->msg_text);
        return NULL;
    } else if (self->result != EM_OK) {
        PyErr_Format(GiraffeError, "%d: %s", self->status, self->dbc->msg_text);
        return NULL;
    }
    if (self->status == PCL_FAIL) {
        struct CliFailureType *ErrorFail = (struct CliFailureType *) self->dbc->fet_data_ptr;
        PyErr_Format(GiraffeError, "%d: %s", ErrorFail->Code, ErrorFail->Msg);
        return NULL;
    } else if (self->status == PCL_ERR) {
        struct CliErrorType *ErrorFail = (struct CliErrorType *) self->dbc->fet_data_ptr;
        PyErr_Format(GiraffeError, "%d: %s", ErrorFail->Code, ErrorFail->Msg);
        return NULL;
    }
    switch (self->dbc->fet_parcel_flavor) {
        case PclRECORD:
            row = PyList_New(self->columns->length);
            /*unpack_row(&self->dbc->fet_data_ptr, self->dbc->fet_ret_data_len, self->columns, row);*/
            unpack_row_x((unsigned char**)&self->dbc->fet_data_ptr, self->dbc->fet_ret_data_len, self->columns, row);
            break;
        case PclSTATEMENTINFO:
            s = (StatementInfo*)malloc(sizeof(StatementInfo));
            stmt_info_init(s, 1);
            unpack_stmt_info((unsigned char**)&self->dbc->fet_data_ptr, s, self->dbc->fet_ret_data_len);
            self->columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
            columns_init(self->columns, 1);
            for (i=0; i<s->length; i++) {
                scolumn = &s->array[i];
                column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
                column->Name = strdup(scolumn->Name);
                column->Type = scolumn->Type;
                column->Length = scolumn->Length;
                column->Precision = scolumn->Precision;
                column->Scale = scolumn->Scale;
                column->Alias = scolumn->Alias;
                column->Title = scolumn->Title;
                column->Format = scolumn->Format;
                column->Default = scolumn->Default;
                column->Nullable = scolumn->CanReturnNull;
                column->GDType = tdtype_to_gdtype(scolumn->Type);
                columns_append(self->columns, *column);
            }
            self->columns->header_length = (int)ceil(self->columns->length/8.0);
            stmt_info_free(s);
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
    if (row == NULL) {
        Py_RETURN_NONE;
    }
    return row;
}

static PyMethodDef Cmd_methods[] = {
    {"close", (PyCFunction)Cmd_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Cmd_columns, METH_NOARGS, ""},
    {"execute", (PyCFunction)Cmd_execute, METH_VARARGS, ""},
    {"fetch_one", (PyCFunction)Cmd_fetch_one, METH_VARARGS, ""},
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
