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

#include "mloadobject.h"

// Teradata Parallel Transporter API
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>

// Python 2/3 C API and Windows compatibility
#include "_compat.h"


// TODO: switch to init
static PyObject* MLoad_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    MLoad *self;
    self = (MLoad*)type->tp_alloc(type, 0);
    self->error_msg = strdup(string("").c_str());
    self->connection_status = 0;
    self->connected = false;
    self->table_set = false;
    return (PyObject*)self;
}

static int MLoad_init(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *host=NULL, *username=NULL, *password=NULL;
    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return -1;
    }
    self->table_schema = new Schema(strdup(string("input").c_str()));
    self->conn = new Connection();
    self->conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_UPDATE);
    self->conn->AddAttribute(TD_DROPLOGTABLE, strdup(string("Y").c_str()));
    self->conn->AddAttribute(TD_DROPWORKTABLE, strdup(string("Y").c_str()));
    self->conn->AddAttribute(TD_DROPERRORTABLE, strdup(string("Y").c_str()));
    self->conn->AddAttribute(TD_TDP_ID, host);
    self->conn->AddAttribute(TD_USER_NAME, username);
    self->conn->AddAttribute(TD_USER_PASSWORD, password);
    self->conn->AddAttribute(TD_MAX_SESSIONS, 20);
    self->conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);
    self->conn->AddAttribute(TD_TENACITY_HOURS, 1);
    self->conn->AddAttribute(TD_TENACITY_SLEEP, 1);
    self->conn->AddAttribute(TD_ERROR_LIMIT, 1);
    return 0;
}

static PyObject* MLoad_add_attribute(MLoad *self, PyObject *args, PyObject *kwargs) {
    int key = -1;
    PyObject *value = NULL;
    if (!PyArg_ParseTuple(args, "iO", &key, &value)) {
        return NULL;
    }
    if PyLong_Check(value) {
        self->conn->AddAttribute((TD_Attribute)key, (int)PyLong_AsLong(value));
    } else {
        self->conn->AddAttribute((TD_Attribute)key, PyUnicode_AsUTF8(value));
    }
    Py_RETURN_NONE;
}

static PyObject* MLoad_apply_rows(MLoad *self) {
    self->conn->ApplyRows();
    Py_RETURN_NONE;
}

static PyObject* MLoad_checkpoint(MLoad *self) {
    int r;
    char *data;
    TD_Length length = 0;
    PyObject *result = NULL;
    r = self->conn->Checkpoint(&data, &length);
    result = Py_BuildValue("i", r);
    Py_INCREF(result);
    return result;
}

static PyObject* MLoad_close(MLoad *self) {
    if (self->connected) {
        self->connection_status = self->conn->Terminate();
    }
    self->connected = false;
    Py_RETURN_NONE;
}

// TODO: ensure this doesn't cause segfault
static void MLoad_dealloc(MLoad *self) {
    delete self->conn;
    delete self->table_schema;
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* MLoad_end_acquisition(MLoad *self) {
    self->conn->EndAcquisition();
    Py_RETURN_NONE;
}

static PyObject* MLoad_error_message(MLoad *self) {
    if (self->error_msg == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(self->error_msg);
}

static PyObject* MLoad_get_error_info(MLoad *self) {
    self->conn->GetErrorInfo(&self->error_msg, &self->error_type);
    Py_RETURN_NONE;
}

static PyObject* MLoad_get_event(MLoad *self, PyObject *args, PyObject *kwargs) {
    TD_EventType event_type;
    TD_Index event_index = 0;
    char *data = NULL;
    TD_Length length = 0;
    int status;
    if (!PyArg_ParseTuple(args, "i|i", &event_type, &event_index)) {
        return NULL;
    }
    status = self->conn->GetEvent(event_type, &data, &length, event_index);
    if (status == TD_Error || status == TD_Unavailable) {
        Py_RETURN_NONE;
    }
    return PyBytes_FromStringAndSize(data, length);
}

static PyObject* MLoad_initiate(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *dml = NULL;
    TD_Index dmlGroupIndex;
    int dml_option, data_type, size, precision, scale;
    Py_ssize_t len, i;
    PyObject *tuple_list;
    char *name = NULL;
    if (!PyArg_ParseTuple(args, "Osi", &tuple_list, &dml, &dml_option)) {
        return NULL;
    }
    len = PyList_Size(tuple_list);
    for (i = 0; i < len; i++) {
        if (!PyArg_ParseTuple(PyList_GetItem(tuple_list, i), "siiii",
                    &name, &data_type, &size, &precision, &scale)) {
            Py_RETURN_NONE;
        }
        if (self->table_set) {
            self->table_schema->AddColumn(name, (TD_DataType)data_type, size, precision, scale);
        }
    }
    if (self->table_set) {
        self->conn->AddSchema(self->table_schema);
    }
    if (self->table_set) {
        DMLGroup* dml_group = new DMLGroup();
        dml_group->AddStatement(dml);
        dml_group->AddDMLOption((DMLOption) dml_option);
        self->conn->AddDMLGroup(dml_group, &dmlGroupIndex);
        delete dml_group;
    }
    Py_DECREF(tuple_list);
    if (self->table_set) {
        self->connection_status = self->conn->Initiate();
        if (self->connection_status >= TD_Error) {
            self->conn->GetErrorInfo(&self->error_msg, &self->error_type);
        } else {
            self->connected = true;
        }
    }
    return Py_BuildValue("i", self->connection_status);
}

// TODO: is archive using buffer? it should
static PyObject* MLoad_put_buffer(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *buffer;
    int length;
    if (!PyArg_ParseTuple(args, "s#", &buffer, &length)) {
        return NULL;
    }
    self->row_status = self->conn->PutBuffer(buffer, length, 1);
    return Py_BuildValue("i", self->row_status);
}

static PyObject* MLoad_put_row(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *row;
    int length;
    if (!PyArg_ParseTuple(args, "s#", &row, &length)) {
        return NULL;
    }
    self->row_status = self->conn->PutRow(row, length);
    return Py_BuildValue("i", self->row_status);
}

static PyObject* MLoad_set_table(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *_table_name = NULL;
    if (!PyArg_ParseTuple(args, "s", &_table_name)) {
        return NULL;
    }
    std::string table_name = std::string(_table_name);
    self->conn->AddAttribute(TD_TARGET_TABLE, strdup(table_name.c_str()));
    std::string log_table = table_name + "_log";
    self->conn->AddAttribute(TD_LOG_TABLE, strdup(log_table.c_str()));
    std::string work_table = table_name + "_wt";
    self->conn->AddArrayAttribute(TD_WORK_TABLE, 1, strdup(work_table.c_str()), NULL);
    std::string e1_table = table_name + "_e1";
    std::string e2_table = table_name + "_e2";
    self->conn->AddArrayAttribute(TD_ERROR_TABLE_1, 1, strdup(e1_table.c_str()), NULL);
    self->conn->AddArrayAttribute(TD_ERROR_TABLE_2, 1, strdup(e2_table.c_str()), NULL);
    self->table_set = true;
    Py_RETURN_NONE;
}

static PyObject* MLoad_status(MLoad *self) {
    return Py_BuildValue("i", self->connection_status);
}

static PyMethodDef MLoad_methods[] = {
    {"add_attribute", (PyCFunction)MLoad_add_attribute, METH_VARARGS, ""},
    {"apply_rows", (PyCFunction)MLoad_apply_rows, METH_NOARGS, ""},
    {"checkpoint", (PyCFunction)MLoad_checkpoint, METH_NOARGS, ""},
    {"close", (PyCFunction)MLoad_close, METH_NOARGS, ""},
    {"end_acquisition", (PyCFunction)MLoad_end_acquisition, METH_NOARGS, ""},
    {"error_message", (PyCFunction)MLoad_error_message, METH_NOARGS, ""},
    {"get_error_info", (PyCFunction)MLoad_get_error_info, METH_NOARGS, ""},
    {"get_event", (PyCFunction)MLoad_get_event, METH_VARARGS, ""},
    {"initiate", (PyCFunction)MLoad_initiate, METH_VARARGS, "" },
    {"put_buffer", (PyCFunction)MLoad_put_buffer, METH_VARARGS, ""},
    {"put_row", (PyCFunction)MLoad_put_row, METH_VARARGS, ""},
    {"set_table", (PyCFunction)MLoad_set_table, METH_VARARGS, ""},
    {"status", (PyCFunction)MLoad_status, METH_NOARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject LoadType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_tpt.MLoad",                                   /* tp_name */
    sizeof(MLoad),                                  /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)MLoad_dealloc,                      /* tp_dealloc */
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
    "MLoad objects",                                /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    MLoad_methods,                                  /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)MLoad_init,                           /* tp_init */
    0,                                              /* tp_alloc */
    MLoad_new,                                      /* tp_new */
};
