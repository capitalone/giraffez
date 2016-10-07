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

#include "loadobject.h"
#include "compat.h"
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>


static PyObject* Load_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    char* host=NULL, *username=NULL, *password=NULL;
    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return NULL;
    }
    Load* self;
    self = (Load*)type->tp_alloc(type, 0);
    self->error_msg = strdup(string("").c_str());
    self->connection_status = 0;
    self->connected = false;
    self->table_set = false;
    self->table_schema = new Schema(strdup(string("input").c_str()));
    self->conn = new Connection();
    self->conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_UPDATE);
    self->conn->AddAttribute(TD_DROPLOGTABLE, strdup(string("Y").c_str()));
    self->conn->AddAttribute(TD_DROPWORKTABLE, strdup(string("Y").c_str()));
    self->conn->AddAttribute(TD_DROPERRORTABLE, strdup(string("Y").c_str()));
    self->conn->AddAttribute(TD_USER_NAME, username);
    self->conn->AddAttribute(TD_USER_PASSWORD, password);
    self->conn->AddAttribute(TD_TDP_ID, host);
    self->conn->AddAttribute(TD_TENACITY_HOURS, 1);
    self->conn->AddAttribute(TD_TENACITY_SLEEP, 1);
    self->conn->AddAttribute(TD_ERROR_LIMIT, 1);
    self->conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);
    self->conn->AddAttribute(TD_MAX_SESSIONS, 20);
    return (PyObject*)self;
}

static int Load_init(Load* self, PyObject* args, PyObject* kwds) {
    return 0;
}

static PyObject* Load_add_attribute(Load* self, PyObject* args, PyObject* kwds) {
    int key = -1;
    PyObject* value = NULL;

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

static PyObject* Load_apply_rows(Load* self) {
    self->conn->ApplyRows();
    Py_RETURN_NONE;
}

static PyObject* Load_checkpoint(Load* self) {
    int result;
    char* data;
    TD_Length length = 0;
    result = self->conn->Checkpoint(&data, &length);
    return Py_BuildValue("i", result);
}

static PyObject* Load_close(Load* self) {
    if (self->connected) {
        self->connection_status = self->conn->Terminate();
        delete self->conn;
        delete self->table_schema;
    }
    self->connected = false;
    Py_RETURN_NONE;
}

static void Load_dealloc(Load* self) {
    Load_close(self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Load_end_acquisition(Load* self) {
    self->conn->EndAcquisition();
    Py_RETURN_NONE;
}

static PyObject* Load_error_message(Load* self) {
    if (self->error_msg == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(self->error_msg);
}

static PyObject* Load_get_error_info(Load* self) {
    self->conn->GetErrorInfo(&self->error_msg, &self->error_type);
    Py_RETURN_NONE;
}

static PyObject* Load_get_event(Load* self, PyObject* args, PyObject* kwds) {
    TD_EventType event_type;
    TD_Index event_index = 0;
    if (!PyArg_ParseTuple(args, "i|i", &event_type, &event_index)) {
        return NULL;
    }
    char* data = NULL;
    TD_Length length = 0;
    int status = self->conn->GetEvent(event_type, &data, &length, event_index);
    if (status == TD_Error || status == TD_Unavailable) {
        Py_RETURN_NONE;
    }
    return PyBytes_FromStringAndSize(data, length);
}

static PyObject* Load_initiate(Load* self, PyObject* args, PyObject* kwds) {
    PyObject* tuple_list;
    char* dml = NULL;
    int dml_option;

    if (!PyArg_ParseTuple(args, "Osi", &tuple_list, &dml, &dml_option)) {
        return NULL;
    }
    Py_ssize_t len = PyList_Size(tuple_list);
    for (Py_ssize_t i = 0; i < len; i++) {
        char* name = NULL;
        int data_type, size, precision, scale;
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
    TD_Index dmlGroupIndex;
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

static PyObject* Load_put_buffer(Load* self, PyObject* args, PyObject* kwds) {
    char* buffer;
    int length;
    if (!PyArg_ParseTuple(args, "s#", &buffer, &length)) {
        return NULL;
    }
    self->row_status = self->conn->PutBuffer(buffer, length, 1);
    return Py_BuildValue("i", self->row_status);
}

static PyObject* Load_set_table(Load* self, PyObject* args, PyObject* kwds) {
    char* _table_name = NULL;

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

static PyObject* Load_status(Load* self, void* closure) {
    PyObject* status = NULL;
    status = Py_BuildValue("i", self->connection_status);
    Py_INCREF(status);
    return status;
}

static PyGetSetDef Load_getset[] = {
    {(char*)"status", (getter)Load_status, NULL, (char*)"connection status", NULL},
    {NULL}
};

static PyMethodDef Load_methods[] = {
    {"add_attribute", (PyCFunction)Load_add_attribute, METH_VARARGS, ""},
    {"apply_rows", (PyCFunction)Load_apply_rows, METH_NOARGS, ""},
    {"checkpoint", (PyCFunction)Load_checkpoint, METH_NOARGS, ""},
    {"close", (PyCFunction)Load_close, METH_NOARGS, ""},
    {"end_acquisition", (PyCFunction)Load_end_acquisition, METH_NOARGS, ""},
    {"error_message", (PyCFunction)Load_error_message, METH_NOARGS, ""},
    {"get_error_info", (PyCFunction)Load_get_error_info, METH_NOARGS, ""},
    {"get_event", (PyCFunction)Load_get_event, METH_VARARGS, ""},
    {"initiate", (PyCFunction)Load_initiate, METH_VARARGS, "" },
    {"put_buffer", (PyCFunction)Load_put_buffer, METH_VARARGS, ""},
    {"set_table", (PyCFunction)Load_set_table, METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject LoadType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_tpt.Load",                                    /* tp_name */
    sizeof(Load),                                   /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Load_dealloc,                       /* tp_dealloc */
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
    "Load objects",                                 /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Load_methods,                                   /* tp_methods */
    0,                                              /* tp_members */
    Load_getset,                                    /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)Load_init,                            /* tp_init */
    0,                                              /* tp_alloc */
    Load_new,                                       /* tp_new */
};
