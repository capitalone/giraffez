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

#include <Python.h>
#include <sstream>

// Teradata Parallel Transporter API
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>

#include "giraffez.h"
#include "connection.h"


typedef struct {
    PyObject_HEAD
    Giraffez::Connection *conn;
} MLoad;

static void MLoad_dealloc(MLoad *self) {
    delete self->conn;
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* MLoad_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    MLoad *self;
    self = (MLoad*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int MLoad_init(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *host=NULL, *username=NULL, *password=NULL;
    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return -1;
    }
    self->conn = new Giraffez::Connection(host, username, password);
    self->conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_UPDATE);
    self->conn->AddAttribute(TD_DROPLOGTABLE, "Y");
    self->conn->AddAttribute(TD_DROPWORKTABLE, "Y");
    self->conn->AddAttribute(TD_DROPERRORTABLE, "Y");
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
    TD_Attribute key;
    PyObject *value = NULL;
    if (!PyArg_ParseTuple(args, "iO", &key, &value)) {
        return NULL;
    }
    return self->conn->AddAttribute(key, value);
}

static PyObject* MLoad_apply_rows(MLoad *self) {
    return self->conn->ApplyRows();
}

static PyObject* MLoad_checkpoint(MLoad *self) {
    return self->conn->Checkpoint();
}

static PyObject* MLoad_close(MLoad *self) {
    return self->conn->Terminate();
}

static PyObject* MLoad_columns(MLoad *self) {
    return self->conn->Columns();
}

static PyObject* MLoad_end_acquisition(MLoad *self) {
    return self->conn->EndAcquisition();
}

static PyObject* MLoad_get_event(MLoad *self, PyObject *args, PyObject *kwargs) {
    TD_EventType event_type;
    TD_Index event_index = 0;
    if (!PyArg_ParseTuple(args, "i|i", &event_type, &event_index)) {
        return NULL;
    }
    return self->conn->GetEvent(event_type, event_index);
}

static PyObject* MLoad_initiate(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *encoding = NULL;
    PyObject *null, *delimiter;
    char *tbl_name = NULL;
    PyObject *column_list = NULL;
    // TODO:
    DMLOption dml_option = (DMLOption)0;

    static const char *kwlist[] = {"tbl_name", "encoding", "null", "delimiter", "column_list",
        "dml_option", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ssOO|Oii", (char**)kwlist, &tbl_name,
            &encoding, &null, &delimiter, &column_list, &dml_option)) {
        return NULL;
    }

    uint32_t settings = 0; // zero so we have defaults for the else case below
    if (strcmp(encoding, "archive") == 0) {
        // TODO: idk
        settings = ROW_ENCODING_RAW | DATETIME_AS_STRING | DECIMAL_AS_STRING;
        if (self->conn->SetEncoding(settings) == NULL) {
            return NULL;
        }
    } else if (strcmp(encoding, "dict") == 0 || strcmp(encoding, "json") == 0) {
        settings = ENCODER_SETTINGS_JSON;
        if (self->conn->SetEncoding(settings) == NULL) {
            return NULL;
        }
    } else if (strcmp(encoding, "str") == 0) {
        settings = ENCODER_SETTINGS_STRING;
        if (self->conn->SetEncoding(settings, null, delimiter) == NULL) {
            return NULL;
        }
    } else if (strcmp(encoding, "csv") == 0) {
        // TODO: this isn't right
        settings = ENCODER_SETTINGS_STRING;
        if (self->conn->SetEncoding(settings, null, delimiter) == NULL) {
            return NULL;
        }
    } else {
        if (self->conn->SetEncoding(settings) == NULL) {
            return NULL;
        }
    }
    if (column_list != NULL && !(PyList_Check(column_list) || column_list == Py_None)) {
        PyErr_Format(GiraffezError, "Column list must be <list> type");
        return NULL;
    }
    if (self->conn->SetTable(tbl_name) == NULL) {
        return NULL;
    }
    if (self->conn->SetSchema(column_list, dml_option) == NULL) {
        return NULL;
    }
    if ((self->conn->Initiate()) == NULL) {
        DEBUG_PRINTF("%s: %d", "mload status", self->conn->status);
        switch ((TeradataStatus)self->conn->status) {
            case TD_ERROR_WORK_TABLE_MISSING:
            case TD_ERROR_TABLE_MLOAD_EXISTS:
            case TD_ERROR_TRANS_ABORTED:
                PyErr_Clear();
                if (self->conn->Release(tbl_name) == NULL) {
                    return NULL;
                }
                if ((self->conn->Initiate()) == NULL) {
                    return NULL;
                }
                break;
            default:
                return NULL;
        }
    }
    self->conn->connected = true;
    Py_RETURN_NONE;
}

static PyObject* MLoad_exists(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *tbl_name = NULL;
    if (!PyArg_ParseTuple(args, "s", &tbl_name)) {
        return NULL;
    }
    return self->conn->Exists(tbl_name);
}

static PyObject* MLoad_drop_table(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *tbl_name = NULL;
    if (!PyArg_ParseTuple(args, "s", &tbl_name)) {
        return NULL;
    }
    return self->conn->DropTable(tbl_name);
}

static PyObject* MLoad_put_row(MLoad *self, PyObject *args, PyObject *kwargs) {
    PyObject *row = NULL;
    if (!PyArg_ParseTuple(args, "O", &row)) {
        return NULL;
    }
    return self->conn->PutRow(row);
}

static PyObject* MLoad_release(MLoad *self, PyObject *args) {
    char *tbl_name = NULL;
    if (!PyArg_ParseTuple(args, "s", &tbl_name)) {
        return NULL;
    }
    if (self->conn->Release(tbl_name) == NULL) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyMethodDef MLoad_methods[] = {
    {"add_attribute", (PyCFunction)MLoad_add_attribute, METH_VARARGS, ""},
    {"apply_rows", (PyCFunction)MLoad_apply_rows, METH_NOARGS, ""},
    {"checkpoint", (PyCFunction)MLoad_checkpoint, METH_NOARGS, ""},
    {"close", (PyCFunction)MLoad_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)MLoad_columns, METH_NOARGS, ""},
    {"drop_table", (PyCFunction)MLoad_drop_table, METH_VARARGS, ""},
    {"end_acquisition", (PyCFunction)MLoad_end_acquisition, METH_NOARGS, ""},
    {"exists", (PyCFunction)MLoad_exists, METH_VARARGS, ""},
    {"get_event", (PyCFunction)MLoad_get_event, METH_VARARGS, ""},
    {"initiate", (PyCFunction)MLoad_initiate, METH_VARARGS|METH_KEYWORDS, "" },
    {"put_row", (PyCFunction)MLoad_put_row, METH_VARARGS, ""},
    {"release", (PyCFunction)MLoad_release, METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject MLoadType = {
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
