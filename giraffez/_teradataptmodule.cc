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

// Teradata Parallel Transporter API
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>

#include "src/teradatapt.hpp"

#ifdef __cplusplus
extern "C" {
#endif 

typedef struct {
    PyObject_HEAD
    Giraffez::Connection *conn;
} Export;

static void Export_dealloc(Export *self) {
    delete self->conn;
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Export_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    Export *self;
    self = (Export*)type->tp_alloc(type, 0);
    return (PyObject*)self;
}

static int Export_init(Export *self, PyObject *args, PyObject *kwargs) {
    char *host=NULL, *username=NULL, *password=NULL;
    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return -1;
    }

    self->conn = new Giraffez::Connection(host, username, password);
    self->conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_EXPORT);
    self->conn->AddAttribute(TD_TDP_ID, host);
    self->conn->AddAttribute(TD_USER_NAME, username);
    self->conn->AddAttribute(TD_USER_PASSWORD, password);

    // The min and max for sessions has been hard set to reasonable
    // values that *should* be one-size fits-all.
    //self->conn->AddAttribute(TD_MIN_SESSIONS, 5);
    //self->conn->AddAttribute(TD_MAX_SESSIONS, 32);
    self->conn->AddAttribute(TD_MIN_SESSIONS, 2);
    self->conn->AddAttribute(TD_MAX_SESSIONS, 5);
    self->conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);

    // Charset is set to prefer UTF8.  There may need to be changes to
    // the encoder if UTF8 is for whatever reason not supported, and
    // may cause unexpected behavior.
    self->conn->AddAttribute(TD_CHARSET, TERADATA_CHARSET);
    self->conn->AddAttribute(TD_BUFFER_MODE, "YES");
    self->conn->AddAttribute(TD_BLOCK_SIZE, 64330);
    self->conn->AddAttribute(TD_BUFFER_HEADER_SIZE, 2);
    self->conn->AddAttribute(TD_BUFFER_LENGTH_SIZE, 2);
    self->conn->AddAttribute(TD_BUFFER_MAX_SIZE, TD_ROW_MAX_SIZE);
    self->conn->AddAttribute(TD_BUFFER_TRAILER_SIZE, 0);

    // NoSpool sets the preferred spoolmode to attempt pulling the data
    // directly without spooling into temporary space.  In the event
    // that can't happen the job is still allowed but performs the
    // spooling of the query results as needed.
    self->conn->AddAttribute(TD_SPOOLMODE, "NoSpool");

    // Tenacity hours is set to the lowest allowed value.  In cases like
    // unexpected client disconnects without being shutdown properly,
    // the connection will, at a minimum, get discarded by the server
    // in 1 hour.  This should hopefully help prevent scenarios where
    // lots of dead connections are sitting around on the server
    // because of a malfunctioning client.
    self->conn->AddAttribute(TD_TENACITY_HOURS, 1);

    // Tenacity sleep is set to the lowest allowed value.  This ensures
    // that the connection will retry every second should the export
    // job get queued.
    self->conn->AddAttribute(TD_TENACITY_SLEEP, 1);
    return 0;
}

static PyObject* Export_add_attribute(Export *self, PyObject *args, PyObject *kwargs) {
    TD_Attribute key;
    PyObject *value = NULL;
    if (!PyArg_ParseTuple(args, "iO", &key, &value)) {
        return NULL;
    }
    return self->conn->AddAttribute(key, value);
}

static PyObject* Export_close(Export *self) {
    return self->conn->Terminate();
}

static PyObject* Export_columns(Export *self) {
    return self->conn->Columns();
}

static PyObject* Export_get_buffer(Export *self) {
    return self->conn->GetBuffer();
}

static PyObject* Export_get_event(Export *self, PyObject *args, PyObject *kwargs) {
    TD_EventType event_type;
    TD_Index event_index = 0;
    if (!PyArg_ParseTuple(args, "i|i", &event_type, &event_index)) {
        return NULL;
    }
    return self->conn->GetEvent(event_type, event_index);
}

static PyObject* Export_set_encoding(Export *self, PyObject *args) {
    uint32_t new_settings = 0;
    uint32_t settings;
    if (!PyArg_ParseTuple(args, "i", &settings)) {
        return NULL;
    }
    new_settings = self->conn->encoder->Settings;
    if (settings & ROW_RETURN_MASK) {
        new_settings = (new_settings & ~ROW_RETURN_MASK) | settings;
    }
    if (settings & DATETIME_RETURN_MASK) {
        new_settings = (new_settings & ~DATETIME_RETURN_MASK) | settings;
    }
    if (settings & DECIMAL_RETURN_MASK) {
        new_settings = (new_settings & ~DECIMAL_RETURN_MASK) | settings;
    }
    if (encoder_set_encoding(self->conn->encoder, new_settings) != 0) {
        PyErr_Format(PyExc_ValueError, "Encoder set_encoding failed, bad encoding '0x%06x'.", settings);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* Export_set_null(Export *self, PyObject *args) {
    PyObject *null = NULL;
    if (!PyArg_ParseTuple(args, "O", &null)) {
        return NULL;
    }
    Py_RETURN_ERROR(encoder_set_null(self->conn->encoder, null));
    Py_RETURN_NONE;
}

static PyObject* Export_set_delimiter(Export *self, PyObject *args) {
    PyObject *delimiter = NULL;
    if (!PyArg_ParseTuple(args, "O", &delimiter)) {
        return NULL;
    }
    Py_RETURN_ERROR(encoder_set_delimiter(self->conn->encoder, delimiter));
    Py_RETURN_NONE;
}

// TODO: ensure that multiple export jobs can run consecutively within
// the same context
static PyObject* Export_initiate(Export *self) {
    if ((self->conn->Initiate()) == NULL) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* Export_set_query(Export *self, PyObject *args, PyObject *kwargs) {
    char *query;
    if (!PyArg_ParseTuple(args, "s", &query)) {
        return NULL;
    }
    return self->conn->SetQuery(query);
}

static PyMethodDef Export_methods[] = {
    {"add_attribute", (PyCFunction)Export_add_attribute, METH_VARARGS, ""},
    {"close", (PyCFunction)Export_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Export_columns, METH_NOARGS, ""},
    {"get_buffer", (PyCFunction)Export_get_buffer, METH_NOARGS, ""},
    {"get_event", (PyCFunction)Export_get_event, METH_VARARGS, ""},
    {"initiate", (PyCFunction)Export_initiate, METH_NOARGS, ""},
    {"set_encoding", (PyCFunction)Export_set_encoding, METH_VARARGS, ""},
    {"set_null", (PyCFunction)Export_set_null, METH_VARARGS, ""},
    {"set_delimiter", (PyCFunction)Export_set_delimiter, METH_VARARGS, ""},
    {"set_query", (PyCFunction)Export_set_query, METH_VARARGS, ""},
    {NULL}  /* Sentinel */
};

PyTypeObject ExportType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_tpt.Export",                                  /* tp_name */
    sizeof(Export),                                 /* tp_basicsize */
    0,                                              /* tp_itemsize */
    (destructor)Export_dealloc,                     /* tp_dealloc */
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
    "Export objects",                               /* tp_doc */
    0,                                              /* tp_traverse */
    0,                                              /* tp_clear */
    0,                                              /* tp_richcompare */
    0,                                              /* tp_weaklistoffset */
    0,                                              /* tp_iter */
    0,                                              /* tp_iternext */
    Export_methods,                                 /* tp_methods */
    0,                                              /* tp_members */
    0,                                              /* tp_getset */
    0,                                              /* tp_base */
    0,                                              /* tp_dict */
    0,                                              /* tp_descr_get */
    0,                                              /* tp_descr_set */
    0,                                              /* tp_dictoffset */
    (initproc)Export_init,                          /* tp_init */
    0,                                              /* tp_alloc */
    Export_new,                                     /* tp_new */
};

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
    //self->conn->AddAttribute(TD_ERROR_LIMIT, 5);
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

static PyObject* MLoad_set_encoding(MLoad *self, PyObject *args, PyObject *kwargs) {
    uint32_t new_settings = 0;
    uint32_t settings;
    if (!PyArg_ParseTuple(args, "i", &settings)) {
        return NULL;
    }
    if (settings & ROW_RETURN_MASK) {
        new_settings = (self->conn->encoder->Settings & ~ROW_RETURN_MASK) | settings;
    }
    if (settings & DATETIME_RETURN_MASK) {
        new_settings = (self->conn->encoder->Settings & ~DATETIME_RETURN_MASK) | settings;
    }
    if (settings & DECIMAL_RETURN_MASK) {
        new_settings = (self->conn->encoder->Settings & ~DECIMAL_RETURN_MASK) | settings;
    }
    if (encoder_set_encoding(self->conn->encoder, new_settings) != 0) {
        PyErr_Format(PyExc_ValueError, "Encoder set_encoding failed, bad encoding '0x%06x'.", settings);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* MLoad_set_null(MLoad *self, PyObject *args) {
    PyObject *null = NULL;
    if (!PyArg_ParseTuple(args, "O", &null)) {
        return NULL;
    }
    Py_RETURN_ERROR(encoder_set_null(self->conn->encoder, null));
    Py_RETURN_NONE;
}

static PyObject* MLoad_set_delimiter(MLoad *self, PyObject *args) {
    PyObject *delimiter = NULL;
    if (!PyArg_ParseTuple(args, "O", &delimiter)) {
        return NULL;
    }
    Py_RETURN_ERROR(encoder_set_delimiter(self->conn->encoder, delimiter));
    Py_RETURN_NONE;
}

static PyObject* MLoad_initiate(MLoad *self, PyObject *args, PyObject *kwargs) {
    char *tbl_name = NULL;
    PyObject *column_list = NULL;
    DMLOption dml_option = MARK_DUPLICATE_ROWS;

    static const char *kwlist[] = {"tbl_name", "column_list", "dml_option", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|Oi", (char**)kwlist, &tbl_name, &column_list, &dml_option)) {
        return NULL;
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
                    DEBUG_PRINTF("%s: %d", "Release mload status", self->conn->status);
                    return NULL;
                }
                if ((self->conn->Initiate()) == NULL) {
                    DEBUG_PRINTF("%s: %d", "Initiate mload status", self->conn->status);
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
    {"set_encoding", (PyCFunction)MLoad_set_encoding, METH_VARARGS, ""},
    {"set_delimiter", (PyCFunction)MLoad_set_delimiter, METH_VARARGS, ""},
    {"set_null", (PyCFunction)MLoad_set_null, METH_VARARGS, ""},
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

static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinel */
};

MOD_INIT(_teradatapt)
{
    PyObject* m;

    if (PyType_Ready(&ExportType) < 0) {
        return MOD_ERROR_VAL;
    }

    if (PyType_Ready(&MLoadType) < 0) {
        return MOD_ERROR_VAL;
    }

    MOD_DEF(m, "_teradatapt", "", module_methods);

    giraffez_types_import();

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    define_exceptions(m);

    Py_INCREF(&ExportType);
    PyModule_AddObject(m, "Export", (PyObject*)&ExportType);

    Py_INCREF(&MLoadType);
    PyModule_AddObject(m, "MLoad", (PyObject*)&MLoadType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
