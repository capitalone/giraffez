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

#include "exportobject.h"
#include <connection.h>
#include <schema.h>
#include "_compat.h"
#include "encoder/types.h"
#include "encoder/unpack.h"

static void Export_dealloc(Export* self) {
    if (self->connected) {
        self->connection_status = self->conn->Terminate();
        delete self->conn;
    }
    self->connected = false;
    if (self->columns != NULL) {
        columns_free(self->columns);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Export_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    char *host=NULL, *username=NULL, *password=NULL;

    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return NULL;
    }
    Export* self;
    self = (Export*)type->tp_alloc(type, 0);
    self->error_msg = NULL;
    self->dynamic_schema = NULL;
    self->connection_status = 0;
    self->conn = new Connection();
    self->conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_EXPORT);
    self->conn->AddAttribute(TD_TDP_ID, host);
    self->conn->AddAttribute(TD_USER_NAME, username);
    self->conn->AddAttribute(TD_USER_PASSWORD, password);
    self->conn->AddAttribute(TD_MIN_SESSIONS, 5);
    self->conn->AddAttribute(TD_MAX_SESSIONS, 32);
    self->conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);
    self->conn->AddAttribute(TD_CHARSET, strdup(string("UTF8").c_str()));
    self->conn->AddAttribute(TD_BUFFER_MODE, strdup(string("YES").c_str()));
    self->conn->AddAttribute(TD_BLOCK_SIZE, 64330);
    self->conn->AddAttribute(TD_BUFFER_HEADER_SIZE, 2);
    self->conn->AddAttribute(TD_BUFFER_LENGTH_SIZE, 2);
    self->conn->AddAttribute(TD_BUFFER_MAX_SIZE, 64260);
    self->conn->AddAttribute(TD_BUFFER_TRAILER_SIZE, 0);
    self->conn->AddAttribute(TD_SPOOLMODE, strdup(string("NoSpool").c_str()));
    self->conn->AddAttribute(TD_TENACITY_HOURS, 1);
    self->conn->AddAttribute(TD_TENACITY_SLEEP, 1);
    return (PyObject*)self;
}

static int Export_init(Export* self, PyObject* args, PyObject* kwds) {
    return 0;
}

static PyObject* Export_add_attribute(Export* self, PyObject* args, PyObject* kwds) {
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

static PyObject* Export_close(Export* self, PyObject* args, PyObject * kwds) {
    if (self->connected) {
        self->connection_status = self->conn->Terminate();
        free(self->conn);
    }
    self->connected = false;
    Py_RETURN_NONE;
}

static PyObject* Export_columns(Export* self, PyObject* args, PyObject* kwds) {
    PyObject* column_list = PyList_New(0);
    unsigned int count = self->dynamic_schema->GetColumnCount();
    for (std::vector<Column*>::size_type i=0; i < count; i++) {
        Column* c = self->dynamic_schema->GetColumn(i);
        PyObject* column_tuple = Py_BuildValue("(siiiiib)",
                c->GetName(), c->GetDataType(), c->GetSize(),
                c->GetPrecision(), c->GetScale(), c->GetOffset(),
                true);
        PyList_Append(column_list, column_tuple);
        Py_DECREF(column_tuple);
    }
    return column_list;
}

static PyObject* Export_error_message(Export* self, PyObject* args, PyObject * kwds) {
    if (self->error_msg == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(self->error_msg);
}

static PyObject* Export_get_buffer_dict(Export* self, PyObject* args, PyObject* kwds) {
    unsigned char* data = NULL;
    int length;
    int export_status;
    export_status = self->conn->GetBuffer((char**)&data, (TD_Length*)&length);
    if (export_status == TD_END_Method) {
        Py_RETURN_NONE;
    }
    PyObject* rows = PyList_New(0);
    unpack_rows_dict(&data, length, self->columns, rows);
    return rows;
}

static PyObject* Export_get_buffer_raw(Export* self, PyObject* args, PyObject* kwds) {
    unsigned char* data = NULL;
    int length;
    int export_status;
    export_status = self->conn->GetBuffer((char**)&data, (TD_Length*)&length);
    if (export_status == TD_END_Method) {
        Py_RETURN_NONE;
    }
    PyObject* v = PyTuple_New(1);
    PyTuple_SetItem(v, 0, PyBytes_FromStringAndSize((char*)data, length));
    return v;
}

static PyObject* Export_get_buffer_str(Export* self, PyObject* args, PyObject* kwds) {
    unsigned char* data = NULL;
    int length;
    int export_status;
    char* null;
    char* delimiter;
    if (!PyArg_ParseTuple(args, "ss", &null, &delimiter)) {
        return NULL;
    }
    export_status = self->conn->GetBuffer((char**)&data, (TD_Length*)&length);
    if (export_status == TD_END_Method) {
        Py_RETURN_NONE;
    }
    PyObject* rows = PyList_New(0);
    unpack_rows_str(&data, length, self->columns, rows, null, delimiter);
    return rows;
}

static PyObject* Export_get_schema(Export* self, PyObject* args, PyObject* kwds) {
    int schema_status = self->conn->GetSchema(&self->dynamic_schema);
    if (!(self->connection_status < TD_Error) || schema_status != 0) {
        self->conn->GetErrorInfo(&self->error_msg, &self->error_type);
    }
    Py_RETURN_NONE;
}


static PyObject* Export_initiate(Export* self, PyObject* args, PyObject* kwds) {
    self->connection_status = self->conn->Initiate();
    if (self->connection_status >= TD_Error) {
        self->conn->GetErrorInfo(&self->error_msg, &self->error_type);
    } else {
        self->connected = true;
    }
    Py_RETURN_NONE;
}

static PyObject* Export_set_columns(Export* self, PyObject* args, PyObject* kwds) {
    if (self->columns == NULL) {
        self->columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
        self->columns->array = NULL;
    }
    PyObject* columns_obj;
    if (!PyArg_ParseTuple(args, "O", &columns_obj)) {
        return NULL;
    }
    columns_init(self->columns, 1);
    PyObject* iterator = PyObject_GetIter(columns_obj);
    PyObject* column_obj;
    if (iterator == NULL) {
        PyErr_SetString(PyExc_ValueError, "No columns found.");
        return NULL;
    }
    while ((column_obj = PyIter_Next(iterator))) {
        PyObject* column_name = PyObject_GetAttrString(column_obj, "name");
        PyObject* column_type = PyObject_GetAttrString(column_obj, "type");
        PyObject* column_length = PyObject_GetAttrString(column_obj, "length");
        PyObject* column_precision = PyObject_GetAttrString(column_obj, "precision");
        PyObject* column_scale = PyObject_GetAttrString(column_obj, "scale");
        PyObject* gd_type = PyObject_GetAttrString(column_obj, "gd_type");
        GiraffeColumn* column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
        column->Name = strdup(PyUnicode_AsUTF8(column_name));
        column->Type = (uint16_t)PyLong_AsLong(column_type);
        column->Length = (uint64_t)PyLong_AsLong(column_length);
        column->Precision = (uint16_t)PyLong_AsLong(column_precision);
        column->Scale = (uint16_t)PyLong_AsLong(column_scale);
        column->GDType = (uint16_t)PyLong_AsLong(gd_type);
        Py_DECREF(column_name);
        Py_DECREF(column_type);
        Py_DECREF(column_length);
        Py_DECREF(column_precision);
        Py_DECREF(column_scale);
        Py_DECREF(gd_type);
        columns_append(self->columns, *column);
    }
    self->columns->header_length = (int)ceil(self->columns->length/8.0);
    Py_DECREF(iterator);
    Py_RETURN_NONE;
}

static PyObject* Export_status(Export* self) {
    PyObject* status = NULL;
    status = Py_BuildValue("i", self->connection_status);
    Py_INCREF(status);
    return status;
}

static PyMethodDef Export_methods[] = {
    {"add_attribute", (PyCFunction)Export_add_attribute, METH_VARARGS, ""},
    {"close", (PyCFunction)Export_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Export_columns, METH_NOARGS, ""},
    {"error_message", (PyCFunction)Export_error_message, METH_NOARGS, ""},
    {"get_buffer_dict", (PyCFunction)Export_get_buffer_dict, METH_NOARGS, ""},
    {"get_buffer_raw", (PyCFunction)Export_get_buffer_raw, METH_NOARGS, ""},
    {"get_buffer_str", (PyCFunction)Export_get_buffer_str, METH_VARARGS, ""},
    {"get_schema", (PyCFunction)Export_get_schema, METH_NOARGS, ""},
    {"initiate", (PyCFunction)Export_initiate, METH_NOARGS, ""},
    {"set_columns", (PyCFunction)Export_set_columns, METH_VARARGS, ""},
    {"status", (PyCFunction)Export_status, METH_NOARGS, ""},
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
