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

// Teradata Parallel Transporter API
#include <connection.h>
#include <schema.h>

// Python 2/3 C API and Windows compatibility
#include "_compat.h"

#include "errors.h"
#include "pytypes.h"
#include "types.h"
#include "unpack.h"


static EncoderSettings settings = {
    ROW_ENCODING_DICT,
    ITEM_ENCODING_BUILTIN_TYPES,
    DECIMAL_AS_STRING
};

static void Export_dealloc(Export *self) {
    if (self->connected) {
        self->status = self->conn->Terminate();
        delete self->conn;
    }
    self->connected = false;
    if (self->encoder != NULL) {
        encoder_free(self->encoder);
        self->encoder = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Export_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    Export *self;
    self = (Export*)type->tp_alloc(type, 0);
    self->connected = false;
    self->status = 0;
    self->encoder = NULL;
    return (PyObject*)self;
}

static int Export_init(Export *self, PyObject *args, PyObject *kwargs) {
    char *host=NULL, *username=NULL, *password=NULL;
    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return -1;
    }
    self->encoder = encoder_new(NULL, &settings);
    self->conn = new Connection();
    self->conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_EXPORT);
    self->conn->AddAttribute(TD_TDP_ID, host);
    self->conn->AddAttribute(TD_USER_NAME, username);
    self->conn->AddAttribute(TD_USER_PASSWORD, password);

    // The min and max for sessions has been hard set to reasonable
    // values that *should* be one-size fits-all.
    self->conn->AddAttribute(TD_MIN_SESSIONS, 5);
    self->conn->AddAttribute(TD_MAX_SESSIONS, 32);
    self->conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);

    // Charset is set to prefer UTF8.  There may need to be changes to
    // the encoder if UTF8 is for whatever reason not supported, and
    // may cause unexpected behavior.
    self->conn->AddAttribute(TD_CHARSET, strdup(string("UTF8").c_str()));
    self->conn->AddAttribute(TD_BUFFER_MODE, strdup(string("YES").c_str()));
    self->conn->AddAttribute(TD_BLOCK_SIZE, 64330);
    self->conn->AddAttribute(TD_BUFFER_HEADER_SIZE, 2);
    self->conn->AddAttribute(TD_BUFFER_LENGTH_SIZE, 2);
    self->conn->AddAttribute(TD_BUFFER_MAX_SIZE, 64260);
    self->conn->AddAttribute(TD_BUFFER_TRAILER_SIZE, 0);

    // NoSpool sets the preferred spoolmode to attempt pulling the data
    // directly without spooling into temporary space.  In the event
    // that can't happen the job is still allowed but performs the
    // spooling of the query results as needed.
    self->conn->AddAttribute(TD_SPOOLMODE, strdup(string("NoSpool").c_str()));

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

static PyObject* Export_close(Export *self) {
    if (self->connected) {
        self->status = self->conn->Terminate();
        free(self->conn);
    }
    self->connected = false;
    Py_RETURN_NONE;
}

static PyObject* Export_columns(Export *self) {
    if (self->encoder == NULL || self->encoder->Columns == NULL) {
        Py_RETURN_NONE;
    }
    return columns_to_pylist(self->encoder->Columns);
}

static PyObject* Export_get_buffer(Export *self) {
    unsigned char *data = NULL;
    int length;
    PyObject *result = NULL;
    if ((self->status = self->conn->GetBuffer((char**)&data, (TD_Length*)&length)) == TD_END_Method) {
        Py_RETURN_NONE;
    }
    result = self->encoder->UnpackRowsFunc(self->encoder, &data, length);
    return result;
}

// TODO: ensure that multiple export jobs can run consecutively within
// the same context
static PyObject* Export_initiate(Export *self, PyObject *args, PyObject *kwargs) {
    char *error_msg;
    TD_ErrorType error_type;
    Schema* dynamic_schema;
    size_t count;
    GiraffeColumn *column;
    GiraffeColumns *columns;
    char *encoding;
    PyObject *null, *delimiter;
    if (!PyArg_ParseTuple(args, "sOO", &encoding, &null, &delimiter)) {
        return NULL;
    }
    if ((self->status = self->conn->Initiate()) >= TD_Error) {
        self->conn->GetErrorInfo(&error_msg, &error_type);
        PyErr_Format(GiraffeError, "%d: %s", self->status, error_msg);
        return NULL;
    }
    self->connected = true;
    if (self->conn->GetSchema(&dynamic_schema) != 0) {
        self->conn->GetErrorInfo(&error_msg, &error_type);
        PyErr_Format(GiraffeError, "%d: %s", self->status, error_msg);
        return NULL;
    }
    count = dynamic_schema->GetColumnCount();
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);
    for (std::vector<Column*>::size_type i=0; i < count; i++) {
        Column* c = dynamic_schema->GetColumn(i);
        column = column_new();
        column->Name = strdup(c->GetName());
        column->Type = c->GetDataType();
        column->Length = c->GetSize();
        column->Precision = c->GetPrecision();
        column->Scale = c->GetScale();
        columns_append(columns, *column);
    }
    encoder_clear(self->encoder);
    self->encoder->Columns = columns;
    if (strcmp(encoding, "archive") == 0) {
        settings.row_encoding_type = ROW_ENCODING_RAW;
    } else if (strcmp(encoding, "dict") == 0 || strcmp(encoding, "json") == 0) {
        settings.row_encoding_type = ROW_ENCODING_DICT;
        settings.item_encoding_type = ITEM_ENCODING_BUILTIN_TYPES;
        settings.decimal_return_type = DECIMAL_AS_STRING;
        encoder_set_encoding(self->encoder, &settings);
    } else if (strcmp(encoding, "str") == 0) {
        if (!_PyUnicode_Check(delimiter)) {
            PyErr_SetString(PyExc_TypeError, "Delimiter must be string");
            return NULL;
        }
        if (!(_PyUnicode_Check(null) || null == Py_None)) {
            PyErr_SetString(PyExc_TypeError, "Null must be string or NoneType");
            return NULL;
        }
        settings.row_encoding_type = ROW_ENCODING_STRING;
        settings.item_encoding_type = ITEM_ENCODING_STRING;
        settings.decimal_return_type = DECIMAL_AS_STRING;
        encoder_set_encoding(self->encoder, &settings);
        encoder_set_delimiter(self->encoder, delimiter);
        encoder_set_null(self->encoder, null);
    }
    Py_RETURN_NONE;
}

static PyMethodDef Export_methods[] = {
    {"add_attribute", (PyCFunction)Export_add_attribute, METH_VARARGS, ""},
    {"close", (PyCFunction)Export_close, METH_NOARGS, ""},
    {"columns", (PyCFunction)Export_columns, METH_NOARGS, ""},
    {"get_buffer", (PyCFunction)Export_get_buffer, METH_NOARGS, ""},
    {"initiate", (PyCFunction)Export_initiate, METH_VARARGS, ""},
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
