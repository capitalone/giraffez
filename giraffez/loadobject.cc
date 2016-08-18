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
#include "teradata/load.h"


static void Load_dealloc(Load* self) {
    delete self->update;
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* Load_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    char* host=NULL, *username=NULL, *password=NULL;

    if (!PyArg_ParseTuple(args, "sss", &host, &username, &password)) {
        return NULL;
    }
    Load* self;
    self = (Load*)type->tp_alloc(type, 0);
    self->update = new TeradataUpdate(host, username, password);
    return (PyObject*)self;
}

static int Load_init(Load* self, PyObject* args, PyObject* kwds) {
    return 0;
}

static PyObject* Load_add_attribute(Load* self, PyObject* args, PyObject* kwds) {
    int key = -1;
    char* value = NULL;

    if (!PyArg_ParseTuple(args, "is", &key, &value)) {
        Py_RETURN_NONE;
    }
    self->update->add_attribute(key, strdup(value));
    Py_RETURN_NONE;
}

static PyObject* Load_apply_rows(Load* self, PyObject* args, PyObject* kwds) {
    self->update->apply_rows();
    Py_RETURN_NONE;
}

static PyObject* Load_checkpoint(Load* self, PyObject* args, PyObject* kwds) {
    int result = self->update->checkpoint();
    return Py_BuildValue("i", result);
}

static PyObject* Load_close(Load* self, PyObject* args, PyObject* kwds) {
    self->update->close();
    Py_RETURN_NONE;
}

static PyObject* Load_end_acquisition(Load* self, PyObject* args, PyObject* kwds) {
    self->update->end_acquisition();
    Py_RETURN_NONE;
}

static PyObject* Load_error_message(Load* self, PyObject* args, PyObject* kwds) {
    char* msg = self->update->error_message();
    if (msg == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(msg);
}

static PyObject* Load_get_error_info(Load* self, PyObject* args, PyObject* kwds) {
    self->update->get_error_info();
    Py_RETURN_NONE;
}

static PyObject* Load_get_event(Load* self, PyObject* args, PyObject* kwds) {
    int event_type;
    int event_index = 0;
    if (!PyArg_ParseTuple(args, "i|i", &event_type, &event_index)) {
        Py_RETURN_NONE;
    }
    char* data = NULL;
    int length = 0;
    int status = self->update->get_event(event_type, &data, &length, event_index);
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
        self->update->add_column(name, data_type, size, precision, scale);
    }
    self->update->add_schema();
    self->update->add_dmlgroup(dml, dml_option);
    Py_DECREF(tuple_list);
    int result = self->update->initiate();
    return Py_BuildValue("i", result);
}

static PyObject* Load_put_buffer(Load* self, PyObject* args, PyObject* kwds) {
    char* buffer;
    int length;
    if (!PyArg_ParseTuple(args, "s#", &buffer, &length)) {
        Py_RETURN_NONE;
    }
    int result = self->update->put_buffer(buffer, length);
    return Py_BuildValue("i", result);
}

static PyObject* Load_put_row(Load* self, PyObject* args, PyObject* kwds) {
    char* row;
    int length;
    if (!PyArg_ParseTuple(args, "s#", &row, &length)) {
        Py_RETURN_NONE;
    }
    int result = self->update->put_row(row, length);
    return Py_BuildValue("i", result);
}

static PyObject* Load_set_table(Load* self, PyObject* args, PyObject* kwds) {
    char* table_name = NULL;

    if (!PyArg_ParseTuple(args, "s", &table_name)) {
        Py_RETURN_NONE;
    }
    self->update->set_table(std::string(table_name));
    Py_RETURN_NONE;
}

static PyObject* Load_status(Load* self, void* closure) {
    PyObject* status = NULL;
    status = Py_BuildValue("i", self->update->connection_status);
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
    {"put_row", (PyCFunction)Load_put_row, METH_VARARGS, "" },
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
