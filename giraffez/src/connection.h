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

#ifndef __GIRAFFEZ_CONNECTION_H
#define __GIRAFFEZ_CONNECTION_H

#include <Python.h>

// Teradata Parallel Transporter API
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>

#include <sstream>

#include "cmdobject.h"
#include "encoder.h"
#include "errors.h"
#include "pytypes.h"
#include "tdcli.h"

// Python 2/3 C API and Windows compatibility
#include "_compat.h"


typedef teradata::client::API::Connection TConn;


namespace Giraffez {
    class Connection : public TConn {
    private:
        TeradataEncoder *encoder;
        std::string table_name;
        const char *host, *username, *password;
    public:
        TD_StatusCode status;
        bool connected;

        Connection(const char *host, const char *username, const char *password) {
            this->host = strdup(host);
            this->username = strdup(username);
            this->password = strdup(password);
            encoder = NULL;
            encoder = encoder_new(NULL, NULL);
        }
        ~Connection() {
            if (encoder != NULL) {
                encoder_free(encoder);
                encoder = NULL;
            }
        }

        PyObject* SetEncoding(EncoderSettings *settings) {
            encoder_set_encoding(encoder, settings);
            Py_RETURN_NONE;
        }

        PyObject* SetEncoding(EncoderSettings *settings, PyObject *null, PyObject *delimiter) {
            if (!_PyUnicode_Check(delimiter)) {
                PyErr_SetString(PyExc_TypeError, "Delimiter must be string");
                return NULL;
            }
            if (!(_PyUnicode_Check(null) || null == Py_None)) {
                PyErr_SetString(PyExc_TypeError, "Null must be string or NoneType");
                return NULL;
            }
            this->SetEncoding(settings);
            encoder_set_null(encoder, null);
            encoder_set_delimiter(encoder, delimiter);
            Py_RETURN_NONE;
        }

        void ClearSchemaList() {
            TConn::ClearSchemaList();
        }

        void SetInputSchema(teradata::client::API::Schema *schema) {
            TConn::SetInputSchema(schema);
        }

        void AddAttribute(TD_Attribute key, const char *value) {
            TConn::AddAttribute(key, (char*)value);
        }

        void AddAttribute(TD_Attribute key, TD_IntValue value) {
            TConn::AddAttribute(key, value);
        }

        PyObject* AddAttribute(TD_Attribute key, PyObject *value) {
            if PyLong_Check(value) {
                TConn::AddAttribute(key, (int)PyLong_AsLong(value));
            } else if (_PyUnicode_Check(value)) {
                TConn::AddAttribute(key, PyUnicode_AsUTF8(value));
            } else {
                // TODO: err
            }
            Py_RETURN_NONE;
        }

        PyObject* ApplyRows() {
            if ((status = TConn::ApplyRows()) >= TD_Error) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* Checkpoint() {
            // TODO: need free?
            char *data;
            TD_Length length = 0;
            if ((status = TConn::Checkpoint(&data, &length)) >= TD_Error) {
                return this->HandleError();
            }
            return PyBytes_FromStringAndSize(data, length);
        }

        PyObject* Columns() {
            if (encoder == NULL || encoder->Columns == NULL) {
                PyErr_Format(GiraffeError, "Columns not set");
                return NULL;
            }
            return giraffez_columns_from_columns(encoder->Columns);
        }

        PyObject* EndAcquisition() {
            if ((status = TConn::EndAcquisition()) >= TD_Error) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* GetBuffer() {
            unsigned char *data = NULL;
            int length;
            if (TConn::GetBuffer((char**)&data, (TD_Length*)&length) == TD_END_Method) {
                Py_RETURN_NONE;
            }
            return encoder->UnpackRowsFunc(encoder, &data, length);
        }

        PyObject* GetEvent(TD_EventType event_type, TD_Index index) {
            char *data = NULL;
            TD_Length length = 0;
            // TODO: check TD_Unavailable and raise different exception
            if ((status = TConn::GetEvent(event_type, &data, &length, index)) >= TD_Error) {
                return this->HandleError();
            }
            return PyBytes_FromStringAndSize(data, length);
        }

        PyObject* Initiate() {
            if ((status = TConn::Initiate()) >= TD_Error) {
                return this->HandleError();
            }
            connected = true;
            Py_RETURN_NONE;
        }

        PyObject* PutBuffer(char *data, TD_Length length, TD_Boolean indicator) {
            if ((status = TConn::PutBuffer(data, length, indicator)) != TD_Success) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* PutRow(char *data, TD_Length length) {
            if ((status = TConn::PutRow(data, length)) != TD_Success) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* Terminate() {
            if (connected) {
                if ((status = TConn::Terminate()) >= TD_Error) {
                    return this->HandleError();
                }
                connected = false;
            }
            Py_RETURN_NONE;
        }

        PyObject* HandleError() {
            char *error_msg;
            TD_ErrorType error_type;
            Connection::GetErrorInfo(&error_msg, &error_type);
            PyErr_Format(GiraffeError, "%d: %s", status, error_msg);
            return NULL;
        }

        PyObject* SetQuery(const char *query) {
            this->AddAttribute(TD_SELECT_STMT, query);
            encoder_clear(encoder);
            TeradataConnection *cmd;
            if ((cmd = teradata_connect(host, username, password)) == NULL) {
                return NULL;
            }
            if (teradata_execute_p(cmd, encoder, query) == NULL) {
                return NULL;
            }
            if (teradata_close(cmd) == NULL) {
                return NULL;
            }
            Py_RETURN_NONE;
        }

        PyObject* SetTable(char *tbl_name) {
            table_name = std::string(tbl_name);
            this->AddAttribute(TD_TARGET_TABLE, strdup(table_name.c_str()));
            this->AddAttribute(TD_LOG_TABLE, strdup((table_name + "_log").c_str()));
            this->AddArrayAttribute(TD_WORK_TABLE, 1, strdup((table_name + "_wt").c_str()), NULL);
            this->AddArrayAttribute(TD_ERROR_TABLE_1, 1, strdup((table_name + "_e1").c_str()), NULL);
            this->AddArrayAttribute(TD_ERROR_TABLE_2, 1, strdup((table_name + "_e2").c_str()), NULL);
            encoder_clear(encoder);
            TeradataConnection *cmd;
            if ((cmd = teradata_connect(host, username, password)) == NULL) {
                return NULL;
            }
            if (teradata_execute_p(cmd, encoder, ("select * from " + table_name).c_str()) == NULL) {
                return NULL;
            }
            if (teradata_close(cmd) == NULL) {
                return NULL;
            }
            Py_RETURN_NONE;
        }

        PyObject* SetSchema(PyObject *column_list, DMLOption dml_option) {
            using namespace teradata::client::API;
            Schema *table_schema;
            DMLGroup *dml_group;
            TD_Index dml_group_index;
            GiraffeColumn *column;
            std::stringstream dml, column_names, safe_column_names;
            PyObject *column_name;
            size_t j;
            Py_ssize_t len, i;
            table_schema = new Schema((char*)"input");
            // TODO: either use this or SetInputSchema
            this->ClearSchemaList();
            len = PyList_Size(column_list);
            for (i = 0; i < len; i++) {
                // TODO: check to ensure that the supplied value is valid as a
                // a column
                column_name = PyList_GetItem(column_list, i);
                for (j=0; j<encoder->Columns->length; j++) {
                    column = &encoder->Columns->array[j];
                    if (strcmp(column->Name, PyUnicode_AsUTF8(column_name)) == 0) {
                        table_schema->AddColumn(column->Name, (TD_DataType)column->TPTType, column->Length,
                            column->Precision, column->Scale);
                        column_names << "\"" << column->Name << "\",";
                        safe_column_names << ":" << column->SafeName << ",";
                    }
                }
            }
            // TODO: May not be necessary to specify the column names, since it
            // appears to pull that from what is added via AddColumn (possibly)
            dml << "insert into " << table_name << " ("
                << column_names.str().substr(0, column_names.str().size() - 1)
                << ") values ("
                << safe_column_names.str().substr(0, safe_column_names.str().size() - 1)
                << ");";
            //std::cerr << "DML: " << dml.str() << std::endl;
            this->AddSchema(table_schema);
            //this->SetInputSchema(table_schema);
            delete table_schema;
            dml_group = new DMLGroup();
            dml_group->AddStatement(strdup(dml.str().c_str()));
            dml_group->AddDMLOption(dml_option);
            if ((status = this->AddDMLGroup(dml_group, &dml_group_index)) >= TD_Error) {
                return this->HandleError();
            }
            delete dml_group;
            Py_RETURN_NONE;
        }
    };
}

#endif
