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

#include "common.h"
#include "columns.h"
#include "convert.h"
#include "encoder.h"
#include "teradata.h"
#include <sstream>

// Teradata Parallel Transporter API
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>


typedef teradata::client::API::Connection TConn;


namespace Giraffez {
    class Connection : public TConn {
    private:
        std::string table_name;
        const char *host, *username, *password;
        unsigned char *row_buffer;
    public:
        TeradataEncoder *encoder;
        int status;
        bool connected;

        Connection(const char *host, const char *username, const char *password) {
            this->host = strdup(host);
            this->username = strdup(username);
            this->password = strdup(password);
            this->row_buffer = (unsigned char*)malloc(sizeof(unsigned char)*TD_ROW_MAX_SIZE);
            encoder = encoder_new(NULL, 0);
        }
        ~Connection() {
            if (encoder != NULL) {
                encoder_free(encoder);
                encoder = NULL;
            }
            free(this->row_buffer);
        }

        PyObject* SetEncoding(uint32_t settings) {
            encoder_set_encoding(encoder, settings);
            Py_RETURN_NONE;
        }

        PyObject* SetEncoding(uint32_t settings, PyObject *null, PyObject *delimiter) {
            if (null != NULL && !PyStr_Check(delimiter)) {
                PyErr_SetString(PyExc_TypeError, "Delimiter must be string");
                return NULL;
            }
            if (null != NULL && !(PyStr_Check(null) || null == Py_None)) {
                PyErr_SetString(PyExc_TypeError, "Null must be string or NoneType");
                return NULL;
            }
            this->SetEncoding(settings);
            encoder_set_null(encoder, null);
            encoder_set_delimiter(encoder, delimiter);
            Py_RETURN_NONE;
        }

        void AddAttribute(TD_Attribute key, const char *value) {
            TConn::AddAttribute(key, (char*)value);
        }

        void AddAttribute(TD_Attribute key, TD_IntValue value) {
            TConn::AddAttribute(key, value);
        }

        PyObject* AddAttribute(PyObject *attrs) {
            if (!PyList_Check(attrs)) {
                PyErr_Format(GiraffezError, "Expected list but received '%s'", Py_TYPE(attrs)->tp_name);
                return NULL;
            }
            PyObject *obj;
            TD_Attribute key;
            PyObject *value;
            Py_ssize_t len, i;
            len = PyList_Size(attrs);
            for (i = 0; i < len; i++) {
                Py_RETURN_ERROR(obj = PyList_GetItem(attrs, i));
                if (!PyArg_ParseTuple(obj, "(iO)", &key, &value)) {
                    return NULL;
                }
                this->AddAttribute(key, value);
            }
            Py_RETURN_NONE;
        }

        PyObject* AddAttribute(TD_Attribute key, PyObject *value) {
            if PyLong_Check(value) {
                TConn::AddAttribute(key, (int)PyLong_AsLong(value));
            } else if (PyStr_Check(value)) {
                TConn::AddAttribute(key, PyUnicode_AsUTF8(value));
            } else {
                // TODO: err
            }
            Py_RETURN_NONE;
        }

        PyObject* ApplyRows() {
            if ((status = TConn::ApplyRows()) >= TD_ERROR) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* Checkpoint() {
            char *data = NULL;
            TD_Length length = 0;
            if ((status = TConn::Checkpoint(&data, &length)) != TD_END_METHOD) {
                return Py_BuildValue("i", status);
            }
            return PyBytes_FromStringAndSize(data, length);
        }

        PyObject* Columns() {
            if (encoder == NULL || encoder->Columns == NULL) {
                PyErr_Format(GiraffezError, "Columns not set");
                return NULL;
            }
            return giraffez_columns_to_pyobject(encoder->Columns);
        }

        PyObject* EndAcquisition() {
            status = 0;
            if ((status = TConn::EndAcquisition()) >= TD_ERROR) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* GetBuffer() {
            unsigned char *data = NULL;
            int length;
            if ((int)TConn::GetBuffer((char**)&data, (TD_Length*)&length) == TD_END_METHOD) {
                Py_RETURN_NONE;
            }
            return encoder->UnpackRowsFunc(encoder, &data, length);
        }

        PyObject* GetEvent(TD_EventType event_type, TD_Index index) {
            char *data = NULL;
            TD_Length length = 0;
            // TODO: check TD_Unavailable and raise different exception.  Necessary?
            if ((status = TConn::GetEvent(event_type, &data, &length, index)) >= TD_ERROR) {
                return this->HandleError();
            }
            return PyBytes_FromStringAndSize(data, length);
        }

        PyObject* Initiate() {
            if ((status = TConn::Initiate()) >= TD_ERROR) {
                return this->HandleError();
            }
            connected = true;
            Py_RETURN_NONE;
        }

        PyObject* PutRow(char *data, TD_Length length) {
            if ((status = TConn::PutRow(data, length)) != TD_SUCCESS) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* PutRow(PyObject *items) {
            uint16_t length = 0;
            // PackRowFunc advances the data pointer, save it as a separate variable
            unsigned char *data = this->row_buffer;
            if (encoder->PackRowFunc(encoder, items, &data, &length) == NULL) {
                return NULL;
            }
            status = 0;
            if ((status = TConn::PutRow((char*)this->row_buffer, (TD_Length)length)) != TD_SUCCESS) {
                return this->HandleError();
            }
            Py_RETURN_NONE;
        }

        PyObject* Release(char *tbl_name) {
            table_name = std::string(tbl_name);
            TeradataEncoder *e;
            int status;
            e = encoder_new(NULL, ENCODER_SETTINGS_DEFAULT);
            TeradataConnection *cmd;
            std::string query = "release mload "  + table_name;
            if ((cmd = teradata_connect(host, username, password)) == NULL) {
                return NULL;
            }
            if (teradata_execute_rc(cmd, e, strdup(query.c_str()), &status) == NULL) {
                if (status != TD_ERROR_CANNOT_RELEASE_MLOAD) {
                    return NULL;
                }
                PyErr_Clear();
                query = query + " in apply";
                if (teradata_execute_rc(cmd, e, strdup(query.c_str()), &status) == NULL) {
                    return NULL;
                }
            }
            if (teradata_close(cmd) == NULL) {
                return NULL;
            }
            if (e != NULL) {
                encoder_free(e);
                e = NULL;
            }
            Py_RETURN_NONE;
        }

        PyObject* Terminate() {
            if (connected) {
                if ((status = TConn::Terminate()) >= TD_ERROR) {
                    return this->HandleError();
                }
                connected = false;
            }
            Py_RETURN_NONE;
        }

        PyObject* HandleError() {
            if (status == TD_CALL_ENDACQ) {
                this->EndAcquisition();
            }
            char *error_msg = NULL;
            TD_ErrorType error_type;
            Connection::GetErrorInfo(&error_msg, &error_type);
            PyErr_Format(TeradataError, "%d: %s", status, error_msg);
            return NULL;
        }

        PyObject* Exists(const char *tbl_name) {
            table_name = std::string(tbl_name);
            TeradataEncoder *e;
            int status;
            e = encoder_new(NULL, 0);
            TeradataConnection *cmd;
            std::string query;
            PyObject *ret;
            ret = Py_False;
            Py_INCREF(ret);
            Py_RETURN_ERROR(cmd = teradata_connect(host, username, password));
            query = "show table " + table_name;
            if (teradata_execute_rc(cmd, e, strdup(query.c_str()), &status) == NULL) {
                if (status != TD_ERROR_OBJECT_NOT_EXIST) {
                    return NULL;
                }
                PyErr_Clear();
            }
            Py_RETURN_ERROR(teradata_close(cmd));
            if (e != NULL) {
                encoder_free(e);
                e = NULL;
            }
            if (status == TD_SUCCESS) {
                // TODO: ref correct?
                ret = Py_True;
            }
            return ret;
        }

        PyObject* DropTable(const char *tbl_name) {
            std::string query = std::string(tbl_name);
            TeradataEncoder *e;
            int status;
            e = encoder_new(NULL, 0);
            TeradataConnection *cmd;
            query = "drop table " + query;
            Py_RETURN_ERROR(cmd = teradata_connect(host, username, password));
            Py_RETURN_ERROR(teradata_execute_rc(cmd, e, strdup(query.c_str()), &status));
            Py_RETURN_ERROR(teradata_close(cmd));
            if (e != NULL) {
                encoder_free(e);
                e = NULL;
            }
            Py_RETURN_NONE;
        }

        PyObject* SetQuery(const char *query) {
            this->AddAttribute(TD_SELECT_STMT, query);
            encoder_clear(encoder);
            TeradataConnection *cmd;
            Py_RETURN_ERROR(cmd = teradata_connect(host, username, password));
            Py_RETURN_ERROR(teradata_execute_p(cmd, encoder, query));
            Py_RETURN_ERROR(teradata_close(cmd));
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
            Py_RETURN_ERROR(cmd = teradata_connect(host, username, password));
            Py_RETURN_ERROR(teradata_execute_p(cmd, encoder, ("select top 1 * from " + table_name).c_str()));
            Py_RETURN_ERROR(teradata_close(cmd));
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
            GiraffeColumns *ncolumns;
            ncolumns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
            size_t j;
            Py_ssize_t len, i;
            table_schema = new Schema((char*)"input");
            // TODO: either use this or SetInputSchema
            if (column_list == NULL || column_list == Py_None) {
                for (j=0; j<encoder->Columns->length; j++) {
                    column = &encoder->Columns->array[j];
                    table_schema->AddColumn(column->Name, (TD_DataType)column->TPTType, column->Length,
                        column->Precision, column->Scale);
                    column_names << "\"" << column->Name << "\",";
                    safe_column_names << ":" << column->SafeName << ",";
                }
            } else {
                columns_init(ncolumns, 0);
                len = PyList_Size(column_list);
                for (i = 0; i < len; i++) {
                    // TODO: check to ensure that the supplied value is valid as a
                    // a column
                    // TODO: should probably just improve with better abstraction
                    Py_RETURN_ERROR((column_name = PyList_GetItem(column_list, i)));
                    for (j=0; j<encoder->Columns->length; j++) {
                        column = &encoder->Columns->array[j];
                        if (compare_name(column->Name, PyUnicode_AsUTF8(column_name)) == 0) {
                            table_schema->AddColumn(column->Name, (TD_DataType)column->TPTType, column->Length,
                                column->Precision, column->Scale);
                            column_names << "\"" << column->Name << "\",";
                            safe_column_names << ":" << column->SafeName << ",";
                            columns_append(ncolumns, *column);
                        }
                    }
                }
                encoder->Columns = ncolumns;
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
            if ((status = this->AddDMLGroup(dml_group, &dml_group_index)) >= TD_ERROR) {
                return this->HandleError();
            }
            delete dml_group;
            Py_RETURN_NONE;
        }
    };
}

#endif
