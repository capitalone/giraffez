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

#include "load.h"
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>
#include "compat.h"


TeradataUpdate::TeradataUpdate(std::string server, std::string username, std::string password) {
    error_msg = strdup(string("").c_str());
    connection_status = 0;
    connected = false;
    table_set = false;

    table_schema = new Schema(strdup(string("input").c_str()));

    conn = new Connection();
    conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_UPDATE);

    conn->AddAttribute(TD_DROPLOGTABLE, strdup(string("Y").c_str()));
    conn->AddAttribute(TD_DROPWORKTABLE, strdup(string("Y").c_str()));
    conn->AddAttribute(TD_DROPERRORTABLE, strdup(string("Y").c_str()));

    conn->AddAttribute(TD_USER_NAME, strdup(username.c_str()));
    conn->AddAttribute(TD_USER_PASSWORD, strdup(password.c_str()));
    conn->AddAttribute(TD_TDP_ID, strdup(server.c_str()));

    conn->AddAttribute(TD_TENACITY_HOURS, 1);
    conn->AddAttribute(TD_TENACITY_SLEEP, 1);
    conn->AddAttribute(TD_ERROR_LIMIT, 1);
    conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);
    conn->AddAttribute(TD_MAX_SESSIONS, 20);
}

TeradataUpdate::~TeradataUpdate() {
    this->close();
}

void TeradataUpdate::set_table(std::string table_name) {
    conn->AddAttribute(TD_TARGET_TABLE, strdup(table_name.c_str()));

    std::string log_table = table_name + "_log";
    conn->AddAttribute(TD_LOG_TABLE, strdup(log_table.c_str()));
    std::string work_table = table_name + "_wt";
    conn->AddArrayAttribute(TD_WORK_TABLE, 1, strdup(work_table.c_str()), NULL);
    std::string e1_table = table_name + "_e1";
    std::string e2_table = table_name + "_e2";
    conn->AddArrayAttribute(TD_ERROR_TABLE_1, 1, strdup(e1_table.c_str()), NULL);
    conn->AddArrayAttribute(TD_ERROR_TABLE_2, 1, strdup(e2_table.c_str()), NULL);

    table_set = true;
}

void TeradataUpdate::add_column(char* name, int data_type, int size, int precision, int scale) {
    if (!table_set) {
        return;
    }
    table_schema->AddColumn(name, (TD_DataType)data_type, size, precision, scale);
}

void TeradataUpdate::add_schema() {
    if (!table_set) {
        return;
    }
    conn->AddSchema(table_schema);
}

void TeradataUpdate::add_dmlgroup(char* dml, int dml_option) {
    if (!table_set) {
        return;
    }
    TD_Index dmlGroupIndex;
    dml_group = new DMLGroup();
    dml_group->AddStatement(dml);
    dml_group->AddDMLOption((DMLOption) dml_option);
    conn->AddDMLGroup(dml_group, &dmlGroupIndex);
    delete dml_group;
}

void TeradataUpdate::add_attribute(int attr_name, char* attr_value) {
    conn->AddAttribute((TD_Attribute) attr_name, attr_value);
}

void TeradataUpdate::add_attribute(int attr_name, int attr_value) {
    conn->AddAttribute((TD_Attribute) attr_name, attr_value);
}

int TeradataUpdate::initiate() {
    if (table_set) {
        connection_status = conn->Initiate();
        if (connection_status >= TD_Error) {
            conn->GetErrorInfo(&error_msg, &error_type);
        } else {
            connected = true;
        }
    }
    return connection_status;
}

int TeradataUpdate::put_row(char* row, int length) {
    row_status = conn->PutRow(row, length);
    return row_status;
}

int TeradataUpdate::put_buffer(char* buffer, int length) {
    row_status = conn->PutBuffer(buffer, length, 1);
    return row_status;
}

void TeradataUpdate::end_acquisition() {
    conn->EndAcquisition();
}

void TeradataUpdate::apply_rows() {
    conn->ApplyRows();
}

int TeradataUpdate::checkpoint() {
    int return_code;
    char* cp_data;
    TD_Length cp_data_len = 0;
    return_code = conn->Checkpoint(&cp_data, &cp_data_len);
    return return_code;
}

int TeradataUpdate::get_event(int event_type, char **event_data, int *data_len, int event_index) {
    return conn->GetEvent((TD_EventType)event_type, event_data, (TD_Length*) data_len, (TD_Index)event_index);
}

void TeradataUpdate::get_error_info() {
    conn->GetErrorInfo(&error_msg, &error_type);
}

char* TeradataUpdate::error_message() {
    return error_msg;
}

void TeradataUpdate::close () {
    if (connected) {
        connection_status = conn->Terminate();
        delete conn;
        delete table_schema;
    }
    connected = false;
}
