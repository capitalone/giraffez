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

#include "export.h"
#include <connection.h>
#include <schema.h>
#include "compat.h"


TeradataExport::TeradataExport(std::string server, std::string username,
               std::string password) {
    error_msg = NULL;
    connection_status = 0;
    dynamic_schema = NULL;
    conn = new Connection();
    conn->AddAttribute(TD_SYSTEM_OPERATOR, TD_EXPORT);
    conn->AddAttribute(TD_TDP_ID, strdup(server.c_str()));
    conn->AddAttribute(TD_USER_NAME, strdup(username.c_str()));
    conn->AddAttribute(TD_USER_PASSWORD, strdup(password.c_str()));
    conn->AddAttribute(TD_MIN_SESSIONS, 5);
    conn->AddAttribute(TD_MAX_SESSIONS, 32);
    conn->AddAttribute(TD_MAX_DECIMAL_DIGITS, 38);
    conn->AddAttribute(TD_CHARSET, strdup(string("UTF8").c_str()));

    // Buffer mode
    conn->AddAttribute(TD_BUFFER_MODE, strdup(string("YES").c_str()));
    conn->AddAttribute(TD_BLOCK_SIZE, 64330);
    conn->AddAttribute(TD_BUFFER_HEADER_SIZE, 2);
    conn->AddAttribute(TD_BUFFER_LENGTH_SIZE, 2);
    conn->AddAttribute(TD_BUFFER_MAX_SIZE, 64260);
    conn->AddAttribute(TD_BUFFER_TRAILER_SIZE, 0);

    conn->AddAttribute(TD_SPOOLMODE, strdup(string("NoSpool").c_str()));

    conn->AddAttribute(TD_TENACITY_HOURS, 1);
    conn->AddAttribute(TD_TENACITY_SLEEP, 1);
}

TeradataExport::~TeradataExport() {
    this->close();
}

void TeradataExport::initiate() {
    connection_status = conn->Initiate();
    if (connection_status >= TD_Error) {
        conn->GetErrorInfo(&error_msg, &error_type);
    } else {
        connected = true;
    }
}

void TeradataExport::get_schema() {
    int schema_status = conn->GetSchema(&dynamic_schema);
    if (!(connection_status < TD_Error) || schema_status != 0) {
        conn->GetErrorInfo(&error_msg, &error_type);
    }
}

void TeradataExport::add_attribute(int attr_name, char* attr_value) {
    conn->AddAttribute((TD_Attribute) attr_name, attr_value);
}

void TeradataExport::add_attribute(int attr_name, int attr_value) {
    conn->AddAttribute((TD_Attribute) attr_name, attr_value);
}

int TeradataExport::get_one(unsigned char** row, int* length) {
    int export_status = 0;
    export_status = conn->GetRow((char**)row, (TD_Length*)length);
    return export_status;
}

int TeradataExport::get_buffer(unsigned char** row, int* length) {
    int export_status = 0;
    export_status = conn->GetBuffer((char**)row, (TD_Length*)length);
    return export_status;
}

std::vector<Column*> TeradataExport::get_columns() {
    unsigned int count = dynamic_schema->GetColumnCount();
    std::vector<Column*> columns(count);
    for (std::vector<Column*>::size_type i=0; i < count; i++) {
        columns[i] = dynamic_schema->GetColumn(i);
    }
    return columns;
}

void TeradataExport::close () {
    if (connected) {
        connection_status = conn->Terminate();
        delete conn;
    }
    connected = false;
}
