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

#ifndef __GIRAFFE_LOAD_H
#define __GIRAFFE_LOAD_H

#include <Python.h>
#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>

#ifdef __cplusplus
extern "C" {
#endif

using namespace teradata::client::API;

typedef struct {
    PyObject_HEAD
    char* select_stmt;
    Schema* table_schema;
    DMLGroup* dml_group;
    Connection* conn;
    char* error_msg;
    TD_ErrorType error_type;
    bool table_set;
    bool connected;
    int connection_status;
    int row_status;
} Load;

extern PyTypeObject LoadType;

#ifdef __cplusplus
}
#endif

#endif
