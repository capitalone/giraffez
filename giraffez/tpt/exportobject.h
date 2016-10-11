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

#ifndef __GIRAFFE_EXPORT_H
#define __GIRAFFE_EXPORT_H

#include <Python.h>
#include <connection.h>
#include <schema.h>
#include "encoder/types.h"

#ifdef __cplusplus
extern "C" {
#endif

using namespace teradata::client::API;

typedef struct {
    PyObject_HEAD
    bool connected;
    Schema* dynamic_schema;
    Connection* conn;
    int connection_status;
    char* error_msg;
    TD_ErrorType error_type;
    GiraffeColumns* columns;
} Export;

extern PyTypeObject ExportType;

#ifdef __cplusplus
}
#endif

#endif
