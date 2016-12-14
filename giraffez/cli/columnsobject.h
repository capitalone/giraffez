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

#ifndef __CLI_COLUMN_H
#define __CLI_COLUMN_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>

#include "encoder/columns.h"


typedef struct {
    PyObject_HEAD

    GiraffeColumns *columns;
} Columns;

PyObject* Columns_new(PyTypeObject *type, PyObject *columns_list, PyObject *whatever);
PyObject* Columns_FromC(PyTypeObject *type, GiraffeColumns *columns);

extern PyTypeObject ColumnsType;

#define GiraffezColumns_FromList(column_map, values) \
        Columns_new(&ColumnsType, column_map, values)

#ifdef __cplusplus
}
#endif

#endif
