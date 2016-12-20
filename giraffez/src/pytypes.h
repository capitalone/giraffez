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

#ifndef __GIRAFFEZ_PYTYPES_H
#define __GIRAFFEZ_PYTYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>

#include "columns.h"


int giraffez_columns_import();
int giraffez_decimal_import();
int giraffez_datetime_import();

PyObject* columns_to_pylist(GiraffeColumns *columns);
PyObject* giraffez_columns_from_columns(GiraffeColumns *c);
GiraffeColumns* columns_to_giraffez_columns(PyObject *obj);
PyObject* giraffez_decimal_from_pystring(PyObject *obj);
PyObject* giraffez_date_from_datetime(int year, int month, int day, int hour, int minute,
    int second, int microsecond);
PyObject* giraffez_time_from_time(int hour, int minute, int second, int microsecond);
PyObject* giraffez_ts_from_datetime(int year, int month, int day, int hour, int minute, int second,
    int microsecond);
PyObject* giraffez_result_from_rows(PyObject *columns, PyObject *row);
PyObject* giraffez_row_from_list(PyObject *columns, PyObject *row);

#ifdef __cplusplus
}
#endif

#endif
