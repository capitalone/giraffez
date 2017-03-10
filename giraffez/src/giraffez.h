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

#ifndef __GIRAFFEZ_API_H
#define __GIRAFFEZ_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#include <structmember.h>
#include "common.h"
#include "convert.h"
#include "columns.h"
#include "encoder.h"
#include "teradata.h"

#ifdef DEBUG_LOGGING
#define DEBUG_PRINTF(fmt, ...) debug_printf(fmt, __VA_ARGS__)
#else
#define DEBUG_PRINTF(fmt, ...)
#endif

void debug_printf(const char *fmt, ...);

PyObject* teradata_close(TeradataConnection *conn);
TeradataConnection* teradata_connect(const char *host, const char *username,
    const char *password);
PyObject* teradata_execute_rc(TeradataConnection *conn, TeradataEncoder *e, const char *command, int *rc);
PyObject* teradata_execute(TeradataConnection *conn, TeradataEncoder *e, const char *command);
PyObject* teradata_execute_p(TeradataConnection *conn, TeradataEncoder *e, const char *command);
PyObject* teradata_handle_record(TeradataEncoder *e, const uint32_t parcel_t, unsigned char **data,
    const uint32_t length);

PyObject* define_exceptions(PyObject *module);

int giraffez_types_import();

PyObject* giraffez_columns_to_pyobject(GiraffeColumns *columns);
GiraffeColumns* giraffez_columns_from_pyobject(PyObject *columns_obj);

// TODO: make macros instead?
PyObject* giraffez_decimal_from_pystring(PyObject *obj);
PyObject* giraffez_date_from_datetime(int year, int month, int day, int hour, int minute,
    int second, int microsecond);
PyObject* giraffez_time_from_time(int hour, int minute, int second, int microsecond);
PyObject* giraffez_ts_from_datetime(int year, int month, int day, int hour, int minute, int second,
    int microsecond);

// pack
PyObject* teradata_row_from_pydict(const TeradataEncoder *e, PyObject *row, unsigned char **data,
    uint16_t *length);
PyObject* teradata_row_from_pybytes(const TeradataEncoder *e, PyObject *row, unsigned char **data,
    uint16_t *length);
PyObject* teradata_row_from_pystring(const TeradataEncoder *e, PyObject *row, unsigned char **data,
    uint16_t *length);
PyObject* teradata_row_from_pytuple(const TeradataEncoder *e, PyObject *row, unsigned char **data,
    uint16_t *length);
PyObject* teradata_item_from_pyobject(const TeradataEncoder *e, const GiraffeColumn *column,
    PyObject *item, unsigned char **data, uint16_t *length);

// unpack
uint32_t teradata_buffer_count_rows(unsigned char *data, const uint32_t length);
PyObject* teradata_buffer_to_pybytes(const TeradataEncoder *e, unsigned char **data, const uint32_t length);
PyObject* teradata_buffer_to_pylist(const TeradataEncoder *e, unsigned char **data, const uint32_t length);

PyObject* teradata_row_to_pybytes(const TeradataEncoder *e, unsigned char **data, const uint16_t length);
PyObject* teradata_row_to_pydict(const TeradataEncoder *e, unsigned char **data, const uint16_t length);
PyObject* teradata_row_to_pystring(const TeradataEncoder *e, unsigned char **data, const uint16_t length);
PyObject* teradata_row_to_pytuple(const TeradataEncoder *e, unsigned char **data, const uint16_t length);

PyObject* teradata_item_to_pyobject(const TeradataEncoder *e, unsigned char **data,
    const GiraffeColumn *column);
PyObject* teradata_item_from_pystring(const TeradataEncoder *e, const GiraffeColumn *column,
    PyObject *item, unsigned char **data, uint16_t *length);

#ifdef __cplusplus
}
#endif

#endif
