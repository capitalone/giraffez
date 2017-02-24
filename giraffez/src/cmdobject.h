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

#ifndef __GIRAFFEZ_CMD_H
#define __GIRAFFEZ_CMD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>

#include "columns.h"
#include "encoder.h"
#include "tdcli.h"


typedef struct {
    PyObject_HEAD
    TeradataConnection *conn;
    TeradataEncoder *encoder;
} Cmd;

extern PyTypeObject CmdType;

PyObject* check_tdcli_error(unsigned char *dataptr);
PyObject* check_tdcli_failure(unsigned char *dataptr);
PyObject* check_parcel_error(TeradataConnection *conn);
PyObject* check_error(TeradataConnection *conn);
PyObject* teradata_close(TeradataConnection *conn);
TeradataConnection* teradata_connect(const char *host, const char *username,
    const char *password);
PyObject* teradata_execute_rc(TeradataConnection *conn, TeradataEncoder *e, const char *command, int *rc);
PyObject* teradata_execute(TeradataConnection *conn, TeradataEncoder *e, const char *command);
PyObject* teradata_execute_p(TeradataConnection *conn, TeradataEncoder *e, const char *command);
PyObject* teradata_handle_record(TeradataEncoder *e, const uint32_t parcel_t, unsigned char **data,
    const uint32_t length);

#ifdef __cplusplus
}
#endif

#endif
