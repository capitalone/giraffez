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

#ifndef __GIRAFFE_CMD_H
#define __GIRAFFE_CMD_H

#ifdef __cplusplus
extern "C" {
#endif

#define NO_CLIV2_ERROR_T
#define STRING_CLI

#define CONNECTED     0
#define NOT_CONNECTED 1
#define OK            0
#define STOP          1
#define FAILED       -1
#define PCL_FAIL     -2
#define PCL_ERR      -3


#include <Python.h>
#include <dbcarea.h>
#include <parcel.h>


typedef struct DBCAREA dbcarea_t;

typedef struct {
    PyObject_HEAD
    dbcarea_t *dbc;
    int connected;
    Int32 result;
    int status;
} Cmd;

extern PyObject* GiraffeError;
extern PyTypeObject CmdType;

#ifdef __cplusplus
}
#endif

#endif
