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

#ifndef __GIRAFFEZ_COMMON_H
#define __GIRAFFEZ_COMMON_H

// common.h contains commonly included standard libraries and macro
// definitions.

#ifdef __cplusplus
extern "C" {
#endif

#include <ctype.h>
#include <math.h>
#include <stddef.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Python 2/3 C API and Windows compatibility
#include "compat.h"

// General compile-time settings
#include "config.h"

extern PyTypeObject CmdType;
extern PyTypeObject EncoderType;
extern PyTypeObject ExportType;
extern PyTypeObject MLoadType;

extern PyObject *TeradataError;
extern PyObject *GiraffezError;
extern PyObject *EncoderError;
extern PyObject *InvalidCredentialsError;

// CLIv2 control flow exceptions
extern PyObject *EndStatementError;
extern PyObject *EndRequestError;


#define Py_RETURN_ERROR(s) if ((s) == NULL) return NULL;

#define DEFAULT_DELIMITER PyUnicode_FromString("|")
#define DEFAULT_NULLVALUE (Py_INCREF(Py_None), Py_None)
#define DEFAULT_NULLVALUE_STR PyUnicode_FromString("NULL")

#define ENCODER_SETTINGS_DEFAULT (ROW_ENCODING_LIST|DATETIME_AS_STRING|DECIMAL_AS_FLOAT)
#define ENCODER_SETTINGS_STRING  (ROW_ENCODING_STRING|DATETIME_AS_STRING|DECIMAL_AS_STRING)
#define ENCODER_SETTINGS_JSON  (ROW_ENCODING_DICT|DATETIME_AS_STRING|DECIMAL_AS_FLOAT)

#ifdef __cplusplus
}
#endif

#endif
