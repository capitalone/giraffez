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

#ifndef __GIRAFFEZ_ERRORS_H
#define __GIRAFFEZ_ERRORS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>


typedef struct {
    PyException_HEAD
    PyObject *message;
    PyObject *code;
} TeradataErrorObject;

extern PyObject *TeradataError;
extern PyObject *GiraffezError;
extern PyObject *EncoderError;

//extern PyObject *TeradataError;
extern PyObject *InvalidCredentialsError;

#ifdef DEBUG
#define DEBUG_PRINTF(fmt, ...) debug_printf(fmt, __VA_ARGS__)
#else
#define DEBUG_PRINTF(fmt, ...)
#endif

void debug_printf(const char *fmt, ...);
//void define_exceptions(const char *name, PyObject *module);
PyObject* define_exceptions(PyObject *module);

#ifdef __cplusplus
}
#endif

#endif
