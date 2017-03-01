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

#ifndef __GIRAFFEZ_PACK_H
#define __GIRAFFEZ_PACK_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif

#include "columns.h"
#include "encoder.h"


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

#ifdef __cplusplus
}
#endif

#endif
