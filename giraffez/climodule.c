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

#include <Python.h>
#include "_compat.h"

#include "cli/cmdobject.h"
#include "cli/columnsobject.h"
#include "cli/rowobject.h"
#include "cli/errors.h"
#include "encoder/pytypes.h"


#ifdef __cplusplus
extern "C" {
#endif 


static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_cli", "", -1, module_methods
};
#endif


MOD_INIT(_cli)
{
    PyObject* m;

    if (PyType_Ready(&CmdType) < 0) {
        return MOD_ERROR_VAL;
    }
    if (PyType_Ready(&ColumnsType) < 0) {
        return MOD_ERROR_VAL;
    }
    if (PyType_Ready(&RowType) < 0) {
        return MOD_ERROR_VAL;
    }

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_cli", module_methods);
#endif

    giraffez_columns_import();
    giraffez_datetime_import();
    giraffez_decimal_import();

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    define_exceptions(m);

    Py_INCREF(&CmdType);
    PyModule_AddObject(m, "Cmd", (PyObject*)&CmdType);
    Py_INCREF(&ColumnsType);
    PyModule_AddObject(m, "Columns", (PyObject*)&ColumnsType);
    Py_INCREF(&RowType);
    PyModule_AddObject(m, "Row", (PyObject*)&RowType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
