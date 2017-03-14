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

#include "src/giraffez.h"


#ifdef __cplusplus
extern "C" {
#endif 

static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinel */
};

MOD_INIT(_tpt)
{
    PyObject* m;

    if (PyType_Ready(&ExportType) < 0) {
        return MOD_ERROR_VAL;
    }

    if (PyType_Ready(&MLoadType) < 0) {
        return MOD_ERROR_VAL;
    }

    MOD_DEF(m, "_tpt", "", module_methods);

    giraffez_types_import();

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    define_exceptions(m);

    Py_INCREF(&ExportType);
    PyModule_AddObject(m, "Export", (PyObject*)&ExportType);

    Py_INCREF(&MLoadType);
    PyModule_AddObject(m, "MLoad", (PyObject*)&MLoadType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
