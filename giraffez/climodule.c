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
#include "compat.h"


#include "cmdobject.h"


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

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_cli", module_methods);
#endif

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    GiraffeError = PyErr_NewException((char*)"_cli.error", NULL, NULL);
    Py_INCREF(GiraffeError);
    PyModule_AddObject(m, "error", GiraffeError);

    Py_INCREF(&CmdType);
    PyModule_AddObject(m, "Cmd", (PyObject*)&CmdType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
