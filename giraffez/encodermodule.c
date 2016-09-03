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


#include "encoderobject.h"


#ifdef __cplusplus
extern "C" {
#endif 


static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_encoder", "", -1, module_methods
};
#endif

MOD_INIT(_encoder)
{
    PyObject* m;

    if (PyType_Ready(&EncoderType) < 0) {
        return MOD_ERROR_VAL;
    }

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_encoder", module_methods);
#endif

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    Py_INCREF(&EncoderType);
    PyModule_AddObject(m, "Encoder", (PyObject*)&EncoderType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
