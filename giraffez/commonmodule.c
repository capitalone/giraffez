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
#include <signal.h>
#include "compat.h"


#ifdef __cplusplus
extern "C" {
#endif 


static int py_quit(void* n) {
    Py_Exit(1);
    return -1;
}

static void cold_shutdown(int signum) {
    PyGILState_STATE state;
    fprintf(stderr, "*** Received SIGINT. Closing ...\n");
    // Py_Exit() is added as a pending call just in case the Python main
    // thread has any chance of finalizing. This will not allow for any
    // __exit__ code to run if it is successful in completing Py_Exit.
    // If unsuccessful, the standard C exit() function is called since
    // this code is reached when the user wants the process to end
    // regardless of the state of any open connections.
    state = PyGILState_Ensure();
    Py_AddPendingCall(&py_quit, NULL);
    PyGILState_Release(state);
    PyErr_SetInterrupt();
    exit(1);
}

static void warm_shutdown(int signum) {
    PyGILState_STATE state;
    fprintf(stderr, "\n*** Received SIGINT. Shutting down (clean) ...\n");
    // GIL must be acquired before setting interrupt
    state = PyGILState_Ensure();
    // Interrupt sent to Python main thread
    PyErr_SetInterrupt();
    PyGILState_Release(state);

    // Set secondary interrupt to allow for improper shutdown
    signal(SIGINT, cold_shutdown);
}

static PyObject* register_shutdown(PyObject* self) {
    signal(SIGINT, &warm_shutdown);
    signal(SIGTERM, &warm_shutdown);
    Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
    {"register_graceful_shutdown_signal", register_shutdown, METH_NOARGS, NULL},
    {NULL}  /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_common", "", -1, module_methods
};
#endif

MOD_INIT(_common)
{
    PyObject* m;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_common", module_methods);
#endif

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
