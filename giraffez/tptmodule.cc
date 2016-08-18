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


#include "exportobject.h"
#include "loadobject.h"


#ifdef __cplusplus
extern "C" {
#endif 


static int py_quit(void *) {
    Py_Exit(1);
    return -1;
}

static void py_cold_shutdown(int signum) {
    fprintf(stderr, "*** Received SIGINT. Closing ...\n");
    // Py_Exit() is added as a pending call just in case the Python main
    // thread has any chance of finalizing. This will not allow for any
    // __exit__ code to run if it is successful in completing Py_Exit.
    // If unsuccessful, the standard C exit() function is called since
    // this code is reached when the user wants the process to end
    // regardless of the state of any open connections.
    PyGILState_STATE state = PyGILState_Ensure();
    Py_AddPendingCall(&py_quit, NULL);
    PyGILState_Release(state);
    PyErr_SetInterrupt();
    exit(1);
}

static void default_signal_handler(int signum) {
    fprintf(stderr, "\n*** Received SIGINT. Shutting down (clean) ...\n");
    // GIL must be acquired before setting interrupt
    PyGILState_STATE state = PyGILState_Ensure();
    // Interrupt sent to Python main thread
    PyErr_SetInterrupt();
    PyGILState_Release(state);

    // Set secondary interrupt to allow for improper shutdown
    signal(SIGINT, py_cold_shutdown);
}

static PyMethodDef module_methods[] = {
    {NULL}  /* Sentinel */
};

MOD_INIT(_tpt)
{
    PyObject* m;

    signal(SIGINT, &default_signal_handler);
    signal(SIGTERM, &default_signal_handler);

    if (PyType_Ready(&ExportType) < 0) {
        return MOD_ERROR_VAL;
    }

    if (PyType_Ready(&LoadType) < 0) {
        return MOD_ERROR_VAL;
    }

    MOD_DEF(m, "_tpt", "", module_methods);

    if (m == NULL) {
        return MOD_ERROR_VAL;
    }

    Py_INCREF(&ExportType);
    PyModule_AddObject(m, "Export", (PyObject*)&ExportType);

    Py_INCREF(&LoadType);
    PyModule_AddObject(m, "Load", (PyObject*)&LoadType);
    return MOD_SUCCESS_VAL(m);
}

#ifdef __cplusplus
}
#endif
