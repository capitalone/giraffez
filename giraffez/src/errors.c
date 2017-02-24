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

#include "errors.h"

#include <Python.h>
#include <string.h>


PyObject *GiraffeError;
PyObject *EndStatementError;
PyObject *EndRequestError;

char* concat(const char *s1, const char *s2) {
    const char *sep = ".";
    char *result = (char*)malloc(sizeof(char)*(strlen(s1)+strlen(s2)+strlen(sep)+1));
    strcpy(result, s1);
    strcat(result, sep);
    strcat(result, s2);
    return result;
}

void debug_printf(const char *fmt, ...) {
    PyObject *s;
    va_list vargs;
    va_start(vargs, fmt);
    s = PyUnicode_FromFormatV(fmt, vargs);
    va_end(vargs);
    fprintf(stderr, "DEBUG: %s\n", PyUnicode_AsUTF8(s));
}

void define_exceptions(const char *name, PyObject *module) {
    GiraffeError = PyErr_NewException(concat(name, "error"), NULL, NULL);
    PyModule_AddObject(module, "error", GiraffeError);
    EndStatementError = PyErr_NewException(concat(name, "StatementEnded"), NULL, NULL);
    PyModule_AddObject(module, "StatementEnded", EndStatementError);
    EndRequestError = PyErr_NewException(concat(name, "RequestEnded"), NULL, NULL);
    PyModule_AddObject(module, "RequestEnded", EndRequestError);
}
