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

#ifndef __GIRAFFEZ_COMPAT_H
#define __GIRAFFEZ_COMPAT_H

#ifndef PyVarObject_HEAD_INIT
    #define PyVarObject_HEAD_INIT(type, size) \
        PyObject_HEAD_INIT(type) size,
#endif

#ifndef Py_TYPE
    #define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#ifndef Py_SETREF
  #define Py_SETREF(op, op2)                      \
      do {                                        \
          PyObject *_py_tmp = (PyObject *)(op);   \
          (op) = (op2);                           \
          Py_DECREF(_py_tmp);                     \
      } while (0)
#endif
#ifndef Py_XSETREF
  #define Py_XSETREF(op, op2)                     \
      do {                                        \
          PyObject *_py_tmp = (PyObject *)(op);   \
          (op) = (op2);                           \
          Py_XDECREF(_py_tmp);                    \
      } while (0)
#endif

#if PY_MAJOR_VERSION >= 3
  #define MOD_ERROR_VAL NULL
  #define MOD_SUCCESS_VAL(val) val
  #define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
  #define MOD_DEF(ob, name, doc, methods) \
          static struct PyModuleDef moduledef = { \
            PyModuleDef_HEAD_INIT, name, doc, -1, methods, }; \
          ob = PyModule_Create(&moduledef);

  #define Py_TPFLAGS_HAVE_ITER 0
  #define MOD_ERROR_VAL NULL
  #define PyStr_Check(ob) PyUnicode_Check(ob)
  #define _PyLong_Check(ob) PyLong_Check(ob)
  #define _PyFloat_FromString(ob) PyFloat_FromString(ob)

  #define TEXT_T Py_UNICODE
#else
  #define PyUnicode_AsUTF8 PyString_AsString
  #define PyUnicode_AsUTF8AndSize(pystr, sizeptr) \
    ((*sizeptr=PyString_Size(pystr)), PyString_AsString(pystr))
  #define PyStr_Check(ob) (PyString_Check(ob) || PyUnicode_Check(ob))

  #define PyBytes_Check PyString_Check
  #define PyBytes_FromStringAndSize PyString_FromStringAndSize

  #define PyNumber_FloorDivide PyNumber_Divide

  #define _PyLong_Check(ob) (PyLong_Check(ob) || PyInt_Check(ob))
  #define _PyFloat_FromString(ob) PyFloat_FromString(ob, NULL)
  #define PyLong_FromUnicodeObject(ob, b) (PyUnicode_Check(ob) ? \
      PyLong_FromUnicode(PyUnicode_AS_UNICODE(ob), PyUnicode_GetSize(ob), b) : \
      PyLong_FromString(PyString_AsString(ob), NULL, b))

  #define PyException_HEAD PyObject_HEAD PyObject *dict;\
             PyObject *args; PyObject *traceback;\
             PyObject *context; PyObject *cause;\
             char suppress_context;

  #define MOD_ERROR_VAL
  #define MOD_SUCCESS_VAL(val)
  #define MOD_INIT(name) void init##name(void)
  #define MOD_DEF(ob, name, doc, methods) \
          ob = Py_InitModule3(name, methods, doc);
#endif    


#if defined(WIN32) || defined(WIN64)
#define strdup _strdup
#define snprintf _snprintf
#define llabs _abs64
#endif

#endif
