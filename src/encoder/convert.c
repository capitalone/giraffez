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

#include "convert.h"
#include <Python.h>
#ifdef _WIN32
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>
#include "compat.h"
#include "util.h"


PyObject* byte_to_pylong(unsigned char** data) {
    int8_t b;
    unpack_int8_t(data, &b);
    return PyLong_FromLong((long)b);
}

PyObject* byte_to_pystring(unsigned char** data) {
    int8_t b;
    unpack_int8_t(data, &b);
    return PyUnicode_FromFormat("%d", b);
}

PyObject* char_to_pystring(unsigned char** data, const uint64_t column_length) {
    PyObject* str = PyUnicode_FromStringAndSize((char*)*data, column_length);
    *data += column_length;
    return str;
}

PyObject* date_to_pystring(unsigned char** data) {
    int32_t l, year, month, day;
    char s[11];
    unpack_int32_t(data, &l);
    l += 19000000;
    year = l / 10000;
    month = (l % 10000) / 100;
    day = l % 100;
    sprintf(s, "%04d-%02d-%02d", year, month, day);
    return PyUnicode_FromStringAndSize(s, 10);
}

static PyObject* decimal8_to_pystring(unsigned char** data, const uint16_t column_scale) {
    int8_t b;
    PyObject* s;
    unpack_int8_t(data, &b);
    if (column_scale > 0) {
        const char* fmt = b < 0 ? "-%d.%0*d" : "%d.%0*d";
        int8_t scale = (int8_t)pow(10, column_scale);
        int8_t x = abs(b / scale);
        int8_t y = abs(b % scale);
        s = pystring_from_cformat(fmt, x, column_scale, y);
    } else {
        s = PyUnicode_FromFormat("%d", b);
    }
    return s;
}

static PyObject* decimal16_to_pystring(unsigned char** data, const uint16_t column_scale) {
    int16_t h;
    PyObject* s;
    unpack_int16_t(data, &h);
    if (column_scale > 0) {
        const char* fmt = h < 0 ? "-%d.%0*d" : "%d.%0*d";
        int16_t scale = (int16_t)pow(10, column_scale);
        int16_t x = abs(h / scale);
        int16_t y = abs(h % scale);
        s = pystring_from_cformat(fmt, x, column_scale, y);
    } else {
        s = PyUnicode_FromFormat("%d", h);
    }
    return s;
}

static PyObject* decimal32_to_pystring(unsigned char** data, const uint16_t column_scale) {
    int32_t l;
    PyObject* s;
    unpack_int32_t(data, &l);
    if (column_scale > 0) {
        const char* fmt = l < 0 ? "-%d.%0*d" : "%d.%0*d";
        int32_t scale = (int32_t)pow(10, column_scale);
        int32_t x = labs(l / scale);
        int32_t y = labs(l % scale);
        s = pystring_from_cformat(fmt, x, column_scale, y);
    } else {
        s = PyUnicode_FromFormat("%d", l);
    }
    return s;
}

static PyObject* decimal64_to_pystring(unsigned char** data, const uint16_t column_scale) {
    int64_t q;
    PyObject* s;
    unpack_int64_t(data, &q);
    if (column_scale > 0) {
        const char* fmt = q < 0 ? "-%ld.%0*ld" : "%ld.%0*ld";
        int64_t scale = (int64_t)pow(10, column_scale);
        int64_t x = llabs(q / scale);
        int64_t y = llabs(q % scale);
        s = pystring_from_cformat(fmt, x, column_scale, y);
    } else {
        s = PyUnicode_FromFormat("%ld", q);
    }
    return s;
}

static PyObject* decimal128_to_pystring(unsigned char** data, const uint16_t column_scale) {
    int64_t q; uint64_t Q;
    PyObject* s;
    PyObject* shift;
    PyObject* upper;
    PyObject* lower;
    PyObject* us;
    PyObject* v;
    unpack_uint64_t(data, &Q);
    unpack_int64_t(data, &q);
    shift = PyLong_FromLong((long)64);
    upper = PyLong_FromLongLong(q);
    lower = PyLong_FromUnsignedLongLong(Q);
    us = PyNumber_Lshift(upper, shift);
    v = PyNumber_Or(us, lower);
    if (column_scale > 0) {
        PyObject* pw = PyLong_FromLong((long)10);
        PyObject* sd = PyLong_FromLong((long)column_scale);
        PyObject* scale = PyNumber_Power(pw, sd, Py_None);
        PyObject* x = PyNumber_FloorDivide(v, scale);
        PyObject* y = PyNumber_Remainder(v, scale);
        PyObject* fmt = PyUnicode_FromFormat("%%d.%%0%dd", column_scale);
        PyObject* tup = Py_BuildValue("(OO)", x, y);
        s = PyUnicode_Format(fmt, tup);
        Py_DECREF(v);
        Py_DECREF(sd);
        Py_DECREF(scale);
        Py_DECREF(x);
        Py_DECREF(y);
        Py_DECREF(fmt);
        Py_DECREF(tup);
    } else {
        s = PyObject_Str(v);
    }
    Py_XDECREF(shift);
    Py_XDECREF(upper);
    Py_XDECREF(lower);
    Py_XDECREF(us);
    return s;
}

PyObject* decimal_to_pystring(unsigned char** data, const uint64_t column_length,
        const uint16_t column_scale) {
    PyObject* s;
    switch (column_length) {
        case DECIMAL8:
            s = decimal8_to_pystring(data, column_scale);
            break;
        case DECIMAL16:
            s = decimal16_to_pystring(data, column_scale);
            break;
        case DECIMAL32:
            s = decimal32_to_pystring(data, column_scale);
            break;
        case DECIMAL64:
            s = decimal64_to_pystring(data, column_scale);
            break;
        case DECIMAL128:
            s = decimal128_to_pystring(data, column_scale);
            break;
        default:
            s = PyUnicode_FromStringAndSize("Err", 3);
    }
    return s;
}

PyObject* float_to_pyfloat(unsigned char** data) {
    double d;
    unpack_float(data, &d);
    return PyFloat_FromDouble(d);
}

PyObject* float_to_pystring(unsigned char** data) {
    return PyObject_Str(float_to_pyfloat(data));
}

PyObject* int_to_pylong(unsigned char** data) {
    int32_t l;
    unpack_int32_t(data, &l);
    return PyLong_FromLong(l);
}

PyObject* int_to_pystring(unsigned char** data) {
    int32_t l;
    unpack_int32_t(data, &l);
    return PyUnicode_FromFormat("%d", l);
}

PyObject* long_to_pylong(unsigned char** data) {
    int64_t q;
    unpack_int64_t(data, &q);
    return PyLong_FromLong((long)q);
}

PyObject* long_to_pystring(unsigned char** data) {
    int64_t q;
    unpack_int64_t(data, &q);
    return PyUnicode_FromFormat("%ld", q);
}

PyObject* pystring_from_cformat(const char* fmt, ...) {
    int length;
    char* ss;
    PyObject* s;
    va_list vargs;
    va_start(vargs, fmt);
    length = vsnprintf(NULL, 0, fmt, vargs);
    va_end(vargs);
    ss = malloc(sizeof(char)*length+1);
    va_start(vargs, fmt);
    vsprintf(ss, fmt, vargs);
    va_end(vargs);
    s = PyUnicode_FromStringAndSize(ss, length);
    free(ss);
    return s;
}

PyObject* short_to_pylong(unsigned char** data) {
    int16_t h;
    unpack_int16_t(data, &h);
    return PyLong_FromLong((long)h);
}

PyObject* short_to_pystring(unsigned char** data) {
    int16_t h;
    unpack_int16_t(data, &h);
    return PyUnicode_FromFormat("%d", h);
}

PyObject* vchar_to_pystring(unsigned char** data) {
    PyObject* str;
    uint16_t H;
    unpack_uint16_t(data, &H);
    str = PyUnicode_FromStringAndSize((char*)*data, H);
    *data += H;
    return str;
}
