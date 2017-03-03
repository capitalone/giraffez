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
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>

// Python 2/3 C API and Windows compatibility
#include "_compat.h"

#include "config.h"
#include "errors.h"
#include "pytypes.h"
#include "util.h"


static char buffer[BUFFER_SIZE];

// Character types
PyObject* teradata_char_to_pystring(unsigned char **data, const uint64_t column_length) {
    PyObject *str = PyUnicode_FromStringAndSize((char*)*data, column_length);
    *data += column_length;
    return str;
}

PyObject* teradata_vchar_to_pystring(unsigned char **data) {
    PyObject *str;
    uint16_t H;
    unpack_uint16_t(data, &H);
    str = PyUnicode_FromStringAndSize((char*)*data, H);
    *data += H;
    return str;
}

// Numeric types
PyObject* teradata_byteint_to_pylong(unsigned char **data) {
    int8_t b;
    unpack_int8_t(data, &b);
    return PyLong_FromLong((long)b);
}

PyObject* teradata_byteint_to_pystring(unsigned char **data) {
    int8_t b;
    unpack_int8_t(data, &b);
    return PyUnicode_FromFormat("%d", b);
}

PyObject* teradata_smallint_to_pylong(unsigned char **data) {
    int16_t h;
    unpack_int16_t(data, &h);
    return PyLong_FromLong((long)h);
}

PyObject* teradata_smallint_to_pystring(unsigned char **data) {
    int16_t h;
    unpack_int16_t(data, &h);
    return PyUnicode_FromFormat("%d", h);
}

PyObject* teradata_int_to_pylong(unsigned char **data) {
    int32_t l;
    unpack_int32_t(data, &l);
    return PyLong_FromLong(l);
}

PyObject* teradata_int_to_pystring(unsigned char **data) {
    int32_t l;
    unpack_int32_t(data, &l);
    return PyUnicode_FromFormat("%d", l);
}

PyObject* teradata_bigint_to_pylong(unsigned char **data) {
    int64_t q;
    unpack_int64_t(data, &q);
    return PyLong_FromLongLong(q);
}

PyObject* teradata_bigint_to_pystring(unsigned char **data) {
    int64_t q;
    unpack_int64_t(data, &q);
    // TODO: this might not be enough.
    return PyUnicode_FromFormat("%ld", q);
}

PyObject* teradata_float_to_pyfloat(unsigned char **data) {
    double d;
    unpack_float(data, &d);
    return PyFloat_FromDouble(d);
}

PyObject* teradata_float_to_pystring(unsigned char **data) {
    PyObject *obj;
    PyObject *s;
    obj = teradata_float_to_pyfloat(data);
    s = PyObject_Str(obj);
    Py_DECREF(obj);
    return s;
}

// Dates
int teradata_date_to_cstring(unsigned char **data, char *buf) {
    int32_t l, year, month, day;
    unpack_int32_t(data, &l);
    l += 19000000;
    year = l / 10000;
    month = (l % 10000) / 100;
    day = l % 100;
    return snprintf(buf, 11, "%04d-%02d-%02d", year, month, day);
}

PyObject* teradata_date_to_giraffez_date(unsigned char **data) {
    int32_t l, year, month, day;
    unpack_int32_t(data, &l);
    l += 19000000;
    year = l / 10000;
    month = (l % 10000) / 100;
    day = l % 100;
    return giraffez_date_from_datetime(year, month, day, 0, 0, 0, 0);
}

PyObject* teradata_date_to_pystring(unsigned char **data) {
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

// TODO: add switch for handling different types of common time/timestamp
// or maybe not.  could just allow passing a date format like pandas
PyObject* teradata_time_to_giraffez_time(unsigned char **data, const uint64_t column_length) {
    struct tm tm;
    memset(&tm, '\0', sizeof(tm));
    memcpy(buffer, (char*)*data, column_length);
    buffer[column_length] = '\0';
    if (strptime(buffer, "%H:%M:%S", &tm) != NULL) {
        *data += column_length;
        return giraffez_time_from_time(tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
    }
    return teradata_char_to_pystring(data, column_length);
}

PyObject* teradata_ts_to_giraffez_ts(unsigned char **data, const uint64_t column_length) {
    struct tm tm;
    memset(&tm, '\0', sizeof(tm));
    memcpy(buffer, (char*)*data, column_length);
    buffer[column_length] = '\0';
    if (strptime(buffer, "%Y-%m-%d %H:%M:%S", &tm) != NULL) {
        *data += column_length;
        return giraffez_ts_from_datetime(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour,
            tm.tm_min, tm.tm_sec, 0);
    }
    return teradata_char_to_pystring(data, column_length);
}

// Decimal
int teradata_decimal_to_cstring(unsigned char **data, const uint64_t column_length,
        const uint16_t column_scale, char *buf) {
    switch (column_length) {
        case DECIMAL8:
            return teradata_decimal8_to_cstring(data, column_scale, buf);
        case DECIMAL16:
            return teradata_decimal16_to_cstring(data, column_scale, buf);
        case DECIMAL32:
            return teradata_decimal32_to_cstring(data, column_scale, buf);
        case DECIMAL64:
            return teradata_decimal64_to_cstring(data, column_scale, buf);
        case DECIMAL128:
            return teradata_decimal128_to_cstring(data, column_scale, buf);
    }
    return -1;
}

int teradata_decimal8_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf) {
    int8_t b, x, y, scale;
    unpack_int8_t(data, &b);
    if (column_scale > 0) {
        const char *fmt = b < 0 ? "-%d.%0*d" : "%d.%0*d";
        scale = (int8_t)pow(10, column_scale);
        x = abs(b / scale);
        y = abs(b % scale);
        return sprintf(buf, fmt, x, column_scale, y);
    } else {
        return sprintf(buf, "%d", b);
    }
    return -1;
}

int teradata_decimal16_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf) {
    int16_t h, x, y, scale;
    unpack_int16_t(data, &h);
    if (column_scale > 0) {
        const char *fmt = h < 0 ? "-%d.%0*d" : "%d.%0*d";
        scale = (int16_t)pow(10, column_scale);
        x = abs(h / scale);
        y = abs(h % scale);
        return sprintf(buf, fmt, x, column_scale, y);
    } else {
        return sprintf(buf, "%d", h);
    }
    return -1;
}

int teradata_decimal32_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf) {
    int32_t l, x, y, scale;
    unpack_int32_t(data, &l);
    if (column_scale > 0) {
        const char *fmt = l < 0 ? "-%d.%0*d" : "%d.%0*d";
        scale = (int32_t)pow(10, column_scale);
        x = labs(l / scale);
        y = labs(l % scale);
        return sprintf(buf, fmt, x, column_scale, y);
    } else {
        return sprintf(buf, "%d", l);
    }
    return -1;
}

int teradata_decimal64_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf) {
    int64_t q, x, y, scale;
    unpack_int64_t(data, &q);
    if (column_scale > 0) {
        const char *fmt = q < 0 ? "-%lld.%0*lld" : "%lld.%0*lld";
        scale = (int64_t)pow(10, column_scale);
        x = labs(q / scale);
        y = labs(q % scale);
        return sprintf(buf, fmt, x, column_scale, y);
    } else {
        return sprintf(buf, "%ld", q);
    }
    return -1;
}


int teradata_decimal128_to_cstring(unsigned char **data, const uint16_t column_scale, char *buf) {
    char s[38];
    int i = 0, j = 0;
    uint64_t complement, sign, r, d, Q;
    int64_t q;
    uint64_t n[4];
    unpack_uint64_t(data, &Q);
    unpack_int64_t(data, &q);
    sign = (q >> 63);
    complement = q;
    if (sign) {
        complement = ~complement;
        Q = ~Q + 1;
        if (Q == 0) {
            complement += 1;
        }
    }
    n[0] = complement >> 32;
    n[1] = complement & 0xffffffff;
    n[2] = (Q >> 32) & 0xffffffff;
    n[3] = Q & 0xffffffff;
    memset(s, 0, sizeof(s));
    do {
        r = n[0];
        d = r / 10;
        r = ((r - d * 10) << 32) + n[1];
        n[0] = d;
        d = r / 10;
        r = ((r - d * 10) << 32) + n[2];
        n[1] = d;
        d = r / 10;
        r = ((r - d * 10) << 32) + n[3];
        n[2] = d;
        d = r / 10;
        r = r - d * 10;
        n[3] = d;
        s[i] = r + '0';
        i++;
    } while (n[0] || n[1] || n[2] || n[3]);
    if (sign) {
        buf[j++] = '-';
    }
    while (i != 0) {
        if (i == column_scale) {
            buf[j++] = '.';
        }
        buf[j++] = s[--i];
    }
    buf[j] = '\0';
    return j;
}

PyObject* cstring_to_pyfloat(const char *buf, const int length) {
    return PyFloat_FromString(cstring_to_pystring(buf, length));
}

PyObject* cstring_to_giraffez_decimal(const char *buf, const int length) {
    return giraffez_decimal_from_pystring(cstring_to_pystring(buf, length));
}

inline PyObject* cstring_to_pystring(const char *buf, const int length) {
    return PyUnicode_FromStringAndSize(buf, length);
}

PyObject* pystring_from_cformat(const char *fmt, ...) {
    int length;
    // TODO: is this better than using malloc like before and does it 
    // need to be this big.
    char ss[1024];
    va_list vargs;
    va_start(vargs, fmt);
    length = vsprintf(ss, fmt, vargs);
    va_end(vargs);
    return PyUnicode_FromStringAndSize(ss, length);
}


// PACK
PyObject* teradata_varchar_from_pystring(PyObject *s, unsigned char **buf, uint16_t *len) {
    Py_ssize_t length;
    PyObject *temp = NULL;
    char *str;
    // TODO: should check for max length?
    if (_PyUnicode_Check(s)) {
        if ((temp = PyUnicode_AsUTF8String(s)) == NULL) {
            if ((temp = PyUnicode_AsUTF8String(s)) == NULL) {
                printf("OOOH NO\n");
                return NULL;
            }
        }
    } else if (PyBytes_Check(s)) {
        temp = s;
    } else {
        // TODO:
    }
    // XXX:
    if (temp == NULL) {
        return NULL;
    }
    if ((str = PyBytes_AsString(temp)) == NULL) {
        return NULL;
    }
    length = PyBytes_Size(temp);
    *len += pack_string(buf, str, length);
    /*Py_DECREF(temp);*/
    Py_RETURN_NONE;
}

PyObject* teradata_char_from_pystring(PyObject *s, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    Py_ssize_t length;
    PyObject *temp = NULL;
    int fill, i;
    char *str;
    // TODO: should check for max length?
    if (_PyUnicode_Check(s)) {
        temp = PyUnicode_AsASCIIString(s);
    } else if (PyBytes_Check(s)) {
        temp = s;
    }
    length = PyBytes_Size(temp);
    if ((str = PyBytes_AsString(temp)) == NULL) {
        return NULL;
    }
    memcpy(*buf, str, length);
    *buf += length;
    fill = column_length - length;
    for (i = 0; i < fill; i++) {
        *((*buf)++) = (unsigned char)0x20;
    }
    /*printf("char cl: %d\n", column_length);*/
    *len += column_length;
    /*Py_DECREF(temp);*/
    Py_RETURN_NONE;
}

PyObject* teradata_byte_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    int8_t b;
    if (!PyLong_Check(item)) {
        return NULL;
    }
    b = PyLong_AsLong(item);
    if (b == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int8_t(buf, b);
    *len += column_length;
    Py_RETURN_NONE;
}

PyObject* teradata_short_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    int16_t h;
    if (!PyLong_Check(item)) {
        return NULL;
    }
    h = PyLong_AsLong(item);
    if (h == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int16_t(buf, h);
    *len += column_length;
    Py_RETURN_NONE;
}

PyObject* teradata_int_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    int32_t l;
    if (!PyLong_Check(item)) {
        return NULL;
    }
    l = PyLong_AsLong(item);
    if (l == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int32_t(buf, l);
    *len += column_length;
    Py_RETURN_NONE;
}

PyObject* teradata_long_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    int64_t q;
    if (!PyLong_Check(item)) {
        return NULL;
    }
    q = PyLong_AsLong(item);
    if (q == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int64_t(buf, q);
    *len += column_length;
    Py_RETURN_NONE;
}

PyObject* teradata_float_from_pyfloat(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    double d;
    PyObject *f;
    PyObject *s;
    if (!PyFloat_Check(item)) {
        if (PyUnicode_Check(item)) {
            if ((s = PyUnicode_AsASCIIString(item)) == NULL) {
                return NULL;
            }
        } else {
            return NULL;
        }

        if ((f = _PyFloat_FromString(s)) == NULL) {
            return NULL;
        }
    } else {
        f = item;
    }
    d = PyFloat_AsDouble(f);
    if (d == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
    pack_float(buf, d);
    *len += column_length;
    Py_RETURN_NONE;
}

PyObject* teradata_int_from_pydate(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *len) {
    // check unicode/string and datetime
    char *str;
    struct tm tm;
    int32_t l = 0;

    // TODO: do we want to check for PyDate/PyDateTime etc here? if so we have to include datetime.h and do PyDateTime_IMPORT
    if ((str = PyUnicode_AsUTF8(item)) == NULL) {
        return NULL;
    }
    memset(&tm, '\0', sizeof(tm));
    // TODO: while over format strings
    if (strptime(str, "%Y-%m-%d", &tm) == NULL) {
        PyErr_Format(EncoderError, "Unable to parse date string '%s'", str);
        return NULL;
    }
    l += (tm.tm_year+1900) * 10000;
    l += (tm.tm_mon+1) * 100;
    l += tm.tm_mday;
    l -= 19000000;
    pack_int32_t(buf, l);
    *len += 4;
    Py_RETURN_NONE;
}


PyObject* teradata_decimal_from_pystring(PyObject *item, const uint16_t column_length, const uint16_t column_scale, unsigned char **buf, uint16_t *len) {
    int8_t b; int16_t h; int32_t l; int64_t q; uint64_t Q;
    char dbuf[ITEM_BUFFER_SIZE];
    PyObject *str = NULL;
    PyObject *dot = PyUnicode_FromString(".");
    // CheckUnicode/String
    PyObject *parts;
    if (PyUnicode_Check(item)) {
        str = item;
        Py_INCREF(str);
    } else {
        if ((str = PyObject_Str(item)) == NULL) {
            return NULL;
        }
    }
    if ((parts = PyUnicode_Split(str, dot, 1)) == NULL) {
        return NULL;
    }
    Py_DECREF(dot);

    Py_ssize_t ll = PyList_Size(parts);
    const char *x = "", *y = "";
    PyObject *a, *s;
    if (ll == 1) {
        Py_RETURN_ERROR(a = PyList_GetItem(parts, 0));
        if ((x = PyUnicode_AsUTF8(a)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
        y = "";
    } else if (ll == 2) {
        Py_RETURN_ERROR(a = PyList_GetItem(parts, 0));
        Py_RETURN_ERROR(x = PyUnicode_AsUTF8(a));

        Py_RETURN_ERROR(a = PyList_GetItem(parts, 1));
        Py_RETURN_ERROR(y = PyUnicode_AsUTF8(a));
    }

    int x_length = strlen(x);
    int y_length = strlen(y);
    int padding_needed = column_scale - y_length;

    memcpy(dbuf, x, x_length);
    if (padding_needed > 0) {
        memcpy(dbuf+x_length, y, y_length);
        memset(dbuf+x_length+y_length, '0', padding_needed);
        dbuf[x_length+column_scale] = '\0';
    } else {
        snprintf(dbuf+x_length, column_scale + 1, "%s", y);
    }

    Py_DECREF(parts);

    if ((s = PyLong_FromString(dbuf, NULL, 10)) == NULL) {
        PyErr_Clear();
        PyErr_Format(EncoderError, "value is not a valid decimal: %R", str);
        Py_XDECREF(str);
        return NULL;
    }
    Py_XDECREF(str);

    PyObject *upper, *lower, *lower1, *shift;
    shift = PyLong_FromLong(64);
    switch (column_length) {
        case DECIMAL8:
            b = PyLong_AsLongLong(s);
            if (PyErr_Occurred()) {
                return NULL;
            }
            pack_int8_t(buf, b);
            break;
        case DECIMAL16:
            h = PyLong_AsLongLong(s);
            if (PyErr_Occurred()) {
                return NULL;
            }
            pack_int16_t(buf, h);
            break;
        case DECIMAL32:
            l = PyLong_AsLongLong(s);
            if (PyErr_Occurred()) {
                return NULL;
            }
            pack_int32_t(buf, l);
            break;
        case DECIMAL64:
            q = PyLong_AsLongLong(s);
            if (PyErr_Occurred()) {
                return NULL;
            }
            pack_int64_t(buf, q);
            break;
        case DECIMAL128:
            if ((upper = PyNumber_Rshift(s, shift)) == NULL) {
                return NULL;
            }
            if ((lower1 = PyNumber_Lshift(upper, shift)) == NULL) {
                return NULL;
            }
            if ((lower = PyNumber_Xor(lower1, s)) == NULL) {
                return NULL;
            }
            Q = PyLong_AsUnsignedLongLong(lower);
            if (PyErr_Occurred()) {
                return NULL;
            }
            q = PyLong_AsLongLong(upper);
            if (PyErr_Occurred()) {
                return NULL;
            }
            pack_uint64_t(buf, Q);
            pack_int64_t(buf, q);
            Py_DECREF(upper);
            Py_DECREF(lower);
            Py_DECREF(lower1);
            break;
    }
    Py_DECREF(shift);
    Py_DECREF(s);
    *len += column_length;
    Py_RETURN_NONE;
}
