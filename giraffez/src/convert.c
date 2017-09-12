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

#include "common.h"
#include "columns.h"

#include "convert.h"


void pack_int8_t(unsigned char **data, int8_t val) {
    *((*data)++) = val;
}

void pack_int16_t(unsigned char **data, int16_t val) {
    *((*data)++) = val;
    *((*data)++) = val >> 8;
}

void pack_uint16_t(unsigned char **data, uint16_t val) {
    *((*data)++) = val;
    *((*data)++) = val >> 8;
}

void pack_int32_t(unsigned char **data, int32_t val) {
    *((*data)++) = val;
    *((*data)++) = val >> 8;
    *((*data)++) = val >> 16;
    *((*data)++) = val >> 24;
}

void pack_uint32_t(unsigned char **data, uint32_t val) {
    *((*data)++) = val;
    *((*data)++) = val >> 8;
    *((*data)++) = val >> 16;
    *((*data)++) = val >> 24;
}

void pack_int64_t(unsigned char **data, int64_t val) {
    *((*data)++) = val;
    *((*data)++) = val >> 8;
    *((*data)++) = val >> 16;
    *((*data)++) = val >> 24;
    *((*data)++) = val >> 32;
    *((*data)++) = val >> 40;
    *((*data)++) = val >> 48;
    *((*data)++) = val >> 56;
}

void pack_uint64_t(unsigned char **data, uint64_t val) {
    *((*data)++) = val;
    *((*data)++) = val >> 8;
    *((*data)++) = val >> 16;
    *((*data)++) = val >> 24;
    *((*data)++) = val >> 32;
    *((*data)++) = val >> 40;
    *((*data)++) = val >> 48;
    *((*data)++) = val >> 56;
}

void pack_float(unsigned char **data, double val) {
    dsplit_t u;

    u.d = val;

    *((*data)++) = u.b[0];
    *((*data)++) = u.b[1];
    *((*data)++) = u.b[2];
    *((*data)++) = u.b[3];
    *((*data)++) = u.b[4];
    *((*data)++) = u.b[5];
    *((*data)++) = u.b[6];
    *((*data)++) = u.b[7];
}

uint16_t pack_string(unsigned char **data, const char *str, const uint16_t length) {
    pack_uint16_t(data, length);
    if (length > 0) {
        memcpy(*data, str, sizeof(unsigned char)*length);
        *data += length;
        return length + 2;
    }
    return 2;
}

void unpack_int8_t(unsigned char **data, int8_t *dst) {
    *dst = *((*data)++);
}

void unpack_int16_t(unsigned char **data, int16_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffLL) << 8;
}

void unpack_uint16_t(unsigned char **data, uint16_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffULL) << 8;
}

void unpack_int32_t(unsigned char **data, int32_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffLL) << 8;
    *dst |= (*((*data)++) & 0xffLL) << 16;
    *dst |= (*((*data)++) & 0xffLL) << 24;
}

void unpack_int64_t(unsigned char **data, int64_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffLL) << 8;
    *dst |= (*((*data)++) & 0xffLL) << 16;
    *dst |= (*((*data)++) & 0xffLL) << 24;
    *dst |= (*((*data)++) & 0xffLL) << 32;
    *dst |= (*((*data)++) & 0xffLL) << 40;
    *dst |= (*((*data)++) & 0xffLL) << 48;
    *dst |= (*((*data)++) & 0xffLL) << 56;
}

void unpack_uint64_t(unsigned char **data, uint64_t *dst) {
    *dst = *((*data)++);
    *dst |= (*((*data)++) & 0xffULL) << 8;
    *dst |= (*((*data)++) & 0xffULL) << 16;
    *dst |= (*((*data)++) & 0xffULL) << 24;
    *dst |= (*((*data)++) & 0xffULL) << 32;
    *dst |= (*((*data)++) & 0xffULL) << 40;
    *dst |= (*((*data)++) & 0xffULL) << 48;
    *dst |= (*((*data)++) & 0xffULL) << 56;
}

void unpack_float(unsigned char **data, double *dst) {
    dsplit_t u;

    u.b[0] = *((*data)++);
    u.b[1] = *((*data)++);
    u.b[2] = *((*data)++);
    u.b[3] = *((*data)++);
    u.b[4] = *((*data)++);
    u.b[5] = *((*data)++);
    u.b[6] = *((*data)++);
    u.b[7] = *((*data)++);
    *dst = u.d;
}

void unpack_char(unsigned char **data, char **str) {
    char *dest = (char*)malloc(sizeof(char)+1);
    memcpy(dest, (char*)*data, 1);
    dest[1] = '\0';
    *str = dest;
    *data += 1;
}

void unpack_uchar(unsigned char **data, unsigned char **str) {
    unsigned char* dest = (unsigned char*)malloc(sizeof(unsigned char)+1);
    memcpy(dest, (unsigned char*)*data, 1);
    dest[1] = '\0';
    *str = dest;
    *data += 1;
}

uint16_t unpack_string(unsigned char **data, char **str) {
    uint16_t len = 0;
    unpack_uint16_t(data, &len);
    if (len > 0) {
        char *dst = (char*)malloc(len+1);
        memcpy(dst, (char*)*data, len);
        dst[len] = '\0';
        *str = dst;
        *data += len;
    } else {
        *str = (char*)malloc(1);
        (*str)[0] = 0;
    }
    return len;
}

// Character types
PyObject* teradata_char_to_pystring(unsigned char **data, const uint64_t column_length) {
    PyObject *str = PyUnicode_FromStringAndSize((char*)*data, column_length);
    *data += column_length;
    return str;
}

PyObject* teradata_char_to_pystring_f(unsigned char **data, const uint64_t column_length, const uint64_t format_length) {
    PyObject *str = teradata_char_to_pystring(data, column_length);
    if (format_length > 0 && format_length <= column_length) {
        PyObject *slice = PySlice_New(PyLong_FromLong(0),
                PyLong_FromUnsignedLongLong(format_length), PyLong_FromLong(1));
        Py_SETREF(str, PyObject_GetItem(str, slice));
        Py_DECREF(slice);
    }
    return str;
}

PyObject* teradata_byte_to_pybytes(unsigned char **data, const uint64_t column_length) {
    PyObject *str = PyBytes_FromStringAndSize((char*)*data, column_length);
    *data += column_length;
    return str;
}

PyObject* teradata_varchar_to_pystring(unsigned char **data) {
    PyObject *str;
    uint16_t H;
    unpack_uint16_t(data, &H);
    str = PyUnicode_FromStringAndSize((char*)*data, H);
    *data += H;
    return str;
}

PyObject* teradata_varbyte_to_pybytes(unsigned char **data) {
    PyObject *str;
    uint16_t H;
    unpack_uint16_t(data, &H);
    str = PyBytes_FromStringAndSize((char*)*data, H);
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
    return pystring_from_cformat("%lld", q);
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
    char buffer[BUFFER_STRPTIME_SIZE];
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
    char buffer[BUFFER_STRPTIME_SIZE];
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
#ifdef _MSC_VER
        const char *fmt = q < 0 ? "-%I64d.%0*I64d" : "%I64d.%0*I64d";
#else
        const char *fmt = q < 0 ? "-%lld.%0*lld" : "%lld.%0*lld";
#endif
        scale = (int64_t)pow(10, column_scale);
        x = llabs(q / scale);
        y = llabs(q % scale);
        return sprintf(buf, fmt, x, column_scale, y);
    } else {
#ifdef _MSC_VER
        return sprintf(buf, "%I64d", q);
#else
        return sprintf(buf, "%ld", q);
#endif
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
    } while (i <= column_scale || n[0] || n[1] || n[2] || n[3]);
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
    return _PyFloat_FromString(cstring_to_pystring(buf, length));
}

PyObject* cstring_to_giraffez_decimal(const char *buf, const int length) {
    return giraffez_decimal_from_pystring(cstring_to_pystring(buf, length));
}

PyObject* cstring_to_pystring(const char *buf, const int length) {
    return PyUnicode_FromStringAndSize(buf, length);
}

PyObject* pystring_from_cformat(const char *fmt, ...) {
    int length;
    char ss[BUFFER_ITEM_SIZE];
    va_list vargs;
    va_start(vargs, fmt);
    length = vsnprintf(ss, BUFFER_ITEM_SIZE, fmt, vargs);
    va_end(vargs);
    return PyUnicode_FromStringAndSize(ss, length);
}

// PACK
PyObject* teradata_varchar_from_pystring(PyObject *s, unsigned char **buf, uint16_t *packed_length) {
    const char *str;
    Py_ssize_t length;
    PyObject *stmp = NULL, *utmp = NULL;
    if (!PyUnicode_Check(s)) {
        if (PyBytes_Check(s)) {
            if ((utmp = PyUnicode_FromEncodedObject(s, "UTF-8", NULL)) == NULL) {
                goto error;
            }
            s = utmp;
        } else {
            PyErr_Format(EncoderError, "Expected string type and received '%s'", Py_TYPE(s)->tp_name);
            goto error;
        }
    }
#if PY_MAJOR_VERSION < 3
    if ((stmp = PyUnicode_AsEncodedString(s, "UTF-8", NULL)) == NULL) {
        goto error;
    }
    s = stmp;
#endif
    if ((str = PyUnicode_AsUTF8AndSize(s, &length)) == NULL) {
        goto error;
    }
    if (length > TD_ROW_MAX_SIZE) {
        PyErr_Format(EncoderError, "VARCHAR field value length %ld exceeds maximum allowed.", length);
        goto error;
    }
    *packed_length += pack_string(buf, str, length);
    Py_XDECREF(stmp);
    Py_XDECREF(utmp);
    Py_RETURN_NONE;
error:
    Py_XDECREF(stmp);
    Py_XDECREF(utmp);
    return NULL;
}

PyObject* teradata_datetime_from_pystring(PyObject *s, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    PyObject *ret = NULL, *temp = NULL;
    if (!PyUnicode_Check(s) && !PyBytes_Check(s)) {
        Py_RETURN_ERROR((temp = PyObject_Str(s)));
        s = temp;
    }
    ret = teradata_char_from_pystring(s, column_length, buf, packed_length);
    Py_XDECREF(temp);
    return ret;
}

PyObject* teradata_char_from_pystring(PyObject *s, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    int fill;
    const char *str;
    Py_ssize_t length;
    PyObject *stmp = NULL, *utmp = NULL;
    if (!PyUnicode_Check(s)) {
        if (PyBytes_Check(s)) {
            if ((utmp = PyUnicode_FromEncodedObject(s, "UTF-8", NULL)) == NULL) {
                goto error;
            }
            s = utmp;
        } else {
            PyErr_Format(EncoderError, "Expected string type and received '%s'", Py_TYPE(s)->tp_name);
            goto error;
        }
    }
#if PY_MAJOR_VERSION < 3
    if ((stmp = PyUnicode_AsEncodedString(s, "UTF-8", NULL)) == NULL) {
        goto error;
    }
    s = stmp;
#endif
    if ((str = PyUnicode_AsUTF8AndSize(s, &length)) == NULL) {
        goto error;
    }
    if (length > TD_ROW_MAX_SIZE) {
        PyErr_Format(EncoderError, "CHAR field value length %ld exceeds maximum allowed.", length);
        goto error;
    } else if (length > column_length) {
        PyErr_Format(EncoderError, "CHAR field value length %ld exceeds column length %d.", length, column_length);
        goto error;
    }
    memcpy(*buf, str, length);
    *buf += length;
    fill = column_length - length;
    memset(*buf, 0x20, fill);
    *buf += fill;
    *packed_length += column_length;
    Py_XDECREF(stmp);
    Py_XDECREF(utmp);
    Py_RETURN_NONE;
error:
    Py_XDECREF(stmp);
    Py_XDECREF(utmp);
    return NULL;
}

PyObject* teradata_byteint_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    PyObject *temp = NULL;
    int8_t b;
    if (!_PyLong_Check(item)) {
        if (!PyUnicode_Check(item)) {
            PyErr_Format(EncoderError, "Expected integer/string type and received '%s'", Py_TYPE(item)->tp_name);
            return NULL;
        }
        Py_RETURN_ERROR(temp = PyLong_FromUnicodeObject(item, 10));
    } else {
        temp = item;
        Py_INCREF(temp);
    }
    b = PyLong_AsLong(temp);
    if (b == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int8_t(buf, b);
    *packed_length += column_length;
    Py_DECREF(temp);
    Py_RETURN_NONE;
}

PyObject* teradata_smallint_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    PyObject *temp = NULL;
    int16_t h;
    if (!_PyLong_Check(item)) {
        if (!PyUnicode_Check(item)) {
            PyErr_Format(EncoderError, "Expected integer/string type and received '%s'", Py_TYPE(item)->tp_name);
            return NULL;
        }
        Py_RETURN_ERROR(temp = PyLong_FromUnicodeObject(item, 10));
    } else {
        temp = item;
        Py_INCREF(temp);
    }
    h = PyLong_AsLong(temp);
    if (h == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int16_t(buf, h);
    *packed_length += column_length;
    Py_DECREF(temp);
    Py_RETURN_NONE;
}

PyObject* teradata_int_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    PyObject *temp = NULL;
    int32_t l;
    if (!_PyLong_Check(item)) {
        if (!PyUnicode_Check(item)) {
            PyErr_Format(EncoderError, "Expected integer/string type and received '%s'", Py_TYPE(item)->tp_name);
            return NULL;
        }
        Py_RETURN_ERROR(temp = PyLong_FromUnicodeObject(item, 10));
    } else {
        temp = item;
        Py_INCREF(temp);
    }
    l = PyLong_AsLong(temp);
    if (l == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int32_t(buf, l);
    *packed_length += column_length;
    Py_DECREF(temp);
    Py_RETURN_NONE;
}

PyObject* teradata_bigint_from_pylong(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    PyObject *temp = NULL;
    int64_t q;
    if (!_PyLong_Check(item)) {
        if (!PyUnicode_Check(item)) {
            PyErr_Format(EncoderError, "Expected integer/string type and received '%s'", Py_TYPE(item)->tp_name);
            return NULL;
        }
        Py_RETURN_ERROR(temp = PyLong_FromUnicodeObject(item, 10));
    } else {
        temp = item;
        Py_INCREF(temp);
    }
    q = PyLong_AsLongLong(temp);
    if (q == (long)-1 && PyErr_Occurred()) {
        return NULL;
    }
    pack_int64_t(buf, q);
    *packed_length += column_length;
    Py_DECREF(temp);
    Py_RETURN_NONE;
}

PyObject* teradata_float_from_pyfloat(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
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
    *packed_length += column_length;
    Py_RETURN_NONE;
}

PyObject* teradata_dateint_from_pystring(PyObject *item, const uint16_t column_length, unsigned char **buf, uint16_t *packed_length) {
    PyObject *temp;
    char *str;
    struct tm tm;
    int32_t l = 0;
    
    if (PyStr_Check(item)) {
        Py_RETURN_ERROR((str = PyUnicode_AsUTF8(item)));
    } else {
        Py_RETURN_ERROR((temp = PyObject_Str(item)));
        Py_RETURN_ERROR((str = PyUnicode_AsUTF8(temp)));
        Py_DECREF(temp);
    }


    memset(&tm, '\0', sizeof(tm));
    if (strptime(str, "%Y-%m-%d", &tm) == NULL) {
        PyErr_Format(EncoderError, "Unable to parse date string '%s', format must be '%%Y-%%m-%%d'.", str);
        return NULL;
    }
    l += (tm.tm_year+1900) * 10000;
    l += (tm.tm_mon+1) * 100;
    l += tm.tm_mday;

    pack_int32_t(buf, l - 19000000);
    *packed_length += 4;
    Py_RETURN_NONE;
}

PyObject* teradata_decimal_from_pystring(PyObject *item, const uint16_t column_length, const uint16_t column_scale, unsigned char **buf, uint16_t *packed_length) {
    int8_t b; int16_t h; int32_t l; int64_t q; uint64_t Q;
    char decbuf[BUFFER_ITEM_SIZE];
    PyObject *str = NULL;
    PyObject *dot = PyUnicode_FromString(".");
    PyObject *parts;
    Py_ssize_t ll;
    const char *x = "", *y = "";
    PyObject *a, *s;
    PyObject *upper, *lower, *lower1, *shift;
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
    ll = PyList_Size(parts);
    if (ll == 1) {
        if ((a = PyList_GetItem(parts, 0)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
        if ((x = PyUnicode_AsUTF8(a)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
    } else if (ll == 2) {
        if ((a = PyList_GetItem(parts, 0)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
        if ((x = PyUnicode_AsUTF8(a)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
        if ((a = PyList_GetItem(parts, 1)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
        if ((y = PyUnicode_AsUTF8(a)) == NULL) {
            Py_XDECREF(str);
            return NULL;
        }
    }
    memcpy(decbuf, x, strlen(x));
    if (column_scale > strlen(y)) {
        memcpy(decbuf+strlen(x), y, strlen(y));
        memset(decbuf+strlen(x)+strlen(y), '0', column_scale - strlen(y));
        decbuf[strlen(x)+column_scale] = '\0';
    } else {
        memcpy(decbuf+strlen(x), y, column_scale);
        decbuf[strlen(x)+column_scale] = '\0';
    }
    Py_DECREF(parts);
    if ((s = PyLong_FromString(decbuf, NULL, 10)) == NULL) {
        PyObject *repr = PyObject_Repr(str);
        PyErr_Clear();
        PyErr_Format(EncoderError, "value is not a valid decimal: %s", PyUnicode_AsUTF8(repr));
        Py_XDECREF(repr);
        Py_XDECREF(str);
        return NULL;
    }
    Py_XDECREF(str);
    shift = PyLong_FromLong(64);
    switch (column_length) {
        case DECIMAL8:
            b = PyLong_AsLongLong(s);
            if (b == -1 && PyErr_Occurred()) {
                return NULL;
            }
            pack_int8_t(buf, b);
            break;
        case DECIMAL16:
            h = PyLong_AsLongLong(s);
            if (h == -1 && PyErr_Occurred()) {
                return NULL;
            }
            pack_int16_t(buf, h);
            break;
        case DECIMAL32:
            l = PyLong_AsLongLong(s);
            if (l == -1 && PyErr_Occurred()) {
                return NULL;
            }
            pack_int32_t(buf, l);
            break;
        case DECIMAL64:
            q = PyLong_AsLongLong(s);
            if (q == -1 && PyErr_Occurred()) {
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
            if (q == -1 && PyErr_Occurred()) {
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
    *packed_length += column_length;
    Py_RETURN_NONE;
}

static PyObject *ColumnsType;
static PyObject *DecimalType;
static PyObject *DateType;
static PyObject *TimeType;
static PyObject *TimestampType;


int giraffez_types_import() {
    PyObject *mod;
    if ((mod = PyImport_ImportModule("giraffez.types")) == NULL) {
        return -1;
    };
    if ((ColumnsType = PyObject_GetAttrString(mod, "Columns")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Columns into C extension");
        return -1;

    }
    if ((DateType = PyObject_GetAttrString(mod, "Date")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Date into C extension");
        return -1;

    };
    if ((TimeType = PyObject_GetAttrString(mod, "Time")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Time into C extension");
        return -1;

    }
    if ((TimestampType = PyObject_GetAttrString(mod, "Timestamp")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Timestamp into C extension");
        return -1;

    }
    if ((DecimalType = PyObject_GetAttrString(mod, "Decimal")) == NULL) {
        PyErr_SetString(PyExc_ImportError, "Unable to import giraffez.Decimal into C extension");
        return -1;
    }
    Py_DECREF(mod);
    return 0;
}

static PyObject* giraffez_column_to_pydict(GiraffeColumn *column) {
    PyObject *column_dict, *item;
    column_dict = PyDict_New();
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Name));
    PyDict_SetItemString(column_dict, "name", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Title));
    PyDict_SetItemString(column_dict, "title", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Alias));
    PyDict_SetItemString(column_dict, "alias", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromLong(column->Type));
    PyDict_SetItemString(column_dict, "type", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromUnsignedLongLong(column->Length));
    PyDict_SetItemString(column_dict, "length", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromLong(column->Precision));
    PyDict_SetItemString(column_dict, "precision", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyLong_FromLong(column->Scale));
    PyDict_SetItemString(column_dict, "scale", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Nullable));
    PyDict_SetItemString(column_dict, "nullable", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Default));
    PyDict_SetItemString(column_dict, "default", item);
    Py_DECREF(item);
    Py_RETURN_ERROR(item = PyUnicode_FromString(column->Format));
    PyDict_SetItemString(column_dict, "format", item);
    Py_DECREF(item);
    return column_dict;
}

PyObject* giraffez_columns_to_pyobject(GiraffeColumns *columns) {
    size_t i;
    GiraffeColumn *column;
    PyObject *column_dict, *columns_list, *obj;
    columns_list = PyList_New(columns->length);
    for (i=0; i<columns->length; i++) {
        column = &columns->array[i];
        if ((column_dict = giraffez_column_to_pydict(column)) == NULL) {
            return NULL;
        }
        PyList_SetItem(columns_list, i, column_dict);
    }
    obj =  PyObject_CallFunction(ColumnsType, "O", columns_list);
    Py_DECREF(columns_list);
    return obj;
}

// TODO: Add PyUnicode_Check and other checks just in case
GiraffeColumns* giraffez_columns_from_pyobject(PyObject *columns_obj) {
    PyObject *item, *column_obj, *iterator;
    GiraffeColumn *column;
    GiraffeColumns *columns;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);
    if ((iterator = PyObject_GetIter(columns_obj)) == NULL) {
        return NULL;
    }
    while ((column_obj = PyIter_Next(iterator))) {
        column = column_new();
        item = PyObject_GetAttrString(column_obj, "name");
        if (item != NULL && item != Py_None) {
            column->Name = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "title");
        if (item != NULL && item != Py_None) {
            column->Title = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "alias");
        if (item != NULL && item != Py_None) {
            column->Alias = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "type");
        if (item != NULL && item != Py_None) {
            column->Type = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "length");
        if (item != NULL && item != Py_None) {
            column->Length = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "precision");
        if (item != NULL && item != Py_None) {
            column->Precision = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "scale");
        if (item != NULL && item != Py_None) {
            column->Scale = (uint16_t)PyLong_AsLong(item);
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "_nullable");
        if (item != NULL && item != Py_None) {
            column->Nullable = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "_default");
        if (item != NULL && item != Py_None) {
            column->Default = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        item = PyObject_GetAttrString(column_obj, "_format");
        if (item != NULL && item != Py_None) {
            column->Format = strdup(PyUnicode_AsUTF8(item));
        }
        Py_XDECREF(item);
        columns_append(columns, *column);
        free(column);
        Py_DECREF(column_obj);
    }
    Py_DECREF(iterator);
    return columns;
}

PyObject* giraffez_date_from_datetime(int year, int month, int day, int hour, int minute,
        int second, int microsecond) {
    return PyObject_CallFunction(DateType, "iiiiiii", year, month, day, hour, minute, second,
        microsecond);
}

PyObject* giraffez_time_from_time(int hour, int minute, int second, int microsecond) {
    return PyObject_CallFunction(TimeType, "iiii", hour, minute, second, microsecond);
}

PyObject* giraffez_ts_from_datetime(int year, int month, int day, int hour, int minute, int second,
        int microsecond) {
    return PyObject_CallFunction(TimestampType, "iiiiiii", year, month, day, hour, minute, second,
        microsecond);
}

PyObject* giraffez_decimal_from_pystring(PyObject *s) {
    PyObject *obj;
    obj = PyObject_CallFunction(DecimalType, "O", s);
    Py_DECREF(s);
    return obj;
}
