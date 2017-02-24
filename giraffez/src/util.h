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

#ifndef __GIRAFFEZ_UTIL_H
#define __GIRAFFEZ_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif

#define Py_RETURN_ERROR(s) if ((s) == NULL) return NULL;

typedef union {
    double d;
    unsigned char b[sizeof(double)];
} dsplit_t;

char* safe_name(const char *name);

void pack_int8_t(unsigned char **data, int8_t val);
void pack_int16_t(unsigned char **data, int16_t val);
void pack_uint16_t(unsigned char **data, uint16_t val);
void pack_int32_t(unsigned char **data, int32_t val);
void pack_uint32_t(unsigned char **data, uint32_t val);
void pack_int64_t(unsigned char **data, int64_t val);
void pack_uint64_t(unsigned char **data, uint64_t val);
void pack_float(unsigned char **data, double val);
uint16_t pack_string(unsigned char **data, const char *str, const uint16_t length);
void unpack_int8_t(unsigned char **data, int8_t *dst);
void unpack_int16_t(unsigned char **data, int16_t *dst);
void unpack_uint16_t(unsigned char **data, uint16_t *dst);
void unpack_int32_t(unsigned char **data, int32_t *dst);
void unpack_int64_t(unsigned char **data, int64_t *dst);
void unpack_uint64_t(unsigned char **data, uint64_t *dst);
void unpack_float(unsigned char **data, double *dst);
void unpack_char(unsigned char **data, char **str);
void unpack_uchar(unsigned char **data, unsigned char **str);
uint16_t unpack_string(unsigned char **data, char **str);

#ifdef __cplusplus
}
#endif

#endif
