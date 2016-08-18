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

#include "util.h"
#ifdef _WIN32
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>
#include <string.h>


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

void unpack_char(unsigned char** data, char **str) {
    char* dest = (char*)malloc(sizeof(char)+1);
    memcpy(dest, (char*)*data, 1);
    dest[1] = '\0';
    *str = dest;
    *data += 1;
}

void unpack_uchar(unsigned char** data, unsigned char **str) {
    unsigned char* dest = (unsigned char*)malloc(sizeof(unsigned char)+1);
    memcpy(dest, (unsigned char*)*data, 1);
    dest[1] = '\0';
    *str = dest;
    *data += 1;
}

uint16_t unpack_string(unsigned char** data, char **str) {
    uint16_t len = 0;
    unpack_uint16_t(data, &len);
    if (len > 0) {
        char* dst = (char*)malloc(len+1);
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
