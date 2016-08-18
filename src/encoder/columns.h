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

#ifndef __ENCODER_COLUMNS_H
#define __ENCODER_COLUMNS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#ifdef _WIN32
#include <pstdint.h>
#else
#include <stdint.h>
#endif


typedef struct {
    char* Database;
    char* Table;
    char* Name;
    uint16_t Type;
    uint64_t Length;
    uint16_t Precision;
    uint16_t Interval;
    uint16_t Scale;
    uint16_t GDType;
    char* Alias;
    char* Title;
    char* Format;
    char* Default;
    char* Nullable;
} GiraffeColumn;

typedef struct {
    size_t size;
    size_t length;
    size_t header_length;
    GiraffeColumn* array;
} GiraffeColumns;

void columns_init(GiraffeColumns *c, size_t initial_size);
void columns_append(GiraffeColumns *c, GiraffeColumn element);
void columns_free(GiraffeColumns *c);

#ifdef __cplusplus
}
#endif

#endif
