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

#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#if defined(WIN32) || defined(WIN64)
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

    uint16_t NullLength;
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

void indicator_init(unsigned char** ind, unsigned char** data, size_t header_length);
int indicator_read(unsigned char* ind, size_t pos);
void indicator_free(unsigned char** ind);

typedef struct {
    uint16_t ExtensionLayout;
    uint16_t ExtensionType;
    uint16_t ExtensionLength;
    
    char* Database;
    char* Table;
    char* Name;

    uint16_t Position;

    char* Alias;
    char* Title;
    char* Format;
    char* Default;
    char* IdentityColumn;
    char* DefinitelyWritable;
    char* NotDefinedNotNull;
    char* CanReturnNull;
    char* PermittedInWhere;
    char* Writable;
    uint16_t Type;
    uint16_t UDType;

    char* TypeName;
    char* DataTypeMiscInfo;

    uint64_t Length;
    uint16_t Precision;
    uint16_t Interval;
    uint16_t Scale;
    unsigned char* CharacterSetType;
    uint64_t TotalNumberCharacters;
    unsigned char* CaseSensitive;
    unsigned char* NumericItemSigned;
    unsigned char* UniquelyDescribesRow;
    unsigned char* OnlyMemberUniqueIndex;
    unsigned char* IsExpression;
    unsigned char* PermittedInOrderBy;
} StatementInfoColumn;

typedef struct {
    size_t size;
    size_t length;
    StatementInfoColumn* array;
} StatementInfo;

void stmt_info_init(StatementInfo *s, size_t initial_size);
void stmt_info_append(StatementInfo *s, StatementInfoColumn element);
void stmt_info_free(StatementInfo *s);

#ifdef __cplusplus
}
#endif

#endif
