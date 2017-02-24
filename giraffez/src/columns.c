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

#include "columns.h"

#include <math.h>
#include <stddef.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>
#include <string.h>

#include "types.h"
#include "util.h"

GiraffeColumn* column_new() {
    GiraffeColumn *column;
    column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
    column->Database = NULL;
    column->Table = NULL;
    column->Name = NULL;
    column->Type = 0;
    column->Length = 0;
    column->Precision = 0;
    column->Interval = 0;
    column->Scale = 0;
    column->GDType = 0;
    column->TPTType = 0;
    column->Alias = NULL;
    column->Title = NULL;
    column->Format = NULL;
    column->Default = NULL;
    column->Nullable = NULL;
    column->NullLength = 0;
    column->SafeName = NULL;
    return column;
}

void columns_init(GiraffeColumns *c, size_t initial_size) {
    c->array = (GiraffeColumn*)malloc(initial_size * sizeof(GiraffeColumn));
    c->length = 0;
    c->size = initial_size;
    c->header_length = 0;
    c->raw = (RawStatementInfo*)malloc(sizeof(RawStatementInfo));
    c->raw->data = NULL;
    c->raw->length = 0;
    c->buffer = (unsigned char*)malloc(c->header_length * sizeof(unsigned char));
}

void columns_append(GiraffeColumns *c, GiraffeColumn element) {
    if (c->length == c->size) {
        c->size *= 2;
        c->array = (GiraffeColumn*)realloc(c->array, c->size * sizeof(GiraffeColumn));
    }
    if (element.Type < BLOB_NN) {
        element.TPTType = element.Type;
        element.Type = tpt_type_to_teradata_type(element.Type);
    } else {
        element.TPTType = teradata_type_to_tpt_type(element.Type);
    }
    element.GDType = teradata_type_to_giraffez_type(element.Type);
    if (element.GDType == GD_VARCHAR) {
        element.NullLength = VARCHAR_NULL_LENGTH;
    } else {
        element.NullLength = element.Length;
    }
    element.SafeName = safe_name(element.Name);
    c->array[c->length++] = element;
    c->header_length = (int)ceil(c->length/8.0);
    c->buffer = (unsigned char*)realloc(c->buffer, c->header_length * sizeof(unsigned char));
}

void columns_free(GiraffeColumns *c) {
    free(c->array);
    free(c->buffer);
    free(c->raw);
    c->array = NULL;
    c->buffer = NULL;
    c->length = c->size = 0;
}

void indicator_set(GiraffeColumns *columns, unsigned char **data) {
    size_t i;
    static const unsigned char reverse_lookup[256] = {
        0x00, 0x80, 0x40, 0xc0, 0x20, 0xa0, 0x60, 0xe0,
        0x10, 0x90, 0x50, 0xd0, 0x30, 0xb0, 0x70, 0xf0,
        0x08, 0x88, 0x48, 0xc8, 0x28, 0xa8, 0x68, 0xe8,
        0x18, 0x98, 0x58, 0xd8, 0x38, 0xb8, 0x78, 0xf8,
        0x04, 0x84, 0x44, 0xc4, 0x24, 0xa4, 0x64, 0xe4,
        0x14, 0x94, 0x54, 0xd4, 0x34, 0xb4, 0x74, 0xf4,
        0x0c, 0x8c, 0x4c, 0xcc, 0x2c, 0xac, 0x6c, 0xec,
        0x1c, 0x9c, 0x5c, 0xdc, 0x3c, 0xbc, 0x7c, 0xfc,
        0x02, 0x82, 0x42, 0xc2, 0x22, 0xa2, 0x62, 0xe2,
        0x12, 0x92, 0x52, 0xd2, 0x32, 0xb2, 0x72, 0xf2,
        0x0a, 0x8a, 0x4a, 0xca, 0x2a, 0xaa, 0x6a, 0xea,
        0x1a, 0x9a, 0x5a, 0xda, 0x3a, 0xba, 0x7a, 0xfa,
        0x06, 0x86, 0x46, 0xc6, 0x26, 0xa6, 0x66, 0xe6,
        0x16, 0x96, 0x56, 0xd6, 0x36, 0xb6, 0x76, 0xf6,
        0x0e, 0x8e, 0x4e, 0xce, 0x2e, 0xae, 0x6e, 0xee,
        0x1e, 0x9e, 0x5e, 0xde, 0x3e, 0xbe, 0x7e, 0xfe,
        0x01, 0x81, 0x41, 0xc1, 0x21, 0xa1, 0x61, 0xe1,
        0x11, 0x91, 0x51, 0xd1, 0x31, 0xb1, 0x71, 0xf1,
        0x09, 0x89, 0x49, 0xc9, 0x29, 0xa9, 0x69, 0xe9,
        0x19, 0x99, 0x59, 0xd9, 0x39, 0xb9, 0x79, 0xf9,
        0x05, 0x85, 0x45, 0xc5, 0x25, 0xa5, 0x65, 0xe5,
        0x15, 0x95, 0x55, 0xd5, 0x35, 0xb5, 0x75, 0xf5,
        0x0d, 0x8d, 0x4d, 0xcd, 0x2d, 0xad, 0x6d, 0xed,
        0x1d, 0x9d, 0x5d, 0xdd, 0x3d, 0xbd, 0x7d, 0xfd,
        0x03, 0x83, 0x43, 0xc3, 0x23, 0xa3, 0x63, 0xe3,
        0x13, 0x93, 0x53, 0xd3, 0x33, 0xb3, 0x73, 0xf3,
        0x0b, 0x8b, 0x4b, 0xcb, 0x2b, 0xab, 0x6b, 0xeb,
        0x1b, 0x9b, 0x5b, 0xdb, 0x3b, 0xbb, 0x7b, 0xfb,
        0x07, 0x87, 0x47, 0xc7, 0x27, 0xa7, 0x67, 0xe7,
        0x17, 0x97, 0x57, 0xd7, 0x37, 0xb7, 0x77, 0xf7,
        0x0f, 0x8f, 0x4f, 0xcf, 0x2f, 0xaf, 0x6f, 0xef,
        0x1f, 0x9f, 0x5f, 0xdf, 0x3f, 0xbf, 0x7f, 0xff,
    };

    for (i=0; i<columns->header_length; i++) {
        columns->buffer[i] = reverse_lookup[*((*data)++)];
    }
}

void indicator_clear(unsigned char **ind, size_t n) {
    memset(*ind, 0, sizeof(unsigned char) * n);
}

int indicator_read(unsigned char *ind, size_t pos) {
    return (ind[pos/8] & (1 << (pos % 8)));
}

void indicator_write(unsigned char **ind, size_t pos, int value) {
    (*ind)[pos/8] |= (value << (7 - (pos % 8)));
}

void stmt_info_init(StatementInfo *s, size_t initial_size) {
    s->array = (StatementInfoColumn*)malloc(initial_size * sizeof(StatementInfoColumn));
    s->length = 0;
    s->size = initial_size;
}

void stmt_info_append(StatementInfo *s, StatementInfoColumn element) {
    if (s->length == s->size) {
        s->size *= 2;
        s->array = (StatementInfoColumn*)realloc(s->array, s->size
            * sizeof(StatementInfoColumn));
    }
    s->array[s->length++] = element;
}

void stmt_info_free(StatementInfo *s) {
    free(s->array);
    s->array = NULL;
    s->length = s->size = 0;
}

GiraffeColumns* unpack_stmt_info_to_columns(unsigned char **data, const uint32_t length) {
    StatementInfoColumn *scolumn;
    StatementInfo *s;
    GiraffeColumn *column;
    GiraffeColumns *columns;
    size_t i;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);

    // save raw statement info for debug
    columns->raw->data = malloc(sizeof(char) * length);
    memcpy(columns->raw->data, *data, length);
    columns->raw->length = length;

    s = (StatementInfo*)malloc(sizeof(StatementInfo));
    stmt_info_init(s, 1);
    unpack_stmt_info(data, s, length);
    for (i=0; i<s->length; i++) {
        scolumn = &s->array[i];
        column = (GiraffeColumn*)malloc(sizeof(GiraffeColumn));
        column->Name = strdup(scolumn->Name);
        column->Type = scolumn->Type;
        column->Length = scolumn->Length;
        column->Precision = scolumn->Precision;
        column->Scale = scolumn->Scale;
        column->Alias = scolumn->Alias;
        column->Title = scolumn->Title;
        column->Format = scolumn->Format;
        column->Default = scolumn->Default;
        column->Nullable = scolumn->CanReturnNull;
        columns_append(columns, *column);
    }
    stmt_info_free(s);
    return columns;
}

void unpack_stmt_info(unsigned char** data, StatementInfo* s, const uint32_t length) {
    unsigned char *ext, *start;
    start = *data;
    StatementInfoColumn *column;
    while ((*data - start) < length) {
        column = (StatementInfoColumn*)malloc(sizeof(StatementInfoColumn));
        unpack_uint16_t(data, &column->ExtensionLayout);
        unpack_uint16_t(data, &column->ExtensionType);
        unpack_uint16_t(data, &column->ExtensionLength);
        if (column->ExtensionLayout != 1) {
            *data += column->ExtensionLength;
            continue;
        }
        ext = *data;
        unpack_string(data, &column->Database);
        unpack_string(data, &column->Table);
        unpack_string(data, &column->Name);
        unpack_uint16_t(data, &column->Position);
        unpack_string(data, &column->Alias);
        unpack_string(data, &column->Title);
        unpack_string(data, &column->Format);
        unpack_string(data, &column->Default);
        unpack_char(data, &column->IdentityColumn);
        unpack_char(data, &column->DefinitelyWritable);
        unpack_char(data, &column->NotDefinedNotNull);
        unpack_char(data, &column->CanReturnNull);
        unpack_char(data, &column->PermittedInWhere);
        unpack_char(data, &column->Writable);
        unpack_uint16_t(data, &column->Type);
        unpack_uint16_t(data, &column->UDType);
        unpack_string(data, &column->TypeName);
        unpack_string(data, &column->DataTypeMiscInfo);
        unpack_uint64_t(data, &column->Length);
        unpack_uint16_t(data, &column->Precision);
        unpack_uint16_t(data, &column->Interval);
        unpack_uint16_t(data, &column->Scale);
        unpack_uchar(data, &column->CharacterSetType);
        unpack_uint64_t(data, &column->TotalNumberCharacters);
        unpack_uchar(data, &column->CaseSensitive);
        unpack_uchar(data, &column->NumericItemSigned);
        unpack_uchar(data, &column->UniquelyDescribesRow);
        unpack_uchar(data, &column->OnlyMemberUniqueIndex);
        unpack_uchar(data, &column->IsExpression);
        unpack_uchar(data, &column->PermittedInOrderBy);
        // Sometimes extra data...
        if ((*data-ext) < column->ExtensionLength) {
            *data += (column->ExtensionLength - (*data-ext));
        }
        stmt_info_append(s, *column);
    }
}
