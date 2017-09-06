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
#include "convert.h"
#include "teradata.h"

#include "columns.h"


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
    column->FormatLength = 0;
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
        if (c->size == 0) {
            c->size = 1;
        }
        c->size *= 2;
        c->array = (GiraffeColumn*)realloc(c->array, c->size * sizeof(GiraffeColumn));
    }
    if (element.Type < BLOB_NN) {
        element.TPTType = element.Type;
        element.Type = teradata_type_from_tpt_type(element.Type);
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
    if (element.Title == NULL || strlen(element.Title) == 0) {
        element.Title = strdup(element.SafeName);
    } else {
        char *tmp = element.Title;
        element.Title = safe_name(tmp);
        free(tmp);
    }
    if (element.GDType == GD_CHAR && element.Format != NULL) {
        element.FormatLength = format_length(element.Format);
    }
    c->array[c->length++] = element;
    c->header_length = (int)ceil(c->length/8.0);
    c->buffer = (unsigned char*)realloc(c->buffer, c->header_length * sizeof(unsigned char));
}

void columns_free(GiraffeColumns *c) {
    GiraffeColumn *column;
    size_t i;
    for (i=0; i<c->length; i++) {
        column = &c->array[i];
        free(column->Database);
        free(column->Table);
        free(column->Name);
        free(column->Alias);
        free(column->Title);
        free(column->Format);
        free(column->Default);
        free(column->Nullable);
        free(column->SafeName);
    }
    free(c->array);
    free(c->buffer);
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

StatementInfoColumn* stmt_info_column_new() {
    StatementInfoColumn *column;
    column = (StatementInfoColumn*)malloc(sizeof(StatementInfoColumn));
    column->ExtensionLayout = 0;
    column->ExtensionType = 0;
    column->ExtensionLength = 0;
    column->Database = NULL;
    column->Table = NULL;
    column->Name = NULL;
    column->Position = 0;
    column->Alias = NULL;
    column->Title = NULL;
    column->Format = NULL;
    column->Default = NULL;
    column->IdentityColumn = NULL;
    column->DefinitelyWritable = NULL;
    column->NotDefinedNotNull = NULL;
    column->CanReturnNull = NULL;
    column->PermittedInWhere = NULL;
    column->Writable = NULL;
    column->Type = 0;
    column->UDType = 0;
    column->TypeName = NULL;
    column->DataTypeMiscInfo = NULL;
    column->Length = 0;
    column->Precision = 0;
    column->Interval = 0;
    column->Scale = 0;
    column->CharacterSetType = NULL;
    column->TotalNumberCharacters = 0;
    column->CaseSensitive = NULL;
    column->NumericItemSigned = NULL;
    column->UniquelyDescribesRow = NULL;
    column->OnlyMemberUniqueIndex = NULL;
    column->IsExpression = NULL;
    column->PermittedInOrderBy = NULL;
    return column;
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
    StatementInfoColumn *column;
    size_t i;
    for (i=0; i<s->length; i++) {
        column = &s->array[i];
        free(column->Database);
        free(column->Table);
        free(column->Name);
        free(column->Alias);
        free(column->Title);
        free(column->Format);
        free(column->Default);
        free(column->IdentityColumn);
        free(column->DefinitelyWritable);
        free(column->NotDefinedNotNull);
        free(column->CanReturnNull);
        free(column->PermittedInWhere);
        free(column->Writable);
        free(column->TypeName);
        free(column->DataTypeMiscInfo);
        free(column->CharacterSetType);
        free(column->CaseSensitive);
        free(column->NumericItemSigned);
        free(column->UniquelyDescribesRow);
        free(column->OnlyMemberUniqueIndex);
        free(column->IsExpression);
        free(column->PermittedInOrderBy);
    }
    free(s->array);
    s->array = NULL;
    s->length = s->size = 0;
}

uint64_t format_length(const char *format) {
    int l;
    int n = sscanf(format, "X(%d)", &l);
    if (n != 1) {
        return 0;
    }
    return l;
}

char* safe_name(const char *name) {
    char *s;
    size_t i;
    size_t length;
    length = strlen(name);
    s = (char*)malloc(length+1);
    for (i=0; i<length;i++) {
        if (name[i] == ' ') {
            s[i] = '_';
        } else {
            s[i] = tolower(name[i]);
        }
    }
    s[length] = '\0';
    return s;
}

GiraffeColumns* columns_from_stmtinfo(unsigned char **data, const uint32_t length) {
    StatementInfoColumn *c;
    StatementInfo *s;
    GiraffeColumn *column;
    GiraffeColumns *columns;
    unsigned char *ext, *start;
    size_t i;
    columns = (GiraffeColumns*)malloc(sizeof(GiraffeColumns));
    columns_init(columns, 1);

    // save raw statement info for debug
    columns->raw->data = malloc(sizeof(char) * length);
    memcpy(columns->raw->data, *data, length);
    columns->raw->length = length;
    s = (StatementInfo*)malloc(sizeof(StatementInfo));
    stmt_info_init(s, 1);
    start = *data;
    while ((*data - start) < length) {
        c = stmt_info_column_new();
        unpack_uint16_t(data, &c->ExtensionLayout);
        unpack_uint16_t(data, &c->ExtensionType);
        unpack_uint16_t(data, &c->ExtensionLength);
        if (c->ExtensionLayout != 1) {
            *data += c->ExtensionLength;
            continue;
        }
        ext = *data;
        unpack_string(data, &c->Database);
        unpack_string(data, &c->Table);
        unpack_string(data, &c->Name);
        unpack_uint16_t(data, &c->Position);
        unpack_string(data, &c->Alias);
        unpack_string(data, &c->Title);
        unpack_string(data, &c->Format);
        unpack_string(data, &c->Default);
        unpack_char(data, &c->IdentityColumn);
        unpack_char(data, &c->DefinitelyWritable);
        unpack_char(data, &c->NotDefinedNotNull);
        unpack_char(data, &c->CanReturnNull);
        unpack_char(data, &c->PermittedInWhere);
        unpack_char(data, &c->Writable);
        unpack_uint16_t(data, &c->Type);
        unpack_uint16_t(data, &c->UDType);
        unpack_string(data, &c->TypeName);
        unpack_string(data, &c->DataTypeMiscInfo);
        unpack_uint64_t(data, &c->Length);
        unpack_uint16_t(data, &c->Precision);
        unpack_uint16_t(data, &c->Interval);
        unpack_uint16_t(data, &c->Scale);
        unpack_uchar(data, &c->CharacterSetType);
        unpack_uint64_t(data, &c->TotalNumberCharacters);
        unpack_uchar(data, &c->CaseSensitive);
        unpack_uchar(data, &c->NumericItemSigned);
        unpack_uchar(data, &c->UniquelyDescribesRow);
        unpack_uchar(data, &c->OnlyMemberUniqueIndex);
        unpack_uchar(data, &c->IsExpression);
        unpack_uchar(data, &c->PermittedInOrderBy);
        // Sometimes extra data...
        if ((*data-ext) < c->ExtensionLength) {
            *data += (c->ExtensionLength - (*data-ext));
        }
        stmt_info_append(s, *c);
        free(c);
    }
    for (i=0; i<s->length; i++) {
        c = &s->array[i];
        column = column_new();
        column->Name = strdup(c->Name);
        column->Type = c->Type;
        column->Length = c->Length;
        column->Precision = c->Precision;
        column->Scale = c->Scale;
        column->Alias = strdup(c->Alias);
        column->Title = strdup(c->Title);
        column->Format = strdup(c->Format);
        column->Default = strdup(c->Default);
        column->Nullable = strdup(c->CanReturnNull);
        columns_append(columns, *column);
        free(column);
    }
    stmt_info_free(s);
    return columns;
}
