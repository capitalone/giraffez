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

#include "types.h"
#include <stddef.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>
#include "unpack.h"


void columns_init(GiraffeColumns *c, size_t initial_size) {
    c->array = (GiraffeColumn*)malloc(initial_size * sizeof(GiraffeColumn));
    c->length = 0;
    c->size = initial_size;
}

void columns_append(GiraffeColumns *c, GiraffeColumn element) {
    if (c->length == c->size) {
        c->size *= 2;
        c->array = (GiraffeColumn*)realloc(c->array, c->size
            * sizeof(GiraffeColumn));
    }
    c->array[c->length++] = element;
}

void columns_free(GiraffeColumns *c) {
    free(c->array);
    c->array = NULL;
    c->length = c->size = 0;
}

void indicator_init(unsigned char** ind, unsigned char** data, size_t header_length) {
    size_t i;
    *ind = (unsigned char*)malloc(sizeof(unsigned char)*header_length);
    for (i=0; i<header_length; i++) {
        (*ind)[i] = reverse_lookup[*((*data)++)];
    }
}

int indicator_read(unsigned char* ind, size_t pos) {
    return (ind[pos/8] & (1 << (pos % 8)));
}

void indicator_free(unsigned char** ind) {
    free(*ind);
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

void parse_stmt_info(unsigned char** data, StatementInfo* s, const uint32_t length) {
    unsigned char* start = *data;
    StatementInfoColumn* column;
    while ((*data - start) < length) {
        column = (StatementInfoColumn*)malloc(sizeof(StatementInfoColumn));
        unpack_uint16_t(data, &column->ExtensionLayout);
        unpack_uint16_t(data, &column->ExtensionType);
        unpack_uint16_t(data, &column->ExtensionLength);
        if (column->ExtensionLayout != 1) {
            *data += column->ExtensionLength;
            continue;
        }
        parse_ext(data, column, column->ExtensionLength);
        stmt_info_append(s, *column);
    }
}

void parse_ext(unsigned char** data, StatementInfoColumn* column, const uint16_t length) {
    unsigned char* start = *data;
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
    if ((*data-start) < length) {
        *data += (length - (*data-start));
    }
}
