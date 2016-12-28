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
    c->array = NULL;
    c->buffer = NULL;
    c->length = c->size = 0;
}

void indicator_set(GiraffeColumns* columns, unsigned char** data) {
    size_t i;
    for (i=0; i<columns->header_length; i++) {
        columns->buffer[i] = reverse_lookup[*((*data)++)];
    }
}

int indicator_read(unsigned char* ind, size_t pos) {
    return (ind[pos/8] & (1 << (pos % 8)));
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
