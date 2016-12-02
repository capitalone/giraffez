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
#include <stddef.h>
#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif
#include <stdlib.h>

#include "util.h"

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
