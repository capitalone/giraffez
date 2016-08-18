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

#include "indicator.h"
#include <stddef.h>
#include <stdlib.h>
#include "util.h"


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
