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

#include "buffer.h"
#include "common.h"


buffer_t* buffer_new(int buffer_size) {
    buffer_t *b;
    b = (buffer_t*)malloc(sizeof(buffer_t));
    b->length = 0;
    b->pos = 0;
    b->data = malloc(sizeof(char) * buffer_size);
    return b;
}

void buffer_write(buffer_t *b, char *data, int length) {
    memcpy(b->data+b->pos, data, length);
    b->pos += length;
    b->length += length;
}

void buffer_writef(buffer_t *b, const char *fmt, ...) {
    int length;
    va_list vargs;
    va_start(vargs, fmt);
    length = vsprintf(b->data+b->pos, fmt, vargs);
    va_end(vargs);
    b->pos += length;
    b->length += length;
}

void buffer_reset(buffer_t *b, size_t n) {
    b->pos = n;
    b->length = n;
}
