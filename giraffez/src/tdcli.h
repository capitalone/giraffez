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

#ifndef __GIRAFFEZ_TDCLI_H
#define __GIRAFFEZ_TDCLI_H

#ifdef __cplusplus
extern "C" {
#endif

#define NO_CLIV2_ERROR_T
#define STRING_CLI

#define CONNECTED     0
#define NOT_CONNECTED 1
#define OK            0
#define STOP          1
#define FAILED       -1
#define PCL_FAIL     -2
#define PCL_ERR      -3

// This header file does not make use of the Python C API, however, the
// Python C API is designed in such a way that in order to prevent
// redefinition of macros set by the C standard library it must be
// included before any C standard library.
#include <Python.h>

// Teradata CLIv2
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>


typedef struct DBCAREA dbcarea_t;

typedef struct TeradataConnection {
    dbcarea_t* dbc;
    char cnta[4];
    char logonstr[1024];
    char session_charset[36];
    Int32 result;
} TeradataConnection;

typedef struct CliFailureType TeradataFailure;
typedef struct CliErrorType TeradataError;

TeradataError* tdcli_read_error(char *dataptr);
TeradataFailure* tdcli_read_failure(char *dataptr);
TeradataConnection* tdcli_new();
uint16_t tdcli_connect(TeradataConnection *conn, const char *host, const char *username, const char *password);
uint16_t tdcli_fetch(TeradataConnection *conn);
uint16_t tdcli_fetch_record(TeradataConnection *conn);
uint16_t tdcli_execute(TeradataConnection *conn, const char *command);
uint16_t tdcli_end_request(TeradataConnection *conn);
void tdcli_close(TeradataConnection *conn, uint16_t connected);
void tdcli_free(TeradataConnection *conn);

#ifdef __cplusplus
}
#endif

#endif
