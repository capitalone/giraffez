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

#ifndef __GIRAFFEZ_TERADATA_H
#define __GIRAFFEZ_TERADATA_H

#ifdef __cplusplus
extern "C" {
#endif

#define CONNECTED      0
#define NOT_CONNECTED  1
#define REQUEST_OPEN   1
#define REQUEST_CLOSED 0
#define OK             0

#include "common.h"
#include "columns.h"
#include "encoder.h"

// Teradata CLIv2
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>

// Silences unused warning that appears every time this header is
// included.
static const size_t _ = sizeof(D8XIIIRX);

typedef struct DBCAREA dbcarea_t;

typedef struct TeradataConnection {
    dbcarea_t* dbc;
    char cnta[4];
    char logonstr[1024];
    char session_charset[36];
    Int32 result;
    int connected;
    int request_status;
} TeradataConnection;

typedef struct TeradataErr {
    int  Code;
    char *Msg;
} TeradataErr;

typedef enum TeradataStatus {
    TD_SUCCESS = 0,
    TD_SYNC_BARRIER,
    //TD_SYNC_TELINFO,
    TD_END_METHOD = 3,
    TD_UNAVAILABLE,
    TD_SYNC_SCHEMA,
    TD_ERROR = 99,
    TD_ERROR_REQUEST_EXHAUSTED = 307,
    TD_ERROR_CANNOT_RELEASE_MLOAD = 2572,
    TD_ERROR_TABLE_MLOAD_EXISTS = 2574,
    TD_ERROR_WORK_TABLE_MISSING = 2583,
    TD_ERROR_TRANS_ABORTED = 2631,
    TD_ERROR_USER_NO_SELECT_ACCESS = 3523,
    TD_ERROR_OBJECT_NOT_EXIST = 3807,
    TD_ERROR_OBJECT_NOT_TABLE = 3853,
    TD_ERROR_OBJECT_NOT_VIEW = 3854,
    TD_ERROR_INVALID_USER = 8017,
    TD_CALL_ENDACQ = 25000
} TeradataStatus;

typedef struct TeradataTypes {
    enum {
        BLOB_NN = 400,
        BLOB_N = 401,
        BLOB_AS_DEFERRED_NN = 404,
        BLOB_AS_DEFERRED_N = 405,
        BLOB_AS_LOCATOR_NN = 408,
        BLOB_AS_LOCATOR_N = 409,
        BLOB_AS_DEFERRED_NAME_NN = 412,
        BLOB_AS_DEFERRED_NAME_N = 413,
        CLOB_NN = 416,
        CLOB_N = 417,
        CLOB_AS_DEFERRED_NN = 420,
        CLOB_AS_DEFERRED_N = 421,
        CLOB_AS_LOCATOR_NN = 424,
        CLOB_AS_LOCATOR_N = 425,
        UDT_NN = 432,
        UDT_N = 433,
        DISTINCT_UDT_NN = 436,
        DISTINCT_UDT_N = 437,
        STRUCT_UDT_NN = 440,
        STRUCT_UDT_N = 441,
        VARCHAR_NN = 448,
        VARCHAR_N = 449,
        CHAR_NN = 452,
        CHAR_N = 453,
        LONG_VARCHAR_NN = 456,
        LONG_VARCHAR_N = 457,
        VARGRAPHIC_NN = 464,
        VARGRAPHIC_N = 465,
        GRAPHIC_NN = 468,
        GRAPHIC_N = 469,
        LONG_VARGRAPHIC_NN = 472,
        LONG_VARGRAPHIC_N = 473,
        FLOAT_NN = 480,
        FLOAT_N = 481,
        DECIMAL_NN = 484,
        DECIMAL_N = 485,
        INTEGER_NN = 496,
        INTEGER_N = 497,
        SMALLINT_NN = 500,
        SMALLINT_N = 501,
        ARRAY_1D_NN = 504,
        ARRAY_1D_N = 505,
        ARRAY_ND_NN = 508,
        ARRAY_ND_N = 509,
        BIGINT_NN = 600,
        BIGINT_N = 601,
        VARBYTE_NN = 688,
        VARBYTE_N = 689,
        BYTE_NN = 692,
        BYTE_N = 693,
        LONG_VARBYTE_NN = 696,
        LONG_VARBYTE_N = 697,
        DATE_NNA = 748,
        DATE_NA = 749,
        DATE_NN = 752,
        DATE_N = 753,
        BYTEINT_NN = 756,
        BYTEINT_N = 757,
        TIME_NN = 760,
        TIME_N = 761,
        TIMESTAMP_NN = 764,
        TIMESTAMP_N = 765,
        TIME_NNZ = 768,
        TIME_NZ = 769,
        TIMESTAMP_NNZ = 772,
        TIMESTAMP_NZ = 773,
        INTERVAL_YEAR_NN = 776,
        INTERVAL_YEAR_N = 777,
        INTERVAL_YEAR_TO_MONTH_NN = 780,
        INTERVAL_YEAR_TO_MONTH_N = 781,
        INTERVAL_MONTH_NN = 784,
        INTERVAL_MONTH_N = 785,
        INTERVAL_DAY_NN = 788,
        INTERVAL_DAY_N = 789,
        INTERVAL_DAY_TO_HOUR_NN = 792,
        INTERVAL_DAY_TO_HOUR_N = 793,
        INTERVAL_DAY_TO_MINUTE_NN = 796,
        INTERVAL_DAY_TO_MINUTE_N = 797,
        INTERVAL_DAY_TO_SECOND_NN = 800,
        INTERVAL_DAY_TO_SECOND_N = 801,
        INTERVAL_HOUR_NN = 804,
        INTERVAL_HOUR_N = 805,
        INTERVAL_HOUR_TO_MINUTE_NN = 808,
        INTERVAL_HOUR_TO_MINUTE_N = 809,
        INTERVAL_HOUR_TO_SECOND_NN = 812,
        INTERVAL_HOUR_TO_SECOND_N = 813,
        INTERVAL_MINUTE_NN = 816,
        INTERVAL_MINUTE_N = 817,
        INTERVAL_MINUTE_TO_SECOND_NN = 820,
        INTERVAL_MINUTE_TO_SECOND_N = 821,
        INTERVAL_SECOND_NN = 824,
        INTERVAL_SECOND_N = 825,
        PERIOD_DATE_NN = 832,
        PERIOD_DATE_N = 833,
        PERIOD_TIME_NN = 836,
        PERIOD_TIME_N = 837,
        PERIOD_TIME_NNZ = 840,
        PERIOD_TIME_NZ = 841,
        PERIOD_TIMESTAMP_NN = 844,
        PERIOD_TIMESTAMP_N = 845,
        PERIOD_TIMESTAMP_NNZ = 848,
        PERIOD_TIMESTAMP_NZ = 849,
        XML_TEXT_NN = 852,
        XML_TEXT_N = 853,
        XML_TEXT_DEFERRED_NN = 856,
        XML_TEXT_DEFERRED_N = 857,
        XML_TEXT_LOCATOR_NN = 860,
        XML_TEXT_LOCATOR_N = 861
    } CliTypes;
    enum {
        TD_NONE = 0,
        TD_INTEGER = 1,
        TD_SMALLINT = 2,
        TD_FLOAT = 3,
        TD_DECIMAL = 4,
        TD_CHAR = 5,
        TD_BYTEINT = 6,
        TD_VARCHAR = 7,
        TD_LONGVARCHAR = 8,
        TD_BYTE = 9,
        TD_VARBYTE = 10,
        TD_DATE = 11,
        TD_GRAPHIC = 12,
        TD_VARGRAPHIC = 13,
        TD_LONGVARGRAPHIC = 14,
        TD_DATE_ANSI = 15,
        TD_TIME = 35,
        TD_TIME_WITHTIMEZONE = 36,
        TD_BIGINT = 37,
        TD_BLOB = 39,
        TD_CLOB = 40,
        TD_BLOB_AS_DEFERRED_BY_NAME = 41,
        TD_CLOB_AS_DEFERRED_BY_NAME = 42,
        TD_TIMESTAMP = 43,
        TD_TIMESTAMP_WITHTIMEZONE = 44,
        TD_INTERVAL_YEAR = 45,
        TD_INTERVAL_YEARTOMONTH = 46,
        TD_INTERVAL_MONTH = 47,
        TD_INTERVAL_DAY = 48,
        TD_INTERVAL_DAYTOHOUR = 49,
        TD_INTERVAL_DAYTOMINUTE = 50,
        TD_INTERVAL_DAYTOSECOND = 51,
        TD_INTERVAL_HOUR = 52,
        TD_INTERVAL_HOURTOMINUTE = 53,
        TD_INTERVAL_HOURTOSECOND = 54,
        TD_INTERVAL_MINUTE = 55,
        TD_INTERVAL_MINUTETOSECOND = 56,
        TD_INTERVAL_SECOND = 57,
        TD_PERIOD_DATE = 58,
        TD_PERIOD_TIME = 59,
        TD_PERIOD_TIME_TZ = 60,
        TD_PERIOD_TS = 61,
        TD_PERIOD_TS_TZ = 62,
        TD_NUMBER = 63
    } TptTypes;
} TeradataTypes;

TeradataErr* __teradata_read_error(char *dataptr);
TeradataErr* __teradata_read_failure(char *dataptr);
TeradataConnection* __teradata_new();
uint16_t __teradata_init(TeradataConnection *conn);
uint16_t __teradata_connect(TeradataConnection *conn, const char *host, const char *username,
    const char *password);
uint16_t __teradata_fetch(TeradataConnection *conn);
uint16_t __teradata_fetch_record(TeradataConnection *conn);
uint16_t __teradata_execute(TeradataConnection *conn, const char *command);
uint16_t __teradata_end_request(TeradataConnection *conn);
void __teradata_close(TeradataConnection *conn);
void __teradata_free(TeradataConnection *conn);

PyObject* teradata_check_error(TeradataConnection *conn);
PyObject* teradata_close(TeradataConnection *conn);
TeradataConnection* teradata_connect(const char *host, const char *username,
    const char *password);
PyObject* teradata_execute_rc(TeradataConnection *conn, TeradataEncoder *e, const char *command, int *rc);
PyObject* teradata_execute(TeradataConnection *conn, TeradataEncoder *e, const char *command);
PyObject* teradata_execute_p(TeradataConnection *conn, TeradataEncoder *e, const char *command);
PyObject* teradata_handle_record(TeradataEncoder *e, const uint32_t parcel_t, unsigned char **data,
    const uint32_t length);

uint16_t teradata_type_to_tpt_type(uint16_t t);
uint16_t teradata_type_from_tpt_type(uint16_t t);
uint16_t teradata_type_to_giraffez_type(uint16_t t);

#ifdef __cplusplus
}
#endif

#endif
