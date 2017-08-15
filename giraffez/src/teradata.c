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
#include "encoder.h"

#include "teradata.h"

// Teradata CLIv2
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>

// CLIv2 control flow exceptions
PyObject *EndStatementError;
PyObject *EndStatementInfoError;
PyObject *EndRequestError;

TeradataErr* __teradata_read_error(char *dataptr) {
    TeradataErr *err;
    struct CliErrorType *e;
    err = (TeradataErr*)malloc(sizeof(TeradataErr));
    e = (struct CliErrorType*)dataptr;
    err->Code = e->Code;
    err->Msg = e->Msg;
    return err;
}

TeradataErr* __teradata_read_failure(char *dataptr) {
    TeradataErr *err;
    struct CliFailureType *e;
    err = (TeradataErr*)malloc(sizeof(TeradataErr));
    e = (struct CliFailureType*)dataptr;
    err->Code = e->Code;
    err->Msg = e->Msg;
    return err;
}

TeradataConnection* __teradata_new() {
    TeradataConnection *conn;
    conn = (TeradataConnection*)malloc(sizeof(TeradataConnection));
    conn->result = 0;
    conn->connected = NOT_CONNECTED;
    conn->request_status = REQUEST_CLOSED;
    conn->dbc = (dbcarea_t*)malloc(sizeof(dbcarea_t));
    conn->dbc->total_len = sizeof(*conn->dbc);
    return conn;
}

uint16_t __teradata_init(TeradataConnection *conn) {
    DBCHINI(&conn->result, conn->cnta, conn->dbc);
    return conn->result;
}

uint16_t __teradata_connect(TeradataConnection *conn, const char *host, const char *username,
        const char *password) {
    conn->dbc->change_opts = 'Y';
    conn->dbc->resp_mode = 'I';
    conn->dbc->use_presence_bits = 'N';
    conn->dbc->keep_resp = 'N';
    conn->dbc->wait_across_crash = 'N';
    conn->dbc->tell_about_crash = 'Y';
    conn->dbc->loc_mode = 'Y';
    conn->dbc->var_len_req = 'N';
    conn->dbc->var_len_fetch = 'N';
    conn->dbc->save_resp_buf = 'N';
    conn->dbc->two_resp_bufs = 'Y';
    conn->dbc->ret_time = 'N';
    conn->dbc->parcel_mode = 'Y';
    conn->dbc->wait_for_resp = 'Y';
    conn->dbc->req_proc_opt = 'B';
    conn->dbc->return_statement_info = 'Y';
    conn->dbc->req_buf_len = 65535;
    conn->dbc->maximum_parcel = 'H';
    conn->dbc->max_decimal_returned = 38;
    conn->dbc->charset_type = 'N';
    conn->dbc->tx_semantics = 'T';
    conn->dbc->consider_APH_resps = 'Y';
    snprintf(conn->session_charset, sizeof(conn->session_charset), "%-*s",
        (int)(sizeof(conn->session_charset)-strlen(TERADATA_CHARSET)), TERADATA_CHARSET);
    conn->dbc->inter_ptr = conn->session_charset;
    sprintf(conn->logonstr, "%s/%s,%s", host, username, password);
    conn->dbc->logon_ptr = conn->logonstr;
    conn->dbc->logon_len = (UInt32)strlen(conn->logonstr);
    conn->dbc->func = DBFCON;
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    return conn->result;
}

uint16_t __teradata_fetch(TeradataConnection *conn) {
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFFET;
    return __teradata_fetch_record(conn);
}

uint16_t __teradata_fetch_record(TeradataConnection *conn) {
    Py_BEGIN_ALLOW_THREADS
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    Py_END_ALLOW_THREADS
    return conn->result;
}

uint16_t __teradata_execute(TeradataConnection *conn, const char *command) {
    conn->dbc->req_ptr = (char*)command;
    conn->dbc->req_len = (UInt32)strlen(command);
    conn->dbc->func = DBFIRQ;
    Py_BEGIN_ALLOW_THREADS
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    Py_END_ALLOW_THREADS
    if (conn->result == OK) {
        conn->request_status = REQUEST_OPEN;
    }
    return conn->result;
}

uint16_t __teradata_end_request(TeradataConnection *conn) {
    if (conn->request_status == REQUEST_CLOSED) {
        return conn->result;
    }
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFERQ;
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    if (conn->result == OK) {
        conn->request_status = REQUEST_CLOSED;
    }
    return conn->result;
}

void __teradata_close(TeradataConnection *conn) {
    if (conn->connected == CONNECTED) {
        conn->dbc->func = DBFDSC;
        Py_BEGIN_ALLOW_THREADS
        DBCHCL(&conn->result, conn->cnta, conn->dbc);
        Py_END_ALLOW_THREADS
    }
    // TODO: this presumably cleans up some of the global variables
    // used by CLIv2 and if done when working with multiple sessions
    // concurrently will cause a segfault. It is possible that
    // running it at all isn't really necessary and this can get cleaned
    // up with the process dying. Alternatives might be using atexit
    // calls to clean it up formally before the process exits and
    // also providing a function that can be called should it be necessary
    // for someone to clean this up (for like long running processes).
    // DBCHCLN(&conn->result, conn->cnta);
}

void __teradata_free(TeradataConnection *conn) {
    if (conn != NULL) {
        if (conn->dbc != NULL) {
            free(conn->dbc);
        }
        free(conn);
        conn = NULL;
    }
}

PyObject* check_parcel_error(TeradataConnection *conn) {
    TeradataErr *err;
    switch (conn->dbc->fet_parcel_flavor) {
    case PclSUCCESS:
        Py_RETURN_NONE;
    case PclFAILURE:
        err = __teradata_read_failure(conn->dbc->fet_data_ptr);
        if (err->Code == TD_ERROR_INVALID_USER) {
            PyErr_Format(InvalidCredentialsError, "%d: %s", err->Code, err->Msg);
        } else {
            PyErr_Format(TeradataError, "%d: %s", err->Code, err->Msg);
        }
        return NULL;
    case PclERROR:
        err = __teradata_read_error(conn->dbc->fet_data_ptr);
        PyErr_Format(TeradataError, "%d: %s", err->Code, err->Msg);
        return NULL;
    }
    return NULL;
}

PyObject* teradata_check_error(TeradataConnection *conn) {
    if (conn->result == REQEXHAUST && conn->connected) {
        if (__teradata_end_request(conn) != OK) {
            PyErr_Format(TeradataError, "%d: %s", conn->result, conn->dbc->msg_text);
            return NULL;
        }
        conn->connected = NOT_CONNECTED;
    } else if (conn->result != OK) {
        PyErr_Format(TeradataError, "%d: %s", conn->result, conn->dbc->msg_text);
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject* teradata_close(TeradataConnection *conn) {
    if (conn == NULL) {
        Py_RETURN_NONE;
    }
    if (__teradata_end_request(conn) != OK) {
        PyErr_Format(TeradataError, "%d: %s", conn->result, conn->dbc->msg_text);
        return NULL;
    }
    __teradata_close(conn);
    __teradata_free(conn);
    conn = NULL;
    Py_RETURN_NONE;
}

TeradataConnection* teradata_connect(const char *host, const char *username,
        const char *password) {
    int status;
    TeradataConnection *conn;
    conn = __teradata_new();
    if ((status = __teradata_init(conn)) != OK) {
        PyErr_Format(TeradataError, "%d: CLIv2[init]: %s", conn->result, conn->dbc->msg_text);
        __teradata_free(conn);
        return NULL;
    }
    if ((status = __teradata_connect(conn, host, username, password)) != OK) {
        PyErr_Format(TeradataError, "%d: CLIv2[connect]: %s", conn->result, conn->dbc->msg_text);
        __teradata_free(conn);
        return NULL;
    }
    if ((status = __teradata_fetch(conn)) != OK) {
        PyErr_Format(TeradataError, "%d: CLIv2[fetch]: %s", conn->result, conn->dbc->msg_text);
        __teradata_free(conn);
        return NULL;
    } else {
        if (check_parcel_error(conn) == NULL) {
            __teradata_free(conn);
            return NULL;
        }
    }
    if (__teradata_end_request(conn) != OK) {
        PyErr_Format(TeradataError, "%d: CLIv2[end_request]: %s", conn->result, conn->dbc->msg_text);
        __teradata_free(conn);
        return NULL;
    }
    conn->connected = CONNECTED;
    return conn;
}

PyObject* teradata_execute(TeradataConnection *conn, TeradataEncoder *e, const char *command) {
    PyObject *row = NULL;
    size_t count;
    if (__teradata_execute(conn, command) != OK) {
        PyErr_Format(TeradataError, "%d: CLIv2[execute_init]: %s", conn->result, conn->dbc->msg_text);
        return NULL;
    }
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFFET;
    count = 0;
    // Find first statement info parcel only
    while (__teradata_fetch_record(conn) == OK && count < MAX_PARCEL_ATTEMPTS) {
        if ((row = teradata_handle_record(e, conn->dbc->fet_parcel_flavor,
                (unsigned char**)&conn->dbc->fet_data_ptr,
                conn->dbc->fet_ret_data_len)) == NULL) {
            return NULL;
        }
        if (e != NULL && e->Columns != NULL) {
            Py_RETURN_NONE;
        }
        count++;
    }
    return teradata_check_error(conn);
}

PyObject* teradata_execute_rc(TeradataConnection *conn, TeradataEncoder *e, const char *command, int *rc) {
    TeradataErr *err;
    if (__teradata_execute(conn, command) != OK) {
        PyErr_Format(TeradataError, "%d: CLIv2[execute_init]: %s", conn->result, conn->dbc->msg_text);
        return NULL;
    }
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFFET;
    while (__teradata_fetch_record(conn) == OK) {
        switch (conn->dbc->fet_parcel_flavor) {
            case PclFAILURE:
                err = __teradata_read_failure(conn->dbc->fet_data_ptr);
                *rc = err->Code;
                PyErr_Format(TeradataError, "%d: %s", err->Code, err->Msg);
                return NULL;
            case PclERROR:
                err = __teradata_read_error(conn->dbc->fet_data_ptr);
                *rc = err->Code;
                PyErr_Format(TeradataError, "%d: %s", err->Code, err->Msg);
                return NULL;
        }
    }
    if (conn->result == TD_ERROR_REQUEST_EXHAUSTED && conn->connected == CONNECTED) {
        if (__teradata_end_request(conn) != OK) {
            *rc = conn->result;
            PyErr_Format(TeradataError, "%d: %s", conn->result, conn->dbc->msg_text);
            return NULL;
        }
    } else if (conn->result != OK) {
        *rc = conn->result;
        PyErr_Format(TeradataError, "%d: %s", conn->result, conn->dbc->msg_text);
        return NULL;
    }
    *rc = conn->result;
    Py_RETURN_NONE;
}

PyObject* teradata_execute_p(TeradataConnection *conn, TeradataEncoder *e, const char *command) {
    PyObject *result;
    const char tmp = conn->dbc->req_proc_opt;
    conn->dbc->req_proc_opt = 'P';
    result = teradata_execute(conn, e, command);
    conn->dbc->req_proc_opt = tmp;
    return result;
}

PyObject* teradata_handle_record(TeradataEncoder *e, const uint32_t parcel_t, unsigned char **data, const uint32_t length) {
    TeradataErr *err;
    PyGILState_STATE state;
    PyObject *row = NULL;
    switch (parcel_t) {
        case PclRECORD:
            state = PyGILState_Ensure();
            if ((row = e->UnpackRowFunc(e, data, length)) == NULL) {
                return NULL;
            }
            PyGILState_Release(state);
            return row;
        case PclSTATEMENTINFO:
            encoder_clear(e);
            e->Columns = e->UnpackStmtInfoFunc(data, length);
            break;
        case PclSTATEMENTINFOEND:
            PyErr_SetNone(EndStatementInfoError);
            return NULL;
        case PclENDSTATEMENT:
            PyErr_SetNone(EndStatementError);
            return NULL;
        case PclENDREQUEST:
            PyErr_SetNone(EndRequestError);
            return NULL;
        case PclFAILURE:
            err = __teradata_read_failure((char*)*data);
            if (err->Code == TD_ERROR_INVALID_USER) {
                PyErr_Format(InvalidCredentialsError, "%d: %s", err->Code, err->Msg);
            } else {
                PyErr_Format(TeradataError, "%d: %s", err->Code, err->Msg);
            }
            return NULL;
        case PclERROR:
            err = __teradata_read_error((char*)*data);
            PyErr_Format(TeradataError, "%d: %s", err->Code, err->Msg);
            return NULL;
    }
    Py_RETURN_NONE;
}

uint16_t teradata_type_to_tpt_type(uint16_t t) {
    switch (t) {
        case BLOB_NN:
        case BLOB_N:
        case BLOB_AS_DEFERRED_NN:
        case BLOB_AS_DEFERRED_N:
        case BLOB_AS_LOCATOR_NN:
        case BLOB_AS_LOCATOR_N:
            return TD_BLOB;
        case BLOB_AS_DEFERRED_NAME_NN:
        case BLOB_AS_DEFERRED_NAME_N:
            return TD_BLOB_AS_DEFERRED_BY_NAME;
        case CLOB_NN:
        case CLOB_N:
            return TD_CLOB;
        case CLOB_AS_DEFERRED_NN:
        case CLOB_AS_DEFERRED_N:
            return TD_CLOB_AS_DEFERRED_BY_NAME;
        case CLOB_AS_LOCATOR_NN:
        case CLOB_AS_LOCATOR_N:
            return TD_CLOB;
        case UDT_NN:
        case UDT_N:
        case DISTINCT_UDT_NN:
        case DISTINCT_UDT_N:
        case STRUCT_UDT_NN:
        case STRUCT_UDT_N:
            return TD_CHAR;
        case VARCHAR_NN:
        case VARCHAR_N:
            return TD_VARCHAR;
        case CHAR_NN:
        case CHAR_N:
            return TD_CHAR;
        case LONG_VARCHAR_NN:
        case LONG_VARCHAR_N:
            return TD_LONGVARCHAR;
        case VARGRAPHIC_NN:
        case VARGRAPHIC_N:
            return TD_VARGRAPHIC;
        case GRAPHIC_NN:
        case GRAPHIC_N:
            return TD_GRAPHIC;
        case LONG_VARGRAPHIC_NN:
        case LONG_VARGRAPHIC_N:
            return TD_LONGVARGRAPHIC;
        case FLOAT_NN:
        case FLOAT_N:
            return TD_FLOAT;
        case DECIMAL_NN:
        case DECIMAL_N:
            return TD_DECIMAL;
        case INTEGER_NN:
        case INTEGER_N:
            return TD_INTEGER;
        case SMALLINT_NN:
        case SMALLINT_N:
            return TD_SMALLINT;
        case ARRAY_1D_NN:
        case ARRAY_1D_N:
        case ARRAY_ND_NN:
        case ARRAY_ND_N:
            return TD_CHAR;
        case BIGINT_NN:
        case BIGINT_N:
            return TD_BIGINT;
        case VARBYTE_NN:
        case VARBYTE_N:
            return TD_VARBYTE;
        case BYTE_NN:
        case BYTE_N:
            return TD_BYTE;
        case LONG_VARBYTE_NN:
        case LONG_VARBYTE_N:
            return TD_LONGVARCHAR;
        case DATE_NNA:
        case DATE_NA:
            return TD_CHAR;
        case DATE_NN:
        case DATE_N:
            return TD_DATE;
        case BYTEINT_NN:
        case BYTEINT_N:
            return TD_BYTEINT;
        case TIME_NN:
        case TIME_N:
            return TD_TIME;
        case TIMESTAMP_NN:
        case TIMESTAMP_N:
            return TD_TIMESTAMP;
        case TIME_NNZ:
        case TIME_NZ:
            return TD_TIME_WITHTIMEZONE;
        case TIMESTAMP_NNZ:
        case TIMESTAMP_NZ:
            return TD_TIMESTAMP_WITHTIMEZONE;
        case INTERVAL_YEAR_NN:
        case INTERVAL_YEAR_N:
            return TD_INTERVAL_YEAR;
        case INTERVAL_YEAR_TO_MONTH_NN:
        case INTERVAL_YEAR_TO_MONTH_N:
            return TD_INTERVAL_YEARTOMONTH;
        case INTERVAL_MONTH_NN:
        case INTERVAL_MONTH_N:
            return TD_INTERVAL_MONTH;
        case INTERVAL_DAY_NN:
        case INTERVAL_DAY_N:
            return TD_INTERVAL_DAY;
        case INTERVAL_DAY_TO_HOUR_NN:
        case INTERVAL_DAY_TO_HOUR_N:
            return TD_INTERVAL_DAYTOHOUR;
        case INTERVAL_DAY_TO_MINUTE_NN:
        case INTERVAL_DAY_TO_MINUTE_N:
            return TD_INTERVAL_DAYTOMINUTE;
        case INTERVAL_DAY_TO_SECOND_NN:
        case INTERVAL_DAY_TO_SECOND_N:
            return TD_INTERVAL_DAYTOSECOND;
        case INTERVAL_HOUR_NN:
        case INTERVAL_HOUR_N:
            return TD_INTERVAL_HOUR;
        case INTERVAL_HOUR_TO_MINUTE_NN:
        case INTERVAL_HOUR_TO_MINUTE_N:
            return TD_INTERVAL_HOURTOMINUTE;
        case INTERVAL_HOUR_TO_SECOND_NN:
        case INTERVAL_HOUR_TO_SECOND_N:
            return TD_INTERVAL_HOURTOSECOND;
        case INTERVAL_MINUTE_NN:
        case INTERVAL_MINUTE_N:
            return TD_INTERVAL_MINUTE;
        case INTERVAL_MINUTE_TO_SECOND_NN:
        case INTERVAL_MINUTE_TO_SECOND_N:
            return TD_INTERVAL_MINUTETOSECOND;
        case INTERVAL_SECOND_NN:
        case INTERVAL_SECOND_N:
            return TD_INTERVAL_SECOND;
        case PERIOD_DATE_NN:
        case PERIOD_DATE_N:
            return TD_PERIOD_DATE;
        case PERIOD_TIME_NN:
        case PERIOD_TIME_N:
            return TD_PERIOD_TIME;
        case PERIOD_TIME_NNZ:
        case PERIOD_TIME_NZ:
            return TD_PERIOD_TIME_TZ;
        case PERIOD_TIMESTAMP_NN:
        case PERIOD_TIMESTAMP_N:
            return TD_PERIOD_TS;
        case PERIOD_TIMESTAMP_NNZ:
        case PERIOD_TIMESTAMP_NZ:
            return TD_PERIOD_TS_TZ;
        case XML_TEXT_NN:
        case XML_TEXT_N:
        case XML_TEXT_DEFERRED_NN:
        case XML_TEXT_DEFERRED_N:
        case XML_TEXT_LOCATOR_NN:
        case XML_TEXT_LOCATOR_N:
            return TD_CHAR;
    }
    return TD_CHAR;
}

uint16_t teradata_type_from_tpt_type(uint16_t t) {
    switch (t) {
        case TD_INTEGER:
            return INTEGER_NN;
        case TD_SMALLINT:
            return SMALLINT_NN;
        case TD_FLOAT:
            return FLOAT_NN;
        case TD_DECIMAL:
            return DECIMAL_NN;
        case TD_CHAR:
            return CHAR_NN;
        case TD_BYTEINT:
            return BYTEINT_NN;
        case TD_VARCHAR:
            return VARCHAR_NN;
        case TD_LONGVARCHAR:
            return LONG_VARCHAR_NN;
        case TD_BYTE:
            return BYTE_NN;
        case TD_VARBYTE:
            return VARBYTE_NN;
        case TD_DATE:
            return DATE_NN;
        case TD_GRAPHIC:
            return GRAPHIC_NN;
        case TD_VARGRAPHIC:
            return VARGRAPHIC_NN;
        case TD_LONGVARGRAPHIC:
            return LONG_VARGRAPHIC_NN;
        case TD_DATE_ANSI:
            return DATE_NNA;
        case TD_TIME:
            return TIME_NN;
        case TD_TIME_WITHTIMEZONE:
            return TIME_NNZ;
        case TD_BIGINT:
            return BIGINT_NN;
        case TD_BLOB:
            return BLOB_NN;
        case TD_CLOB:
            return CLOB_NN;
        case TD_BLOB_AS_DEFERRED_BY_NAME:
            return BLOB_AS_DEFERRED_NAME_NN;
        case TD_CLOB_AS_DEFERRED_BY_NAME:
            return CLOB_AS_DEFERRED_NN;
        case TD_TIMESTAMP:
            return TIMESTAMP_NN;
        case TD_TIMESTAMP_WITHTIMEZONE:
            return TIMESTAMP_NNZ;
        case TD_INTERVAL_YEAR:
            return INTERVAL_YEAR_NN;
        case TD_INTERVAL_YEARTOMONTH:
            return INTERVAL_YEAR_TO_MONTH_NN;
        case TD_INTERVAL_MONTH:
            return INTERVAL_MONTH_NN;
        case TD_INTERVAL_DAY:
            return INTERVAL_DAY_NN;
        case TD_INTERVAL_DAYTOHOUR:
            return INTERVAL_DAY_TO_HOUR_NN;
        case TD_INTERVAL_DAYTOMINUTE:
            return INTERVAL_DAY_TO_MINUTE_NN;
        case TD_INTERVAL_DAYTOSECOND:
            return INTERVAL_DAY_TO_SECOND_NN;
        case TD_INTERVAL_HOUR:
            return INTERVAL_HOUR_NN;
        case TD_INTERVAL_HOURTOMINUTE:
            return INTERVAL_HOUR_TO_MINUTE_NN;
        case TD_INTERVAL_HOURTOSECOND:
            return INTERVAL_HOUR_TO_SECOND_NN;
        case TD_INTERVAL_MINUTE:
            return INTERVAL_MINUTE_NN;
        case TD_INTERVAL_MINUTETOSECOND:
            return INTERVAL_MINUTE_TO_SECOND_NN;
        case TD_INTERVAL_SECOND:
            return INTERVAL_SECOND_NN;
        case TD_PERIOD_DATE:
            return PERIOD_DATE_NN;
        case TD_PERIOD_TIME:
            return PERIOD_TIME_NN;
        case TD_PERIOD_TIME_TZ:
            return PERIOD_TIME_NNZ;
        case TD_PERIOD_TS:
            return PERIOD_TIMESTAMP_NN;
        case TD_PERIOD_TS_TZ:
            return PERIOD_TIMESTAMP_NNZ;
        case TD_NUMBER:
            return INTEGER_NN;
    }
    return CHAR_NN;
}

uint16_t teradata_type_to_giraffez_type(uint16_t t) {
    switch (t) {
        case BLOB_NN:
        case BLOB_N:
        case BLOB_AS_DEFERRED_NN:
        case BLOB_AS_DEFERRED_N:
        case BLOB_AS_LOCATOR_NN:
        case BLOB_AS_LOCATOR_N:
        case BLOB_AS_DEFERRED_NAME_NN:
        case BLOB_AS_DEFERRED_NAME_N:
        case CLOB_NN:
        case CLOB_N:
        case CLOB_AS_DEFERRED_NN:
        case CLOB_AS_DEFERRED_N:
        case CLOB_AS_LOCATOR_NN:
        case CLOB_AS_LOCATOR_N:
        case UDT_NN:
        case UDT_N:
        case DISTINCT_UDT_NN:
        case DISTINCT_UDT_N:
        case STRUCT_UDT_NN:
        case STRUCT_UDT_N:
            return GD_DEFAULT;
        case VARCHAR_NN:
        case VARCHAR_N:
            return GD_VARCHAR;
        case CHAR_NN:
        case CHAR_N:
            return GD_CHAR;
        case LONG_VARCHAR_NN:
        case LONG_VARCHAR_N:
        case VARGRAPHIC_NN:
        case VARGRAPHIC_N:
            return GD_VARCHAR;
        case GRAPHIC_NN:
        case GRAPHIC_N:
            return GD_DEFAULT;
        case LONG_VARGRAPHIC_NN:
        case LONG_VARGRAPHIC_N:
            return GD_VARCHAR;
        case FLOAT_NN:
        case FLOAT_N:
            return GD_FLOAT;
        case DECIMAL_NN:
        case DECIMAL_N:
            return GD_DECIMAL;
        case INTEGER_NN:
        case INTEGER_N:
            return GD_INTEGER;
        case SMALLINT_NN:
        case SMALLINT_N:
            return GD_SMALLINT;
        case ARRAY_1D_NN:
        case ARRAY_1D_N:
        case ARRAY_ND_NN:
        case ARRAY_ND_N:
            return GD_DEFAULT;
        case BIGINT_NN:
        case BIGINT_N:
            return GD_BIGINT;
        case VARBYTE_NN:
        case VARBYTE_N:
            return GD_VARBYTE;
        case BYTE_NN:
        case BYTE_N:
            return GD_BYTE;
        case LONG_VARBYTE_NN:
        case LONG_VARBYTE_N:
            return GD_VARBYTE;
        case DATE_NNA:
        case DATE_NA:
            return GD_DEFAULT;
        case DATE_NN:
        case DATE_N:
            return GD_DATE;
        case BYTEINT_NN:
        case BYTEINT_N:
            return GD_BYTEINT;
        case TIME_NN:
        case TIME_N:
            return GD_TIME;
        case TIMESTAMP_NN:
        case TIMESTAMP_N:
            return GD_TIMESTAMP;
        case TIME_NNZ:
        case TIME_NZ:
            return GD_CHAR;
        case TIMESTAMP_NNZ:
        case TIMESTAMP_NZ:
            return GD_CHAR;
        case INTERVAL_YEAR_NN:
        case INTERVAL_YEAR_N:
        case INTERVAL_YEAR_TO_MONTH_NN:
        case INTERVAL_YEAR_TO_MONTH_N:
        case INTERVAL_MONTH_NN:
        case INTERVAL_MONTH_N:
        case INTERVAL_DAY_NN:
        case INTERVAL_DAY_N:
        case INTERVAL_DAY_TO_HOUR_NN:
        case INTERVAL_DAY_TO_HOUR_N:
        case INTERVAL_DAY_TO_MINUTE_NN:
        case INTERVAL_DAY_TO_MINUTE_N:
        case INTERVAL_DAY_TO_SECOND_NN:
        case INTERVAL_DAY_TO_SECOND_N:
        case INTERVAL_HOUR_NN:
        case INTERVAL_HOUR_N:
        case INTERVAL_HOUR_TO_MINUTE_NN:
        case INTERVAL_HOUR_TO_MINUTE_N:
        case INTERVAL_HOUR_TO_SECOND_NN:
        case INTERVAL_HOUR_TO_SECOND_N:
        case INTERVAL_MINUTE_NN:
        case INTERVAL_MINUTE_N:
        case INTERVAL_MINUTE_TO_SECOND_NN:
        case INTERVAL_MINUTE_TO_SECOND_N:
        case INTERVAL_SECOND_NN:
        case INTERVAL_SECOND_N:
        case PERIOD_DATE_NN:
        case PERIOD_DATE_N:
        case PERIOD_TIME_NN:
        case PERIOD_TIME_N:
        case PERIOD_TIME_NNZ:
        case PERIOD_TIME_NZ:
        case PERIOD_TIMESTAMP_NN:
        case PERIOD_TIMESTAMP_N:
        case PERIOD_TIMESTAMP_NNZ:
        case PERIOD_TIMESTAMP_NZ:
        case XML_TEXT_NN:
        case XML_TEXT_N:
        case XML_TEXT_DEFERRED_NN:
        case XML_TEXT_DEFERRED_N:
        case XML_TEXT_LOCATOR_NN:
        case XML_TEXT_LOCATOR_N:
            return GD_DEFAULT;
    }
    return GD_DEFAULT;
}
