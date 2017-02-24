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


#include "tdcli.h"

#include <stdio.h>

// Teradata CLIv2
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>


TeradataError* tdcli_read_error(char *dataptr) {
    TeradataError *err;
    err = (TeradataError*)malloc(sizeof(TeradataError));
    struct CliErrorType *e;
    e = (struct CliErrorType*)dataptr;
    err->Code = e->Code;
    err->Msg = e->Msg;
    return err;
}

/*TeradataFailure* tdcli_read_failure(char *dataptr) {*/
TeradataError* tdcli_read_failure(char *dataptr) {
    TeradataError *err;
    err = (TeradataError*)malloc(sizeof(TeradataError));
    struct CliFailureType *e;
    e = (struct CliFailureType*)dataptr;
    err->Code = e->Code;
    err->Msg = e->Msg;
    return err;
}

TeradataConnection* tdcli_new() {
    TeradataConnection *conn;
    conn = (TeradataConnection*)malloc(sizeof(TeradataConnection));
    conn->result = 0;
    conn->connected = NOT_CONNECTED;
    conn->request_status = REQUEST_CLOSED;
    conn->dbc = (dbcarea_t*)malloc(sizeof(dbcarea_t));
    conn->dbc->total_len = sizeof(*conn->dbc);
    return conn;
}

uint16_t tdcli_init(TeradataConnection *conn) {
    DBCHINI(&conn->result, conn->cnta, conn->dbc);
    return conn->result;
}

uint16_t tdcli_connect(TeradataConnection *conn, const char *host, const char *username, const char *password) {
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
    // TODO: should this be in ANSI or Teradata mode? The default on the current system is Teradata
    /*conn->dbc->tx_semantics = 'T';*/
    /*conn->dbc->tx_semantics = 'A';*/
    conn->dbc->consider_APH_resps = 'Y';
    snprintf(conn->session_charset, 32, "%-30s", "UTF8");
    conn->dbc->inter_ptr = conn->session_charset;
    sprintf(conn->logonstr, "%s/%s,%s", host, username, password);
    conn->dbc->logon_ptr = conn->logonstr;
    conn->dbc->logon_len = (UInt32)strlen(conn->logonstr);
    conn->dbc->func = DBFCON;
    // TODO: may cause weird race condition? not sure just happened once
    Py_BEGIN_ALLOW_THREADS
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    Py_END_ALLOW_THREADS
    return conn->result;
}

uint16_t tdcli_fetch(TeradataConnection *conn) {
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFFET;
    return tdcli_fetch_record(conn);
}

// TODO: Py_BEGIN_ALLOW_THREADS should maybe be moved so that
// this file can be free of depedency on Python
// TODO: or should it???
uint16_t tdcli_fetch_record(TeradataConnection *conn) {
    Py_BEGIN_ALLOW_THREADS
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    Py_END_ALLOW_THREADS
    return conn->result;
}

uint16_t tdcli_execute(TeradataConnection *conn, const char *command) {
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

uint16_t tdcli_end_request(TeradataConnection *conn) {
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

void tdcli_close(TeradataConnection *conn) {
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

void tdcli_free(TeradataConnection *conn) {
    if (conn != NULL) {
        if (conn->dbc != NULL) {
            free(conn->dbc);
        }
        free(conn);
        conn = NULL;
    }
}
