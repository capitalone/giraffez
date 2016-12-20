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
    err = (struct CliErrorType*)dataptr;
    return err;
}

TeradataFailure* tdcli_read_failure(char *dataptr) {
    TeradataFailure *err;
    err = (struct CliFailureType*)dataptr;
    return err;
}

TeradataConnection* tdcli_new() {
    TeradataConnection *conn;
    conn = (TeradataConnection*)malloc(sizeof(TeradataConnection));
    conn->result = 0;
    conn->dbc = (dbcarea_t*)malloc(sizeof(dbcarea_t));
    conn->dbc->total_len = sizeof(*conn->dbc);
    DBCHINI(&conn->result, conn->cnta, conn->dbc);
    return conn;
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
    /*conn->dbc->req_proc_opt = 'P';*/
    conn->dbc->return_statement_info = 'Y';
    conn->dbc->req_buf_len = 65535;
    conn->dbc->maximum_parcel = 'H';
    conn->dbc->max_decimal_returned = 38;
    conn->dbc->charset_type = 'N';
    conn->dbc->consider_APH_resps = 'Y';
    snprintf(conn->session_charset, 32, "%-30s", "UTF8");
    conn->dbc->inter_ptr = conn->session_charset;
    sprintf(conn->logonstr, "%s/%s,%s", host, username, password);
    conn->dbc->logon_ptr = conn->logonstr;
    conn->dbc->logon_len = (UInt32)strlen(conn->logonstr);
    conn->dbc->func = DBFCON;
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    return conn->result;
}

uint16_t tdcli_fetch(TeradataConnection *conn) {
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFFET;
    return tdcli_fetch_record(conn);
}

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
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    return conn->result;
}

uint16_t tdcli_end_request(TeradataConnection *conn) {
    conn->dbc->i_sess_id = conn->dbc->o_sess_id;
    conn->dbc->i_req_id = conn->dbc->o_req_id;
    conn->dbc->func = DBFERQ;
    DBCHCL(&conn->result, conn->cnta, conn->dbc);
    return conn->result;
}

void tdcli_close(TeradataConnection *conn, uint16_t connected) {
    if (connected == CONNECTED) {
        conn->dbc->func = DBFDSC;
        DBCHCL(&conn->result, conn->cnta, conn->dbc);
    }
    DBCHCLN(&conn->result, conn->cnta);
}

void tdcli_free(TeradataConnection *conn) {
    if (conn != NULL) {
        if (conn->dbc != NULL) {
            free(conn->dbc);
        }
        free(conn);
    }
}
