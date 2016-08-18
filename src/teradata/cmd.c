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

#include "cmd.h"
#include <string.h>
#include <stdio.h>
#include <coperr.h>
#include <dbcarea.h>
#include <parcel.h>
#include "compat.h"


char session_charset[36];

int set_options(dbcarea_t* dbc) {
    dbc->change_opts = 'Y';
    dbc->resp_mode = 'I';
    dbc->use_presence_bits = 'N';
    dbc->keep_resp = 'N';
    dbc->wait_across_crash = 'N';
    dbc->tell_about_crash = 'Y';
    dbc->loc_mode = 'Y';
    dbc->var_len_req = 'N';
    dbc->var_len_fetch = 'N';
    dbc->save_resp_buf = 'N';
    dbc->two_resp_bufs = 'N';
    dbc->ret_time = 'N';
    dbc->parcel_mode = 'Y';
    dbc->wait_for_resp = 'Y';
    dbc->req_proc_opt = 'B';
    dbc->return_statement_info = 'Y';
    dbc->req_buf_len = 65535;
    dbc->maximum_parcel = 'H';
    dbc->max_decimal_returned = 38;
    dbc->charset_type = 'N';
    snprintf(session_charset, 32, "%-30s", "UTF8");
    dbc->inter_ptr = session_charset;
    return 0;
}

int exitout(dbcarea_t* dbc, char cnta[], int status) {
    Int32 result = EM_OK;
    if (status == CONNECTED)
    {
        dbc->func = DBFDSC;
        DBCHCL(&result, cnta, dbc);
        if(result != EM_OK) {
            // disconnect failed
        }
    }
    DBCHCLN(&result, cnta);
    if(result != EM_OK) {
        // cleanup failed
    }
    return result;
}

int initialize_dbcarea(dbcarea_t* dbc, char cnta[]) {
    Int32 result = EM_OK;
    dbc->total_len = sizeof(dbcarea_t);
    DBCHINI(&result, cnta, dbc);
    if (result != EM_OK) {
        // init failed
        exitout(dbc, cnta, NOT_CONNECTED);
    }
    return result;
}
int logon_to_dbc(dbcarea_t* dbc, char cnta[], char logonstr[]) {
    Int32 result = EM_OK;
    dbc->logon_ptr = logonstr;
    dbc->logon_len = (UInt32) strlen(logonstr);
    dbc->func = DBFCON;

    DBCHCL(&result, cnta, dbc);

    if (result != EM_OK) {
        // connect failed
        exitout(dbc, cnta, NOT_CONNECTED);
    }
    return result;
}

int open_request(dbcarea_t* dbc, char cnta[], char rqst[]) {
    Int32 result = EM_OK;
    dbc->req_ptr = rqst;
    dbc->req_len = (UInt32) strlen(rqst);
    dbc->func = DBFIRQ;

    DBCHCL(&result, cnta, dbc);
    if (result != EM_OK) {
        // initiate request failed
        exitout(dbc, cnta, CONNECTED);
    }
    return result;
}

int handle_record(dbcarea_t* dbc, char cnta[]) {
    Int32 result = EM_OK;
    int status = OK;
    DBCHCL(&result, cnta, dbc);
    if (result == REQEXHAUST) {
        status = STOP;
    } else if (result != EM_OK) {
        status = FAILED;
    } else {
        switch ((Int16) (dbc->fet_parcel_flavor)) {
        case PclSUCCESS:
            break;
        case PclRECORD: 
            break;
        case PclFAILURE:
            status = PCL_FAIL;
            break;
        case PclERROR:
            status = PCL_ERR;
            break;
        }
    }
    return status;
}

int fetch_request(dbcarea_t* dbc, char cnta[], Int32 req_id, Int32 sess_id) {
    dbc->i_sess_id = sess_id;
    dbc->i_req_id = req_id;
    dbc->func = DBFFET;
    return 0;
}

int close_request(dbcarea_t* dbc, char cnta[], Int32 req_id, Int32 sess_id) {
    Int32 result = EM_OK;
    dbc->i_sess_id = sess_id;
    dbc->i_req_id = req_id;
    dbc->func=DBFERQ;

    DBCHCL(&result, cnta, dbc);
    if (result != EM_OK) {
        // end request failed
        exitout(dbc, cnta, CONNECTED);
    }
    return result;
}

int open_connection(dbcarea_t* dbc, char cnta[], char logonstr[]) {
    int status = 0;
    initialize_dbcarea(dbc, cnta);
    set_options(dbc);
    logon_to_dbc(dbc, cnta, logonstr);
    fetch_request(dbc, cnta, dbc->o_req_id, dbc->o_sess_id);
    status = handle_record(dbc, cnta);
    close_request(dbc, cnta, dbc->o_req_id, dbc->o_sess_id);
    return status;
}

int close_connection(dbcarea_t* dbc, char cnta[]) {
    return exitout(dbc, cnta, CONNECTED);
}
