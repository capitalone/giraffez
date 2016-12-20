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

#include "types.h"

#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif


const uint16_t VARCHAR_NULL_LENGTH = 2;


uint16_t tpt_type_to_teradata_type(uint16_t t) {
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
    if (t < BLOB_NN) {
        t = tpt_type_to_teradata_type(t);
    }
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
            return GD_VARCHAR;
        case BYTE_NN:
        case BYTE_N:
            return GD_CHAR;
        case LONG_VARBYTE_NN:
        case LONG_VARBYTE_N:
            return GD_VARCHAR;
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
