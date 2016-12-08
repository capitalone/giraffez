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

#ifndef __ENCODER_TYPES_H
#define __ENCODER_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(WIN32) || defined(WIN64)
#include <pstdint.h>
#else
#include <stdint.h>
#endif


extern const uint16_t VARCHAR_NULL_LENGTH;

enum GiraffeTypes {
    GD_DEFAULT = 0,
    GD_BYTEINT,
    GD_SMALLINT,
    GD_INTEGER,
    GD_BIGINT,
    GD_FLOAT,
    GD_DECIMAL,
    GD_CHAR,
    GD_VARCHAR,
    GD_DATE,
    GD_TIME,
    GD_TIMESTAMP
};

enum TeradataTypes {
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
};

uint16_t teradata_type_to_giraffez_type(uint16_t t);

#ifdef __cplusplus
}
#endif

#endif
