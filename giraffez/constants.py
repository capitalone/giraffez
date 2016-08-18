# -*- coding: utf-8 -*-

"""
Contains all constants used by giraffez, for settings and for
communicating with Teradata.
"""

ascii_giraffe = """
               ._ O O
               \_`-\|_
            ,""       \       giraffez
          ,":::  |   o o.
        ,"''''  ,-\__    `.
      ,".   .::/     `--._;)
    ,":::  :::/           U
  ,".::::.  :/
"""

GIRAFFE_MAGIC = b"\x5e\x1f\x1e"
GZIP_MAGIC = b"\x1f\x8b"

SUCCESS = 0
FAILURE = 1

SILENCE = -1
INFO    = 0
VERBOSE = 1
DEBUG   = 2

CLI_BLOCK_SIZE = 64260
MLOAD_THRESHOLD = 100000
DEFAULT_NULL = "NULL"
DEFAULT_ENCODING = "text"
DEFAULT_DELIMITER = "|"
DEFAULT_CHECKPOINT_INTERVAL = 50000

### Teradata

# TPT types
TD_NONE                         = 0
TD_INTEGER                      = 1
TD_SMALLINT                     = 2
TD_FLOAT                        = 3
TD_DECIMAL                      = 4
TD_CHAR                         = 5
TD_BYTEINT                      = 6
TD_VARCHAR                      = 7
TD_LONGVARCHAR                  = 8
TD_BYTE                         = 9
TD_VARBYTE                      = 10
TD_DATE                         = 11
TD_GRAPHIC                      = 12
TD_VARGRAPHIC                   = 13
TD_LONGVARGRAPHIC               = 14
TD_DATE_ANSI                    = 15
TD_TIME                         = 35
TD_TIME_WITHTIMEZONE            = 36
TD_BIGINT                       = 37
TD_BLOB                         = 39
TD_CLOB                         = 40
TD_BLOB_AS_DEFERRED_BY_NAME     = 41
TD_CLOB_AS_DEFERRED_BY_NAME     = 42
TD_TIMESTAMP                    = 43
TD_TIMESTAMP_WITHTIMEZONE       = 44
TD_INTERVAL_YEAR                = 45
TD_INTERVAL_YEARTOMONTH         = 46
TD_INTERVAL_MONTH               = 47
TD_INTERVAL_DAY                 = 48
TD_INTERVAL_DAYTOHOUR           = 49
TD_INTERVAL_DAYTOMINUTE         = 50
TD_INTERVAL_DAYTOSECOND         = 51
TD_INTERVAL_HOUR                = 52
TD_INTERVAL_HOURTOMINUTE        = 53
TD_INTERVAL_HOURTOSECOND        = 54
TD_INTERVAL_MINUTE              = 55
TD_INTERVAL_MINUTETOSECOND      = 56
TD_INTERVAL_SECOND              = 57
TD_PERIOD_DATE                  = 58
TD_PERIOD_TIME                  = 59
TD_PERIOD_TIME_TZ               = 60
TD_PERIOD_TS                    = 61
TD_PERIOD_TS_TZ                 = 62
TD_NUMBER                       = 63


#CLIv2 types
BLOB_NN                         = 400
BLOB_N                          = 401
BLOB_AS_DEFERRED_NN             = 404
BLOB_AS_DEFERRED_N              = 405
BLOB_AS_LOCATOR_NN              = 408
BLOB_AS_LOCATOR_N               = 409
BLOB_AS_DEFERRED_NAME_NN        = 412
BLOB_AS_DEFERRED_NAME_N         = 413
CLOB_NN                         = 416
CLOB_N                          = 417
CLOB_AS_DEFERRED_NN             = 420
CLOB_AS_DEFERRED_N              = 421
CLOB_AS_LOCATOR_NN              = 424
CLOB_AS_LOCATOR_N               = 425
UDT_NN                          = 432
UDT_N                           = 433
DISTINCT_UDT_NN                 = 436
DISTINCT_UDT_N                  = 437
STRUCT_UDT_NN                   = 440
STRUCT_UDT_N                    = 441
VARCHAR_NN                      = 448
VARCHAR_N                       = 449
CHAR_NN                         = 452
CHAR_N                          = 453
LONG_VARCHAR_NN                 = 456
LONG_VARCHAR_N                  = 457
VARGRAPHIC_NN                   = 464
VARGRAPHIC_N                    = 465
GRAPHIC_NN                      = 468
GRAPHIC_N                       = 469
LONG_VARGRAPHIC_NN              = 472
LONG_VARGRAPHIC_N               = 473
FLOAT_NN                        = 480
FLOAT_N                         = 481
DECIMAL_NN                      = 484
DECIMAL_N                       = 485
INTEGER_NN                      = 496
INTEGER_N                       = 497
SMALLINT_NN                     = 500
SMALLINT_N                      = 501
ARRAY_1D_NN                     = 504
ARRAY_1D_N                      = 505
ARRAY_ND_NN                     = 508
ARRAY_ND_N                      = 509
BIGINT_NN                       = 600
BIGINT_N                        = 601
VARBYTE_NN                      = 688
VARBYTE_N                       = 689
BYTE_NN                         = 692
BYTE_N                          = 693
LONG_VARBYTE_NN                 = 696
LONG_VARBYTE_N                  = 697
DATE_NNA                        = 748
DATE_NA                         = 749
DATE_NN                         = 752
DATE_N                          = 753
BYTEINT_NN                      = 756
BYTEINT_N                       = 757
TIME_NN                         = 760
TIME_N                          = 761
TIMESTAMP_NN                    = 764
TIMESTAMP_N                     = 765
TIME_NNZ                        = 768
TIME_NZ                         = 769
TIMESTAMP_NNZ                   = 772
TIMESTAMP_NZ                    = 773
INTERVAL_YEAR_NN                = 776
INTERVAL_YEAR_N                 = 777
INTERVAL_YEAR_TO_MONTH_NN       = 780
INTERVAL_YEAR_TO_MONTH_N        = 781
INTERVAL_MONTH_NN               = 784
INTERVAL_MONTH_N                = 785
INTERVAL_DAY_NN                 = 788
INTERVAL_DAY_N                  = 789
INTERVAL_DAY_TO_HOUR_NN         = 792
INTERVAL_DAY_TO_HOUR_N          = 793
INTERVAL_DAY_TO_MINUTE_NN       = 796
INTERVAL_DAY_TO_MINUTE_N        = 797
INTERVAL_DAY_TO_SECOND_NN       = 800
INTERVAL_DAY_TO_SECOND_N        = 801
INTERVAL_HOUR_NN                = 804
INTERVAL_HOUR_N                 = 805
INTERVAL_HOUR_TO_MINUTE_NN      = 808
INTERVAL_HOUR_TO_MINUTE_N       = 809
INTERVAL_HOUR_TO_SECOND_NN      = 812
INTERVAL_HOUR_TO_SECOND_N       = 813
INTERVAL_MINUTE_NN              = 816
INTERVAL_MINUTE_N               = 817
INTERVAL_MINUTE_TO_SECOND_NN    = 820
INTERVAL_MINUTE_TO_SECOND_N     = 821
INTERVAL_SECOND_NN              = 824
INTERVAL_SECOND_N               = 825
PERIOD_DATE_NN                  = 832
PERIOD_DATE_N                   = 833
PERIOD_TIME_NN                  = 836
PERIOD_TIME_N                   = 837
PERIOD_TIME_NNZ                 = 840
PERIOD_TIME_NZ                  = 841
PERIOD_TIMESTAMP_NN             = 844
PERIOD_TIMESTAMP_N              = 845
PERIOD_TIMESTAMP_NNZ            = 848
PERIOD_TIMESTAMP_NZ             = 849
XML_TEXT_NN                     = 852
XML_TEXT_N                      = 853
XML_TEXT_DEFERRED_NN            = 856
XML_TEXT_DEFERRED_N             = 857
XML_TEXT_LOCATOR_NN             = 860
XML_TEXT_LOCATOR_N              = 861

# CLIv2 type code -> name
COL_TYPE_NAMES = {
    BLOB_NN: "BLOB_NN",
    BLOB_N: "BLOB_N",
    BLOB_AS_DEFERRED_NN: "BLOB_AS_DEFERRED_NN",
    BLOB_AS_DEFERRED_N: "BLOB_AS_DEFERRED_N",
    BLOB_AS_LOCATOR_NN: "BLOB_AS_LOCATOR_NN",
    BLOB_AS_LOCATOR_N: "BLOB_AS_LOCATOR_N",
    BLOB_AS_DEFERRED_NAME_NN: "BLOB_AS_DEFERRED_NAME_NN",
    BLOB_AS_DEFERRED_NAME_N: "BLOB_AS_DEFERRED_NAME_N",
    CLOB_NN: "CLOB_NN",
    CLOB_N: "CLOB_N",
    CLOB_AS_DEFERRED_NN: "CLOB_AS_DEFERRED_NN",
    CLOB_AS_DEFERRED_N: "CLOB_AS_DEFERRED_N",
    CLOB_AS_LOCATOR_NN: "CLOB_AS_LOCATOR_NN",
    CLOB_AS_LOCATOR_N: "CLOB_AS_LOCATOR_N",
    UDT_NN: "UDT_NN",
    UDT_N: "UDT_N",
    DISTINCT_UDT_NN: "DISTINCT_UDT_NN",
    DISTINCT_UDT_N: "DISTINCT_UDT_N",
    STRUCT_UDT_NN: "STRUCT_UDT_NN",
    STRUCT_UDT_N: "STRUCT_UDT_N",
    VARCHAR_NN: "VARCHAR_NN",
    VARCHAR_N: "VARCHAR_N",
    CHAR_NN: "CHAR_NN",
    CHAR_N: "CHAR_N",
    LONG_VARCHAR_NN: "LONG_VARCHAR_NN",
    LONG_VARCHAR_N: "LONG_VARCHAR_N",
    VARGRAPHIC_NN: "VARGRAPHIC_NN",
    VARGRAPHIC_N: "VARGRAPHIC_N",
    GRAPHIC_NN: "GRAPHIC_NN",
    GRAPHIC_N: "GRAPHIC_N",
    LONG_VARGRAPHIC_NN: "LONG_VARGRAPHIC_NN",
    LONG_VARGRAPHIC_N: "LONG_VARGRAPHIC_N",
    FLOAT_NN: "FLOAT_NN",
    FLOAT_N: "FLOAT_N",
    DECIMAL_NN: "DECIMAL_NN",
    DECIMAL_N: "DECIMAL_N",
    INTEGER_NN: "INTEGER_NN",
    INTEGER_N: "INTEGER_N",
    SMALLINT_NN: "SMALLINT_NN",
    SMALLINT_N: "SMALLINT_N",
    ARRAY_1D_NN: "ARRAY_1D_NN",
    ARRAY_1D_N: "ARRAY_1D_N",
    ARRAY_ND_NN: "ARRAY_ND_NN",
    ARRAY_ND_N: "ARRAY_ND_N",
    BIGINT_NN: "BIGINT_NN",
    BIGINT_N: "BIGINT_N",
    VARBYTE_NN: "VARBYTE_NN",
    VARBYTE_N: "VARBYTE_N",
    BYTE_NN: "BYTE_NN",
    BYTE_N: "BYTE_N",
    LONG_VARBYTE_NN: "LONG_VARBYTE_NN",
    LONG_VARBYTE_N: "LONG_VARBYTE_N",
    DATE_NNA: "DATE_NNA",
    DATE_NA: "DATE_NA",
    DATE_NN: "DATE_NN",
    DATE_N: "DATE_N",
    BYTEINT_NN: "BYTEINT_NN",
    BYTEINT_N: "BYTEINT_N",
    TIME_NN: "TIME_NN",
    TIME_N: "TIME_N",
    TIMESTAMP_NN: "TIMESTAMP_NN",
    TIMESTAMP_N: "TIMESTAMP_N",
    TIME_NNZ: "TIME_NNZ",
    TIME_NZ: "TIME_NZ",
    TIMESTAMP_NNZ: "TIMESTAMP_NNZ",
    TIMESTAMP_NZ: "TIMESTAMP_NZ",
    INTERVAL_YEAR_NN: "INTERVAL_YEAR_NN",
    INTERVAL_YEAR_N: "INTERVAL_YEAR_N",
    INTERVAL_YEAR_TO_MONTH_NN: "INTERVAL_YEAR_TO_MONTH_NN",
    INTERVAL_YEAR_TO_MONTH_N: "INTERVAL_YEAR_TO_MONTH_N",
    INTERVAL_MONTH_NN: "INTERVAL_MONTH_NN",
    INTERVAL_MONTH_N: "INTERVAL_MONTH_N",
    INTERVAL_DAY_NN: "INTERVAL_DAY_NN",
    INTERVAL_DAY_N: "INTERVAL_DAY_N",
    INTERVAL_DAY_TO_HOUR_NN: "INTERVAL_DAY_TO_HOUR_NN",
    INTERVAL_DAY_TO_HOUR_N: "INTERVAL_DAY_TO_HOUR_N",
    INTERVAL_DAY_TO_MINUTE_NN: "INTERVAL_DAY_TO_MINUTE_NN",
    INTERVAL_DAY_TO_MINUTE_N: "INTERVAL_DAY_TO_MINUTE_N",
    INTERVAL_DAY_TO_SECOND_NN: "INTERVAL_DAY_TO_SECOND_NN",
    INTERVAL_DAY_TO_SECOND_N: "INTERVAL_DAY_TO_SECOND_N",
    INTERVAL_HOUR_NN: "INTERVAL_HOUR_NN",
    INTERVAL_HOUR_N: "INTERVAL_HOUR_N",
    INTERVAL_HOUR_TO_MINUTE_NN: "INTERVAL_HOUR_TO_MINUTE_NN",
    INTERVAL_HOUR_TO_MINUTE_N: "INTERVAL_HOUR_TO_MINUTE_N",
    INTERVAL_HOUR_TO_SECOND_NN: "INTERVAL_HOUR_TO_SECOND_NN",
    INTERVAL_HOUR_TO_SECOND_N: "INTERVAL_HOUR_TO_SECOND_N",
    INTERVAL_MINUTE_NN: "INTERVAL_MINUTE_NN",
    INTERVAL_MINUTE_N: "INTERVAL_MINUTE_N",
    INTERVAL_MINUTE_TO_SECOND_NN: "INTERVAL_MINUTE_TO_SECOND_NN",
    INTERVAL_MINUTE_TO_SECOND_N: "INTERVAL_MINUTE_TO_SECOND_N",
    INTERVAL_SECOND_NN: "INTERVAL_SECOND_NN",
    INTERVAL_SECOND_N: "INTERVAL_SECOND_N",
    PERIOD_DATE_NN: "PERIOD_DATE_NN",
    PERIOD_DATE_N: "PERIOD_DATE_N",
    PERIOD_TIME_NN: "PERIOD_TIME_NN",
    PERIOD_TIME_N: "PERIOD_TIME_N",
    PERIOD_TIME_NNZ: "PERIOD_TIME_NNZ",
    PERIOD_TIME_NZ: "PERIOD_TIME_NZ",
    PERIOD_TIMESTAMP_NN: "PERIOD_TIMESTAMP_NN",
    PERIOD_TIMESTAMP_N: "PERIOD_TIMESTAMP_N",
    PERIOD_TIMESTAMP_NNZ: "PERIOD_TIMESTAMP_NNZ",
    PERIOD_TIMESTAMP_NZ: "PERIOD_TIMESTAMP_NZ",
    XML_TEXT_NN: "XML_TEXT_NN",
    XML_TEXT_N: "XML_TEXT_N",
    XML_TEXT_DEFERRED_NN: "XML_TEXT_DEFERRED_NN",
    XML_TEXT_DEFERRED_N: "XML_TEXT_DEFERRED_N",
    XML_TEXT_LOCATOR_NN: "XML_TEXT_LOCATOR_NN",
    XML_TEXT_LOCATOR_N: "XML_TEXT_LOCATOR_N",
}

NULLABLE_TYPES = {
    BLOB_N,
    BLOB_AS_DEFERRED_N,
    BLOB_AS_LOCATOR_N,
    BLOB_AS_DEFERRED_NAME_N,
    CLOB_N,
    CLOB_AS_DEFERRED_N,
    CLOB_AS_LOCATOR_N,
    UDT_N,
    DISTINCT_UDT_N,
    STRUCT_UDT_N,
    VARCHAR_N,
    CHAR_N,
    LONG_VARCHAR_N,
    VARGRAPHIC_N,
    GRAPHIC_N,
    LONG_VARGRAPHIC_N,
    FLOAT_N,
    DECIMAL_N,
    INTEGER_N,
    SMALLINT_N,
    ARRAY_1D_N,
    ARRAY_ND_N,
    BIGINT_N,
    VARBYTE_N,
    BYTE_N,
    LONG_VARBYTE_N,
    DATE_NA,
    DATE_N,
    BYTEINT_N,
    TIME_N,
    TIMESTAMP_N,
    TIME_NZ,
    TIMESTAMP_NZ,
    INTERVAL_YEAR_N,
    INTERVAL_YEAR_TO_MONTH_N,
    INTERVAL_MONTH_N,
    INTERVAL_DAY_N,
    INTERVAL_DAY_TO_HOUR_N,
    INTERVAL_DAY_TO_MINUTE_N,
    INTERVAL_DAY_TO_SECOND_N,
    INTERVAL_HOUR_N,
    INTERVAL_HOUR_TO_MINUTE_N,
    INTERVAL_HOUR_TO_SECOND_N,
    INTERVAL_MINUTE_N,
    INTERVAL_MINUTE_TO_SECOND_N,
    INTERVAL_SECOND_N,
    PERIOD_DATE_N,
    PERIOD_TIME_N,
    PERIOD_TIME_NZ,
    PERIOD_TIMESTAMP_N,
    PERIOD_TIMESTAMP_NZ,
    XML_TEXT_N,
    XML_TEXT_DEFERRED_N,
    XML_TEXT_LOCATOR_N
}

# Teradata uses space-padded characters representing nulls
STRING_NULLS = {
    CHAR_N,
    CHAR_NN,
    TIME_N,
    TIME_NN,
    DATE_N,
    DATE_NN,
    TIMESTAMP_N,
    TIMESTAMP_NN,
    TIMESTAMP_NZ
}

# Null is 0-length variable header
VARIABLE_NULLS = {
    VARCHAR_N,
    VARCHAR_NN,
    LONG_VARCHAR_N,
    LONG_VARCHAR_NN,
    VARBYTE_N,
    VARBYTE_NN,
    LONG_VARBYTE_N,
    LONG_VARBYTE_NN,
    VARGRAPHIC_NN,
    VARGRAPHIC_N,
    LONG_VARGRAPHIC_NN,
    LONG_VARGRAPHIC_N,
}

BYTE_TYPES = {
    BYTE_N,
    BYTE_NN
}

CHAR_TYPES = {
    CHAR_N,
    CHAR_NN
}

# Numeric types
BYTEINT_TYPES = {
    BYTEINT_N,
    BYTEINT_NN
}

SMALLINT_TYPES = {
    SMALLINT_N,
    SMALLINT_NN
}

INTEGER_TYPES = {
    INTEGER_N,
    INTEGER_NN
}

BIGINT_TYPES = {
    BIGINT_N,
    BIGINT_NN
}

ALL_INTEGER_TYPES = {
    BYTEINT_N,
    BYTEINT_NN,
    SMALLINT_N,
    SMALLINT_NN,
    INTEGER_N,
    INTEGER_NN,
    BIGINT_N,
    BIGINT_NN
}

FLOAT_TYPES = {
    FLOAT_N,
    FLOAT_NN
}

DECIMAL_TYPES = {
    DECIMAL_N,
    DECIMAL_NN
}

VAR_TYPES = {
    VARCHAR_N,
    VARCHAR_NN,
    LONG_VARCHAR_N,
    LONG_VARCHAR_NN,
    VARBYTE_N,
    VARBYTE_NN,
    LONG_VARBYTE_N,
    LONG_VARBYTE_NN,
    VARGRAPHIC_NN,
    VARGRAPHIC_N,
    LONG_VARGRAPHIC_NN,
    LONG_VARGRAPHIC_N,
}

DATE_TYPES = {
    DATE_N,
    DATE_NN
}

TIME_TYPES = {
    TIME_N,
    TIME_NN
}

TIMESTAMP_TYPES = {
    TIMESTAMP_N,
    TIMESTAMP_NN,
    TIMESTAMP_NZ
}

# Event codes
TD_Evt_BufferLayout             = 1
TD_Evt_ConnectStatus            = 2
TD_Evt_RowCounts                = 3
TD_Evt_BlockCount               = 4
TD_Evt_ApplyCount               = 5
TD_Evt_ExportCount              = 6
TD_Evt_ErrorTable1              = 7
TD_Evt_ErrorTable2              = 8
TD_Evt_CLIError                 = 9
TD_Evt_DBSError                 = 10
TD_Evt_CPUTime                  = 11
TD_Evt_LastError                = 12
TD_Evt_ExitCode                 = 13
TD_Evt_RunStats                 = 14
TD_Evt_UserCommand              = 15
TD_Evt_RowsCheckpointed         = 16
TD_Evt_BufferModeSupport        = 17
TD_Evt_MacroNames               = 18
TD_Evt_PackFactor               = 19
TD_Evt_OperVersion              = 20
TD_Evt_RowCounts64              = 21
TD_Evt_RowsCheckpointed64       = 22
TD_Evt_ExportCount64            = 23
TD_Evt_RunStats64               = 24
TD_Evt_ErrorTable1_64           = 25
TD_Evt_ApplyCount64             = 26
TD_Evt_AcquisitionPhaseStats    = 27
TD_Evt_ApplicationPhaseStat     = 28
TD_Evt_LoadPhaseStats           = 29
TD_Evt_ExportPhaseStats         = 30

TD_Evt_Version                  = 101

# Status codes
TD_SUCCESS                      = 0
TD_UNAVAILABLE                  = 4
TD_ERROR                        = 99

# Error codes
TD_ERR_WORK_TABLE_MISSING       = 2583
TD_ERR_TRANS_ABORT              = 2631
TD_ERR_USER_NO_SELECT_ACCESS    = 3523
TD_ERR_OBJECT_NOT_EXIST         = 3807
TD_ERR_OBJECT_NOT_TABLE         = 3853
TD_ERR_OBJECT_NOT_VIEW          = 3854

# CLIv2 Error codes
CLI_ERR_CANNOT_RELEASE_MLOAD    = 2572
CLI_ERR_TABLE_MLOAD_EXISTS      = 2574
CLI_ERR_INVALID_USER            = 8017

# Putting the most common message texts here for helping to debug loads. There
# might be a better way to do this, but may not be worth it to code something
# that looks for and digs through the binary message catalog.
MESSAGES = {
    2620: "The format or data contains a bad character.",
    2655: "Invalid parcel sequences during Load.",
    2665: "Invalid date.",
    2673: "The source parcel length does not match data that was defined.",
    2674: "Precision loss during data conversion.",
    2675: "Numerical overflow occurred during computation.",
    2679: "The format or data contains a bad character.",
    2682: "Precision loss during data conversion.",
    2683: "Numerical overflow occurred during computation.",
    2687: "The format or data contains a bad character.",
    2689: "Field is null for column defined as NOT NULL.",
    2694: "The row is incorrectly formatted.",
    2644: "No more room in database.", # Does NOT populate in the error log(s) (_e1, _e2)
}

# Attributes
TD_SYSTEM_OPERATOR              = 0
TD_USER_NAME                    = 1
TD_USER_PASSWORD                = 2
TD_TDP_ID                       = 3
TD_LOG_TABLE                    = 4
TD_TARGET_TABLE                 = 5
TD_ERROR_TABLE_1                = 6
TD_ERROR_TABLE_2                = 7
TD_ERROR_TABLE                  = 8
TD_BUFFERS                      = 9
TD_BUFFER_SIZE                  = 10
TD_BUFFER_MODE                  = 11
TD_BUFFER_MAX_SIZE              = 12
TD_BUFFER_HEADER_SIZE           = 13
TD_BUFFER_LENGTH_SIZE           = 14
TD_BUFFER_TRAILER_SIZE          = 15
TD_ERROR_LIMIT                  = 16
TD_MAX_SESSIONS                 = 17
TD_MIN_SESSIONS                 = 18
TD_RESTARTMODE                  = 19
TD_INSTANCE_NUM                 = 20
TD_MAX_INSTANCES                = 21
TD_CHARSET                      = 22
TD_TENACITY_HOURS               = 23
TD_TENACITY_SLEEP               = 24
TD_ACCOUNT_ID                   = 25
TD_DATE_FORM                    = 26
TD_NOTIFY_EXIT                  = 27
TD_NOTIFY_EXIT_IS_DLL           = 28
TD_NOTIFY_LEVEL                 = 29
TD_NOTIFY_METHOD                = 30
TD_NOTIFY_STRING                = 31
TD_PRIVATE_LOG_NAME             = 32
TD_TRACE_LEVEL                  = 33
TD_TRACE_OUTPUT                 = 34
TD_WORKINGDATABASE              = 35
TD_PAUSE_ACQ                    = 36
TD_AMP_CHECK                    = 37
TD_DELETE_TASK                  = 38
TD_WORK_TABLE                   = 39
TD_PACK                         = 40
TD_PACKMAXIMUM                  = 41
TD_MACRODATABASE                = 42
TD_ROBUST                       = 43
TD_SELECT_STMT                  = 44
TD_BLOCK_SIZE                   = 45
TD_DATA_ENCRYPTION              = 46
TD_LOGON_MECH                   = 47
TD_LOGON_MECH_DATA              = 48
TD_WILDCARDINSERT               = 49
TD_REPLICATION_OVERRIDE         = 50
TD_MAX_DECIMAL_DIGITS           = 51 
TD_IGNORE_MAX_DECIMAL_DIGITS    = 52
TD_QUERY_BAND_SESS_INFO         = 53
TD_ARRAYSUPPORT                 = 54
TD_MSG_ENCODING                 = 55
TD_APPENDERRORTABLE             = 56
TD_DROPERRORTABLE               = 57
TD_DROPLOGTABLE                 = 58
TD_DROPWORKTABLE                = 59
TD_DROPMACRO                    = 60
TD_OUTLIMIT                     = 61
TD_SPOOLMODE                    = 62
TD_LOGSQL                       = 63
TD_AUTORESTART                  = 64
TD_TMSMSUPPORT                  = 65
TD_RATE                         = 66
TD_PERIODICITY                  = 67
TD_MACROCHARSET                 = 68
TD_RETN_ACT_DATATYPE            = 69

MARK_DUPLICATE_ROWS             = 0
MARK_DUPLICATE_INSERT_ROWS      = 1
MARK_DUPLICATE_UPDATE_ROWS      = 2
MARK_MISSING_ROWS               = 3
MARK_MISSING_UPDATE_ROWS        = 4
MARK_MISSING_DELETE_ROWS        = 5
IGNORE_DUPLICATE_ROWS           = 6
IGNORE_DUPLICATE_INSERT_ROWS    = 7
IGNORE_DUPLICATE_UPDATE_ROWS    = 8
IGNORE_MISSING_ROWS             = 9
IGNORE_MISSING_UPDATE_ROWS      = 10
IGNORE_MISSING_DELETE_ROWS      = 11
INSERT_FOR_MISSING_UPDATE_ROWS  = 12
MARK_EXTRA_ROWS                 = 13
MARK_EXTRA_UPDATE_ROWS          = 14
MARK_EXTRA_DELETE_ROWS          = 15
IGNORE_EXTRA_ROWS               = 16
IGNORE_EXTRA_UPDATE_ROWS        = 17
IGNORE_EXTRA_DELETE_ROWS        = 18

# Type maps between TPT and CLIv2
tpt_type_map = {
    BLOB_NN: TD_BLOB,
    BLOB_N: TD_BLOB,
    BLOB_AS_DEFERRED_NN: TD_BLOB,
    BLOB_AS_DEFERRED_N: TD_BLOB,
    BLOB_AS_LOCATOR_NN: TD_BLOB,
    BLOB_AS_LOCATOR_N: TD_BLOB,
    BLOB_AS_DEFERRED_NAME_NN: TD_BLOB_AS_DEFERRED_BY_NAME,
    BLOB_AS_DEFERRED_NAME_N: TD_BLOB_AS_DEFERRED_BY_NAME,
    CLOB_NN: TD_CLOB,
    CLOB_N: TD_CLOB,
    CLOB_AS_DEFERRED_NN: TD_CLOB_AS_DEFERRED_BY_NAME,
    CLOB_AS_DEFERRED_N: TD_CLOB_AS_DEFERRED_BY_NAME,
    CLOB_AS_LOCATOR_NN: TD_CLOB,
    CLOB_AS_LOCATOR_N: TD_CLOB,
    UDT_NN: TD_CHAR,
    UDT_N: TD_CHAR,
    DISTINCT_UDT_NN: TD_CHAR,
    DISTINCT_UDT_N: TD_CHAR,
    STRUCT_UDT_NN: TD_CHAR,
    STRUCT_UDT_N: TD_CHAR,
    VARCHAR_NN: TD_VARCHAR,
    VARCHAR_N: TD_VARCHAR,
    CHAR_NN: TD_CHAR,
    CHAR_N: TD_CHAR,
    LONG_VARCHAR_NN: TD_LONGVARCHAR,
    LONG_VARCHAR_N: TD_LONGVARCHAR,
    VARGRAPHIC_NN: TD_VARGRAPHIC,
    VARGRAPHIC_N: TD_VARGRAPHIC,
    GRAPHIC_NN: TD_GRAPHIC,
    GRAPHIC_N: TD_GRAPHIC,
    LONG_VARGRAPHIC_NN: TD_LONGVARGRAPHIC,
    LONG_VARGRAPHIC_N: TD_LONGVARGRAPHIC,
    FLOAT_NN: TD_FLOAT,
    FLOAT_N: TD_FLOAT,
    DECIMAL_NN: TD_DECIMAL,
    DECIMAL_N: TD_DECIMAL,
    INTEGER_NN: TD_INTEGER,
    INTEGER_N: TD_INTEGER,
    SMALLINT_NN: TD_SMALLINT,
    SMALLINT_N: TD_SMALLINT,
    ARRAY_1D_NN: TD_CHAR,
    ARRAY_1D_N: TD_CHAR,
    ARRAY_ND_NN: TD_CHAR,
    ARRAY_ND_N: TD_CHAR,
    BIGINT_NN: TD_BIGINT,
    BIGINT_N: TD_BIGINT,
    VARBYTE_NN: TD_VARBYTE,
    VARBYTE_N: TD_VARBYTE,
    BYTE_NN: TD_BYTE,
    BYTE_N: TD_BYTE,
    LONG_VARBYTE_NN: TD_LONGVARCHAR,
    LONG_VARBYTE_N: TD_LONGVARCHAR,
    DATE_NNA: TD_CHAR,
    DATE_NA: TD_CHAR,
    DATE_NN: TD_DATE,
    DATE_N: TD_DATE,
    BYTEINT_NN: TD_BYTEINT,
    BYTEINT_N: TD_BYTEINT,
    TIME_NN: TD_TIME,
    TIME_N: TD_TIME,
    TIMESTAMP_NN: TD_TIMESTAMP,
    TIMESTAMP_N: TD_TIMESTAMP,
    TIME_NNZ: TD_TIME_WITHTIMEZONE,
    TIME_NZ: TD_TIME_WITHTIMEZONE,
    TIMESTAMP_NNZ: TD_TIMESTAMP_WITHTIMEZONE,
    TIMESTAMP_NZ: TD_TIMESTAMP_WITHTIMEZONE,
    INTERVAL_YEAR_NN: TD_INTERVAL_YEAR,
    INTERVAL_YEAR_N: TD_INTERVAL_YEAR,
    INTERVAL_YEAR_TO_MONTH_NN: TD_INTERVAL_YEARTOMONTH,
    INTERVAL_YEAR_TO_MONTH_N: TD_INTERVAL_YEARTOMONTH,
    INTERVAL_MONTH_NN: TD_INTERVAL_MONTH,
    INTERVAL_MONTH_N: TD_INTERVAL_MONTH,
    INTERVAL_DAY_NN: TD_INTERVAL_DAY,
    INTERVAL_DAY_N: TD_INTERVAL_DAY,
    INTERVAL_DAY_TO_HOUR_NN: TD_INTERVAL_DAYTOHOUR,
    INTERVAL_DAY_TO_HOUR_N: TD_INTERVAL_DAYTOHOUR,
    INTERVAL_DAY_TO_MINUTE_NN: TD_INTERVAL_DAYTOMINUTE,
    INTERVAL_DAY_TO_MINUTE_N: TD_INTERVAL_DAYTOMINUTE,
    INTERVAL_DAY_TO_SECOND_NN: TD_INTERVAL_DAYTOSECOND,
    INTERVAL_DAY_TO_SECOND_N: TD_INTERVAL_DAYTOSECOND,
    INTERVAL_HOUR_NN: TD_INTERVAL_HOUR,
    INTERVAL_HOUR_N: TD_INTERVAL_HOUR,
    INTERVAL_HOUR_TO_MINUTE_NN: TD_INTERVAL_HOURTOMINUTE,
    INTERVAL_HOUR_TO_MINUTE_N: TD_INTERVAL_HOURTOMINUTE,
    INTERVAL_HOUR_TO_SECOND_NN: TD_INTERVAL_HOURTOSECOND,
    INTERVAL_HOUR_TO_SECOND_N: TD_INTERVAL_HOURTOSECOND,
    INTERVAL_MINUTE_NN: TD_INTERVAL_MINUTE,
    INTERVAL_MINUTE_N: TD_INTERVAL_MINUTE,
    INTERVAL_MINUTE_TO_SECOND_NN: TD_INTERVAL_MINUTETOSECOND,
    INTERVAL_MINUTE_TO_SECOND_N: TD_INTERVAL_MINUTETOSECOND,
    INTERVAL_SECOND_NN: TD_INTERVAL_SECOND,
    INTERVAL_SECOND_N: TD_INTERVAL_SECOND,
    PERIOD_DATE_NN: TD_PERIOD_DATE,
    PERIOD_DATE_N: TD_PERIOD_DATE,
    PERIOD_TIME_NN: TD_PERIOD_TIME,
    PERIOD_TIME_N: TD_PERIOD_TIME,
    PERIOD_TIME_NNZ: TD_PERIOD_TIME_TZ,
    PERIOD_TIME_NZ: TD_PERIOD_TIME_TZ,
    PERIOD_TIMESTAMP_NN: TD_PERIOD_TS,
    PERIOD_TIMESTAMP_N: TD_PERIOD_TS,
    PERIOD_TIMESTAMP_NNZ: TD_PERIOD_TS_TZ,
    PERIOD_TIMESTAMP_NZ: TD_PERIOD_TS_TZ,
    XML_TEXT_NN: TD_CHAR,
    XML_TEXT_N: TD_CHAR,
    XML_TEXT_DEFERRED_NN: TD_CHAR,
    XML_TEXT_DEFERRED_N: TD_CHAR,
    XML_TEXT_LOCATOR_NN: TD_CHAR,
    XML_TEXT_LOCATOR_N: TD_CHAR,
}


cmd_type_map = {
    TD_INTEGER: INTEGER_NN,
    TD_SMALLINT: SMALLINT_NN,
    TD_FLOAT: FLOAT_NN,
    TD_DECIMAL: DECIMAL_NN,
    TD_CHAR: CHAR_NN,
    TD_BYTEINT: BYTEINT_NN,
    TD_VARCHAR: VARCHAR_NN,
    TD_LONGVARCHAR: LONG_VARCHAR_NN,
    TD_BYTE: BYTE_NN,
    TD_VARBYTE: VARBYTE_NN,
    TD_DATE: DATE_NN,
    TD_GRAPHIC: GRAPHIC_NN,
    TD_VARGRAPHIC: VARGRAPHIC_NN,
    TD_LONGVARGRAPHIC: LONG_VARGRAPHIC_NN,
    TD_DATE_ANSI: DATE_NNA,
    TD_TIME: TIME_NN,
    TD_TIME_WITHTIMEZONE: TIME_NNZ,
    TD_BIGINT: BIGINT_NN,
    TD_BLOB: BLOB_NN,
    TD_CLOB: CLOB_NN,
    TD_BLOB_AS_DEFERRED_BY_NAME: BLOB_AS_DEFERRED_NAME_NN,
    TD_CLOB_AS_DEFERRED_BY_NAME: CLOB_AS_DEFERRED_NN,
    TD_TIMESTAMP: TIMESTAMP_NN,
    TD_TIMESTAMP_WITHTIMEZONE: TIMESTAMP_NNZ,
    TD_INTERVAL_YEAR: INTERVAL_YEAR_NN,
    TD_INTERVAL_YEARTOMONTH: INTERVAL_YEAR_TO_MONTH_NN,
    TD_INTERVAL_MONTH: INTERVAL_MONTH_NN,
    TD_INTERVAL_DAY: INTERVAL_DAY_NN,
    TD_INTERVAL_DAYTOHOUR: INTERVAL_DAY_TO_HOUR_NN,
    TD_INTERVAL_DAYTOMINUTE: INTERVAL_DAY_TO_MINUTE_NN,
    TD_INTERVAL_DAYTOSECOND: INTERVAL_DAY_TO_SECOND_NN,
    TD_INTERVAL_HOUR: INTERVAL_HOUR_NN,
    TD_INTERVAL_HOURTOMINUTE: INTERVAL_HOUR_TO_MINUTE_NN,
    TD_INTERVAL_HOURTOSECOND: INTERVAL_HOUR_TO_SECOND_NN,
    TD_INTERVAL_MINUTE: INTERVAL_MINUTE_NN,
    TD_INTERVAL_MINUTETOSECOND: INTERVAL_MINUTE_TO_SECOND_NN,
    TD_INTERVAL_SECOND: INTERVAL_SECOND_NN,
    TD_PERIOD_DATE: PERIOD_DATE_NN,
    TD_PERIOD_TIME: PERIOD_TIME_NN,
    TD_PERIOD_TIME_TZ: PERIOD_TIME_NNZ,
    TD_PERIOD_TS: PERIOD_TIMESTAMP_NN,
    TD_PERIOD_TS_TZ: PERIOD_TIMESTAMP_NNZ,
    TD_NUMBER: INTEGER_NN, # not sure on this one
}

# giraffez data types
# These types are used internally for the purpose of reducing
# the complexity of the underlying encoder type switch while
# not losing any Teradata specific type information.

# GD_DEFAULT will always be equivalent to GD_CHAR in that it advances
# the buffer and then stores that value by the column length (fixed). 
# It will be used when the exact method for handling the type is not
# known and fixed-length is presumed, unless it is discovered that
# type is not fixed-length.  This is done because some less common
# types should be noted but may not be explicitly handled as of yet.
GD_DEFAULT          = 0
GD_BYTEINT          = 1
GD_SMALLINT         = 2
GD_INTEGER          = 3
GD_BIGINT           = 4
GD_FLOAT            = 5
GD_DECIMAL          = 6
GD_CHAR             = 7
GD_VARCHAR          = 8
GD_DATE             = 9

gd_type_map = {
    BLOB_NN: GD_DEFAULT,
    BLOB_N: GD_DEFAULT,
    BLOB_AS_DEFERRED_NN: GD_DEFAULT,
    BLOB_AS_DEFERRED_N: GD_DEFAULT,
    BLOB_AS_LOCATOR_NN: GD_DEFAULT,
    BLOB_AS_LOCATOR_N: GD_DEFAULT,
    BLOB_AS_DEFERRED_NAME_NN: GD_DEFAULT,
    BLOB_AS_DEFERRED_NAME_N: GD_DEFAULT,
    CLOB_NN: GD_DEFAULT,
    CLOB_N: GD_DEFAULT,
    CLOB_AS_DEFERRED_NN: GD_DEFAULT,
    CLOB_AS_DEFERRED_N: GD_DEFAULT,
    CLOB_AS_LOCATOR_NN: GD_DEFAULT,
    CLOB_AS_LOCATOR_N: GD_DEFAULT,
    UDT_NN: GD_DEFAULT,
    UDT_N: GD_DEFAULT,
    DISTINCT_UDT_NN: GD_DEFAULT,
    DISTINCT_UDT_N: GD_DEFAULT,
    STRUCT_UDT_NN: GD_DEFAULT,
    STRUCT_UDT_N: GD_DEFAULT,
    VARCHAR_NN: GD_VARCHAR,
    VARCHAR_N: GD_VARCHAR,
    CHAR_NN: GD_CHAR,
    CHAR_N: GD_CHAR,
    LONG_VARCHAR_NN: GD_VARCHAR,
    LONG_VARCHAR_N: GD_VARCHAR,
    VARGRAPHIC_NN: GD_VARCHAR,
    VARGRAPHIC_N: GD_VARCHAR,
    GRAPHIC_NN: GD_DEFAULT,
    GRAPHIC_N: GD_DEFAULT,
    LONG_VARGRAPHIC_NN: GD_VARCHAR,
    LONG_VARGRAPHIC_N: GD_VARCHAR,
    FLOAT_NN: GD_FLOAT,
    FLOAT_N: GD_FLOAT,
    DECIMAL_NN: GD_DECIMAL,
    DECIMAL_N: GD_DECIMAL,
    INTEGER_NN: GD_INTEGER,
    INTEGER_N: GD_INTEGER,
    SMALLINT_NN: GD_SMALLINT,
    SMALLINT_N: GD_SMALLINT,
    ARRAY_1D_NN: GD_DEFAULT,
    ARRAY_1D_N: GD_DEFAULT,
    ARRAY_ND_NN: GD_DEFAULT,
    ARRAY_ND_N: GD_DEFAULT,
    BIGINT_NN: GD_BIGINT,
    BIGINT_N: GD_BIGINT,
    VARBYTE_NN: GD_VARCHAR,
    VARBYTE_N: GD_VARCHAR,
    BYTE_NN: GD_CHAR,
    BYTE_N: GD_CHAR,
    LONG_VARBYTE_NN: GD_VARCHAR,
    LONG_VARBYTE_N: GD_VARCHAR,
    DATE_NNA: GD_DEFAULT,
    DATE_NA: GD_DEFAULT,
    DATE_NN: GD_DATE,
    DATE_N: GD_DATE,
    BYTEINT_NN: GD_BYTEINT,
    BYTEINT_N: GD_BYTEINT,
    TIME_NN: GD_CHAR,
    TIME_N: GD_CHAR,
    TIMESTAMP_NN: GD_CHAR,
    TIMESTAMP_N: GD_CHAR,
    TIME_NNZ: GD_CHAR,
    TIME_NZ: GD_CHAR,
    TIMESTAMP_NNZ: GD_CHAR,
    TIMESTAMP_NZ: GD_CHAR,
    INTERVAL_YEAR_NN: GD_DEFAULT,
    INTERVAL_YEAR_N: GD_DEFAULT,
    INTERVAL_YEAR_TO_MONTH_NN: GD_DEFAULT,
    INTERVAL_YEAR_TO_MONTH_N: GD_DEFAULT,
    INTERVAL_MONTH_NN: GD_DEFAULT,
    INTERVAL_MONTH_N: GD_DEFAULT,
    INTERVAL_DAY_NN: GD_DEFAULT,
    INTERVAL_DAY_N: GD_DEFAULT,
    INTERVAL_DAY_TO_HOUR_NN: GD_DEFAULT,
    INTERVAL_DAY_TO_HOUR_N: GD_DEFAULT,
    INTERVAL_DAY_TO_MINUTE_NN: GD_DEFAULT,
    INTERVAL_DAY_TO_MINUTE_N: GD_DEFAULT,
    INTERVAL_DAY_TO_SECOND_NN: GD_DEFAULT,
    INTERVAL_DAY_TO_SECOND_N: GD_DEFAULT,
    INTERVAL_HOUR_NN: GD_DEFAULT,
    INTERVAL_HOUR_N: GD_DEFAULT,
    INTERVAL_HOUR_TO_MINUTE_NN: GD_DEFAULT,
    INTERVAL_HOUR_TO_MINUTE_N: GD_DEFAULT,
    INTERVAL_HOUR_TO_SECOND_NN: GD_DEFAULT,
    INTERVAL_HOUR_TO_SECOND_N: GD_DEFAULT,
    INTERVAL_MINUTE_NN: GD_DEFAULT,
    INTERVAL_MINUTE_N: GD_DEFAULT,
    INTERVAL_MINUTE_TO_SECOND_NN: GD_DEFAULT,
    INTERVAL_MINUTE_TO_SECOND_N: GD_DEFAULT,
    INTERVAL_SECOND_NN: GD_DEFAULT,
    INTERVAL_SECOND_N: GD_DEFAULT,
    PERIOD_DATE_NN: GD_DEFAULT,
    PERIOD_DATE_N: GD_DEFAULT,
    PERIOD_TIME_NN: GD_DEFAULT,
    PERIOD_TIME_N: GD_DEFAULT,
    PERIOD_TIME_NNZ: GD_DEFAULT,
    PERIOD_TIME_NZ: GD_DEFAULT,
    PERIOD_TIMESTAMP_NN: GD_DEFAULT,
    PERIOD_TIMESTAMP_N: GD_DEFAULT,
    PERIOD_TIMESTAMP_NNZ: GD_DEFAULT,
    PERIOD_TIMESTAMP_NZ: GD_DEFAULT,
    XML_TEXT_NN: GD_DEFAULT,
    XML_TEXT_N: GD_DEFAULT,
    XML_TEXT_DEFERRED_NN: GD_DEFAULT,
    XML_TEXT_DEFERRED_N: GD_DEFAULT,
    XML_TEXT_LOCATOR_NN: GD_DEFAULT,
    XML_TEXT_LOCATOR_N: GD_DEFAULT,
}


FORMAT_LENGTH = {1: "b", 2: "h", 4: "i", 8 : "q", 16: "Qq"}

DATE_FORMATS = [
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y%m%d",
]

TIME_FORMATS = {
    8: "%H:%M:%S",
    15: "%H:%M:%S.%f",
    19: "%Y-%m-%d %H:%M:%S",
    22: "%Y-%m-%d %H:%M:%S.%f",
    26: "%Y-%m-%d %H:%M:%S.%f",
    32: "%Y-%m-%d %H:%M:%S.%f"
}

DELIMITED_TEXT_FILE     = 1
GIRAFFE_ARCHIVE_FILE    = 2

FILE_TYPE_MAP = {
    DELIMITED_TEXT_FILE: 'Delimited File',
    GIRAFFE_ARCHIVE_FILE: 'Giraffez Archive File'
}

MESSAGE_WRITE_DEFAULT = """Successfully created file '{0}'
Set the username and password using the config module:

    giraffez config --set connections.db1.host <host>
    giraffez config --set connections.db1.username <username>
    giraffez config --set connections.db1.password <password>"""
