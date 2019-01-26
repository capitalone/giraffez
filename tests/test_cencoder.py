# -*- coding: utf-8 -*-

import pytest


import decimal
import giraffez
from giraffez._teradata import EncoderError
from giraffez.constants import *
from giraffez.encoders import *
from giraffez.types import *

@pytest.fixture
def encoder():
    return giraffez.Encoder(encoding=DECIMAL_AS_STRING)

def pytest_generate_tests(metafunc):
    if 'TestEncoder' not in str(metafunc.cls):
        return
    # called once per each test function
    funcarglist = metafunc.cls.params.get(metafunc.function.__name__)
    if not funcarglist:
        return
    argnames = sorted(funcarglist[0])
    argnames.remove("name")
    argvalues = [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    idlist = [funcargs["name"] for funcargs in funcarglist]
    # metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")
    metafunc.parametrize(argnames, argvalues, ids=idlist)

def decimal_size(n):
    if n > 20:
        return 16
    elif n > 11:
        return 8
    elif n > 6:
        return 4
    elif n > 3:
        return 2
    else:
        return 1

# Column helpers
Decimal = lambda s, p: ('col1', DECIMAL_NN, decimal_size(s+p), s, p)
Number = lambda s, p: ('col1', NUMBER_NN, decimal_size(s+p), s, p)
Date = ('col1', DATE_NN, 4, 0, 0)
Char = lambda s: ('col1', CHAR_NN, s, 0, 0)
Integer = ('col1', INTEGER_NN, 4, 0, 0)
Timestamp = lambda s: ('col1', TIMESTAMP_NN, 19, 0, 0)
Varchar = lambda s: ('col1', VARCHAR_NN, s, 0, 0)

date_tests = [
    {
        "name": "date_after_1900_01_01",
        "column": Date,
        "input_value": b'\x00\x8b\x90\x11\x00',
        "expected_value": "2015-11-15",
    },
    {
        "name": "date_before_1900_01_01",
        "column": Date,
        "input_value": b'\x00Na\xf8\xff',
        "expected_value": "1850-06-22",
    },
]

decimal_tests = [
    # Test decimal
    {
        "name": "decimal_64",
        "column": Decimal(16, 0),
        "input_value": b'\x00\xd2\x02\x96I\x00\x00\x00\x00',
        "expected_value": "1234567890",
    },
    {
        "name": "decimal_128",
        "column": Decimal(22, 0),
        "input_value": b'\x00\x62\x85\xe9\x7d\xf4\x10\x22\x11\x00\x00\x00\x00\x00\x00\x00\x00',
        "expected_value": "1234567890123457890",
    },

    # Test decimal scale
    {
        "name": "decimal_64_scale",
        "column": Decimal(10, 5),
        "input_value": b'\x00\x8b\x4b\x65\x91\x00\x00\x00\x00',
        "expected_value": "24393.34795",
    },
    {
        "name": "decimal_64_scale",
        "column": Decimal(16, 4),
        "input_value": b'\x00\xd2\x02\x96I\x00\x00\x00\x00',
        "expected_value": '123456.7890',
    },
    {
        "name": "decimal_128_scale",
        "column": Decimal(20, 2),
        "input_value": b'\x00\x9a\x1a\x38\x2f\x81\x9f\x4e\xb1\x06\x00\x00\x00\x00\x00\x00\x00',
        "expected_value": "1234567890123457890.82",
    },

    # Test decimal negative
    {
        "name": "decimal_32_negative",
        "column": Decimal(5, 2),
        "input_value": b'\x00\x4e\xed\xff\xff',
        "expected_value": "-47.86",
    },
    {
        "name": "decimal_64_negative",
        "column": Decimal(16, 4),
        "input_value": b'\x00.\xfdi\xb6\xff\xff\xff\xff',
        "expected_value": '-123456.7890',
    },
    {
        "name": "decimal_128_negative",
        "column": Decimal(24, 2),
        "input_value": b'\x00\x94\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
        "expected_value": "-1.08",
    },
    {
        "name": "decimal_128_negative",
        "column": Decimal(22, 2),
        "input_value": b'\x00\x71\x45\x23\x8d\x8e\xbe\x13\x15\x21\xf0\x28\x40\x1c\x39\xe8\xff',
        "expected_value": '-1234567890123456789012345678901234.07',
    },

    {
        "name": "decimal_32_negative_integer_zero",
        "column": Decimal(5, 2),
        "input_value": b'\x00\xec\xff\xff\xff',
        "expected_value": "-0.20",
    },
    {
        "name": "decimal_128_negative_integer_zero",
        "column": Decimal(24, 2),
        "input_value": b'\x00\xee\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
        "expected_value": "-0.18",
    },

    {
        "name": "decimal_128_negative_integer_zero_scale_leading_zero",
        "column": Decimal(24, 2),
        "input_value": b'\x00\xf8\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
        "expected_value": "-0.08",
    },

    {
        "name": "decimal_32_scale_trailing_zero",
        "column": Decimal(5, 2),
        "input_value": b'\x00\x78\x00\x00\x00',
        "expected_value": "1.20",
    },
    {
        "name": "decimal_32_scale_trailing_zero",
        "column": Decimal(5, 4),
        "input_value": b'\x00\xe0\x2e\x00\x00',
        "expected_value": "1.2000",
    },
    {
        "name": "decimal_64_scale_trailing_zero",
        "column": Decimal(16, 2),
        "input_value": b'\x00\x06\x18\x00\x00\x00\x00\x00\x00',
        "expected_value": "61.50",
    },

    {
        "name": "decimal_8_integer_zero_scale_leading_zero",
        "column": Decimal(1, 2),
        "input_value": b'\x00\x01',
        "expected_value": "0.01",
    },
    {
        "name": "decimal_16_integer_zero_scale_leading_zero",
        "column": Decimal(4, 2),
        "input_value": b'\x00\xb3\x04',
        "expected_value": "12.03",
    },
    {
        "name": "decimal_32_integer_zero_scale_leading_zero",
        "column": Decimal(9, 2),
        "input_value": b'\x00\xc4\xcc\x5b\x07',
        "expected_value": "1234567.08",
    },
    {
        "name": "decimal_64_integer_zero_scale_leading_zero",
        "column": Decimal(18, 2),
        "input_value": b'\x00\x07\xf3\x30\xa6\x4b\x9b\xb6\x01',
        "expected_value": "1234567890123456.07",
    },
    {
        "name": "decimal_128_integer_zero_scale_leading_zero",
        "column": Decimal(38, 2),
        "input_value": b'\x00\x07\xf3\x38\xde\x50\x90\x49\xc4\x13\x33\x02\xf0\xf6\xb0\x49\x09',
        "expected_value": "123456789012345678901234567890123456.07",
    },


    # Testing all decimal sizes (based on byte-length) for integer
    # overflow. Both with and without scale is tested because the
    # cencoder paths differently through the function in each case.
    # This would be helpful in cases where the wrong abs() function
    # is called and the value is unsafely downcast to a lower integer
    # type.
    {
        "name": "decimal_8_overflow",
        "column": Decimal(2, 0),
        "input_value": b'\x00\x63',
        "expected_value": "99",
    },
    {
        "name": "decimal_8_overflow_scale",
        "column": Decimal(2, 1),
        "input_value": b'\x00\x63',
        "expected_value": "9.9",
    },
    {
        "name": "decimal_16_overflow",
        "column": Decimal(4, 0),
        "input_value": b'\x00\x0f\x27',
        "expected_value": "9999",
    },
    {
        "name": "decimal_16_overflow_scale",
        "column": Decimal(4, 1),
        "input_value": b'\x00\x0f\x27',
        "expected_value": "999.9",
    },
    {
        "name": "decimal_32_overflow",
        "column": Decimal(9, 0),
        "input_value": b'\x00\xff\xc9\x9a\x3b',
        "expected_value": "999999999",
    },
    {
        "name": "decimal_32_overflow_scale",
        "column": Decimal(9, 1),
        "input_value": b'\x00\xff\xc9\x9a\x3b',
        "expected_value": "99999999.9",
    },
    {
        "name": "decimal_64_overflow",
        "column": Decimal(18, 0),
        "input_value": b'\x00\xff\xffc\xa7\xb3\xb6\xe0\x0d',
        "expected_value": "999999999999999999",
    },
    {
        "name": "decimal_64_overflow_scale",
        "column": Decimal(18, 1),
        "input_value": b'\x00\xff\xffc\xa7\xb3\xb6\xe0\x0d',
        "expected_value": "99999999999999999.9",
    },
    {
        "name": "decimal_128_overflow",
        "column": Decimal(38, 0),
        "input_value": b'\x00\xff\xff\xff\xff\x3f\x22\x8a\x09\x7a\xc4\x86\x5a\xa8\x4c\x3b\x4b',
        "expected_value": "99999999999999999999999999999999999999",
    },
    {
        "name": "decimal_128_overflow_scale",
        "column": Decimal(38, 1),
        "input_value": b'\x00\xff\xff\xff\xff\x3f\x22\x8a\x09\x7a\xc4\x86\x5a\xa8\x4c\x3b\x4b',
        "expected_value": "9999999999999999999999999999999999999.9",
    },
]

number_tests = [
    {
        "name": "number_8",
        "column": Number(38, 2),
        "input_value": b'\x00\x03\x02\x00\x08',
        "expected_value": "0.08",
    },
    {
        "name": "number_8_negative",
        "column": Number(38, 2),
        "input_value": b'\x00\x03\x02\x00\xf8',
        "expected_value": "-0.08",
    },
    {
        "name": "number_128_overflow_scale",
        "column": Number(38, 2),
        "input_value": b'\x00\x12\x02\x00\xff\xff\xff\xff?"\x8a\tz\xc4\x86Z\xa8L;K',
        "expected_value": "999999999999999999999999999999999999.99",
    },

]

integer_tests = [
    {
        "name": "integer_32",
        "column": Integer,
        "input_value": b'\x00\x2a\x00\x00\x00',
        "expected_value": 42,
    },
]

character_tests = [
    {
        "name": "build_a_snowman",
        "column": Varchar(8),
        "input_value": b'\x00\x03\x00\xe2\x98\x83',
        "expected_value": u"☃",
    },
]

other_tests = [
    {
        "name": "implicitly_handled_type",
        "column": ('col1', DISTINCT_UDT_NN, 3, 0, 0),
        "input_value": b'\x00UDT',
        "expected_value": "UDT",
    },
]


deserialize_only_tests = [
    # Only for deserialization because Teradata returns a column length
    # that is larger than the actual column length defined.  In these
    # cases the encoder uses the format length (where provided) in place
    # of the column length.  For serialization this should not take
    # place.
    {
        "name": "char_format_padding",
        "column": ("col1", TD_CHAR, 8, 0, 0, "N", None, "X(6)"),
        "input_value": b'\x00TEST\x20\x20',
        "expected_value": "TEST  ",
    },
]

serialize_only_tests = [
    {
        "name": "python_date_after_1900_01_01",
        "column": Date,
        "input_value": b'\x00\x8b\x90\x11\x00',
        "expected_value": datetime.date(2015, 11, 15),
    },
    {
        "name": "python_date_before_1900_01_01",
        "column": Date,
        "input_value": b'\x00Na\xf8\xff',
        "expected_value": datetime.date(1850, 6, 22),
    },
    {
        "name": "python_datetime_after_1900_01_01",
        "column": Date,
        "input_value": b'\x00\x8b\x90\x11\x00',
        "expected_value": datetime.datetime(2015,11,15,10,12,55),
    },
    {
        "name": "python_datetime_before_1900_01_01",
        "column": Date,
        "input_value": b'\x00Na\xf8\xff',
        "expected_value": datetime.datetime(1850,6,22,10,12,55),
    },
    {
        "name": "timestamp_0",
        "column": Timestamp(0),
        "input_value": b'\x001850-06-22 10:12:55',
        "expected_value": datetime.datetime(1850,6,22,10,12,55),
    },
    {
        "name": "decimal_32_scale_extra_trailing_zero",
        "column": Decimal(5, 2),
        "input_value": b'\x00\x78\x00\x00\x00',
        "expected_value": "1.20000",
    },
    {
        "name": "decimal_32_scale_extra_trailing_digits",
        "column": Decimal(5, 2),
        "input_value": b'\x00\x78\x00\x00\x00',
        "expected_value": "1.20123",
    },
    {
        "name": "decimal_32_scale_less_trailing_zero",
        "column": Decimal(5, 4),
        "input_value": b'\x00\xe0\x2e\x00\x00',
        "expected_value": "1.20",
    },
    {
        "name": "build_a_bad_snowman",
        "column": Varchar(8),
        "input_value": b'\x00\x03\x00\xe2\x98\x83',
        "expected_value": "☃",
    },
    {
        "name": "int from string",
        "column": Integer,
        "input_value": b'\x00\x2a\x00\x00\x00',
        "expected_value": "42",
    },
    {
        "name": "float from string",
        "column": ("col1", FLOAT_NN, 8, 0, 0),
        "input_value": b'\x00\xcd\xcc\xcc\xcc\xcc\xcc\x10@',
        "expected_value": "4.2",
    },
]

error_tests = [
    {
        "name": "invalid_char_encoding",
        "column": Char(8),
        "expected_value": b'\xc3\x28',
        "error": UnicodeDecodeError,
    },
    {
        "name": "invalid_varchar_encoding",
        "column": Varchar(8),
        "expected_value": b'\xc3\x28',
        "error": UnicodeDecodeError,
    },
    {
        "name": "char_too_large",
        "column": Char(8),
        "expected_value": (10000 * 'test01  ',),
        "error": EncoderError,
    },
    {
        "name": "varchar_too_large",
        "column": Varchar(8),
        "expected_value": (10000 * 'test01  ',),
        "error": EncoderError,
    },
    {
        "name": "invalid_varchar_input_integer",
        "column": Varchar(8),
        "expected_value": 10000,
        "error": EncoderError,
    },
    {
        "name": "invalid_varchar_input_list",
        "column": Varchar(8),
        "expected_value": ['test'],
        "error": EncoderError,
    },
    {
        "name": "invalid_char_input_integer",
        "column": Char(8),
        "expected_value": 10000,
        "error": EncoderError,
    },
    {
        "name": "invalid_char_input_list",
        "column": Char(8),
        "expected_value": ['test'],
        "error": EncoderError,
    },
    {
        "name": "invalid_date_input_float",
        "column": Date,
        "expected_value": 1489062458.3881698,
        "error": EncoderError,
    },
]

test_encoder_data = []
test_encoder_data += date_tests
test_encoder_data += character_tests
test_encoder_data += decimal_tests
test_encoder_data += number_tests
test_encoder_data += integer_tests
test_encoder_data += other_tests

class TestEncoder(object):
    params = {
        "test_deserialize": test_encoder_data + deserialize_only_tests,
        "test_serialize": test_encoder_data + serialize_only_tests,
        # "test_serialize": character_tests,
        "test_serialize_error": error_tests,
    }

    def test_deserialize(self, encoder, column, expected_value, input_value):
        encoder.columns = [column]
        result_text = encoder.read(input_value)[0]
        assert result_text == expected_value

    def test_serialize(self, encoder, column, expected_value, input_value):
        encoder.columns = [column]
        result_text = encoder.serialize([expected_value])
        assert result_text == input_value

    def test_serialize_error(self, encoder, column, expected_value, error):
        encoder.columns = [column]
        with pytest.raises(error):
            result_text = encoder.serialize([expected_value])

    def test_unpack_stmt_info(self, encoder):
        stmt_info = b"""\x03\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x07\x00\x00\x00\x01\x00\x02\x00[\x00\x00\x00\x00\x00\x04\x00col1\x00\x00\x04\x00col1\x04\x00col1\n\x00-------.99\x00\x00NNNNYN\xe4\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00UYNUYYU\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x02\x00U\x00\x00\x00\x00\x00\x04\x00col2\x00\x00\x04\x00col2\x04\x00col2\x04\x00X(2)\x00\x00NNNNYN\xc0\x01\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x00\x00\x00\x00\x00\x00\x00NUNUYYU\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x02\x00\x00\x00"""
        result = encoder.parse_header(stmt_info)
        expected_columns = Columns([
            ("col1", TD_DECIMAL, 4, 8, 2, "N", None, "-------.99"),
            ("col2", TD_VARCHAR, 6, 0, 0, "N", None, "X(2)"),
        ])

        assert(result[0] == expected_columns[0])
        assert(result[1] == expected_columns[1])

    def test_null_and_encodings(self, encoder):
        """
        Ensure encoder handles nulls and multiple output encodings
        """
        encoder.columns = [
            ('col1', TD_INTEGER, 4, 0, 0),
            ('col2', TD_VARCHAR, 50, 0, 0),
            ('col3', TD_VARCHAR, 50, 0, 0),
        ]
        expected_bytes = b'\x80\x00\x00\x00\x00\x06\x00value2\x06\x00value3'
        expected_text = (None, "value2", "value3")
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        expected_text = ("NULL", "value2", "value3")
        encoder.null = "NULL"
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        expected_text = "NULL|value2|value3"
        encoder |= ENCODER_SETTINGS_STRING
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder |= ENCODER_SETTINGS_JSON
        encoder.null = None
        expected_text = {"col1": None, "col2": "value2", "col3": "value3"}
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder.columns = [
            ('col1', TD_INTEGER, 4, 10, 0),
            ('col2', TD_DECIMAL, 4, 8, 2),
            ('col3', TD_CHAR, 2, 0, 0),
            ('col4', TD_DATE, 4, 0, 0)
        ]
        encoder |= ENCODER_SETTINGS_DEFAULT
        encoder |= DECIMAL_AS_STRING
        expected_bytes = b'p\x01\x00\x00\x00\x00\x00\x00\x00  \x00\x00\x00\x00'
        expected_text = (1, None, None, None)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        expected_text = (1, "NULL", "NULL", "NULL")
        encoder.null = "NULL"
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder |= ENCODER_SETTINGS_JSON
        encoder.null = None
        expected_text = {"col1": 1, "col2": None, "col3": None, "col4": None}
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_wrong_number_input(self, encoder):
        encoder.columns = [
            ('col1', TD_INTEGER, 4, 0, 0),
            ('col2', TD_VARCHAR, 50, 0, 0),
            ('col3', TD_VARCHAR, 50, 0, 0),
        ]
        with pytest.raises(EncoderError):
            result_text = encoder.serialize([42, 'value2'])

    def test_buffer_writef_pos(self, encoder):
        """
        Ensure that when encoding to a string, the fields which are written
        using the buffer_writef function are written to the correct location
        of the buffer and do not overwrite other fields.
        """
        encoder.columns = [
            ('col1', TD_VARCHAR, 50, 0, 0),
            ('col2', TD_INTEGER, 4, 0, 0),
            ('col3', TD_FLOAT, 8, 0, 0),
        ]
        expected_bytes = b'\x00\x04\x00test*\x00\x00\x00\x9a\x99\x99\x99\x99\x99\t@'
        expected_text = "test|42|3.2"
        encoder |= ENCODER_SETTINGS_STRING
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_buffer_writef_float(self, encoder):
        """
        Ensure that maximum precision is available when using sprintf to
        convert a float to a decimal string.  In the case of having a C double
        (IEEE 64-bit double-precision floating point number) up to 17
        significant digits are available when converting to a decimal string.
        """
        encoder.columns = [
            ('col1', TD_INTEGER, 4, 0, 0),
            ('col2', TD_FLOAT, 8, 0, 0),
        ]
        expected_bytes = b'\x00*\x00\x00\x00\xe38\x8e\xffd\xcd\xcdA'
        expected_text = "42|999999999.111111"
        encoder |= ENCODER_SETTINGS_STRING
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        expected_bytes = b'\x00*\x00\x00\x00\xd0G\xbb\xff\xff\xff\xff?'
        expected_text = "42|1.999999999"
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text



class TestOther(object):
    def test_teradata_error_bytes(self):
        error = giraffez.TeradataError(b"1000: Error Message.")
        assert error.code == 1000
        assert error.message == " Error Message."
