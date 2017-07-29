# -*- coding: utf-8 -*-

import pytest


import decimal
import giraffez
from giraffez._encoder import EncoderError
from giraffez.constants import *
from giraffez.encoders import *
from giraffez.types import *

@pytest.fixture
def encoder():
    return giraffez.Encoder(encoding=DECIMAL_AS_STRING)


class TestCEncoder(object):
    def test_decimal_128(self, encoder):
        """
        Decode/encode Teradata decimal type values > 18 digits.

        col1
        -------------------
        1234567890123457890
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 20, 0),
        ]
        expected_bytes = b'\x00\x62\x85\xe9\x7d\xf4\x10\x22\x11\x00\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ('1234567890123457890',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_128_with_scale(self, encoder):
        """
        Decode/encode Teradata decimal type values > 18 digits.

        col2
        ----------------------
        1234567890123457890.82
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 20, 2),
        ]
        expected_bytes = b'\x00\x9a\x1a\x38\x2f\x81\x9f\x4e\xb1\x06\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ('1234567890123457890.82',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_64_with_scale(self, encoder):
        """
        Decode/encode Teradata decimal type values < 18 digits.

        col1
        -----------
        24393.34795
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 10, 5),
        ]
        expected_bytes = b'\x00\x8b\x4b\x65\x91\x00\x00\x00\x00'
        expected_text = ('24393.34795',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_negative(self, encoder):
        """
        Decode/encode Teradata decimal type values containing
        negatives.

        col1
        -----------
        -47.86
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 5, 2),
        ]
        expected_bytes = b'\x00\x4e\xed\xff\xff'
        expected_text = ('-47.86',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_negative_more(self, encoder):
        """
        Decode/encode Teradata decimal type values containing
        negatives with an integer-part of 0. This test ensures
        that the signedness is not lost with a zero value.

        col1
        -----------
        -0.20
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 5, 2),
        ]
        expected_bytes = b'\x00\xec\xff\xff\xff'
        expected_text = ('-0.20',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        expected_text = '-0.20'
        encoder |= ENCODER_SETTINGS_STRING
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        expected_text = {'col1': '-0.20'}
        encoder |= ROW_ENCODING_DICT
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_negative_more_again(self):
        """
        Decode/encode Teradata decimal type values containing
        negatives with an integer-part of 0. This test ensures
        that the signedness is not lost with a zero value. This
        test ensures that negative numbers that are 128-bit are
        handled correctly.

        col1
        -----------
        -0.08
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 16, 24, 2),
        ])
        expected_bytes = b'\x00\xf8\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'
        expected_text = ['-0.08']
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_scale_with_trailing_zeros(self):
        """
        Ensure that encoder correctly handles the conversion of scale
        regardless of digits provided.

        col1
        -----------
        61.50
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 16, 2),
        ]
        expected_bytes = b'\x00\x06\x18\x00\x00\x00\x00\x00\x00'
        expected_text = ("61.50",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_scale_with_leading_zeros(self, encoder):
        """
        Testing all decimal sizes (based on byte-length) for
        leading zeros in the fractional digits.
        """
        # Decimal 1
        encoder.columns = [
            ('col1', TD_DECIMAL, 1, 2, 2),
        ]
        expected_bytes = b'\x00\x01'
        expected_text = ("0.01",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text


        # Decimal 2
        encoder.columns = [
            ('col1', TD_DECIMAL, 2, 4, 2),
        ]
        expected_bytes = b'\x00\xb3\x04'
        expected_text = ("12.03",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 4
        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 9, 2),
        ]
        expected_bytes = b'\x00\xc4\xcc\x5b\x07'
        expected_text = ("1234567.08",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 8
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 18, 2),
        ]
        expected_bytes = b'\x00\x07\xf3\x30\xa6\x4b\x9b\xb6\x01'
        expected_text = ("1234567890123456.07",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 16
        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 38, 2),
        ]
        expected_bytes = b'\x00\x07\xf3\x38\xde\x50\x90\x49\xc4\x13\x33\x02\xf0\xf6\xb0\x49\x09'
        expected_text = ("123456789012345678901234567890123456.07",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_decimal_overflow(self, encoder):
        """
        Testing all decimal sizes (based on byte-length) for integer
        overflow. Both with and without scale is tested because the
        cencoder paths differently through the function in each case.
        This would be helpful in cases where the wrong abs() function
        is called and the value is unsafely downcast to a lower integer
        type.
        """

        # Decimal 1
        encoder.columns = [
            ('col1', TD_DECIMAL, 1, 2, 0),
        ]
        expected_bytes = b'\x00\x63'
        expected_text = ("99",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder.columns = [
            ('col1', TD_DECIMAL, 1, 2, 1),
        ]
        expected_bytes = b'\x00\x63'
        expected_text = ("9.9",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 2
        encoder.columns = [
            ('col1', TD_DECIMAL, 2, 4, 0),
        ]
        expected_bytes = b'\x00\x0f\x27'
        expected_text = ("9999",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder.columns = [
            ('col1', TD_DECIMAL, 2, 4, 1),
        ]
        expected_bytes = b'\x00\x0f\x27'
        expected_text = ("999.9",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 4
        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 9, 0),
        ]
        expected_bytes = b'\x00\xff\xc9\x9a\x3b'
        expected_text = ("999999999",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 9, 1),
        ]
        expected_bytes = b'\x00\xff\xc9\x9a\x3b'
        expected_text = ("99999999.9",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 8
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 18, 0),
        ]
        expected_bytes = b'\x00\xff\xffc\xa7\xb3\xb6\xe0\x0d'
        expected_text = ("999999999999999999",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 18, 1),
        ]
        expected_bytes = b'\x00\xff\xffc\xa7\xb3\xb6\xe0\x0d'
        expected_text = ("99999999999999999.9",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        # Decimal 16
        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 38, 0),
        ]
        expected_bytes = b'\x00\xff\xff\xff\xff\x3f\x22\x8a\x09\x7a\xc4\x86\x5a\xa8\x4c\x3b\x4b'
        expected_text = ("99999999999999999999999999999999999999",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 38, 1),
        ]
        expected_bytes = b'\x00\xff\xff\xff\xff\x3f\x22\x8a\x09\x7a\xc4\x86\x5a\xa8\x4c\x3b\x4b'
        expected_text = ("9999999999999999999999999999999999999.9",)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_date_from_integer(self):
        """
        Need a date before and after 1900-01-01

        col1
        ----------
        2015-11-15
        1850-06-22

        """
        encoder = giraffez.Encoder(encoding=DECIMAL_AS_STRING)

        encoder.columns = [
            ('col1', TD_DATE, 4, 0, 0),
        ]
        expected_bytes = b'\x00\x8b\x90\x11\x00'
        expected_text = ('2015-11-15',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder |= ENCODER_SETTINGS_JSON
        expected_text = {'col1': '2015-11-15'}
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder |= ENCODER_SETTINGS_DEFAULT
        encoder |= DECIMAL_AS_STRING
        expected_bytes = b'\x00Na\xf8\xff'
        expected_text = ('1850-06-22',)
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

        encoder |= ENCODER_SETTINGS_JSON
        expected_text = {'col1': '1850-06-22'}
        result_text = encoder.read(expected_bytes)
        assert result_text == expected_text

    def test_null(self, encoder):
        """
        Ensure encoder handles nulls

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

    def test_unpack_stmt_info(self, encoder):
        stmt_info = b"""\x03\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x07\x00\x00\x00\x01\x00\x02\x00[\x00\x00\x00\x00\x00\x04\x00col1\x00\x00\x04\x00col1\x04\x00col1\n\x00-------.99\x00\x00NNNNYN\xe4\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00UYNUYYU\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x02\x00U\x00\x00\x00\x00\x00\x04\x00col2\x00\x00\x04\x00col2\x04\x00col2\x04\x00X(2)\x00\x00NNNNYN\xc0\x01\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x00\x00\x00\x00\x00\x00\x00NUNUYYU\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x02\x00\x00\x00"""
        result = encoder.parse_header(stmt_info)
        expected_columns = Columns([
            ("col1", TD_DECIMAL, 4, 8, 2, "N", None, "-------.99"),
            ("col2", TD_VARCHAR, 6, 0, 0, "N", None, "X(2)"),
        ])

        assert(result[0] == expected_columns[0])
        assert(result[1] == expected_columns[1])


class TestSeralize(object):
    def test_python_to_teradata(self, encoder):
        encoder.columns = [
            ("col1", INTEGER_NN, 4, 0, 0),
            ("col2", FLOAT_NN, 8, 0, 0),
            ("col3", DECIMAL_NN, 8, 16, 2),
            ("col4", DECIMAL_NN, 16, 20, 0),
            ("col5", CHAR_NN, 10, 0, 0),
            ("col6", VARCHAR_NN, 19, 0, 0),
            ("col7", DATE_NN, 4, 0, 0),
        ]
        expected_bytes = b'\x00*\x00\x00\x00\x1f\x85\xebQ\xb8\x1e\t@\x0e\x18\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00value4    \x06\x00value5\x06-\x10\x00'
        expected_text = [42, '3.14', '61.58', '40', 'value4', 'value5', '2006-01-02']
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        #encoder.columns = [
            #("col1", DATE_NN, 4, 0, 0),
        #]
        #expected_bytes = b'\x80    '
        #expected_text = ['6~01-02']
        ##encoder = python_to_teradata(columns)
        ##result_bytes = encoder(expected_text)
        #result_bytes = encoder.serialize(expected_text)
        #assert result_bytes == expected_bytes

    def test_unhandled_type(self, encoder):
        # Testing type not explicitly defined
        encoder.columns = [
            ("col1", DISTINCT_UDT_NN, 3, 0, 0),
        ]
        expected_bytes = b'\x00UDT'
        expected_text = ['UDT']
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_wrong_row_type(self, encoder):
        encoder.columns = [
            ("col1", TD_INTEGER, 4, 0, 0),
        ]


        expected_bytes = b'\x00\x01\x00\x00\x00'
        expected_text = {"col1": 1}
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        encoder |= ROW_ENCODING_DICT
        expected_text = "1"
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        expected_text = [1]
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        encoder |= ROW_ENCODING_STRING
        expected_text = [1]
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    #def test_decimal_overflow(self, encoder):
        #encoder.columns = [
            #('col1', TD_DECIMAL, 2, 2, 2),
        #]
        #expected_text = ('120000',)
        #result_bytes = encoder.serialize(expected_text)

    def test_decimal_64(self, encoder):
        """
        Decode/encode Teradata decimal type values < 18 digits.

        col1
        -----------
        1234567890
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 16, 0),
        ]
        expected_bytes = b'\x00\xd2\x02\x96I\x00\x00\x00\x00'

        # string
        expected_text = ('1234567890',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        # integer
        expected_text = (1234567890,)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        # float
        #expected_text = (1234567890.0,)
        #result_bytes = encoder.serialize(expected_text)
        #assert result_bytes == expected_bytes

        # actual decimal
        expected_text = (decimal.Decimal('1234567890'),)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        # invalid type
        with pytest.raises(EncoderError):
            expected_text = ([1.2,'bango'],)
            result_bytes = encoder.serialize(expected_text)

        with pytest.raises(EncoderError):
            expected_text = ('bango',)
            result_bytes = encoder.serialize(expected_text)

    def test_decimal_64_negative(self, encoder):
        """
        Decode/encode Teradata decimal type values < 18 digits.

        col1
        -----------
        -123456.7890
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 16, 4),
        ]
        expected_bytes = b'\x00.\xfdi\xb6\xff\xff\xff\xff'
        expected_text = ('-123456.7890',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_decimal_64_with_scale(self, encoder):
        """
        Decode/encode Teradata decimal type values < 18 digits.

        col1
        -----------
        123456.7890
        """
        encoder.columns = [
            ('col1', TD_DECIMAL, 8, 16, 4),
        ]
        expected_bytes = b'\x00\xd2\x02\x96I\x00\x00\x00\x00'
        expected_text = ('123456.7890',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_decimal_128(self, encoder):
        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 20, 2),
        ]
        expected_bytes = b'\x00\x9a\x1a\x38\x2f\x81\x9f\x4e\xb1\x06\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ('1234567890123457890.82',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_decimal_128_negative(self, encoder):
        encoder.columns = [
            ('col1', TD_DECIMAL, 16, 20, 2),
        ]
        expected_bytes = b'\x00\x71\x45\x23\x8d\x8e\xbe\x13\x15\x21\xf0\x28\x40\x1c\x39\xe8\xff'
        expected_text = ('-1234567890123456789012345678901234.07',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_decimal_extra_trailing_zeroes(self, encoder):
        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 5, 2),
            ('col2', TD_DECIMAL, 2, 2, 2),
        ]
        expected_bytes = b'\x00\x78\x00\x00\x00\x14\x00'
        expected_text = ('1.20000', '0.20')
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_decimal_scale_padding(self, encoder):
        encoder.columns = [
            ('col1', TD_DECIMAL, 4, 5, 2),
            ('col2', TD_DECIMAL, 4, 5, 4),
        ]
        expected_bytes = b'\x00\x14\x00\x00\x00\xe0\x2e\x00\x00'
        expected_text = ('0.2', '1.20')
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_date_string(self, encoder):
        encoder.columns = [
            ('col1', TD_DATE, 4, 0, 0),
            ('col2', TD_DATE, 4, 0, 0),
        ]
        expected_bytes = b'\x00\x8b\x90\x11\x00Na\xf8\xff'
        expected_text = ('2015-11-15', '1850-06-22')
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_date_from_pydatetime(self, encoder):
        encoder.columns = [
            ('col1', TD_DATE, 4, 0, 0),
            ('col2', TD_DATE, 4, 0, 0),
        ]
        expected_bytes = b'\x00\x8b\x90\x11\x00Na\xf8\xff'
        expected_text = (datetime.datetime(2015,11,15), datetime.datetime(1850,6,22))
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_date_from_pydate(self, encoder):
        encoder.columns = [
            ('col1', TD_DATE, 4, 0, 0),
            ('col2', TD_DATE, 4, 0, 0),
        ]
        expected_bytes = b'\x00\x8b\x90\x11\x00Na\xf8\xff'
        expected_text = (datetime.date(2015,11,15), datetime.date(1850,6,22))
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        expected_bytes = b'\x00\x8b\x90\x11\x00Na\xf8\xff'
        expected_text = (datetime.datetime(2015,11,15,10,12,55), datetime.datetime(1850,6,22,10,12,55))
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_date_from_invalid_type(self, encoder):
        encoder.columns = [
            ('col1', TD_DATE, 4, 0, 0),
        ]
        expected_text = (1489062458.3881698,)
        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

    def test_char(self, encoder):
        encoder.columns = [
            ('col1', TD_CHAR, 8, 0, 0),
        ]

        # needs padding
        expected_bytes = b'\x00test01\x20\x20'
        expected_text = ('test01',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        # padding correct
        expected_bytes = b'\x00test01\x20\x20'
        expected_text = ('test01  ',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes
        expected_bytes = b'\x00test0123'
        expected_text = ('test0123',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        # unicode test
        # TODO: is length defined by column based upon word size or character length?
        expected_bytes = b'\x00\xe2\x98\x83\x20\x20\x20\x20\x20'
        expected_text = ('☃',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_char_bad_encoding(self, encoder):
        encoder.columns = [
            ('col1', TD_CHAR, 8, 0, 0),
        ]
        expected_text = (b'\xc3\x28',)

        with pytest.raises(UnicodeDecodeError):
            result_bytes = encoder.serialize(expected_text)

    def test_char_non_string_input(self, encoder):
        encoder.columns = [
            ('col1', TD_CHAR, 8, 0, 0),
        ]
        expected_text = (10000,)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

        expected_text = (['test'],)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

    def test_char_input_too_large(self, encoder):
        encoder.columns = [
            ('col1', TD_CHAR, 8, 0, 0),
        ]
        expected_text = ('123456789',)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

        expected_text = (10000 * 'test01  ',)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

    def test_varchar(self, encoder):
        encoder.columns = [
            ('col1', TD_VARCHAR, 8, 0, 0),
        ]

        expected_bytes = b'\x00\x00\x00'
        expected_text = ('',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

        expected_bytes = b'\x00\x06\x00test01'
        expected_text = ('test01',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes

    def test_varchar_bad_encoding(self, encoder):
        encoder.columns = [
            ('col1', TD_VARCHAR, 8, 0, 0),
        ]
        expected_text = (b'\xc3\x28',)

        with pytest.raises(UnicodeDecodeError):
            result_bytes = encoder.serialize(expected_text)
            print(result_bytes)

    def test_varchar_non_string_input(self, encoder):
        encoder.columns = [
            ('col1', TD_VARCHAR, 8, 0, 0),
        ]
        expected_text = (10000,)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

        expected_text = (['test'],)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

    def test_varchar_input_too_large(self, encoder):
        encoder.columns = [
            ('col1', TD_VARCHAR, 8, 0, 0),
        ]
        expected_text = (10000 * 'test01  ',)

        with pytest.raises(EncoderError):
            result_bytes = encoder.serialize(expected_text)

    def test_build_a_snowman(self, encoder):
        encoder.columns = [
            ('col1', TD_VARCHAR, 8, 0, 0),
        ]

        expected_bytes = b'\x00\x03\x00\xe2\x98\x83'
        expected_text = ('☃',)
        result_bytes = encoder.serialize(expected_text)
        assert result_bytes == expected_bytes
