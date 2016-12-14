# -*- coding: utf-8 -*-

import pytest


from giraffez import _encoder
from giraffez.constants import *
from giraffez.types import *

class TestCEncoder(object):
    def test_decimal_128(self):
        """
        Decode/encode Teradata decimal type values > 18 digits.

        col1
        -------------------
        1234567890123457890
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 16, 20, 0),
        ])
        expected_bytes = b'\x00\x62\x85\xe9\x7d\xf4\x10\x22\x11\x00\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ['1234567890123457890']
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_128_with_scale(self):
        """
        Decode/encode Teradata decimal type values > 18 digits.

        col2
        ----------------------
        1234567890123457890.82
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 16, 20, 2),
        ])
        expected_bytes = b'\x00\x9a\x1a\x38\x2f\x81\x9f\x4e\xb1\x06\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ['1234567890123457890.82']
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_64_with_scale(self):
        """
        Decode/encode Teradata decimal type values < 18 digits.

        col1
        -----------
        24393.34795
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 8, 10, 5),
        ])
        expected_bytes = b'\x00\x8b\x4b\x65\x91\x00\x00\x00\x00'
        expected_text = ['24393.34795']
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_negative(self):
        """
        Decode/encode Teradata decimal type values containing
        negatives.

        col1
        -----------
        -47.86
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 4, 5, 2),
        ])
        expected_bytes = b'\x00\x4e\xed\xff\xff'
        expected_text = ['-47.86']
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_negative_more(self):
        """
        Decode/encode Teradata decimal type values containing
        negatives with an integer-part of 0. This test ensures
        that the signedness is not lost with a zero value.

        col1
        -----------
        -0.20
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 4, 5, 2),
        ])
        expected_bytes = b'\x00\xec\xff\xff\xff'
        expected_text = ['-0.20']
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = '-0.20'
        encoder.set_encoding(ROW_ENCODING_STRING, ITEM_ENCODING_STRING, DECIMAL_AS_STRING)
        encoder.set_delimiter("|")
        encoder.set_null("NULL")
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = {'col1': '-0.20'}
        encoder.set_encoding(ROW_ENCODING_DICT, ITEM_ENCODING_BUILTIN_TYPES, DECIMAL_AS_STRING)
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
        columns = Columns([
            ('col1', TD_DECIMAL, 8, 16, 2),
        ])
        expected_bytes = b'\x00\x06\x18\x00\x00\x00\x00\x00\x00'
        expected_text = ["61.50"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_scale_with_leading_zeros(self):
        """
        Testing all decimal sizes (based on byte-length) for
        leading zeros in the fractional digits.
        """

        # Decimal 1
        columns = Columns([
            ('col1', TD_DECIMAL, 1, 2, 2),
        ])
        expected_bytes = b'\x00\x01'
        expected_text = ["0.01"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text


        # Decimal 2
        columns = Columns([
            ('col1', TD_DECIMAL, 2, 4, 2),
        ])
        expected_bytes = b'\x00\xb3\x04'
        expected_text = ["12.03"]
        encoder2 = _encoder.Encoder(columns)
        result_text = encoder2.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 4
        columns = Columns([
            ('col1', TD_DECIMAL, 4, 9, 2),
        ])
        expected_bytes = b'\x00\xc4\xcc\x5b\x07'
        expected_text = ["1234567.08"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 8
        columns = Columns([
            ('col1', TD_DECIMAL, 8, 18, 2),
        ])
        expected_bytes = b'\x00\x07\xf3\x30\xa6\x4b\x9b\xb6\x01'
        expected_text = ["1234567890123456.07"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 16
        columns = Columns([
            ('col1', TD_DECIMAL, 16, 38, 2),
        ])
        expected_bytes = b'\x00\x07\xf3\x38\xde\x50\x90\x49\xc4\x13\x33\x02\xf0\xf6\xb0\x49\x09'
        expected_text = ["123456789012345678901234567890123456.07"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_decimal_overflow(self):
        """
        Testing all decimal sizes (based on byte-length) for integer
        overflow. Both with and without scale is tested because the
        cencoder paths differently through the function in each case.
        This would be helpful in cases where the wrong abs() function
        is called and the value is unsafely downcast to a lower integer
        type.
        """

        # Decimal 1
        columns = Columns([
            ('col1', TD_DECIMAL, 1, 2, 0),
        ])
        expected_bytes = b'\x00\x63'
        expected_text = ["99"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        columns = Columns([
            ('col1', TD_DECIMAL, 1, 2, 1),
        ])
        expected_bytes = b'\x00\x63'
        expected_text = ["9.9"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 2
        columns = Columns([
            ('col1', TD_DECIMAL, 2, 4, 0),
        ])
        expected_bytes = b'\x00\x0f\x27'
        expected_text = ["9999"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        columns = Columns([
            ('col1', TD_DECIMAL, 2, 4, 1),
        ])
        expected_bytes = b'\x00\x0f\x27'
        expected_text = ["999.9"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 4
        columns = Columns([
            ('col1', TD_DECIMAL, 4, 9, 0),
        ])
        expected_bytes = b'\x00\xff\xc9\x9a\x3b'
        expected_text = ["999999999"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        columns = Columns([
            ('col1', TD_DECIMAL, 4, 9, 1),
        ])
        expected_bytes = b'\x00\xff\xc9\x9a\x3b'
        expected_text = ["99999999.9"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 8
        columns = Columns([
            ('col1', TD_DECIMAL, 8, 18, 0),
        ])
        expected_bytes = b'\x00\xff\xffc\xa7\xb3\xb6\xe0\x0d'
        expected_text = ["999999999999999999"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        columns = Columns([
            ('col1', TD_DECIMAL, 8, 18, 1),
        ])
        expected_bytes = b'\x00\xff\xffc\xa7\xb3\xb6\xe0\x0d'
        expected_text = ["99999999999999999.9"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        # Decimal 16
        columns = Columns([
            ('col1', TD_DECIMAL, 16, 38, 0),
        ])
        expected_bytes = b'\x00\xff\xff\xff\xff\x3f\x22\x8a\x09\x7a\xc4\x86\x5a\xa8\x4c\x3b\x4b'
        expected_text = ["99999999999999999999999999999999999999"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        columns = Columns([
            ('col1', TD_DECIMAL, 16, 38, 1),
        ])
        expected_bytes = b'\x00\xff\xff\xff\xff\x3f\x22\x8a\x09\x7a\xc4\x86\x5a\xa8\x4c\x3b\x4b'
        expected_text = ["9999999999999999999999999999999999999.9"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_date_from_integer(self):
        """
        Need a date before and after 1900-01-01

        col1
        ----------
        2015-11-15
        1850-06-22

        """
        columns = Columns([
            ('col1', TD_DATE, 4, 0, 0),
        ])
        expected_bytes = b'\x00\x8b\x90\x11\x00'
        expected_text = ['2015-11-15']
        encoder = _encoder.Encoder(columns)
        encoder.set_encoding(ROW_ENCODING_LIST, ITEM_ENCODING_STRING, DECIMAL_AS_STRING)
        encoder.set_null("NULL")
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = {'col1': '2015-11-15'}
        encoder.set_encoding(ROW_ENCODING_DICT, ITEM_ENCODING_BUILTIN_TYPES, DECIMAL_AS_STRING)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_bytes = b'\x00Na\xf8\xff'
        expected_text = ['1850-06-22']
        encoder = _encoder.Encoder(columns)
        encoder.set_encoding(ROW_ENCODING_LIST, ITEM_ENCODING_STRING, DECIMAL_AS_STRING)
        encoder.set_null("NULL")
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = {'col1': '1850-06-22'}
        encoder.set_encoding(ROW_ENCODING_DICT, ITEM_ENCODING_BUILTIN_TYPES, DECIMAL_AS_STRING)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

    def test_null(self):
        """
        Ensure encoder handles nulls

        """
        columns = Columns([
            ('col1', TD_INTEGER, 4, 0, 0),
            ('col2', TD_VARCHAR, 50, 0, 0),
            ('col3', TD_VARCHAR, 50, 0, 0),
        ])
        expected_bytes = b'\x80\x00\x00\x00\x00\x06\x00value2\x06\x00value3'
        expected_text = [None, "value2", "value3"]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = ["NULL", "value2", "value3"]
        encoder.set_encoding(ROW_ENCODING_LIST, ITEM_ENCODING_STRING, DECIMAL_AS_STRING)
        encoder.set_null("NULL")
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = "NULL|value2|value3"
        encoder.set_encoding(ROW_ENCODING_STRING, ITEM_ENCODING_STRING, DECIMAL_AS_STRING)
        encoder.set_delimiter("|")
        encoder.set_null("NULL")
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = {"col1": None, "col2": "value2", "col3": "value3"}
        encoder.set_encoding(ROW_ENCODING_DICT, ITEM_ENCODING_BUILTIN_TYPES, DECIMAL_AS_STRING)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        columns = Columns([
            ('col1', TD_INTEGER, 4, 10, 0),
            ('col2', TD_DECIMAL, 4, 8, 2),
            ('col3', TD_CHAR, 2, 0, 0),
            ('col4', TD_DATE, 4, 0, 0)
        ])
        expected_bytes = b'p\x01\x00\x00\x00\x00\x00\x00\x00  \x00\x00\x00\x00'
        expected_text = [1, None, None, None]
        encoder = _encoder.Encoder(columns)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = ["1", "NULL", "NULL", "NULL"]
        encoder.set_encoding(ROW_ENCODING_LIST, ITEM_ENCODING_STRING, DECIMAL_AS_STRING)
        encoder.set_null("NULL")
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text

        expected_text = {"col1": 1, "col2": None, "col3": None, "col4": None}
        encoder.set_encoding(ROW_ENCODING_DICT, ITEM_ENCODING_BUILTIN_TYPES, DECIMAL_AS_STRING)
        result_text = encoder.unpack_row(expected_bytes)
        assert result_text == expected_text
