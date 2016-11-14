# -*- coding: utf-8 -*-

import pytest


from giraffez.constants import *
from giraffez.encoders import *
from giraffez.errors import *
from giraffez.logging import *
from giraffez.types import *
from giraffez.utils import *

from giraffez._compat import *

class TestEncoders(object):
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
        expected_bytes = b'\x00b\x85\xe9}\xf4\x10"\x11\x00\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ['1234567890123457890']
        encoder = teradata_to_python(columns)
        result_text = encoder(expected_bytes)
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
        expected_bytes = b'\x00\x9a\x1a8/\x81\x9fN\xb1\x06\x00\x00\x00\x00\x00\x00\x00'
        expected_text = ['1234567890123457890.82']
        encoder = teradata_to_python(columns)
        result_text = encoder(expected_bytes)
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
        expected_bytes = b'\x00\x8bKe\x91\x00\x00\x00\x00'
        expected_text = ['24393.34795']
        encoder = teradata_to_python(columns)
        result_text = encoder(expected_bytes)
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
        encoder = teradata_to_python(columns)
        result_text = encoder(expected_bytes)
        assert result_text == expected_text

    def test_decimal_scale_with_trailing_zeros(self):
        """
        Ensure that encoder correctly handles the conversion of scale
        regardless of digits provided.

        col1
        -----------
        61.5
        """
        columns = Columns([
            ('col1', TD_DECIMAL, 8, 16, 2),
        ])
        expected_bytes = b'\x00\x06\x18\x00\x00\x00\x00\x00\x00'
        expected_text = ["61.50"]
        encoder = teradata_to_python(columns)
        result_text = encoder(expected_bytes)
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
            ('col1', DATE_NN, 4, 0, 0),
        ])
        expected_bytes = b'\x00\x8b\x90\x11\x00'
        expected_text = ['2015-11-15']
        encoder = teradata_to_python(columns)
        result_text = encoder(expected_bytes)
        assert list(map(str, result_text)) == expected_text

        expected_bytes = b'\x00Na\xf8\xff'
        expected_text = ['1850-06-22']
        result_text = encoder(expected_bytes)
        assert list(map(str, result_text)) == expected_text

    def test_python_to_sql(self):
        table_name = "db1.test"
        columns = Columns([
            ("col1", TIMESTAMP_NN, 26, 0, 0),
            ("col2", TIMESTAMP_NN, 19, 0, 0),
        ])
        column_names = ",".join(['"{0}"'.format(column) for column in columns.safe_names])

        rows = [
            ("2006-01-02 15:04:05", "2006-01-02 15:04:05"),
            ("2006-01-02 15:04:05.999999", "2006-01-02 15:04:05.999999")
        ]

        insert_tmpl = "ins into {0} ({1}) values ({2});"

        expected_results = [
            insert_tmpl.format(table_name, column_names, "'2006-01-02 15:04:05.000000','2006-01-02 15:04:05'"),
            insert_tmpl.format(table_name, column_names, "'2006-01-02 15:04:05.999999','2006-01-02 15:04:05'"),
        ]
        processor = pipeline([
            null_handler(DEFAULT_NULL),
            python_to_sql(table_name, columns, True)
        ])
        results = []
        for row in rows:
            results.append(processor(row))
        assert results == expected_results

        # Test without date conversion
        expected_results = [
            insert_tmpl.format(table_name, column_names, "'2006-01-02 15:04:05','2006-01-02 15:04:05'"),
            insert_tmpl.format(table_name, column_names, "'2006-01-02 15:04:05.999999','2006-01-02 15:04:05.999999'"),
        ]
        processor = pipeline([
            null_handler(DEFAULT_NULL),
            python_to_sql(table_name, columns, False)
        ])
        results = []
        for row in rows:
            results.append(processor(row))
        assert results == expected_results

    def test_python_to_teradata(self):
        columns = Columns([
            ("col1", INTEGER_NN, 4, 0, 0),
            ("col2", FLOAT_NN, 4, 0, 0),
            ("col3", DECIMAL_NN, 8, 16, 2),
            ("col4", DECIMAL_NN, 16, 20, 0),
            ("col5", CHAR_NN, 10, 0, 0),
            ("col6", VARCHAR_NN, 19, 0, 0),
            ("col7", DATE_NN, 19, 0, 0),
        ])
        expected_bytes = b'\x00*\x00\x00\x00\x1f\x85\xebQ\xb8\x1e\t@\x0e\x18\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00value4    \x06\x00value5\x06-\x10\x00'
        expected_text = [42, '3.14', '61.58', '40', 'value4', 'value5', '2006-01-02']
        encoder = python_to_teradata(columns)
        result_bytes = encoder(expected_text)
        assert result_bytes == expected_bytes

        columns = Columns([
            ("col1", DATE_NN, 4, 0, 0),
        ])
        expected_bytes = b'\x80    '
        expected_text = ['6~01-02']
        encoder = python_to_teradata(columns)
        result_bytes = encoder(expected_text)
        assert result_bytes == expected_bytes

        # Testing type not explicitly defined in python_to_teradata
        columns = Columns([
            ("col1", DISTINCT_UDT_NN, 3, 0, 0),
        ])
        expected_bytes = b'\x00UDT'
        expected_text = ['UDT']
        encoder = python_to_teradata(columns)
        result_bytes = encoder(expected_text)
        assert result_bytes == expected_bytes

    def test_python_to_teradata_precision_loss(self):
        columns = Columns([
            ("col1", INTEGER_NN, 4, 0, 0),
            ("col2", FLOAT_NN, 4, 0, 0),
            ("col3", DECIMAL_NN, 8, 16, 2),
            ("col4", DECIMAL_NN, 16, 20, 0),
            ("col5", CHAR_NN, 10, 0, 0),
            ("col6", VARCHAR_NN, 19, 0, 0),
            ("col7", DATE_NN, 19, 0, 0),
        ])
        expected_bytes = b'\x00*\x00\x00\x00\x1f\x85\xebQ\xb8\x1e\t@\x0e\x18\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00value4    \x06\x00value5\x06-\x10\x00'
        input_text = [42, 3.14, 61.58, 40, 'value4', 'value5', '2006-01-02']
        expected_text = [42, '3.14', '61.58', '40', 'value4', 'value5', '2006-01-02']
        encoder = python_to_teradata(columns, allow_precision_loss=True)
        result_bytes = encoder(input_text)
        assert result_bytes == expected_bytes

        # Precision loss not allowed by default
        with pytest.raises(GiraffeEncodeError):
            input_text = [42, 3.14, 61.58, 40, 'value4', 'value5', '2006-01-02']
            expected_text = [42, '3.14', '61.58', '40', 'value4', 'value5', '2006-01-02']
            encoder = python_to_teradata(columns)
            result_bytes = encoder(input_text)
            assert result_bytes == expected_bytes

        # Scientific notation evaluation
        input_text = [42, 3.14, 6158e-2, 40, 'value4', 'value5', '2006-01-02']
        expected_text = [42, '3.14', '61.58', '40', 'value4', 'value5', '2006-01-02']
        encoder = python_to_teradata(columns, allow_precision_loss=True)
        result_bytes = encoder(input_text)
        assert result_bytes == expected_bytes

    def test_python_to_teradata_nulls(self):
        columns = Columns([
            ("col1", INTEGER_NN, 4, 0, 0),
            ("col2", FLOAT_NN, 4, 0, 0),
            ("col3", DECIMAL_NN, 8, 16, 2),
            ("col4", CHAR_NN, 10, 0, 0),
            ("col5", VARCHAR_NN, 19, 0, 0),
            ("col6", DATE_NN, 19, 0, 0),
        ])
        expected_bytes = b'\xfc\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00          \x00\x00                   '
        expected_text = [None, None, None, None, None, None]
        encoder = python_to_teradata(columns)
        result_bytes = encoder(expected_text)
        assert result_bytes == expected_bytes

    def test_date_handler(self):
        columns = Columns([
            ("col1", DATE_NN, 4, 0, 0),
            ("col2", TIME_NN, 8, 0, 0),
            ("col3", TIMESTAMP_NN, 19, 0, 0),
        ])
        expected_text = [1150501, '22:08:37.123456', '2006-01-02 15:04:05']
        expected_results = ['2015-05-01', '22:08:37.123456', '2006-01-02 15:04:05']

        handler = DateHandler(columns)
        results = handler(expected_text)

        assert isinstance(results[0], Date)
        assert isinstance(results[1], Time)
        assert isinstance(results[2], Timestamp)

        assert str(results[0]) == expected_results[0]
        assert str(results[1]) == expected_results[1]
        assert str(results[2]) == expected_results[2]

    def test_char_handler(self):
        columns = Columns([
            ("col1", CHAR_NN, 10, 0, 0),
            ("col2", VARCHAR_NN, 20, 0, 0),
        ])
        expected_text = ['value1', 'value2\rvalue3']
        expected_result = ['value1', 'value2\nvalue3']
        handler = CharHandler(columns)
        result_bytes = handler(expected_text)
        assert result_bytes == expected_result

    def test_python_to_dict(self):
        columns = Columns([
            ("col1", INTEGER_NN, 4, 0, 0),
            ("col2", FLOAT_NN, 4, 0, 0),
            ("col3", DECIMAL_NN, 8, 16, 2),
            ("col4", DECIMAL_NN, 16, 20, 0),
            ("col5", CHAR_NN, 10, 0, 0),
            ("col6", VARCHAR_NN, 19, 0, 0),
            ("col7", DATE_NN, 19, 0, 0),
        ])
        expected_text = [42, "3.14", "61.58", "40", "value4", "value5", "2006-01-02"]
        expected_dict = {
            "col1": 42,
            "col2": "3.14",
            "col3": "61.58",
            "col4": "40",
            "col5": "value4",
            "col6": "value5",
            "col7": "2006-01-02",
        }
        encoder = python_to_dict(columns)
        result_dict = encoder(expected_text)

        assert result_dict == expected_dict

    def test_python_to_bytes(self):
        null = "NULL"
        expected_text = [42, "3.14", None, "40", "", None, Date.from_string("2006-01-02")]
        expected_strings = [b"42", b"3.14", b"NULL", b"40", b"", b"NULL", b"2006-01-02"]
        encoder = python_to_bytes(null)
        result_strings = encoder(expected_text)
        assert result_strings == expected_strings

    def test_python_to_strings(self):
        null = "NULL"
        expected_text = [42, "3.14", None, "40", "", None, Date.from_string("2006-01-02")]
        expected_strings = ["42", "3.14", "NULL", "40", "", "NULL", "2006-01-02"]
        encoder = python_to_strings(null)
        result_strings = encoder(expected_text)
        assert result_strings == expected_strings

    def test_strings_to_text(self):
        delimiter = "|"
        expected_strings = ["42", "3.14", "NULL", "40", "", "NULL", "2006-01-02"]
        expected_text = "42|3.14|NULL|40||NULL|2006-01-02"
        encoder = strings_to_text(delimiter)
        result_text = encoder(expected_strings)
        assert result_text == expected_text

    def test_teradata_to_archive(self):
        expected_text = b"value1"
        expected_bytes = b"\x06\x00value1"
        result_bytes = teradata_to_archive(expected_text)

        assert result_bytes == expected_bytes

    def test_text_to_strings(self):
        delimiter = "|"
        expected_text = "42|3.14|NULL|40||NULL|2006-01-02"
        expected_strings = ["42", "3.14", "NULL", "40", "", "NULL", "2006-01-02"]
        encoder = text_to_strings(delimiter)
        result_strings = encoder(expected_text)

        assert result_strings == expected_strings

        delimiter = None
        expected_text = "42|3.14|NULL|40||NULL|2006-01-02"
        expected_strings = ["42|3.14|NULL|40||NULL|2006-01-02"]
        encoder = text_to_strings(delimiter)
        result_strings = encoder(expected_text)

        assert result_strings == expected_strings

    def test_unpack(self):
        expected_bytes = b'\x04\x00\x08\x00\x0b\x00'
        expected_text = [4, 8, 11]
        value1, data = unpack("H", expected_bytes)
        value2, data = unpack("H", data)
        value3, data = unpack("H", data)

        assert len(data) == 0
        assert [value1, value2, value3] == expected_text

    def test_unpack_from(self):
        expected_bytes = b'\x07\x00Giraffe\x05\x00Dumps'
        expected_text = b"GiraffeDumps"
        value1, data = unpack_from("H", expected_bytes)
        value2, data = unpack_from("H", data)
        result = b"".join([value1, value2])

        assert len(data) == 0
        assert result == expected_text

    def test_unpack_stmt_info(self):
        stmt_info = b"""\x03\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x07\x00\x00\x00\x01\x00\x02\x00[\x00\x00\x00\x00\x00\x04\x00col1\x00\x00\x04\x00col1\x04\x00col1\n\x00-------.99\x00\x00NNNNYN\xe4\x01\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00UYNUYYU\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x02\x00U\x00\x00\x00\x00\x00\x04\x00col2\x00\x00\x04\x00col2\x04\x00col2\x04\x00X(2)\x00\x00NNNNYN\xc0\x01\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x02\x00\x00\x00\x00\x00\x00\x00NUNUYYU\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x02\x00\x00\x00"""
        result = unpack_stmt_info(stmt_info)
        expected_results = [b"col1|4|484|8|2||-------.99|N", b"col2|6|448|0|0||X(2)|N"]

        assert result[0]['name'] == b"col1"
        assert result[0]['length'] == 4
        assert result[0]['type'] == 484
        assert result[0]['precision'] == 8
        assert result[0]['scale'] == 2
        assert result[0]['default'] == b""
        assert result[0]['format'] == b"-------.99"
        assert result[0]['nullable'] == b"N"

        assert result[1]['name'] == b"col2"
        assert result[1]['length'] == 6
        assert result[1]['type'] == 448
        assert result[1]['precision'] == 0
        assert result[1]['scale'] == 0
        assert result[1]['default'] == b""
        assert result[1]['format'] == b"X(2)"
        assert result[1]['nullable'] == b"N"
