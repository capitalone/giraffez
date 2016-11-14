# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.types import *


def set_indicator(indicator, nulls):
    for i in nulls:
        indicator.set_bit(i)


class TestTypes(object):
    def test_read_indicator_bytes(self):
        indicator = b"\x01\x0f\x80"
        expected = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\x17"
        expected = [0, 0, 0, 1, 0, 1, 1, 1]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\xc8\xc0"
        expected = [1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\xc1\xc8\xc2"
        expected = [1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\xc2\xc9\xc0"
        expected = [1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\x00\x00\x00"
        expected = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\x00"
        expected = [0, 0, 0, 0, 0, 0, 0, 0]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\xff\xff\xff"
        expected = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        assert list(Bytes.read(indicator)) == expected

        indicator = b"\xff"
        expected = [1, 1, 1, 1, 1, 1, 1, 1]
        assert list(Bytes.read(indicator)) == expected

    def test_write_indicator_bytes(self):
        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 17))
        expected_bytes = b"\x01\x0f\x80"
        set_indicator(indicator, [7, 12, 13, 14, 15, 16])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 6))
        expected_bytes = b"\x40"
        set_indicator(indicator, [1])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 10))
        expected_bytes = b"\xc8\xc0"
        set_indicator(indicator, [0, 1, 4, 8, 9])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 24))
        expected_bytes = b"\xc1\xc8\xc2"
        set_indicator(indicator, [0, 1, 7, 8, 9, 12, 16, 17, 22])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 24))
        expected_bytes = b"\xc2\xc9\xc0"
        set_indicator(indicator, [0, 1, 6, 8, 9, 12, 15, 16, 17])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 17))
        expected_bytes = b"\x00\x00\x00"
        set_indicator(indicator, [])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 6))
        expected_bytes = b"\x00"
        set_indicator(indicator, [])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 24))
        expected_bytes = b"\xff\xff\xff"
        set_indicator(indicator, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23])
        assert bytes(indicator) == expected_bytes

        indicator = Bytes(Columns([("", TD_VARCHAR, 10, 0, 0)] * 8))
        expected_bytes = b"\xff"
        set_indicator(indicator, [0, 1, 2, 3, 4, 5, 6, 7])
        assert bytes(indicator) == expected_bytes

    def test_columns_5(self):
        columns = Columns([
            ('col1', TD_CHAR, 10, 0, 0),
            ('col2', TD_VARCHAR, 20, 0, 0),
            ('col3', TD_DATE, 4, 0, 0),
            ('col4', TD_TIMESTAMP, 26, 0, 0),
            ('col5', TD_DECIMAL, 8, 16, 2),
            ('col6', TD_DECIMAL, 16, 20, 0),
            ('col7', TD_DECIMAL, 4, 5, 2),
            ('col8', TD_SMALLINT, 2, 0, 0),
            ('col9', TD_INTEGER, 4, 0, 0),
        ])
        assert columns.header_length == 2
        assert columns.names == ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8',
            'col9']
        assert len(columns) == 9

        assert columns[1].as_tuple == ('col2', TD_VARCHAR, 20, 0, 0)
        assert columns.get('col2') == columns[1]

        columns.set_filter(['col1', 'col3', 'col5', 'col8'])
        assert columns.header_length == 1
        assert columns.names == ['col1', 'col3', 'col5', 'col8']
        assert len(columns) == 4

        # test that indexing works based on filtered columns
        assert columns.get('col3') == columns[1]

        serialized_columns = b'8\x00\xc4\x01\n\x00\x00\x00\x00\x00\x04\x00col1\xf0\x02\x04\x00\x00\x00\x00\x00\x04\x00col3\xe4\x01\x08\x00\x10\x00\x02\x00\x04\x00col5\xf4\x01\x02\x00\x00\x00\x00\x00\x04\x00col8'
        assert columns.serialize() == serialized_columns
        deserialized_columns = Columns.deserialize(serialized_columns[2:])
        assert deserialized_columns == columns

    def test_columns_8(self):
        columns = Columns([
            ('col1', TD_CHAR, 10, 0, 0, None, 'default1', 'X(10)'),
            ('col2', TD_VARCHAR, 20, 0, 0, 'Y', 'default2', 'X(20)'),
            ('col3', TD_DATE, 4, 0, 0, 'N', 'current_date', 'YYYY-MM-DD'),
        ])
        assert columns.header_length == 1
        assert columns.names == ['col1', 'col2', 'col3']
        assert len(columns) == 3

        assert columns[1].as_tuple == ('col2', TD_VARCHAR, 20, 0, 0)
        assert columns.get('col2') == columns[1]

        with pytest.raises(giraffez.GiraffeError):
            columns.get('col1').nullable == None
        assert columns.get('col2').nullable == 'Y'
        assert columns.get('col3').nullable == 'N'

        assert columns.get('col1').default == 'default1'
        assert columns.get('col2').default == 'default2'
        assert columns.get('col3').default == 'current_date'

        assert columns.get('col1').format == 'X(10)'
        assert columns.get('col2').format == 'X(20)'
        assert columns.get('col3').format == 'YYYY-MM-DD'

        columns.set_filter(['col1', 'col3'])
        assert columns.header_length == 1
        assert columns.names == ['col1', 'col3']
        assert len(columns) == 2

        # test that indexing works based on filtered columns
        assert columns.get('col3') == columns[1]

        # Cannot perform serialization test with extended information
        # because not all information is serialized. This extra info
        # such as format and nullable are not kept because they are
        # not necessary as part of the reloading process.  The
        # giraffez archive file is not meant to be completely
        # self describing, rather it is meant to provide the necessary
        # metadata to complete a one-to-one reload of a table.

        # Instead this test is setup to ensure the deserialized format
        # is as expected. If problems come about in the future this
        # may need more extensive testing.
        serialized_columns = b'\x1c\x00\xc4\x01\n\x00\x00\x00\x00\x00\x04\x00col1\xf0\x02\x04\x00\x00\x00\x00\x00\x04\x00col3'
        assert columns.serialize() == serialized_columns
        deserialized_columns = Columns.deserialize(serialized_columns[2:])
        expected_columns = Columns([
            ('col1', TD_CHAR, 10, 0, 0),
            ('col2', TD_VARCHAR, 20, 0, 0),
            ('col3', TD_DATE, 4, 0, 0),
        ])
        expected_columns.set_filter(['col1', 'col3'])

        assert deserialized_columns == expected_columns
