# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import Columns


@pytest.mark.usefixtures('config', 'context')
class TestMLoad(object):
    def test_bulkload_results(self, mocker):
        bulkload_connect_mock = mocker.patch('giraffez.load.TeradataBulkLoad._connect')
        table = "db1.info"
        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])
        rows = [
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
        ]
        load = giraffez.BulkLoad()
        load.mload = mocker.MagicMock()
        load.mload.status.return_value = 0
        load.mload.put_row.return_value = 0
        load.mload.exists.return_value = False
        load.mload.checkpoint.return_value = 0
        load.mload.get_event.side_effect = [
            b'\x00\x00',
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80',
            b'\x00\x00\x00\x00',
            b'\x00\x00',
        ]
        load.table = table
        for row in rows:
            load.put(row)
        exit_code = load.finish()
        load._close()

        bulkload_connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)

        assert load.mload.initiate.called == True
        assert load.mload.put_row.call_count == 3
        assert load.mload.end_acquisition.called == True
        assert load.mload.apply_rows.called == True
        assert load.mload.close.called == True
