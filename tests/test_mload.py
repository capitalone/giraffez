# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.types import Columns
from giraffez.utils import *


@pytest.mark.usefixtures('config')
class TestMLoad(object):
    def test_mload_results(self, mocker):
        mload_connect_mock = mocker.patch('giraffez.MLoad._connect')
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
        mload = giraffez.MLoad()
        mload.load = mocker.MagicMock()
        mload.load.status.return_value = 0
        mload.load.put_row.return_value = 0
        mload.load.checkpoint.return_value = 0
        mload.load.get_event.side_effect = [
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80',
            b'\x00\x00\x00\x00',
            b'\x00\x00',
        ]
        mload.cmd = mocker.Mock()
        mload.cmd.get_columns = mocker.Mock(return_value=columns)

        mload.table = table
        for row in rows:
            mload.load_row(row)
        exit_code = mload.finish()
        mload.close()

        mload_connect_mock.assert_called_with('db1', 'user123', 'pass456')

        assert mload.load.initiate.called == True
        assert mload.load.put_row.call_count == 3
        assert mload.load.end_acquisition.called == True
        assert mload.load.apply_rows.called == True
        assert mload.load.close.called == True
        assert mload.cmd.close.called == True
