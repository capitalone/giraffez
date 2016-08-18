# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import Columns

@pytest.mark.usefixtures('config')
class TestCmd(object):
    def test_results(self, mocker):
        connect_mock = mocker.patch('giraffez.Cmd._connect')

        cmd = giraffez.Cmd()
        query = "select * from db1.info"
        columns = [
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ]
        rows = [
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
        ]

        cmd.close()
        
        # This ensures that the config was proper mocked
        connect_mock.assert_called_with('db1', 'user123', 'pass456')

    def test_invalid_credentials(self, mocker):
        connect_mock = mocker.patch('giraffez.Cmd._connect')
        connect_mock.side_effect = InvalidCredentialsError("test")

        with pytest.raises(InvalidCredentialsError):
            cmd = giraffez.Cmd(protect=True)
            cmd.close()
