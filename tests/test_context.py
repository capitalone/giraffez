# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import *

@pytest.mark.usefixtures('config')
class TestContext(object):
    def test_with_context(self, mocker):
        connect_mock = mocker.patch('giraffez.cmd.TeradataCmd._connect')

        with giraffez.Cmd() as cmd:
            pass
        
        connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)

    def test_without_context(self, mocker):
        connect_mock = mocker.patch('giraffez.cmd.TeradataCmd._connect')

        query = "select * from db1.info"
        with pytest.raises(GiraffeError) as e:
            cmd = giraffez.Cmd()
            cmd.execute(query)
        e.match('used outside of a with context')

    def test_allow_without_context(self, mocker):
        connect_mock = mocker.patch('giraffez.cmd.TeradataCmd._connect')
        execute_mock = mocker.patch('giraffez.cmd.TeradataCmd.execute')

        query = "select * from db1.info"
        cmd = giraffez.Cmd(allow_without_context=True)
        cmd.execute(query)

        connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)

    def test_allow_with_context(self, mocker):
        connect_mock = mocker.patch('giraffez.cmd.TeradataCmd._connect')

        query = "select * from db1.info"
        with giraffez.Cmd(allow_without_context=True) as cmd:
            pass

        connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)

    def test_with_context_and_args(self, mocker):
        connect_mock = mocker.patch('giraffez.export.TeradataBulkExport._connect')
        query_mock = mocker.patch('giraffez.export.TeradataBulkExport.query', new=mocker.PropertyMock)
        close_mock = mocker.patch('giraffez.export.TeradataBulkExport._close')

        query = 'db1.info'
        with giraffez.BulkExport(query, coerce_floats=False) as export:
            assert export.query == query
            assert export.coerce_floats == False

        connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)
        close_mock.assert_called_once()
