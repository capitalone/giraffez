# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import Columns

@pytest.mark.usefixtures('config', 'context')
class TestBulkExport(object):
    def test_export_results(self, mocker):
        connect_mock = mocker.patch('giraffez.export.TeradataBulkExport._connect')
        query = "select * from db1.info"
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
        expected_results = ["|".join(row) for row in rows]

        export = giraffez.BulkExport()
        export.export = mocker.MagicMock()
        export.export.status.return_value = 0
        export.export.get_buffer.side_effect = [expected_results]
        export.export.columns.return_value = columns

        export.query = query
        export._initiate()
        results = list(export.to_list())
        export._close()

        # This ensures that the config was proper mocked
        connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)

        assert results == expected_results
        assert isinstance(export.columns, giraffez.types.Columns) == True
        assert export.export.columns.called == True
        assert len(export.columns) == 3
        #assert export.export.get_buffer.call_count == 2
        assert export.export.close.called == True

    def test_invalid_credentials(self, mocker):
        connect_mock = mocker.patch('giraffez.export.TeradataBulkExport._connect')
        query = "select * from db1.info"
        export = giraffez.BulkExport()
        export.export = mocker.MagicMock()
        export.export.status.return_value = TD_ERROR_INVALID_USER
        export.export.initiate.side_effect = InvalidCredentialsError("...")
        with pytest.raises(InvalidCredentialsError):
            export.query = query
            results = list(export.to_list())
            export._close()

    def test_parse_sql(self, mocker):
        connect_mock = mocker.patch('giraffez.export.TeradataBulkExport._connect')
        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])
        export = giraffez.BulkExport()
        export.export = mocker.MagicMock()
        export.export.columns.return_value = columns

        export.query = """select col1 as ‘c1’, col2 as “c2”from db1.info a\n join db2.info b\n on a.id = b.id"""
        export._close()

        assert '\n' not in export.query
        assert '‘' not in export.query
        assert '’' not in export.query
        assert '“' not in export.query
        assert '”' not in export.query
