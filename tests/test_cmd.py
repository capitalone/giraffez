# -*- coding: utf-8 -*-

import pytest

from giraffez._teradata import RequestEnded, StatementEnded, StatementInfoEnded
import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import *


class ResultsHelper:
    """
    Helps to emulate how exceptions are raised when working with the CLIv2 so
    that the control flow will be adequately represented.
    """

    def __init__(self, rows):
        self.first = True
        self.index = 0
        self.rows = rows

    def get(self):
        if self.first:
            self.first = False
            raise StatementInfoEnded

        if self.index >= len(self.rows):
            raise RequestEnded

        row = self.rows[self.index]
        self.index += 1
        return row

    def __call__(self):
        return self.get()


@pytest.mark.usefixtures('config', 'context')
class TestCmd(object):
    def test_results(self, mocker):
        connect_mock = mocker.patch('giraffez.cmd.TeradataCmd._connect')
        mock_columns = mocker.patch("giraffez.cmd.Cursor._columns")

        cmd = giraffez.Cmd()
        query = "select * from db1.info"
        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns.return_value = columns

        rows = [
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
        ]

        expected_rows = [
            {"col1": "value1", "col2": "value2", "col3": "value3"},
            {"col1": "value1", "col2": "value2", "col3": "value3"},
            {"col1": "value1", "col2": "value2", "col3": "value3"},
        ]
        cmd.cmd = mocker.MagicMock()
        cmd.cmd.fetchone.side_effect = ResultsHelper(rows)
        result = list(cmd.execute(query))

        assert [x.items() for x in result] == expected_rows

        cmd._close()
        
        # This ensures that the config was proper mocked
        connect_mock.assert_called_with('db1', 'user123', 'pass456', None, None)

    def test_invalid_credentials(self, mocker):
        connect_mock = mocker.patch('giraffez.cmd.TeradataCmd._connect')
        connect_mock.side_effect = InvalidCredentialsError("test")

        with pytest.raises(InvalidCredentialsError):
            cmd = giraffez.Cmd(protect=True)
            cmd._close()


@pytest.mark.usefixtures('config', 'context', 'tmpfiles')
class TestInsert(object):
    def test_insert_from_file(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(100):
                rows.append("|".join(["value1", "value2", "value3"]))
            f.write("\n".join(rows))

        with giraffez.Cmd() as cmd:
            result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")
        assert result.get('count') == 100

    def test_insert_from_file_quoted(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(99):
                rows.append("|".join(["value1", "value2", "value3"]))
            rows.append("|".join(["value1",'"value2|withpipe"', "value3"]))
            f.write("\n".join(rows))

        with giraffez.Cmd() as cmd:
            result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")
        assert result.get('count') == 100

    def test_insert_from_file_single_quoted(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(99):
                rows.append("|".join(["value1", "value2", "value3"]))
            rows.append("|".join(["value1","'value2|withpipe'", "value3"]))
            f.write("\n".join(rows))

        with giraffez.Cmd() as cmd:
            result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|", quotechar="'")
        assert result.get('count') == 100


    def test_insert_from_file_nonstandard_quote(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(99):
                rows.append("|".join(["value1", "value2", "value3"]))
            rows.append("|".join(['va"lue1','$value2|withpipe"and"quote$', "value3"]))
            f.write("\n".join(rows))

        with giraffez.Cmd() as cmd:
            result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|", quotechar="$")
        assert result.get('count') == 100

    def test_insert_from_file_error(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3", "value4"]))
            f.write("\n")

        with giraffez.Cmd() as cmd:
            cmd.panic = False
            result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")

    def test_insert_from_file_error_panic(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3", "value4"]))
            f.write("\n")

        with giraffez.Cmd() as cmd:
            with pytest.raises(GiraffeEncodeError):
                result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

    def test_insert_from_file_invalid_header(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        # Invalid column (blank string)
        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3", "", ""]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3"]))
            f.write("\n")

        with giraffez.Cmd() as cmd:
            with pytest.raises(GiraffeError):
                result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

        # Invalid column (wrong name)
        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col4"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3"]))
            f.write("\n")

        with giraffez.Cmd() as cmd:
            with pytest.raises(GiraffeError):
                result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

        # Too many columns (duplicate name)
        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3", "col3"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3"]))
            f.write("\n")

        with giraffez.Cmd() as cmd:
            with pytest.raises(GiraffeEncodeError):
                result = cmd.insert("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

    def test_insert_insert_no_specify_fields(self, mocker):
        mock_connect = mocker.patch("giraffez.cmd.TeradataCmd._connect")
        mock_execute = mocker.patch("giraffez.cmd.TeradataCmd.execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.cmd.TeradataCmd.fetch_columns")

        mock_columns.return_value = columns

        rows = [
            ("value1", "value3"),
            ("value1", "value3"),
            ("value1", "value3"),
        ]

        with giraffez.Cmd() as cmd:
            with pytest.raises(GiraffeEncodeError):
                cmd.insert("db1.test", rows)
