# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.constants import *
from giraffez.errors import *
from giraffez.types import *

@pytest.mark.usefixtures('config', 'tmpfiles')
class TestLoad(object):
    def test_load_from_file(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(5000):
                rows.append("|".join(["value1", "value2", "value3"]))
            f.write("\n".join(rows))

        with giraffez.Load() as load:
            result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")
        assert result.get('count') == 5000

    def test_load_from_file_quoted(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(4999):
                rows.append("|".join(["value1", "value2", "value3"]))
            rows.append("|".join(["value1",'"value2|withpipe"', "value3"]))
            f.write("\n".join(rows))

        with giraffez.Load() as load:
            result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")
        assert result.get('count') == 5000

    def test_load_from_file_single_quoted(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(4999):
                rows.append("|".join(["value1", "value2", "value3"]))
            rows.append("|".join(["value1","'value2|withpipe'", "value3"]))
            f.write("\n".join(rows))

        with giraffez.Load() as load:
            result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|", quotechar="'")
        assert result.get('count') == 5000


    def test_load_from_file_nonstandard_quote(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            rows = []
            for i in range(4999):
                rows.append("|".join(["value1", "value2", "value3"]))
            rows.append("|".join(['va"lue1','$value2|withpipe"and"quote$', "value3"]))
            f.write("\n".join(rows))

        with giraffez.Load() as load:
            result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|", quotechar="$")
        assert result.get('count') == 5000

        
    def test_load_from_file_error(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3", "value4"]))
            f.write("\n")

        with giraffez.Load() as load:
            result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")

    def test_load_from_file_error_panic(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3", "value4"]))
            f.write("\n")

        with giraffez.Load() as load:
            load.panic = True
            with pytest.raises(GiraffeEncodeError):
                result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

    def test_load_from_file_invalid_header(self, mocker, tmpfiles):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        # Invalid column (blank string)
        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3", "", ""]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3"]))
            f.write("\n")

        with giraffez.Load() as load:
            load.panic = True
            with pytest.raises(GiraffeError):
                result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

        # Invalid column (wrong name)
        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col4"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3"]))
            f.write("\n")

        with giraffez.Load() as load:
            load.panic = True
            with pytest.raises(GiraffeError):
                result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

        # Too many columns (duplicate name)
        with open(tmpfiles.load_file, 'w') as f:
            f.write("|".join(["col1", "col2", "col3", "col3"]))
            f.write("\n")
            f.write("|".join(["value1", "value2", "value3"]))
            f.write("\n")

        with giraffez.Load() as load:
            load.panic = True
            with pytest.raises(GiraffeEncodeError):
                result = load.from_file("db1.test", tmpfiles.load_file, delimiter="|")
                print(result)

    def test_load_insert_no_specify_fields(self, mocker):
        mock_connect = mocker.patch("giraffez.Cmd._connect")
        mock_execute = mocker.patch("giraffez.Cmd._execute")

        columns = Columns([
            ("col1", VARCHAR_NN, 50, 0, 0),
            ("col2", VARCHAR_N, 50, 0, 0),
            ("col3", VARCHAR_N, 50, 0, 0),
        ])

        mock_columns = mocker.patch("giraffez.Cmd.get_columns")

        mock_columns.return_value = columns

        rows = [
            ("value1", "value3"),
            ("value1", "value3"),
            ("value1", "value3"),
        ]

        with giraffez.Load() as load:
            load.panic = True
            with pytest.raises(GiraffeEncodeError):
                load.insert("db1.test", rows)
