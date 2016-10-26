# -*- coding: utf-8 -*-

import pytest

from giraffez.io import *
from giraffez.utils import *
from giraffez._compat import StringIO

@pytest.mark.usefixtures('config', 'tmpfiles')
class TestExport(object):
    def test_file_delimiter(self, tmpfiles):
        rows = [
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
        ]

        def test_delimiter(delimiter, expected_delimiter):
            with open(tmpfiles.output_file, 'w') as f:
                for row in rows:
                    f.write(delimiter.join(row))
                    f.write("\n")
            assert file_delimiter(tmpfiles.output_file) == expected_delimiter

        test_delimiter("|", "|")
        test_delimiter(",", ",")
        test_delimiter("\t", "\t")
        test_delimiter("", None)

    def test_readable_time(self):
        result = readable_time(123.456789)
        assert result == "123.457s"

        result = readable_time(325)
        assert result == "5m"
