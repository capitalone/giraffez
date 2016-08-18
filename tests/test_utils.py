# -*- coding: utf-8 -*-

import pytest

from giraffez.utils import *

@pytest.mark.usefixtures('config')
class TestExport(object):
    def test_infer_delimiter(self):
        rows = [
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
            ["value1", "value2", "value3"],
        ]
        pipe_rows = ["|".join(row) for row in rows]
        comma_rows = [",".join(row) for row in rows]
        tab_rows = ["\t".join(row) for row in rows]
        invalid_rows = ["".join(row) for row in rows]

        pipe_delimiter = infer_delimiter(pipe_rows[0])
        comma_delimiter = infer_delimiter(comma_rows[0])
        tab_delimiter = infer_delimiter(tab_rows[0])
        invalid_delimiter = infer_delimiter(invalid_rows[0])

        assert pipe_delimiter == "|"
        assert comma_delimiter == ","
        assert tab_delimiter == "\t"
        assert invalid_delimiter == None

    def test_readable_time(self):
        result = readable_time(123.456789)
        assert result == "123.457s"

        result = readable_time(325)
        assert result == "5m"
