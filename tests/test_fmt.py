# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.fmt import *
from giraffez.types import GiraffeDate


class TestFmt(object):
    def test_format_table(self):
        rows = [
            ('col1', 'col2', 'col3'),
            ('one', 'two', 'three'),
            ('value1', 2, GiraffeDate(2015, 11, 8)),
        ]
        result = format_table(rows)
        expected_result = """col1   | col2 | col3      
--------------------------
one    | two  | three     
value1 | 2    | 2015-11-08"""
        assert result == expected_result

    def test_replace_cr(self):
        result = replace_cr("line1\rline2\n")
        expected_result = "line1\nline2"
        assert result == expected_result
        result = replace_cr(None)
        assert result == None
