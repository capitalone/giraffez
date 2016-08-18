# -*- coding: utf-8 -*-

import pytest

import giraffez
from giraffez.sql import *


class TestSql(object):
    def test_inline_comments(self):
        query_text = """
select-- inline-comment
* from db1.test
where col1 = 'quoted -- text'
"""
        expected = ["select * from db1.test where col1 = 'quoted -- text'"]

        statement = parse_statement(query_text)
        assert statement == expected

    def test_block_comments(self):
        query_text = """select/* block-comment
* from db1.test -- ignored inline-comment
where col1 = */ * from db1.test where col1 = 'quoted /* text'
"""
        expected = ["select * from db1.test where col1 = 'quoted /* text'"]

        statement = parse_statement(query_text)
        assert statement == expected

    def test_case_sensitive(self):
        query_text = """Select * FROM db1.Test Where COL1 = 'Case Sensitive'"""
        expected = ["select * from db1.test where col1 = 'Case Sensitive'"]

        statement = parse_statement(query_text)
        assert statement == expected
