# -*- coding: utf-8 -*-

from __future__ import division

import pytest

import giraffez
from giraffez import _encoder
from giraffez.constants import *
from giraffez.errors import *
from giraffez.encoders import *
from giraffez.types import Columns

import struct

#def unpack_rows(columns, row, n=10):
    #encoder = _encoder.Encoder(columns)
    #rows = row * (64000//len(row))
    #for i in range(n):
        #result = encoder.unpack_rows(rows)

#def test_cencoder_unpack(benchmark):
    #columns = Columns([
        #("col1", VARCHAR_NN, 50, 0, 0),
        #("col2", DECIMAL_NN, 8, 16, 2),
        #("col3", CHAR_NN, 50, 0, 0),
        #("col4", DATE_NN, 4, 0, 0),
        #("col5", VARCHAR_NN, 50, 0, 0),
        #("col6", DECIMAL_NN, 8, 18, 0),
        #("col7", DECIMAL_NN, 8, 18, 0),
        #("col8", INTEGER_NN, 4, 0, 0),
        #("col9", INTEGER_NN, 4, 0, 0),
        #("col10", VARCHAR_NN, 50, 0, 0),
    #])
    #encoder = python_to_teradata(columns)
    #row = encoder(["testing", "100000.02", "testing", "2015-01-01", "testing", "100000000001", "10000000002", 1000, 1001, "testing"])
    #row = struct.pack('h', len(row)) + row

    #result = benchmark(unpack_rows, columns, row, n=1)
