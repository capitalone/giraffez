# -*- coding: utf-8 -*-

import os

import pytest

import giraffez

@pytest.mark.usefixtures('tmpfiles')
class TestInputOutput(object):
    def test_writer(self, tmpfiles):
        with giraffez.Writer(tmpfiles.output_file) as f:
            f.write("value1|value2|value3\n")

    def test_writer_gzip(self, tmpfiles):
        with giraffez.Writer(tmpfiles.output_file, archive=True, use_gzip=True) as f:
            f.write(b"value1|value2|value3\n")
