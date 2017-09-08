# -*- coding: utf-8 -*-
#
# Copyright 2016 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import os
import sys
import gzip
import struct
import csv
import json
import io

from .constants import *
from .errors import *

from .logging import log
from .types import Columns

from ._compat import *


def file_exists(path):
    try:
        with open(path):
            return True
    except (OSError, IOError):
        return False

def file_delimiter(path):
    valid = " abcdefghijklmnopqrstuvwxyz1234567890_"
    possible = ",|\t"
    with open(path, "r") as f:
        header = f.readline().strip()
        for c in header.lower():
            if c not in valid and c in possible:
                return c
        return None

def file_permissions(path):
    return os.stat(path).st_mode & 0o777

def home_file(filename):
    return os.path.join(os.path.expanduser("~"), filename)

def isfile(s):
    if isinstance(s, (str, bytes)) and ' ' not in s and file_exists(s):
        return True
    return False


class BaseReader(object):
    def __init__(self, path, mode='rt'):
        abspath = os.path.abspath(path)
        if not os.path.exists(os.path.dirname(abspath)):
            raise FileNotFound("Path '{}' does not exist.".format(os.path.dirname(abspath)))
        with open(path, "rb") as f:
            data = f.read(2)
            if data == GZIP_MAGIC:
                _open = lambda p, m: io.BufferedReader(gzip.open(p, m))
            else:
                _open = lambda p, m: io.open(p, m)
        self.fd = _open(abspath, mode)
        self._header = None

    @property
    def header(self):
        if self._header is None:
            raise GiraffeError("Property 'header' not found.")
        return self._header

    def read(self, size=-1):
        return self.fd.read(size)

    def readline(self):
        return next(self.fd)

    @classmethod
    def read_all(cls, path, mode='rt'):
        with cls(path, mode) as f:
            return f.read()

    @classmethod
    def read_header(cls, path):
        with cls(path, 'rb') as f:
            return f.read(len(GIRAFFE_MAGIC) + 2)

    def __iter__(self):
        while True:
            line = self.readline()
            if line is None:
                break
            yield line

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if not self.fd.closed:
            self.fd.close()


class FileReader(BaseReader):
    encoding = "str"

    @classmethod
    def check_length(cls, path, length):
        with cls(path, "rb") as f:
            for i, line in enumerate(f, 1):
                if i >= length:
                    return True
            return False


class CSVReader(FileReader):
    encoding = "csv"

    def __init__(self, path, delimiter=None, quotechar='"'):
        super(CSVReader, self).__init__(path)
        self.delimiter = delimiter
        if self.delimiter is None:
            self.delimiter = file_delimiter(path)
        if self.delimiter is None:
            raise GiraffeError("Delimiter could not be inferred.")
        self.reader = csv.reader(self.fd, delimiter=self.delimiter, quotechar=quotechar)
        self._header = next(self.reader)

    def readline(self):
        return next(self.reader)

    def __iter__(self):
        while True:
            yield next(self.reader)


class JSONReader(FileReader):
    encoding = "dict"

    def __init__(self, path):
        super(JSONReader, self).__init__(path)
        self._header = json.loads(next(self.fd)).keys()
        self.fd.seek(0)

    def readline(self):
        return list(json.loads(next(self.fd)).values())


class ArchiveFileReader(BaseReader):
    def __init__(self, path):
        super(ArchiveFileReader, self).__init__(path, 'rb')
        if self.fd.tell() != 0:
            raise GiraffeError("Cannot read columns, file descriptor must be at 0.")
        self.fd.read(len(GIRAFFE_MAGIC))
        self.columns = Columns.deserialize(self.readline())

    @property
    def header(self):
        return self.columns.names

    def readline(self):
        data = self.fd.read(2)
        if not data:
            return
        row_length = struct.unpack("H", data)[0]
        return self.fd.read(row_length)


class Reader(object):
    def __new__(cls, path, **kwargs):
        data = FileReader.read_header(path)
        if data[0:3] == GIRAFFE_MAGIC:
            return ArchiveFileReader(path)
        with open(path) as f:
            data = f.readline().strip()
        try:
            obj = json.loads(data)
            if isinstance(obj, (list, dict)):
                return JSONReader(path)
        except ValueError as error:
            log.debug("Debug[2]", error)
        return CSVReader(path, **kwargs)


class Writer(object):
    def __init__(self, path=None, mode='wt', use_gzip=False):
        self.mode = mode
        if path is None:
            self.name = "stdout"
            if 'b' in mode:
                self.fd = os.fdopen(sys.stdout.fileno(), 'wb')
            else:
                self.fd = sys.stdout
        else:
            self.name = os.path.basename(path)
            abspath = os.path.abspath(path)
            if not os.path.exists(os.path.dirname(abspath)):
                os.makedirs(os.path.dirname(abspath))
            if use_gzip:
                _open = lambda p, m: io.BufferedWriter(gzip.open(p, m))
            else:
                _open = lambda p, m: io.open(p, m)
            self.fd = _open(abspath, mode)

    @property
    def is_stdout(self):
        return self.fd.fileno() == 1

    def write(self, buf):
        try:
            self.fd.write(buf)
        except UnicodeEncodeError as error:
            self.fd.write(buf.encode('UTF-8'))

    def writen(self, buf):
        self.write(buf)
        self.write(u"\n")

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if not self.fd.closed and not self.is_stdout:
            log.debug("Debug[2]", "Closing file descriptor '{}' ...".format(self.fd))
            self.fd.close()
            log.debug("Debug[2]", "File descriptor '{}' closed.".format(self.fd))
