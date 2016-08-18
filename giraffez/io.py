# -*- coding: utf-8 -*-

import os
import sys
import gzip
import struct

from .constants import *
from .errors import *
from .logging import *
from .types import *
from .utils import *

from ._compat import *


__all__ = ['file_exists', 'file_permissions', 'home_file', 'FileReader', 'ArchiveFileReader',
    'Reader', 'Writer']


def file_exists(path):
    try:
        with open(path):
            return True
    except (OSError, IOError):
        return False

def file_permissions(path):
    return os.stat(path).st_mode & 0o777

def home_file(filename):
    return os.path.join(os.path.expanduser("~"), filename)


class FileReader(object):
    def __init__(self, path, delimiter=None, encoding=None):
        self.delimiter = delimiter
        abspath = os.path.abspath(path)
        if not os.path.exists(os.path.dirname(abspath)):
            raise FileNotFound("Path '{}' does not exist.".format(os.path.dirname(abspath)))
        with open(path, "rb") as f:
            data = f.read(2)
            if data == GZIP_MAGIC:
                _open = gzip.open
            else:
                _open = open
        self.fd = _open(abspath, "r")

    @classmethod
    def check_length(cls, path, length):
        with cls(path) as f:
            for i, line in enumerate(f, 1):
                if i >= length:
                    return True
            return False

    @classmethod
    def check_delimiter(cls, path):
        with cls(path, "rb") as f:
            header = f.readline().strip()
            return infer_delimiter(header)

    def field_names(self):
        header = self.fd.readline().strip()
        if not self.delimiter:
            self.delimiter = infer_delimiter(header)
        return [n.lower().strip() for n in header.split(self.delimiter)]

    @property
    def file_type(self):
        return DELIMITED_TEXT_FILE

    def read(self, size=-1):
        return self.fd.read(size)

    def readline(self):
        return next(self.fd)

    @classmethod
    def read_all(cls, path):
        with cls(path, "rb") as f:
            return f.read()

    @classmethod
    def read_header(cls, path):
        with cls(path) as f:
            return f.read(len(GIRAFFE_MAGIC))

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


class ArchiveFileReader(FileReader):
    @property
    def file_type(self):
        return GIRAFFE_ARCHIVE_FILE

    def columns(self):
        if self.fd.tell() != 0:
            raise GiraffeError("Cannot read columns, file descriptor must be at 0.")
        self.fd.read(len(GIRAFFE_MAGIC))
        header = self.readline()
        return Columns.deserialize(header)

    def field_names(self):
        return self.columns().names

    def readline(self):
        data = self.fd.read(2)
        if not data:
            return
        row_length = struct.unpack("H", data)[0]
        return self.fd.read(row_length)


class Reader(object):
    def __new__(cls, path, **kwargs):
        data = FileReader.read_header(path)
        if data == GIRAFFE_MAGIC:
            return ArchiveFileReader(path, **kwargs)
        return FileReader(path, **kwargs)


class Writer(object):
    def __init__(self, path=None, mode='', archive=False, use_gzip=False):
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
            if archive:
                mode = 'b'
            if use_gzip:
                _open = gzip.open
            else:
                _open = open
            self.fd = _open(abspath, "w{}".format(mode))
            if archive:
                self.write(GIRAFFE_MAGIC)
                self.writen = self.write

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
        self.write("\n")

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if not self.fd.closed and not self.is_stdout:
            log.debug("Debug[2]", "Closing file descriptor '{}' ...".format(self.fd))
            self.fd.close()
            log.debug("Debug[2]", "File descriptor '{}' closed.".format(self.fd))
