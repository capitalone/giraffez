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

import time
import datetime
import struct
try:
    import ujson as json
except ImportError:
    import json

from ._teradata import Encoder
from .constants import *
from .errors import *

from .fmt import escape_quotes, quote_string, replace_cr
from .logging import log
from .types import Columns, Date, Decimal, Time, Timestamp
from .utils import pipeline

from ._compat import *



class TeradataEncoder(object):
    """
    The class wrapping the Teradata C encoder.

    Exposed under the alias :class:`giraffez.Encoder`.

    :param list or :class:`giraffez.Columns` columns: Columns used for encoding.
    :param int encoding: Constant value representing the settings used by underlying C encoder.
    """
    def __init__(self, columns=[], encoding=None):
        self.encoding = ENCODER_SETTINGS_DEFAULT
        self._columns = columns
        self._delimiter = '|'
        self._null = None
        self.encoder = Encoder(columns)
        if encoding is not None:
            self |= encoding

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, value):
        if not isinstance(value, Columns):
            value = Columns(value)
        self._columns = value
        self.encoder.set_columns(self._columns)

    @classmethod
    def count(self, data):
        return Encoder.count_rows(data)

    @property
    def delimiter(self):
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        self._delimiter = value
        self.encoder.set_delimiter(self._delimtier)

    @property
    def null(self):
        return self._null

    @null.setter
    def null(self, value):
        self._null = value
        self.encoder.set_null(self._null)

    def parse_header(self, data):
        return self.encoder.unpack_stmt_info(data)

    def read(self, data):
        return self.encoder.unpack_row(data)

    def readbuffer(self, data):
        return self.encoder.unpack_rows(data)

    def serialize(self, data):
        return self.encoder.pack_row(data)

    def setdefaults(self):
        self.encoding = ENCODER_SETTINGS_DEFAULT
        self.encoder.set_encoding(self.encoding)

    def __or__(self, other):
        if other & ROW_RETURN_MASK:
            self.encoding = self.encoding & ~ROW_RETURN_MASK | other
        if other & DATETIME_RETURN_MASK:
            self.encoding = self.encoding & ~DATETIME_RETURN_MASK | other
        if other & DECIMAL_RETURN_MASK:
            self.encoding = self.encoding & ~DECIMAL_RETURN_MASK | other
        self.encoder.set_encoding(self.encoding)
        self.encoder.set_delimiter(self._delimiter)
        self.encoder.set_null(self._null)
        return self

    def __str__(self):
        return "{}|{}|{}".format(
            encoder_settings_map.get(self.encoding & ROW_RETURN_MASK, "Unknown"),
            encoder_settings_map.get(self.encoding & DATETIME_RETURN_MASK, "Unknown"),
            encoder_settings_map.get(self.encoding & DECIMAL_RETURN_MASK, "Unknown"),
        )


class Handler(object):
    """
    Base class for creating data handlers.  This class can be subclassed
    to define a list of functions that run on data that matches the
    Teradata data types provided.  This is useful when dealing with data
    that may not be in the appropriate Python type to be encoded.
    """
    handlers = []

    def __init__(self, columns):
        if not isinstance(columns, Columns):
            raise GiraffeEncodeError("Type '{}' must be type giraffez.types.Columns".format(type(columns)))
        self.columns = columns
        self._columns = []
        for index, column in enumerate(self.columns):
            for cond, fn in self.handlers:
                if column.type in cond:
                    self._columns.append((fn, index))

    def __call__(self, data):
        for fn, index in self._columns:
            if data[index] is not None:
                data[index] = fn(data[index])
        return data


class CharHandler(Handler):
    handlers = [
        (CHAR_TYPES | VAR_TYPES, replace_cr),
    ]


class DateHandler(Handler):
    handlers = [
        (DATE_TYPES, Date.from_integer),
        (TIME_TYPES, Time.from_string),
        (TIMESTAMP_TYPES, Timestamp.from_string)
    ]


class DecimalHandler(Handler):
    handlers = [
        (DECIMAL_TYPES, Decimal),
    ]


class FloatHandler(Handler):
    handlers = [
        (DECIMAL_TYPES, pipeline([float, str])),
    ]


def check_input(columns, items):
    if len(items) != len(columns):
        error_message = "Columns list contains {} columns and {} items provided.".format(len(columns), len(items))
        raise GiraffeEncodeError("{}: \n\t{!r}".format(error_message, items))

def null_handler(null=None):
    nulls = {"", None, null}
    def _handler(items):
        return [item if item not in nulls else None for item in items]
    return _handler

def dict_to_json(record):
    try:
        return json.dumps(record, ensure_ascii=False)
    except UnicodeDecodeError as error:
        log.debug("Debug[2]", "UnicodeDecodeError: {}".format(error.message))

def python_to_sql(table_name, columns, date_conversion=True):
    if date_conversion:
        def convert_time(s):
            value = Time.from_string(s)
            if value is None:
                raise GiraffeEncodeError("Cannot convert time '{}'".format(s))
            return value.to_string()

        def convert_date(s):
            value = Date.from_string(s)
            if value is None:
                raise GiraffeEncodeError("Cannot convert date '{}'".format(s))
            return value.to_string()

        def convert_datetime(s, length):
            if isinstance(s, datetime.date):
                value = Timestamp.from_datetime(s)
            else:
                value = Timestamp.from_string(s)
            if value is None:
                raise GiraffeEncodeError("Cannot convert datetime '{}'".format(s))
            return value.to_string(length)
    else:
        convert_time = lambda a: a
        convert_date = lambda a: a
        convert_datetime = lambda a, b: a
    def _encoder(items):
        check_input(columns, items)
        values = []
        for item, column in zip(items, columns):
            if item is None:
                continue
            if column.type in CHAR_TYPES | VAR_TYPES:
                value = quote_string(escape_quotes(item))
            elif column.type in DATE_TYPES:
                if isinstance(item, datetime.date):
                    value = str(Date.from_datetime(item).to_integer())
                else:
                    value = quote_string(convert_date(escape_quotes(item)))
            elif column.type in TIME_TYPES:
                value = quote_string(convert_time(item))
            elif column.type in TIMESTAMP_TYPES:
                value = quote_string(convert_datetime(item, column.length))
            else:
                value = str(item)
            values.append((column.name, value))
        return "ins into {} ({}) values ({});".format(table_name,
            ",".join(quote_string(x[0], '"') for x in values),
            ",".join(x[1] for x in values))
    return _encoder
