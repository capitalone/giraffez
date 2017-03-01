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

from . import _encoder
from .constants import *
from .errors import *

from .fmt import escape_quotes, quote_string, replace_cr
from .logging import log
from .types import Bytes, Columns, Date, Decimal, Time, Timestamp
from .utils import pipeline

from ._compat import *



FORMAT_LENGTH = {1: "b", 2: "h", 4: "i", 8 : "q", 16: "Qq"}


class TeradataEncoder(object):
    def __init__(self, columns=[], encoding=None):
        self.encoding = ENCODER_SETTINGS_DEFAULT
        self._columns = columns
        self._delimiter = DEFAULT_DELIMITER
        #self._null = DEFAULT_NULL
        self._null = None
        self.encoder = _encoder.Encoder(columns)
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
        #self.encoder = _encoder.Encoder(self._columns, self.encoding)
        #self.encoder.set_delimiter(self._delimiter)
        #self.encoder.set_null(self._null)

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

def python_to_bytes(null):
    null = ensure_bytes(null)
    def _encoder(items):
        return [ensure_bytes(x) if x is not None else null for x in items]
    return _encoder

def python_to_dict(columns):
    def _encoder(items):
        return {x.name: y for x, y in zip(columns, items)}
    return _encoder

def python_to_strings(null):
    def _encoder(items):
        return [str(x) if x is not None else null for x in items]
    return _encoder

def strings_to_text(delimiter):
    def _encoder(items):
        return delimiter.join(items)
    return _encoder

def teradata_to_archive(data):
    return struct.pack("H", len(data)) + data

def text_to_strings(delimiter):
    if delimiter is None:
        _encoder = lambda s: [s.strip("\r\n")]
    else:
        _encoder = lambda s: s.strip("\r\n").split(delimiter)
    return _encoder

def python_to_sql(table_name, columns, date_conversion=True):
    if date_conversion:
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
            elif column.type in TIME_TYPES | TIMESTAMP_TYPES:
                value = quote_string(convert_datetime(item, column.length))
            else:
                value = str(item)
            values.append((column.name, value))
        return "ins into {} ({}) values ({});".format(table_name,
            ",".join(quote_string(x[0], '"') for x in values),
            ",".join(x[1] for x in values))
    return _encoder

def teradata_to_python(columns):
    def _encoder(data):
        items = []
        indicator, data = data[0:columns.header_length], data[columns.header_length:]
        for column, nullable in zip(columns, Bytes.read(indicator)):
            if column.type in ALL_INTEGER_TYPES:
                value, data = unpack_integer(data[0:column.length], column.length), data[column.length:]
            elif column.type in FLOAT_TYPES:
                value, data = struct.unpack("d", data[0:column.length])[0], data[column.length:]
            elif column.type in DECIMAL_TYPES:
                if column.length > 8:
                    lower, upper = struct.unpack(FORMAT_LENGTH.get(column.length), data[0:column.length])
                    value = ((upper << 64) | lower)
                else:
                    value = struct.unpack(FORMAT_LENGTH.get(column.length), data[0:column.length])[0]
                data = data[column.length:]
                if column.scale > 0:
                    (a, b) = divmod(abs(value), 10 ** column.scale)
                    if value < 0:
                        a = -a
                    value = "{}.{:0{}}".format(a, b, column.scale)
                else:
                    value = "{}".format(value)
            elif column.type in CHAR_TYPES:
                value, data = data[0:column.length], data[column.length:]
                value = value.strip().replace("\r", "\n")
            elif column.type in VAR_TYPES:
                length = unpack_integer(data[0:2], 2)
                value, data = data[2:2+length], data[2+length:]
                value = value.replace("\r", "\n")
            elif column.type in DATE_TYPES:
                n, data = unpack_integer(data[0:column.length], column.length), data[column.length:]
                value = Date.from_integer(n)
            elif column.type in TIME_TYPES:
                value, data = Time.from_string(data[0:column.length]), data[column.length:]
            elif column.type in TIMESTAMP_TYPES:
                value, data = Timestamp.from_string(data[0:column.length]), data[column.length:]
            else:
                value, data = data[0:column.length], data[column.length:]
            if nullable:
                value = None
            items.append(value)
        return items
    return _encoder

def python_to_teradata(columns, allow_precision_loss=False):
    if allow_precision_loss:
        _float_handler = FloatHandler(columns)
    else:
        _float_handler = lambda a: a
    def _encoder(items):
        check_input(columns, items)
        items = _float_handler(items)
        indicator = Bytes(columns)
        data = [indicator]
        for i, (item, column) in enumerate(zip(items, columns)):
            if item is None:
                indicator.set_bit(i)
                if column.type in STRING_NULLS:
                    value = b"\x20" * column.length
                elif column.type in VARIABLE_NULLS:
                    value = b"\x00\x00"
                else:
                    value = b"\x00" * column.length
                data.append(value)
                continue
            if column.type in ALL_INTEGER_TYPES:
                pack_char = FORMAT_LENGTH.get(column.length)
                data.append(struct.pack(pack_char, int(item)))
            elif column.type in FLOAT_TYPES:
                data.append(struct.pack("d", float(item)))
            elif column.type in DECIMAL_TYPES:
                try:
                    parts = item.split(".", 1)
                except AttributeError as error:
                    raise GiraffeEncodeError("Invalid value for Decimal data type: {}".format(item))
                if len(parts) == 1 or column.scale == 0:
                     a = parts[0]
                     b = ""
                elif len(parts) == 2:
                    a = parts[0]
                    b = parts[1].replace(".", "")
                try:
                    item = int("{}{}".format(a, b.ljust(column.scale, "0")))
                except ValueError as error:
                    raise GiraffeEncodeError("Invalid value for Decimal data type: {}".format(item))
                pack_char = FORMAT_LENGTH.get(column.length)
                if column.length > 8:
                    upper = item >> 64
                    lower = (upper << 64) ^ item
                    value = struct.pack(pack_char, lower, upper)
                else:
                    value = struct.pack(pack_char, item)
                data.append(value)
            elif column.type in CHAR_TYPES:
                item = ensure_bytes(item[:column.length])
                value = item.ljust(column.length, b"\x20")
                data.append(value)
            elif column.type in VAR_TYPES:
                data.append(struct.pack("H", len(item)) + ensure_bytes(item))
            elif column.type in DATE_TYPES:
                date_value = Date.from_string(item)
                if date_value:
                    value = struct.pack("i", int(date_value))
                else:
                    value = None
                if value is None:
                    indicator.set_bit(i)
                    value = b"\x20" * column.length
                data.append(value)
            else:
                data.append(item)
        return b"".join(ensure_bytes(x) for x in data)
    return _encoder

def unpack(fmt, data):
    """
    Unpack binary data based on struct format string. Simulates
    reading from a file-like object by slicing the data string that is
    returned.

    :return: At tuple of (unpacked value, remaining data)
    :rtype: tuple
    """
    length = struct.calcsize(fmt)
    value = struct.unpack(fmt, data[:length])[0]
    data = data[length:]
    return value, data

def unpack_from(fmt, data):
    """
    Unpack binary data based on struct format string. Simulates
    reading from a file-like object by slicing the data string that is
    returned. Differs from :func:`unpack` because it assumes a packed
    value represents the length of a variable-length value that
    follows.

    :return: At tuple of (unpacked value, remaining data)
    :rtype: tuple
    """
    length = unpack(fmt, data)[0]
    data = data[struct.calcsize(fmt):]
    value = data[:length]
    data = data[length:]
    return value, data

def unpack_integer(s, length):
    return struct.unpack(FORMAT_LENGTH.get(length), s)[0]
