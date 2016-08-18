# -*- coding: utf-8 -*-

import time
import datetime
import struct
try:
    import ujson as json
except ImportError:
    import json

from .constants import *
from .errors import *
from .fmt import *
from .logging import *
from .utils import *
from .types import *

from ._compat import *


__all__ = ['check_input', 'date_handler', 'char_handler', 'null_handler', 'dict_to_json',
    'python_to_bytes', 'python_to_dict', 'python_to_strings', 'strings_to_text',
    'teradata_to_archive', 'text_to_strings', 'teradata_to_python', 'python_to_sql',
    'python_to_teradata', 'unpack', 'unpack_from', 'unpack_integer', 'unpack_stmt_info']


def check_input(columns, items):
    if len(items) != len(columns):
        error_message = "Columns list contains {} columns and {} items provided.".format(len(columns), len(items))
        raise GiraffeEncodeError("{}: \n\t{!r}".format(error_message, items))

def char_handler(columns):
    _columns = []
    for index, column in enumerate(columns):
        if column.type in CHAR_TYPES | VAR_TYPES:
            _columns.append((replace_cr, index))
    def _char_handler(data):
        for fn, index in _columns:
            data[index] = fn(data[index])
        return data
    return _char_handler

def date_handler(columns):
    _columns = []
    for index, column in enumerate(columns):
        if column.type in DATE_TYPES:
            _columns.append((GiraffeDate.from_integer, index))
        elif column.type in TIME_TYPES:
            _columns.append((GiraffeTime.from_string, index))
        elif column.type in TIMESTAMP_TYPES:
            _columns.append((GiraffeTimestamp.from_string, index))
    def _date_handler(data):
        for fn, index in _columns:
            if data[index] is not None:
                data[index] = fn(data[index])
        return data
    return _date_handler

def float_handler(columns):
    _columns = []
    for index, column in enumerate(columns):
        if column.type in DECIMAL_TYPES:
            _columns.append((pipeline([float, str]), index))
    def _float_handler(data):
        for fn, index in _columns:
            if data[index] is not None:
                data[index] = fn(data[index])
        return data
    return _float_handler

def null_handler(null=None):
    nulls = {"", None, null}
    def _null_handler(items):
        return [item if item not in nulls else None for item in items]
    return _null_handler

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

def teradata_to_python(columns):
    def _encoder(data):
        items = []
        indicator, data = data[0:columns.header_length], data[columns.header_length:]
        for column, nullable in zip(columns, GiraffeBytes.read(indicator)):
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
                value = GiraffeDate.from_integer(n)
            elif column.type in TIME_TYPES:
                value, data = GiraffeTime.from_string(data[0:column.length]), data[column.length:]
            elif column.type in TIMESTAMP_TYPES:
                value, data = GiraffeTimestamp.from_string(data[0:column.length]), data[column.length:]
            else:
                value, data = data[0:column.length], data[column.length:]
            if nullable:
                value = None
            items.append(value)
        return items
    return _encoder

def python_to_sql(table_name, columns, date_conversion=True):
    if date_conversion:
        def convert_date(s):
            value = GiraffeDate.from_string(s)
            if value is None:
                raise GiraffeEncodeError("Cannot convert date '{}'".format(s))
            return value.to_string()

        def convert_datetime(s, length):
            if isinstance(s, datetime.date):
                value = GiraffeTimestamp.from_datetime(s)
            else:
                value = GiraffeTimestamp.from_string(s)
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
                    value = str(GiraffeDate.from_datetime(item).to_integer())
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

def python_to_teradata(columns, allow_precision_loss=False):
    if allow_precision_loss:
        _float_handler = float_handler(columns)
    else:
        _float_handler = lambda a: a
    def _encoder(items):
        check_input(columns, items)
        items = _float_handler(items)
        indicator = GiraffeBytes(columns)
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
                date_value = GiraffeDate.from_string(item)
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

def unpack_stmt_info(data):
    columns = []
    while len(data) > 0:
        ext_layout, ext_type, ext_len = struct.unpack("hhh", data[:6])
        data = data[6:]
        ext_data = data[:ext_len]
        data = data[ext_len:]
        if ext_layout != 1:
            continue
        db, ext_data = unpack_from("h", ext_data)
        table, ext_data = unpack_from("h", ext_data)
        column_name, ext_data = unpack_from("h", ext_data)
        some_thang, ext_data = unpack("h", ext_data)
        useless, ext_data = unpack_from("h", ext_data)
        column_title, ext_data = unpack_from("h", ext_data)
        column_format, ext_data = unpack_from("h", ext_data)
        default_value, ext_data = unpack_from("h", ext_data)
        identity_column, ext_data = unpack("s", ext_data)
        mosdef_writable, ext_data = unpack("s", ext_data)
        not_not_null, ext_data = unpack("s", ext_data)
        can_ret_null, ext_data = unpack("s", ext_data)
        permitted_where, ext_data = unpack("s", ext_data)
        modifiable_column, ext_data = unpack("s", ext_data)
        column_type, ext_data = unpack("h", ext_data)
        user_def_type, ext_data = unpack("h", ext_data)
        type_name, ext_data = unpack_from("h", ext_data)
        data_type_misc_info, ext_data = unpack_from("h", ext_data)
        column_length, ext_data = unpack("Q", ext_data) #total_bytes_might_ret
        precision, ext_data = unpack("h", ext_data) #total_digits
        interval_digits, ext_data = unpack("h", ext_data) # if termporal data type
        scale, ext_data = unpack("h", ext_data) #fractional_digits
        char_set_type, ext_data = unpack("b", ext_data)
        total_bytes_ret, ext_data = unpack("Q", ext_data)
        case_sensitive, ext_data = unpack("s", ext_data)
        numeric_signed, ext_data = unpack("s", ext_data)
        uniquely_desc_row, ext_data = unpack("s", ext_data)
        column_only_member_uniq_index, ext_data = unpack("s", ext_data)
        is_expression, ext_data = unpack("s", ext_data)
        permitted_orderby, ext_data = unpack("s", ext_data)
        if column_type in [DECIMAL_N, DECIMAL_NN]:
            if precision <= 2:
                column_length = 1
            elif precision <= 4:
                column_length = 2
            elif precision <= 9:
                column_length = 4
            elif precision <= 18:
                column_length = 8
            else:
                column_length = 16
        else:
            precision = 0
            scale = 0
        if column_name == "":
            column_name = column_title
        columns.append({
            "name": column_name,
            "type": column_type,
            "length": column_length,
            "precision": precision,
            "scale": scale,
            "nullable": can_ret_null,
            "default": default_value,
            "format": column_format
        })
    return columns
