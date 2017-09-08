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

import datetime
import math
import struct

from .constants import *
from .errors import *

from .fmt import safe_name
from .logging import log

from ._compat import *


__all__ = ['Column', 'Columns', 'Date', 'Decimal', 'Time', 'Timestamp', 'Row']


DATE_FORMATS = [
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y%m%d",
]

TIME_FORMATS = {
    8: "%H:%M:%S",
    15: "%H:%M:%S.%f",
    19: "%Y-%m-%d %H:%M:%S",
    22: "%Y-%m-%d %H:%M:%S.%f",
    26: "%Y-%m-%d %H:%M:%S.%f",
    32: "%Y-%m-%d %H:%M:%S.%f"
}


class Column(object):
    """
    An object containing the column information used by Teradata for
    encoding, decoding, and identifying data.

    Implements the :code:`__eq__` magic method for comparison with other instances
    (validating that each field holds the same data), and the :code:`__str__` and
    :code:`__repr__` magic methods for coersion to string values and more pleasant
    visual presentation.

    **Fields:**

    name
        The original name of the column in the table it belongs to
    alias
        The alias of the column in the query
    title
        The title of the column in the query
    type
        Numeric value used by Teradata to indicate column type
    length
        Length in bytes that the field's values occupy
    precision
        Precision of floating-point numeric columns
    scale
        Scale of floating-point numeric columns
    filler
        Indicator that the column belongs to the target table, but
        will not be used in the current operation
    """

    def __init__(self, column, filler=False):
        if isinstance(column, tuple):
            if len(column) in {5, 7}:
                column = {"name": column[0], "type": column[1], "length": column[2], "precision": column[3], "scale": column[4]}
            elif len(column) == 8:
                column = {"name": column[0], "type": column[1], "length": column[2], "precision": column[3], "scale": column[4], "nullable": column[5], "default": column[6], "format": column[7]}
            else:
                raise GiraffeTypeError("Column information is not valid")
        self.original_name = ensure_str(column.get("name")).strip('"').lower()
        self.alias = (column.get("alias", "")).strip('"').lower()
        self.title = (column.get("title", "")).strip('"').lower()
        self.type = column.get("type")
        if self.type in cmd_type_map:
            self.tpt_type = self.type
            self.type = cmd_type_map.get(self.type)
        else:
            self.tpt_type = tpt_type_map.get(self.type)
        self.gd_type = gd_type_map.get(self.type, None)
        if self.type is None or self.gd_type is None:
            raise GiraffeTypeError("Cannot determine data type")
        self.length = column.get("length")
        self.precision = column.get("precision")
        self.scale = column.get("scale")
        self._nullable = column.get("nullable", None)
        self._default = column.get("default", None)
        self._format = column.get("format", None)
        self.filler = filler
        self.order = False

    @property
    def name(self):
        if self.title:
            return self.title
        return self.original_name

    @property
    def as_tuple(self):
        return (self.safe_name, self.tpt_type, self.length,
                self.precision, self.scale)

    @property
    def default(self):
        if self._default is None:
            raise GiraffeError("Default information not available")
        return self._default

    @property
    def format(self):
        if self._format is None:
            raise GiraffeError("Default information not available")
        return self._format

    @property
    def nullable(self):
        if self._nullable is None:
            raise GiraffeError("Nullable information not available")
        return self._nullable

    @property
    def safe_name(self):
        return safe_name(self.name)

    def __eq__(self, other):
        return str(self) == str(other)

    def __repr__(self):
        type_name = COL_TYPE_NAMES.get(self.type, "unknown")
        type_name = type_name.replace('_NN', '').replace('_N' , '').lower()
        if self.type in DECIMAL_TYPES :
            type_name = "{}[{}]({},{})".format(type_name, self.length, self.precision, self.scale)
        else:
            type_name = "{}({})".format(type_name, self.length)
        options = []
        if self._default:
            options.append(self._default)
        if self._nullable:
            options.append(self._nullable)
        if self._format:
            options.append(self._format)
        if options:
            type_name = "{} {}".format(type_name, " ".join(options))
        if self.original_name:
            name = self.original_name
        elif self.alias:
            name = self.alias
        elif self.title:
            name = self.title
        else:
            name = "n/a"
        return "Column({} {})".format(safe_name(name), type_name)

    def __str__(self):
        return self.__repr__()


class Columns(object):
    """
    A set of :class:`~giraffez.types.Column` objects, used to convey
    original table schema without losing information.
    """

    __slots__ = ('columns', '_column_map', '_filtered_columns')

    def __init__(self, items=[]):
        self.columns = []
        for item in items:
            self.columns.append(Column(item))
        self._filtered_columns = []
        self._column_map = {}
        for index, column in enumerate(self.columns):
            self._column_map[column.safe_name] = index
            if column.alias:
                self._column_map[safe_name(column.alias)] = index
            if column.title:
                self._column_map[safe_name(column.title)] = index

    def append(self, item):
        if isinstance(item, Column):
            self.columns.append(item)
        else:
            self.columns.append(Column(item))

    def get(self, column_name):
        """
        Retrieve a column from the list with name value :code:`column_name`

        :param str column_name: The name of the column to get
        :return: :class:`~giraffez.types.Column` with the specified name, or :code:`None` if it does not exist.
        """
        column_name = column_name.lower()
        for c in self.columns:
            if c.name == column_name:
                return c
        return None

    @property
    def header_length(self):
        return int(math.ceil(len(self)/8.0))

    @property
    def names(self):
        """
        :return: The names of the contained :class:`~giraffez.types.Column` objects
        :rtype: list
        """
        return [c.name for c in self]

    @property
    def safe_names(self):
        return [c.safe_name for c in self]

    def set_filter(self, names=None):
        """
        Set the names of columns to be used when iterating through the list,
        retrieving names, etc.

        :param list names: A list of names to be used, or :code:`None` for all
        """
        _names = []
        if names:
            for name in names:
                _safe_name = safe_name(name)
                if _safe_name not in self._column_map:
                    raise GiraffeTypeError("Column '{}' does not exist".format(name))
                if _safe_name in _names:
                    continue
                _names.append(_safe_name)
        self._filtered_columns = _names

    def tuples(self):
        return [c.as_tuple for c in self]

    def __eq__(self, other):
        return all([a == b for a, b in zip(self, other)])

    def __getitem__(self, index):
        if self._filtered_columns:
            return self.get(self._filtered_columns[index])
        return self.columns[index]

    def __iter__(self):
        if self._filtered_columns:
            for c in self._filtered_columns:
                column = self.get(c)
                if column is None:
                    raise GiraffeTypeError("Column '{}' does not exist".format(c))
                yield column
        else:
            for c in self.columns:
                yield c

    def __len__(self):
        return len(self.names)

    def __str__(self):
        return ",".join([c.name for c in self.columns])

    def __repr__(self):
        return "\n".join([repr(c) for c in self.columns])

    def serialize(self):
        """
        Serializes the columns into the giraffez archive header
        binary format::

            0      1      2
            +------+------+------+------+------+------+------+------+
            | Header      | Header Data                             |
            | Length      |                                         |
            +------+------+------+------+------+------+------+------+

                       giraffez Archive Header Format

                                   Fig. 1

            Header Length: 2 bytes
                Full length of archive header 

            Header Data: variable
                Binary data representing N column(s) using the format
                specified in Fig. 2


            0      1      2      3      4      5      6      7      8
            +------+------+------+------+------+------+------+------+
            | Type        | Length      | Precision   | Scale       | 
            +------+------+------+------+------+------+------+------+
            | Name Length | Name                                    |
            +------+------+------+------+------+------+------+------+

                           Binary Column Format

                                   Fig. 2

            Type: 2 bytes
                Numerical representation of column type defined by
                Teradata's CLIv2

            Length: 2 bytes
                Column length

            Scale: 2 bytes
                Column scale

            Precision: 2 bytes
                Column precision

            Name Length: 2 bytes
                Length used for reading variable column name

            Name: variable
                Name of column

        :return: Packed binary data, representing the serialized :class:`~giraffez.types.Columns`
        :rtype: str
        """
        data = b""
        for column in self:
            row = struct.pack("5H", column.type, column.length, column.precision, column.scale,
                len(column.name))
            row += ensure_bytes(column.name)
            data += row
        return struct.pack("H", len(data)) + data

    @classmethod
    def deserialize(cls, data):
        """
        Deserializes giraffez Archive header. See
        :meth:`~giraffez.types.Columns.serialize` for more information.

        :param str data: data in giraffez Archive format, to be deserialized
        :return: :class:`~giraffez.types.Columns` object decoded from data
        """
        column_list = cls()
        while data:
            tup, data = data[:10], data[10:]
            column_type, length, prec, scale, title_len = struct.unpack("5H", tup)
            title, data = data[:title_len], data[title_len:]
            try:
                column_list.append((title, column_type, length, prec, scale))
            except GiraffeTypeError as error:
                raise GiraffeEncodeError(error)
        return column_list


class Date(datetime.datetime):
    """
    Ensures that datetime objects can be proper coerced into a
    string value of a given format.

    There is a Python bug (yes, it is a bug, not a feature) where
    datetime objects representing any dates before 1900 raises an
    error when calling the strftime method. This will ensure that the
    date is coerced safely with str() into something Teradata can use.
    """

    def __init__(self, *args, **kwargs):
        super(Date, self).__init__()

    @classmethod
    def from_datetime(cls, dt):
        if isinstance(dt, datetime.datetime):
            return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
        else:
            return cls(dt.year, dt.month, dt.day, 0, 0, 0)

    @classmethod
    def from_integer(cls, d):
        try:
            if not d:
                return None
            d += 19000000
            year = d // 10000;
            month = (d % 10000) // 100
            day = d % 100
            return cls(year=year, month=month, day=day)
        except ValueError as error:
            log.debug(error)
            return None

    @classmethod
    def from_string(cls, s):
        for fmt in DATE_FORMATS:
            try:
                return cls.strptime(str(s), fmt)
            except ValueError as error:
                pass
        return None

    def to_integer(self):
        try:
            date_value = "{0:04}{1:02}{2:02}".format(self.year, self.month, self.day)
            return int(date_value) - 19000000
        except ValueError as error:
            return None

    def __json__(self):
        return unicode(self.to_string())

    def to_string(self):
        return "{0:04}-{1:02}-{2:02}".format(self.year, self.month, self.day)

    def __bytes__(self):
        return bytes(self.to_string(), "UTF-8")

    def __int__(self):
        return self.to_integer()

    def __str__(self):
        return self.to_string()

    def __unicode__(self):
        return unicode(self.to_string())


class Decimal(decimal.Decimal):
    def __json__(self):
        return self.__str__()


class Row(object):
    """
    A wrapper for a single row object.

    Defines the :code:`__iter__` magic method for convenience:

    .. code-block:: python

       for item in row:
           print(item)

    Defines the :code:`__getattr__` magic method to return a particular field:

    .. code-block:: python

       print(row.first_name) # 'alice'

    Defines the :code:`__getitem__` magic method to return a particular field by name or numeric index:

    .. code-block:: python

       print(row['first_name']) # 'alice'
       print(row['id']) # 'abc123'
       print(row[2]) # 'abc123'
    """

    __slots__ = ('columns', 'row')

    def __init__(self, columns, row):
        self.columns = columns
        self.row = row

    def items(self):
        """
        Represents the contents of the row as a :code:`dict` with the column
        names as keys, and the row's fields as values.

        :rtype: dict
        """
        return {k.name: v for k, v in zip(self.columns, self)}

    def __getattr__(self, name):
        try:
            index = self.columns._column_map[name]
            return self.__getitem__(index)
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                self.__class__.__name__, name))

    def __getitem__(self, key):
        if isinstance(key, basestring):
            try:
                return self.row[self.columns._column_map[key]]
            except KeyError:
                raise AttributeError("Row has no column '{}'".format(key))
        else:
            return self.row.__getitem__(key)

    def __iter__(self):
        for item in self.row:
            yield item

    def __len__(self):
        return len(self.row)

    def __repr__(self):
        return "Row({})".format(self.items())

    def __json__(self):
        return self.items()

    def __str__(self):
        return str(self.items())


class Time(datetime.time):
    """
    Represents Teradata time data types such as TIME(n).
    """

    @classmethod
    def from_string(cls, s):
        fmt = TIME_FORMATS.get(len(s.strip()))
        if fmt is not None:
            try:
                ts = datetime.datetime.strptime(str(s), fmt)
                return cls(ts.hour, ts.minute, ts.second, ts.microsecond)
            except ValueError as error:
                log.debug("GiraffeTime: ", error)
        return None

    def to_string(self, length=None):
        hms = "{0:02}:{1:02}:{2:02}".format(self.hour, self.minute, self.second)
        if length is not None and length > 8:
            ms = "{:06}".format(self.microsecond)[:length - 9]
            return "{0}.{1}".format(hms, ms)
        return hms


class Timestamp(Date):
    """
    Represents Teradata date/time data types such as TIMESTAMP(n).
    """

    def __init__(self, *args, **kwargs):
        super(Timestamp, self).__init__(*args, **kwargs)
        self._original_length = None

    @classmethod
    def from_string(cls, s):
        s = s.strip()
        format = TIME_FORMATS.get(len(s))
        if format is not None:
            try:
                if len(s) > 26:
                    s = s[:26]
                ts = cls.strptime(str(s), format)
                ts._original_length = len(s)
                return ts
            except ValueError as error:
                log.debug("GiraffeTimestamp: ", error)
        return None

    def to_string(self, length=None):
        if length is None:
            length = self._original_length
        if length is not None and length > 20:
            value = "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06}".format(self.year, self.month, self.day,
                    self.hour, self.minute, self.second, self.microsecond)
            value = value[:length]
            return value
        return "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}".format(self.year, self.month, self.day,
                    self.hour, self.minute, self.second)
