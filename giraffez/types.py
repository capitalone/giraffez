# -*- coding: utf-8 -*-

import datetime
import math
import struct

from .constants import *
from .errors import *
from .fmt import *
from .logging import *

from ._compat import *


__all__ = ['Column', 'Columns', 'GiraffeBytes', 'GiraffeDate', 'GiraffeTime', 'GiraffeTimestamp',
    'Result', 'Results', 'Row']


class Column(object):
    """
    An object containing the column information used by Teradata for
    encoding, decoding, and identifying data.

    Implements the `__eq__` magic method for comparison with other instances
    (validating that each field holds the same data), and the `__str__` and
    `__repr__` magic methods for coersion to string values and more pleasant
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
        self.original_name = str(ensure_str(column.get("name"))).strip('"').lower()
        self.alias = str(column.get("alias", "")).strip('"').lower()
        self.title = str(column.get("title", "")).strip('"').lower()
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
        Retrieve a column from the list with name value `column_name`

        :param str column_name: The name of the column to get
        :return: :class:`~giraffez.types.Column` with the specified name, or `None` if it does not exist.
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

        :param list names: A list of names to be used, or `None` for all
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
        

class GiraffeBytes(object):
    def __init__(self, columns):
        self.raw_array = bytearray([0] * columns.header_length)

    @classmethod
    def read(cls, f):
        for b in iterbytes(f):
            for i in reversed(xrange(8)):
                yield (b >> i) & 1

    def set_bit(self, offset):
        byte, bit = divmod(offset, 8)
        self.raw_array[byte] |= 128 >> bit

    def __add__(self, other):
        return b2s(self) + other

    def __bytes__(self):
        return bytes(self.raw_array)

    def __str__(self):
        return str(self.raw_array)


class GiraffeDate(datetime.datetime):
    """
    Ensures that datetime objects can be proper coerced into a
    string value of a given format.

    There is a Python bug (yes, it is a bug, not a feature) where
    datetime objects representing any dates before 1900 raises an
    error when calling the strftime method. This will ensure that the
    date is coerced safely with str() into something Teradata can use.
    """

    def __init__(self, *args, **kwargs):
        super(GiraffeDate, self).__init__()

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
            return cls.strptime(str(d + 19000000), "%Y%m%d")
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

    def to_json(self):
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


class GiraffeTime(datetime.time):
    """
    Represents Teradata date/time data types such as TIME(0). Currently,
    does not keep microsecond for other types like TIME(n).
    """
    def __new__(cls, t):
        return datetime.time.__new__(cls, t.hour, t.minute, t.second, t.microsecond)

    @classmethod
    def from_string(cls, s):
        fmt = TIME_FORMATS.get(len(s.strip()))
        if fmt is not None:
            try:
                ts = datetime.datetime.strptime(str(s), fmt)
                return cls(datetime.time(ts.hour, ts.minute, ts.second, ts.microsecond))
            except ValueError as error:
                log.debug("GiraffeTime: ", error)
        return None


class GiraffeTimestamp(GiraffeDate):
    """
    Represents Teradata date/time data types such as TIMESTAMP(n).
    """

    def __init__(self, *args, **kwargs):
        super(GiraffeTimestamp, self).__init__(*args, **kwargs)
        self._original_length = None

    @classmethod
    def from_string(cls, s):
        format = TIME_FORMATS.get(len(s.strip()))
        if format is not None:
            try:
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


class Result(object):
    """
    Contains the result of a Teradata query

    Defines the `__len__` magic method to get the number of rows returned:

    .. code-block:: python

       result = cli.execute("select * from tablename;")
       num_rows = len(result)

    Defines the `__iter__` magic method to iterate through the returned rows:

    .. code-block:: python

       for row in result:
           print row

    Defines the `__getattr__` magic method to return a list of values for a given column:

    .. code-block:: python

       print results.first_name # ['alice', 'bob', 'charlie', ...]

    Defines the `__getitem__` magic method to return a list of values for a given column, or numeric index:

    .. code-block:: python

       print results.columns.names # first_name, last_name, id
       print results['first_name'] # ['alice', 'bob', 'charlie', ...]
       print results['id'] # ['abc123', 'bcd234', 'cde345', ...]
       print results[2] # ['abc123', 'bcd234', 'cde345', ...]
    """

    def __init__(self, result):
        self.result = result

    @property
    def columns(self):
        """
        The column data for the corresponding rows

        :rtype: :class:`~giraffez.types.Columns` 
        """
        return self.result["columns"]

    def first(self):
        """
        Return the first row (or `None`) if no rows in result

        :return: The first row in the results
        :rtype: :class:`~giraffez.types.Row`
        """
        try:
            return self.result["rows"][0]
        except IndexError:
            return None

    @property
    def rows(self):
        """
        The returned row data

        :return: a list of rows
            (ordered corresponding to :meth:`~giraffez.types.Result.columns`)
        :rtype: list of :class:`~giraffez.types.Row`
        """
        return self.result["rows"]

    def to_json(self):
        return [row.to_json() for row in self.rows]

    def __getattr__(self, name):
        try:
            index = self.columns._column_map[name]
            return [row.__getitem__(index) for row in self.rows]
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                self.__class__.__name__, name))

    def __getitem__(self, key):
        if isinstance(key, basestring):
            try:
                index = self.columns._column_map[key]
                return [row.__getitem__(index) for row in self.rows]
            except KeyError:
                raise AttributeError("'{}' object has no attribute '{}'".format(
                    self.__class__.__name__, key))
        else:
            return self.rows.__getitem__(key)

    def __iter__(self):
        for row in self.result["rows"]:
            yield row

    def __len__(self):
        return len(self.result["rows"])

    def __repr__(self):
        return "\n".join([repr(x) for x in self.rows])

    def __str__(self):
        return self.__repr__()


class Results(object):
    """
    A set of :class:`~giraffez.types.Result` objects returned by queries

    Defines the `__iter__` magic method for convenience:

    .. code-block:: python

       results = cli.execute_many('''select * from table_a;
           select * from table_b;''')
       for result in results:
           print result.rows

    Defines the `__getitem__` magic method to allow numeric indexing:

    .. code-block:: python

       results = cli.execute_many('''select * from table_a;
           select * from table_b;''')
       print results[1] # results from 'select * from table_b'
    """

    def __init__(self, results=[]):
        self.results = []
        for result in results:
            self.append(result)

    def append(self, item):
        if isinstance(item, Result):
            self.results.append(item)
        elif isinstance(item, dict):
            self.results.append(Result(item))
        else:
            raise GiraffeError(("Object of type '{}' is not a valid argument for "
                "`Results.append`.").format(type(item)))

    def one(self):
        """
        Return the first result, or none if there are no result sets

        :return: The first result in the set
        :rtype: :class:`~giraffez.types.Result`
        """
        try:
            return self.results[0]
        except IndexError:
            return None

    def __getitem__(self, key):
        return self.results.__getitem__(key)

    def __iter__(self):
        for result in self.results:
            yield result

    def __repr__(self):
        return "\n".join([str(x) for x in self.results])

    def __str__(self):
        return self.__repr__()


class Row(object):
    """
    A wrapper for a single row within a :class:`~giraffez.types.Result` object.

    Defines the `__iter__` magic method for convenience:

    .. code-block:: python

       row = results.first()
       for item in row:
           print item

    Defines the `__getattr__` magic method to return a particular field:

    .. code-block:: python

       row = results.first()
       print row.first_name # 'alice'

       for row in results:
           print row.first_name # 'alice', 'bob', 'charlie', ...

    Defines the `__getitem__` magic method to return a particular field by name or numeric index:

    .. code-block:: python

       row = results.first()
       print row['first_name'] # 'alice'
       print row['id'] # 'abc123'
       print row[2] # 'abc123'
    """

    def __init__(self, columns, row):
        self.columns = columns
        self.column_map = columns._column_map
        self.row = row

    def to_json(self):
        return {k.name: v for k, v in zip(self.columns, self)}

    def __getattr__(self, name):
        try:
            index = self.column_map[name]
            return self.__getitem__(index)
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                self.__class__.__name__, name))

    def __getitem__(self, key):
        if isinstance(key, basestring):
            try:
                return self.row[self.column_map[key]]
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
        return str(self.to_json())

    def __str__(self):
        return self.__repr__()
