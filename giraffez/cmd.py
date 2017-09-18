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

import threading

from collections import defaultdict

from .constants import *
from .errors import *

from ._teradata import Cmd as _Cmd, RequestEnded, StatementEnded, StatementInfoEnded, TeradataError

from .connection import Connection, Context
from .encoders import check_input, null_handler, python_to_sql
from .fmt import format_indent, truncate
from .io import CSVReader, JSONReader, Reader, isfile
from .logging import log
from .sql import parse_statement, prepare_statement, Statement
from .types import Columns, Row
from .utils import pipeline, suppress_context

from ._compat import *


__all__ = ['Cmd', 'Cursor']

_columns_cache = {}
_columns_cache_lock = threading.Lock()


class Cursor(object):
    """
    The class returned by :meth:`giraffez.Cmd.execute <giraffez.cmd.Cmd.execute>` for iterating
    through Terdata CLIv2 results.

    :param `giraffez.Cmd` conn: The underlying database connection
    :param str command: The SQL command to be executed
    :param bool multi_statement: Execute in parallel statement mode
    :param bool header: If :code:`True`, yields a list of column names between statements
    :param bool prepare_only: Execute in prepare mode
    :param bool coerce_floats: Coerce Teradata Decimal types
        automatically into Python floats
    :param bool parse_dates: Returns date/time types as giraffez
        date/time types (instead of Python strings)
    """

    def __init__(self, conn, command, multi_statement=False, header=False,
            prepare_only=False, coerce_floats=True, parse_dates=False,
            panic=True):
        self.conn = conn
        self.command = command
        self.multi_statement = multi_statement
        self.header = header
        self.prepare_only = prepare_only
        self.coerce_floats = coerce_floats
        self.parse_dates = parse_dates
        self.panic = panic
        self.processor = lambda x, y: Row(x, y)
        if not self.coerce_floats:
            self.conn.set_encoding(DECIMAL_AS_STRING)
        if self.parse_dates:
            self.conn.set_encoding(DATETIME_AS_GIRAFFE_TYPES)
        self.columns = None
        if self.multi_statement:
            self.statements = [Statement(command)]
        else:
            self.statements = parse_statement(command)
        self._cur = 0
        self._execute(self.statements[self._cur])
        if self.prepare_only:
            self.columns = self._columns()

    def _columns(self):
        columns = self.conn.columns()
        log.debug("Debug[2]", repr(self.conn.columns(debug=True)))
        for column in columns:
            log.verbose("Debug[1]", repr(column))
        return columns

    def _execute(self, statement):
        with statement:
            try:
                self.conn.execute(statement, prepare_only=self.prepare_only)
            except TeradataError as error:
                if self.panic:
                    message = "Statement:\n{}".format(format_indent(truncate(statement), indent="  ", initial="  "))
                    raise suppress_context(TeradataError("{}\n\n{} ".format(error, message)))
                statement._error = error

    def _fetchone(self):
        try:
            row = self.conn.fetchone()
            self.statements[self._cur].count += 1
            return row
        except TeradataError as error:
            if error.code == TD_ERROR_REQUEST_EXHAUSTED:
                if self.multi_statement:
                    return self._fetchone()
                # In some cases CLIv2 returns RequestExhausted instead of
                # StatementEnded. For example, when the statement caused
                # a Teradata syntax error, instead of receiving a
                # StatementEnded parcel, it will return this error when
                # attempting to fetch the next parcel.
                if self._cur == len(self.statements)-1:
                    raise StopIteration
                self._cur += 1
                self._execute(self.statements[self._cur])
                return self._fetchone()
            raise
        except StatementInfoEnded:
            # Indicates that new columns were received so get them and
            # fetch the next parcel.
            self.columns = self._columns()
            self.statements[self._cur].columns = self.columns
            if self.header:
                return self.processor(self.columns, self.columns.names)
            return self._fetchone()
        except StatementEnded:
            if self.multi_statement:
                return self._fetchone()
            # Statement has no more parcels so we fetch the next one
            # to determine if there might be more statements or if the
            # request might be closed.
            if self._cur == len(self.statements)-1:
                raise StopIteration
            self._cur += 1
            self._execute(self.statements[self._cur])
            return self._fetchone()
        except RequestEnded:
            raise StopIteration

    def readall(self):
        """
        Exhausts the current connection by iterating over all rows and
        returning the total.

        .. code-block:: python

            with giraffez.Cmd() as cmd:
                results = cmd.execute("select * from dbc.dbcinfo")
                print(results.readall())
        """
        n = 0
        for row in self:
            n += 1
        return n

    def to_dict(self):
        """
        Sets the current encoder output to Python `dict` and returns
        the cursor.  This makes it possible to set the output encoding
        and iterate over the results:

        .. code-block:: python

            with giraffez.Cmd() as cmd:
                for row in cmd.execute(query).to_dict():
                    print(row)

        Or can be passed as a parameter to an object that consumes an iterator:

        .. code-block:: python

            result = cmd.execute(query)
            list(result.to_dict())
        """
        self.conn.set_encoding(ROW_ENCODING_DICT)
        self.processor = lambda x, y: y
        return self

    def to_list(self):
        """
        Set the current encoder output to :class:`giraffez.Row` objects
        and returns the cursor.  This is the default value so it is not
        necessary to select this unless the encoder settings have been
        changed already.
        """
        self.conn.set_encoding(ROW_ENCODING_LIST)
        self.processor = lambda x, y: Row(x, y)
        return self

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self

    def __next__(self):
        if self.prepare_only:
            raise StopIteration
        data = self._fetchone()
        return self.processor(self.columns, data)

    def __repr__(self):
        return "Cursor(statements={}, multi_statement={}, prepare={}, coerce_floats={}, parse_dates={}, panic={})".format(
            len(self.statements), self.multi_statement, self.prepare_only, self.coerce_floats,
            self.parse_dates, self.panic)


class TeradataCmd(Connection):
    """
    The class for connecting to Teradata and executing commands and 
    queries using CLIv2. 

    Exposed under the alias :class:`giraffez.Cmd`.
    
    For large-output queries, :class:`giraffez.BulkExport <giraffez.export.TeradataBulkExport>` should be used.

    :param str host: Omit to read from :code:`~/.girafferc` configuration file.
    :param str username: Omit to read from :code:`~/.girafferc` configuration file.
    :param str password: Omit to read from :code:`~/.girafferc` configuration file.
    :param int log_level: Specify the desired level of output from the job.
        Possible values are :code:`giraffez.SILENCE` :code:`giraffez.INFO` (default),
        :code:`giraffez.VERBOSE` and :code:`giraffez.DEBUG`
    :param str config: Specify an alternate configuration file to be read from, when
        previous paramaters are omitted.
    :param str key_file: Specify an alternate key file to use for configuration decryption
    :param string dsn: Specify a connection name from the configuration file to be
        used, in place of the default.
    :param bool protect: If authentication with Teradata fails and :code:`protect` is :code:`True` 
        locks the connection used in the configuration file. This can be unlocked using the
        command :code:`giraffez config --unlock <connection>` changing the connection password,
        or via the :meth:`~giraffez.config.Config.unlock_connection` method.
    :param string silent: Suppress log output. Used internally only.
    :param bool panic: If :code:`True`, when an error is encountered it will be
        raised.
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.TeradataError`: if the connection cannot be established

    Meant to be used, where possible, with python's :code:`with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete:

    .. code-block:: python

       with giraffez.Cmd() as cmd:
           results = cmd.execute('select * from dbc.dbcinfo')
           # continue executing statements and processing results

    Using the ``with`` context ensures proper exit-handling and disconnection.
    """

    def __init__(self, host=None, username=None, password=None, log_level=INFO, config=None,
            key_file=None, dsn=None, protect=False, silent=False, panic=True):
        super(TeradataCmd, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect, silent=silent)
        self.panic = panic
        self.silent = silent

    def execute(self, command, coerce_floats=True, parse_dates=False, header=False, sanitize=True,
            silent=False, panic=None,  multi_statement=False, prepare_only=False):
        """
        Execute commands using CLIv2.

        :param str command: The SQL command to be executed
        :param bool coerce_floats: Coerce Teradata decimal types into Python floats
        :param bool parse_dates: Parses Teradata datetime types into Python datetimes
        :param bool header: Include row header
        :param bool sanitize: Whether or not to call :func:`~giraffez.sql.prepare_statement`
            on the command
        :param bool silent: Silence console logging (within this function only)
        :param bool panic: If :code:`True`, when an error is encountered it will be
            raised.
        :param bool multi_statement: Execute in multi-statement mode
        :param bool prepare_only: Only prepare the command (no results)
        :return: a cursor over the results of each statement in the command
        :rtype: :class:`~giraffez.cmd.Cursor`
        :raises `giraffez.TeradataError`: if the query is invalid
        :raises `giraffez.errors.GiraffeError`: if the return data could not be decoded
        """
        if panic is None:
            panic = self.panic
        self.options("panic", panic)
        self.options("multi-statement mode", multi_statement, 3)
        if isfile(command):
            self.options("file", command, 2)
            with open(command, 'r') as f:
                command = f.read()
        else:
            if log.level >= VERBOSE:
                self.options("query", command, 2)
            else:
                self.options("query", truncate(command), 2)
        if not silent and not self.silent:
            log.info("Command", "Executing ...")
            log.info(self.options)
        if sanitize:
            command = prepare_statement(command) # accounts for comments and newlines
            log.debug("Debug[2]", "Command (sanitized): {!r}".format(command))
        self.cmd.set_encoding(ENCODER_SETTINGS_DEFAULT)
        return Cursor(self.cmd, command, multi_statement=multi_statement, header=header,
            prepare_only=prepare_only, coerce_floats=coerce_floats, parse_dates=parse_dates,
            panic=panic)

    def exists(self, object_name, silent=False):
        """
        Check that object (table or view) :code:`object_name` exists, by executing a :code:`show table object_name` query, 
        followed by a :code:`show view object_name` query if :code:`object_name` is not a table.

        :param str object_name: The name of the object to check for existence.
        :param bool silent: Silence console logging (within this function only)
        :return: :code:`True` if the object exists, :code:`False` otherwise.
        :rtype: bool
        """
        try:
            self.execute("show table {}".format(object_name), silent=silent)
            return True
        except TeradataError as error:
            if error.code != TD_ERROR_OBJECT_NOT_TABLE:
                return False
        try:
            self.execute("show view {}".format(object_name), silent=silent)
            return True
        except TeradataError as error:
            if error.code not in [TD_ERROR_OBJECT_NOT_VIEW, TD_ERROR_OBJECT_NOT_EXIST]:
                    return True
        return False

    def fetch_columns(self, table_name, silent=False):
        """
        Return the column information for :code:`table_name` by executing a :code:`select top 1 * from table_name` query.

        :param str table_name: The fully-qualified name of the table to retrieve schema for
        :param bool silent: Silence console logging (within this function only)
        :return: the columns of the table
        :rtype: :class:`~giraffez.types.Columns`
        """
        return self.execute("select top 1 * from {}".format(table_name), silent=silent, prepare_only=True).columns

    def insert(self, table_name, rows, fields=None, delimiter=None, null='NULL', parse_dates=False, quotechar='"'):
        """
        Load a text file into the specified :code:`table_name` or Insert Python :code:`list` rows into the specified :code:`table_name`

        :param str table_name: The name of the destination table
        :param list/str rows: A list of rows **or** the name of an input file. Each row
            must be a :code:`list` of field values.
        :param list fields: The names of the target fields, in the order that
            the data will be presented (defaults to :code:`None` for all columns
            in the table).
        :param str delimiter: The delimiter used by the input file (or :code:`None`
            to infer it from the header).
        :param str null: The string used to indicated nulled values in the
            file (defaults to :code:`'NULL'`).
        :param str quotechar: The character used to quote fields containing special
            characters, like the delimiter.
        :param bool parse_dates: If :code:`True`, attempts to coerce date fields
            into a standard format (defaults to :code:`False`).
        :raises `giraffez.errors.GiraffeEncodeError`: if the number of values in a row does not match
            the length of :code:`fields`
        :raises `giraffez.errors.GiraffeError`: if :code:`panic` is set and the insert statement
            caused an error.
        :return: A dictionary containing counts of applied rows and errors
        :rtype: :class:`dict`

        For most insertions, this will be faster and produce less strain on
        Teradata than using :class:`~giraffez.load.TeradataBulkLoad` (:class:`giraffez.BulkLoad <giraffez.load.TeradataBulkLoad>`).

        Requires that any input file be a properly delimited text file, with a
        header that corresponds to the target fields for insertion. Valid delimiters
        include '|', ',', and <tab> or a properly encoded JSON stream.
        """
        if not isfile(rows):
            return self._insert(table_name, rows, fields, parse_dates)
        with Reader(rows, delimiter=delimiter, quotechar=quotechar) as f:
            preprocessor = null_handler(null)
            rows = (preprocessor(l) for l in f)
            if isinstance(f, CSVReader):
                self.options("delimiter", unescape_string(f.reader.dialect.delimiter), 1)
                self.options("quote char", f.reader.dialect.quotechar, 2)
            elif isinstance(f, JSONReader):
                self.options("encoding", "json", 1)
            return self._insert(table_name, rows, f.header, parse_dates)

    def _close(self, exc=None):
        if getattr(self, 'cmd', None):
            self.cmd.close()

    def _connect(self, host, username, password):
        self.cmd = _Cmd(host, username, password)

    def _insert(self, table_name, rows, fields=None, parse_dates=False):
        global _columns_cache
        with _columns_cache_lock:
            if table_name in _columns_cache:
                columns = _columns_cache[table_name]
            else:
                columns = self.fetch_columns(table_name, silent=True)
                if not columns:
                    raise GiraffeError("Fetch columns failed '{}'".format(table_name))
                _columns_cache[table_name] = columns
        if fields is None:
            fields = columns.safe_names
        columns.set_filter(fields)
        check_input(columns, fields)
        stats = defaultdict(int)
        processor = pipeline([
            python_to_sql(table_name, columns, parse_dates)
        ])
        def _fetch():
            stats['count'] = 0
            current_block = ""
            for row in rows:
                try:
                    stmt = processor(row)
                except GiraffeError as error:
                    if self.panic:
                        raise error
                    log.info("Command", error)
                    stats['errors'] += 1
                    continue
                if len(current_block + stmt) > CLI_BLOCK_SIZE:
                    yield current_block
                    current_block = ""
                current_block += stmt
                stats['count'] += 1
            if current_block:
                yield current_block
        log.info("Command", "Executing ...")
        for i, block in enumerate(_fetch()):
            self.execute(block, sanitize=True, multi_statement=True, silent=True)
            if i == 0:
                log.info(self.options)
        return stats

class Cmd(Context):
    __instance__ = TeradataCmd
