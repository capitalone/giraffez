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

from ._cli import Cmd, RequestEnded, StatementEnded, TeradataError

from .constants import *
from .errors import *

from .connection import Connection
from .fmt import truncate
from .logging import log
from .sql import parse_statement, prepare_statement
from .types import Columns, Row
from .utils import suppress_context


__all__ = ['TeradataCmd']


def read_sql(s, config=None, key_file=None, panic=True, log_level=None):
    stats = []
    with TeradataCmd(config=config, key_file=key_file, log_level=log_level) as cmd:
        statements = parse_statement(s)
        for statement in statements:
            try:
                cmd.execute(statement)
                stats.append((statement, None))
            except TeradataError as error:
                stats.append((statement, error))
                if panic:
                    raise error


class Cursor(object):
    def __init__(self, conn, command, multi_statement=False, prepare_only=False,
            coerce_floats=True, parse_dates=False):
        self.conn = conn
        self.command = command
        self.multi_statement = multi_statement
        self.prepare_only = prepare_only
        self.coerce_floats = coerce_floats
        self.parse_dates = parse_dates
        self.processor = lambda x, y: Row(x, y)
        if not self.coerce_floats:
            self.conn.set_encoding(DECIMAL_AS_STRING)
        if self.parse_dates:
            self.conn.set_encoding(DATETIME_AS_GIRAFFE_TYPES)
        self.columns = None
        if self.multi_statement:
            self.statements = [command]
        else:
            self.statements = parse_statement(command)
        self.statements.reverse()
        self._execute(self.statements.pop())

    def _columns(self):
        columns = self.conn.columns()
        log.debug(self.conn.columns(debug=True))
        for column in columns:
            log.verbose("Debug[1]", repr(column))
        return columns

    def _execute(self, statement):
        self.conn.execute(statement, prepare_only=self.prepare_only)
        self.columns = self._columns()

    def _fetchone(self):
        try:
            return self.conn.fetchone()
        except StatementEnded:
            # Multi-statement requests still emit the StatementEnded parcel
            # and therefore must be continually read until the RequestEnded
            # parcel is received.
            if self.multi_statement:
                try:
                    return self._fetchone()
                except RequestEnded:
                    raise StopIteration
            if not self.statements:
                raise StopIteration
            self.columns = None
            self._execute(self.statements.pop())
            return self._fetchone()

    def readall(self):
        n = 0
        for row in self:
            n += 1
        return n

    def items(self):
        self.conn.set_encoding(ROW_ENCODING_DICT)
        self.processor = lambda x, y: y
        return self

    def values(self):
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
        return "Cursor(n_stmts={}, multi={}, prepare={}, coerce_floats={}, parse_dates={})".format(
            len(self.statements), self.multi_statement, self.prepare_only, self.coerce_floats,
            self.parse_dates)


class TeradataCmd(Connection):
    """
    The class for connecting to Teradata and executing commands and 
    queries using CLIv2. 

    Exposed under the alias :class:`giraffez.Cmd`.
    
    For large-output queries, :class:`giraffez.Export` should be used.

    :param str host: Omit to read from :code:`~/.girafferc` configuration file.
    :param str username: Omit to read from :code:`~/.girafferc` configuration file.
    :param str password: Omit to read from :code:`~/.girafferc` configuration file.
    :param int log_level: Specify the desired level of output from the job.
        Possible values are :code:`giraffez.SILENCE` :code:`giraffez.INFO` (default),
        :code:`giraffez.VERBOSE` and :code:`giraffez.DEBUG`
    :param bool panic: If :code:`True` stop executing commands and return after the first error.
    :param str config: Specify an alternate configuration file to be read from, when previous paramaters are omitted.
    :param str key_file: Specify an alternate key file to use for configuration decryption
    :param string dsn: Specify a connection name from the configuration file to be
        used, in place of the default.
    :param bool protect: If authentication with Teradata fails and :code:`protect` is :code:`True` 
        locks the connection used in the configuration file. This can be unlocked using the
        command :code:`giraffez config --unlock <connection>` changing the connection password,
        or via the :meth:`~giraffez.config.Config.unlock_connection` method.
    :param string mload_session: Identifies mload sessions to suppress duplicate log output.
        Used internally only.
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.errors.TeradataError`: if the connection cannot be established

    Meant to be used, where possible, with python's :code:`with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete:

    .. code-block:: python

       with giraffez.Cmd() as cmd:
           results = cmd.execute('select * from dbc.dbcinfo')
           # continue executing statements and processing results

    Use in this manner guarantees proper exit-handling and disconnection 
    when operation is completed.
    """

    def __init__(self, host=None, username=None, password=None, log_level=INFO, panic=False,
            config=None, key_file=None, dsn=None, protect=False, silent=False):
        super(TeradataCmd, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect, silent=silent)

        self.panic = panic
        self.silent = silent

    def _connect(self, host, username, password):
        self.cmd = Cmd(host, username, password)

    def close(self):
        if getattr(self, 'cmd', None):
            self.cmd.close()

    def execute(self, command, sanitize=True, multi_statement=False, prepare_only=False,
            silent=False, coerce_floats=True, parse_dates=False):
        # TODO: Improve/fix this docstring
        """
        Execute commands using CLIv2.

        :param str command: The SQL command to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.sql.prepare_statement` on the command
        :param bool multi_statement: Execute in multi-statement mode
        :param bool prepare_only: Only prepare the command (no results)
        :param bool silent: Silence console logging (within this function only)
        :return: the results of each statement in the command
        :rtype: :class:`~giraffez.types.Rows`
        :raises `giraffez.errors.TeradataError`: if the query is invalid
        :raises `giraffez.errors.GiraffeError`: if the return data could not be decoded
        """
        self.options("panic", self.panic)
        self.options("multi-statement mode", multi_statement, 3)
        if log.level >= VERBOSE:
            self.options("query", command, 2)
        else:
            self.options("query", truncate(command), 2)
        if not silent or not self.silent:
            log.info("Command", "Executing ...")
            log.info(self.options)
        if sanitize:
            command = prepare_statement(command) # accounts for comments and newlines
            log.debug("Debug[2]", "Command (sanitized): {!r}".format(command))
        self.cmd.set_encoding(ENCODER_SETTINGS_DEFAULT)
        return Cursor(self.cmd, command, multi_statement=multi_statement,
            prepare_only=prepare_only, coerce_floats=coerce_floats, parse_dates=parse_dates)

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
        return self.execute("select top 1 * from {}".format(table_name), silent=silent, prepare_only=True).columns;
