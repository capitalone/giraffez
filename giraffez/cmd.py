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

# XXX: still necessary?
GIRAFFE_NOT_FOUND = False
try:
    from . import _encoder
except ImportError:
    GIRAFFE_NOT_FOUND = True

TDCLI_NOT_FOUND = False
try:
    from . import _cli
except ImportError:
    TDCLI_NOT_FOUND = True

from .constants import *
from .errors import *

from .connection import Connection
from .fmt import truncate
from .logging import log
from .sql import parse_statement, prepare_statement
from .types import Columns, Row
from .utils import suppress_context


__all__ = ['TeradataCmd']


# TODO: i put this here to not forget something and then forget why it's here ...
def read_sql():
    pass


class Cursor(object):
    def __init__(self, conn, command, multi_statement=False, prepare_only=False, encoder_settings=ENCODER_SETTINGS_DEFAULT):
        self.conn = conn
        self.command = command
        self.multi_statement = multi_statement
        self.prepare_only = prepare_only
        self.columns = None
        if encoder_settings & ROW_ENCODING_STRING:
            self.processor = lambda x, y: y
        elif encoder_settings & ROW_ENCODING_DICT:
            self.processor = lambda x, y: y
        elif encoder_settings & ROW_ENCODING_LIST:
            self.processor = lambda x, y: Row(x, y)
        else:
            # TODO: better error handling
            raise "help me"
        if self.multi_statement:
            self.statements = [command]
        else:
            self.statements = parse_statement(command)
        self.statements.reverse()
        self._execute(self.statements.pop())

    def _columns(self):
        columns = Columns(self.conn.columns())
        for column in columns:
            log.verbose("Debug[1]", repr(column))
        return columns

    def _execute(self, statement):
        try:
            self.conn.execute(statement, prepare_only=self.prepare_only)
            self.columns = self._columns()
        except _cli.error as error:
            raise suppress_context(TeradataError(error))

    def _fetchone(self):
        try:
            return self.conn.fetchone()
        except _cli.StatementEnded:
            # Multi-statement requests still emit the StatementEnded parcel
            # and therefore must be continually read until the RequestEnded
            # parcel is received.
            if self.multi_statement:
                try:
                    return self._fetchone()
                except _cli.RequestEnded:
                    raise StopIteration
            self.columns = None
            if not self.statements:
                raise StopIteration
            self._execute(self.statements.pop())
            return self._fetchone()
        except _cli.error as error:
            raise suppress_context(TeradataError(error))

    def readall(self):
        n = 0
        for row in self:
            n += 1
        return n

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
        return "Cursor(n_stmts={}, multi={}, prepare={})".format(len(self.statements),
            self.multi_statement, self.prepare_only)


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
            config=None, key_file=None, dsn=None, protect=False, mload_session=False, encoder_settings=ENCODER_SETTINGS_DEFAULT):
        if TDCLI_NOT_FOUND:
            raise TeradataCLIv2NotFound(TeradataCLIv2NotFound.__doc__.rstrip())
        super(TeradataCmd, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect, mload_session, encoder_settings)

        self.panic = panic

    def _connect(self, host, username, password):
        try:
            self.cmd = _cli.Cmd(host, username, password, encoder_settings=self.encoder_settings)
        except _cli.error as error:
            # TODO: make sure protect/lock connection work
            raise TeradataError(error)

    def close(self):
        if getattr(self, 'cmd', None):
            self.cmd.close()

    def collect_stats(self, table_name, predicate="", silent=False):
        """
        Collect statistics on the table :code:`table_name` using the given :code:`predicate` if present

        :param str table_name: The name of the table to collect statistics on
        :param str predicate: The predicate of the collect statistics statement, which follows the table name
        :param bool silent: Silence console logging (within this function only)
        :return: The result of the collect statistics statement
        :rtype: None or :class:`~giraffez.types.Result`
        :raises `giraffez.errors.TeradataError`: if statistic cannot be collected for :code:`table_name`
        :raises `giraffez.errors.ObjectDoesNotExist`: if the given :code:`table_name` does not exist
        """
        self.execute("collect statistics on {} {}".format(table_name, predicate), silent=silent)

    def count_table(self, table_name, silent=False):
        """
        Return the row count of :code:`table_name`

        :param str table_name: The name of the table to be counted
        :param bool silent: Silence console logging (within this function only)
        :return: The number of rows in :code:`table_name`
        :rtype: int
        """
        return self.execute("select count(*) from {}".format(table_name), silent=silent).first()[0]

    def drop_table(self, table_name, silent=False):
        """
        Execute the statement :code:`drop table table_name`

        :param str table_name: The name of the table to be dropped
        :param bool silent: Silence console logging (within this function only)
        :return: :code:`None` if :code:`table_name` does not exist, otherwise the result of
            the statement
        :rtype: None or :class:`~giraffez.types.Result`
        """
        try:
            result = self.execute("drop table {}".format(table_name), silent=silent)
        except ObjectDoesNotExist as error:
            return None
        return result

    def execute(self, command, sanitize=True, multi_statement=False, prepare_only=False,
            silent=False):
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
        # TODO: self.options needs thread lock?
        self.options("panic", self.panic)
        if multi_statement:
            self.options("multi-statement mode", True, 3)
        else:
            self.options("multi-stmtement mode", False, 3)
        if log.level >= VERBOSE:
            self.options("query", command, 2)
        else:
            self.options("query", truncate(command), 2)
        if not silent:
            log.info("Command", "Executing ...")
            log.info(self.options)
        if sanitize:
            command = prepare_statement(command) # accounts for comments and newlines
            log.debug("Debug[2]", "Command (sanitized): {!r}".format(command))
        return Cursor(self.cmd, command, multi_statement=multi_statement,
            prepare_only=prepare_only)

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
        except ObjectNotTable as error:
            self.execute("show view {}".format(object_name), silent=silent)
            return True
        except (ObjectNotView, ObjectDoesNotExist) as error:
            return False

    def get_columns(self, table_name, silent=False):
        """
        Return the column information for :code:`table_name` by executing a :code:`select top 1 * from table_name` query.

        :param str table_name: The fully-qualified name of the table to retrieve schema for
        :param bool silent: Silence console logging (within this function only)
        :return: the columns of the table
        :rtype: :class:`~giraffez.types.Columns`
        """
        return self.execute("select top 1 * from {}".format(table_name), silent=silent, prepare_only=True).columns;

    def release_mload(self, table_name, silent=False):
        """
        Release an MLoad lock on :code:`table_name` by executing the statement :code:`release mload table_name`
        followed by the statement :code:`release mload table_name in apply` if the lock cannot be released

        :param str table_name: The name of the table to be dropped
        :param bool silent: Silence console logging (within this function only)
        :return: :code:`None` if :code:`table_name` does not exist, otherwise the result of 
            the statement
        :rtype: None or :class:`~giraffez.types.Result`
        """
        try:
            return self.execute("release mload {}".format(table_name), silent=silent)
        except CannotReleaseMultiLoad as error:
            return self.execute("release mload {} in apply".format(table_name), silent=silent)
