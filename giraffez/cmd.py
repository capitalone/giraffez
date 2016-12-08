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

from .config import *
from .connection import *
from .constants import *
from .encoders import *
from .errors import *
from .fmt import *
from .io import *
from .logging import *
from .parser import *
from .sql import *
from .types import *
from .utils import *


__all__ = ['TeradataCmd']


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
            config=None, key_file=None, dsn=None, protect=False, mload_session=False):
        if TDCLI_NOT_FOUND:
            raise TeradataCLIv2NotFound(TeradataCLIv2NotFound.__doc__.rstrip())
        super(TeradataCmd, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect, mload_session)

        self.panic = panic

    def _connect(self, host, username, password):
        try:
            self.cmd = _cli.Cmd(host, username, password)
        except _cli.error as error:
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
        self.execute_one("collect statistics on {} {}".format(table_name, predicate), silent=silent)

    def count_table(self, table_name, silent=False):
        """
        Return the row count of :code:`table_name`

        :param str table_name: The name of the table to be counted
        :param bool silent: Silence console logging (within this function only)
        :return: The number of rows in :code:`table_name`
        :rtype: int
        """
        return self.execute_one("select count(*) from {}".format(table_name), silent=silent).first()[0]

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
            result = self.execute_one("drop table {}".format(table_name), silent=silent)
        except ObjectDoesNotExist as error:
            return None
        return result

    def execute(self, command, sanitize=True, silent=False):
        """
        Execute commands using CLIv2.

        :param str command: The SQL command to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.sql.prepare_statement` on the command
        :param bool silent: Silence console logging (within this function only)
        :return: the results of each statement in the command
        :rtype: :class:`~giraffez.types.Results`
        :raises `giraffez.errors.TeradataError`: if the query is invalid
        :raises `giraffez.errors.GiraffeError`: if the return data could not be decoded
        """
        self.options("panic", self.panic)
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
        try:
            self.cmd.execute(command)
            columns = self.cmd.columns()
            return Results(self.cmd.fetch_all())
        except _cli.error as error:
            raise TeradataError(error)

    def execute_one(self, command, sanitize=True, silent=False):
        """
        Execute a single command using CLIv2.

        :param str command: The SQL command to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.utils.clean_query` on the command
        :param bool silent: Silence console logging (within this function only)
        :return: the result set of the first statement in the command (all are executed)
        :rtype: :class:`~giraffez.types.Result`
        :raises `giraffez.errors.TeradataError`: if the query is invalid
        :raises `giraffez.errors.GiraffeError`: if the return data could not be decoded
        """
        return Results(self.execute(command, sanitize, silent)).one()

    def execute_many(self, command, sanitize=True, parallel=False, silent=False):
        """
        Execute multiple commands using CLIv2, splitting on statements and executing them individually. With :code:`parallel` set, the behavior becomes that of :meth:`~giraffez.Cmd.execute`.

        :param str command: The SQL command(s) to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.utils.clean_query` on the statements
        :param bool parallel: If :code:`True` call :meth:`~giraffez.Cmd.execute` instead
        :param bool silent: Silence console logging (within this function only)
        :return: the results of all executed commands
        :rtype: :class:`~giraffez.types.Results`
        """
        if parallel:
            return self.execute(command, sanitize, silent)
        statements = parse_statement(command)
        results = []
        for statement in statements:
            results.append(self.execute_one(statement, sanitize, silent))
        return Results(results)

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
        return self.execute_one("select top 1 * from {}".format(table_name), silent=silent).columns

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
            return self.execute_one("release mload {}".format(table_name), silent=silent)
        except CannotReleaseMultiLoad as error:
            return self.execute_one("release mload {} in apply".format(table_name), silent=silent)
