# -*- coding: utf-8 -*-

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

    :param str host: Omit to read from `~/.girafferc` configuration file.
    :param str username: Omit to read from `~/.girafferc` configuration file.
    :param str password: Omit to read from `~/.girafferc` configuration file.
    :param int log_level: Specify the desired level of output from the job.
        Possible values are `giraffez.SILENCE`, `giraffez.INFO` (default),
        `giraffez.VERBOSE`, and `giraffez.DEBUG`
    :param bool panic: If `True`, stop executing commands and return after the first error.
    :param str config: Specify an alternate configuration file to be read from, when previous paramaters are omitted.
    :param str key_file: Specify an alternate key file to use for configuration decryption
    :param string dsn: Specify a connection name from the configuration file to be
        used, in place of the default.
    :param bool protect: If authentication with Teradata fails and `protect` is `True`, 
        locks the connection used in the configuration file. This can be unlocked using the
        command `giraffez config --unlock <connection>`, changing the connection password,
        or via the :meth:`~giraffez.config.Config.unlock_connection` method.
    :param string mload_session: Identifies mload sessions to suppress duplicate log output.
        Used internally only.
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.errors.TeradataError`: if the connection cannot be established

    Meant to be used, where possible, with python's `with` context handler
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
            raise GiraffeNotFound("giraffez module was not compiled with package")
        super(TeradataCmd, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect, mload_session)

        self.panic = panic

    def _connect(self, host, username, password):
        self.cmd = _cli.Cmd()
        try:
            self.cmd.connect(host, username, password)
        except _cli.error as error:
            raise TeradataError(error)

    def _execute(self, command):
        try:
            results, error = self.cmd.execute(command)
            if error is not None:
                raise TeradataError(error)
            log.debug("Debug[2]", repr(results))
        except _cli.error as error:
            raise TeradataError(error)
        for result in results:
            stmt_info = result.get('stmtinfo', None)
            if not stmt_info:
                raise GiraffeError("Statement Info header not received")
            columns = _encoder.Encoder.unpack_stmt_info(stmt_info)
            result["columns"] = Columns(columns)
            for column in result["columns"]:
                log.verbose("Debug[1]", repr(column))
            encoder = _encoder.Encoder(result["columns"])
            processor = pipeline([
                date_handler(result["columns"]),
                char_handler(result["columns"]),
                lambda s: Row(result["columns"], s)
            ])
            result["rows"] = [processor(row) for row in encoder.unpack_rows(result["rows"])]
        return results

    def close(self):
        if getattr(self, 'cmd', None):
            self.cmd.close()

    def collect_stats(self, table_name, predicate=""):
        """
        Collect statistics on the table `table_name`, using the given `predicate` if present

        :param str table_name: The name of the table to collect statistics on
        :param str predicate: The predicate of the collect statistics statement, which follows the table name
        :return: The result of the collect statistics statement
        :rtype: None or :class:`~giraffez.types.Result`
        :raises `giraffez.errors.TeradataError`: if statistic cannot be collected for `table_name`
        :raises `giraffez.errors.ObjectDoesNotExist`: if the given `table_name` does not exist
        """
        self.execute_one("collect statistics on {} {}".format(table_name, predicate))

    def count_table(self, table_name):
        """
        Return the row count of `table_name`

        :param str table_name: The name of the table to be counted
        :return: The number of rows in `table_name`
        :rtype: int
        """
        return self.execute_one("select count(*) from {}".format(table_name)).first()[0]

    def drop_table(self, table_name):
        """
        Execute the statement `drop table table_name`

        :param str table_name: The name of the table to be dropped
        :return: `None` if `table_name` does not exist, otherwise the result of
            the statement
        :rtype: None or :class:`~giraffez.types.Result`
        """
        try:
            result = self.execute_one("drop table {}".format(table_name))
        except ObjectDoesNotExist as error:
            return None
        return result

    def execute(self, command, sanitize=True):
        """
        Execute commands using CLIv2.

        :param str command: The SQL command to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.sql.prepare_statement` on the command
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
        log.info("Command", "Executing ...")
        log.info(self.options)
        if sanitize:
            command = prepare_statement(command) # accounts for comments and newlines
            log.debug("Debug[2]", "Command (sanitized): {!r}".format(command))
        return Results(self._execute(command))

    def execute_one(self, command, sanitize=True):
        """
        Execute a single command using CLIv2.

        :param str command: The SQL command to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.utils.clean_query` on the command
        :return: the result set of the first statement in the command (all are executed)
        :rtype: :class:`~giraffez.types.Result`
        :raises `giraffez.errors.TeradataError`: if the query is invalid
        :raises `giraffez.errors.GiraffeError`: if the return data could not be decoded
        """
        return self.execute(command, sanitize).one()

    def execute_many(self, command, sanitize=True, parallel=False):
        """
        Execute multiple commands using CLIv2, splitting on statements and executing them individually. With `parallel` set, the behavior becomes that of :meth:`~giraffez.Cmd.execute`.

        :param str command: The SQL command(s) to be executed
        :param bool sanitize: Whether or not to call :func:`~giraffez.utils.clean_query` on the statements
        :param bool parallel: If `True`, call :meth:`~giraffez.Cmd.execute` instead
        :return: the results of all executed commands
        :rtype: :class:`~giraffez.types.Results`
        """
        if parallel:
            return self.execute(command, sanitize)
        statements = parse_statement(command)
        results = []
        for statement in statements:
            results.append(self.execute_one(statement, sanitize))
        return Results(results)

    def exists(self, object_name):
        """
        Check that object (table or view) `object_name` exists, by executing a `show table object_name` query, 
        followed by a `show view object_name` query if `object_name` is not a table.

        :param str object_name: The name of the object to check for existence.
        :return: `True` if the object exists, `False` otherwise.
        :rtype: bool
        """
        try:
            self.execute("show table {}".format(object_name))
            return True
        except ObjectNotTable as error:
            self.execute("show view {}".format(object_name))
            return True
        except (ObjectNotView, ObjectDoesNotExist) as error:
            return False

    def get_columns(self, table_name):
        """
        Return the column information for `table_name`, by executing a `select top 1 * from table_name` query.

        :param str table_name: The fully-qualified name of the table to retrieve schema for
        :return: the columns of the table
        :rtype: :class:`~giraffez.types.Columns`
        """
        return self.execute_one("select top 1 * from {}".format(table_name)).columns

    def release_mload(self, table_name):
        """
        Release an MLoad lock on `table_name` by executing the statement `release mload table_name`,
        followed by the statement `release mload table_name in apply` if the lock cannot be released

        :param str table_name: The name of the table to be dropped
        :return: `None` if `table_name` does not exist, otherwise the result of 
            the statement
        :rtype: None or :class:`~giraffez.types.Result`
        """
        try:
            return self.execute_one("release mload {}".format(table_name))
        except CannotReleaseMultiLoad as error:
            return self.execute_one("release mload {} in apply".format(table_name))
