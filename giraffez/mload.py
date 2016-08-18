# -*- coding: utf-8 -*-

import struct
import warnings

GIRAFFE_NOT_FOUND = False
try:
    from . import _tpt
except ImportError:
    GIRAFFE_NOT_FOUND = True

from .cmd import *
from .connection import *
from .config import *
from .constants import *
from .core import *
from .encoders import *
from .errors import *
from .io import *
from .logging import *
from .types import *
from .utils import *


__all__ = ['TeradataMLoad']


class TeradataMLoad(Connection):
    """
    The class for using the TPT API's UPDATE (MLoad) driver to insert a
    large (> ~100k rows) amount of data into an existing Teradata table.

    Exposed under the alias :class:`giraffez.MLoad`.

    :param str table: The name of the target table for loading.
    :param str host: Omit to read from `~/.girafferc` configuration file.
    :param str username: Omit to read from `~/.girafferc` configuration file.
    :param str password: Omit to read from `~/.girafferc` configuration file.
    :param int log_level: Specify the desired level of output from the job.
        Possible values are `giraffez.SILENCE`, `giraffez.INFO` (default),
        `giraffez.VERBOSE`, and `giraffez.DEBUG`
    :param str config: Specify an alternate configuration file to be read from,
        when previous paramaters are omitted.
    :param str key_file: Specify an alternate key file to use for configuration decryption
    :param string dsn: Specify a connection name from the configuration file to be
        used, in place of the default.
    :param bool protect: If authentication with Teradata fails and `protect` is `True`, 
        locks the connection used in the configuration file. This can be unlocked using the
        command `giraffez config --unlock <connection>`, changing the connection password,
        or via the :meth:`~giraffez.config.Config.unlock_connection` method.
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.errors.TeradataError`: if the connection cannot be established

    If the target table is currently under an MLoad lock (such as if the
    previous operation failed), a `release mload` statement will be 
    executed on the table, and the load job will be re-attempted.

    Meant to be used, where possible, with python's `with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete.
    """
    checkpoint_interval = DEFAULT_CHECKPOINT_INTERVAL
    _valid_encodings = {"text", "archive"}

    def __init__(self, table=None, host=None, username=None, password=None, log_level=INFO,
            config=None, key_file=None, dsn=None, protect=False):
        if GIRAFFE_NOT_FOUND:
            raise GiraffeNotFound("MLoad module was not compiled with package (missing TPT API)")
        super(TeradataMLoad, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect, mload_session=False)

        #: Attributes used with property getter/setters
        self._columns = None
        self._table_name = None
        self._encoding = None

        #: Flags tracking the important stages of MultiLoad to ensure
        #: that the job is shutdown smoothly (if possible).
        self.initiated = False
        self.end_acquisition = False

        self.applied_count = 0
        self.error_count = 0
        self.processor = None

        self.allow_precision_loss = False
        self.encoding = DEFAULT_ENCODING
        if table is not None:
            self.table = table

    def _apply_rows(self):
        log.info("MLoad", "Beginning apply phase ...")
        self.load.apply_rows()
        self._update_apply_count()
        self._update_error_count()
        log.info("MLoad", "Apply phase ended.")

    def _update_apply_count(self):
        data = self.load.get_event(TD_Evt_RowCounts64)
        if data is None:
            log.info("MLoad", "Update apply row count failed.")
            return
        recv, sent, applied = struct.unpack("QQQ", data)
        log.debug("Debug[2]", "Event[RowCounts64]: r:{}, s:{}, a:{}".format(recv, sent, applied))
        self.applied_count = applied

    def _update_error_count(self):
        data = self.load.get_event(TD_Evt_ErrorTable2, 1)
        if data is None:
            log.info("MLoad", "Update error row count failed.")
            return
        count = struct.unpack("I", data)[0]
        log.debug("Debug[2]", "Event[ErrorTable2]: c:{}".format(count))
        self.error_count = count

    def _end_acquisition(self):
        log.info("MLoad", "Ending acquisition phase ...")
        self.load.end_acquisition()
        self.end_acquisition = True
        log.info("MLoad", "Acquisition phase ended.")

    def _connect(self, host, username, password):
        self.cmd = TeradataCmd(host, username, password, log_level=log.level, mload_session=True)
        self.load = _tpt.Load(host, username, password)
        self._handle_error()

    def _handle_error(self, status=None):
        if status is None:
            status = self.status
        if status >= TD_ERROR:
            self.load.get_error_info()
            raise MultiLoadError("{}: {}".format(status, self.load.error_message()))
        return False

    def _initiate(self):
        if not self.table:
            raise GiraffeError("Table must be set prior to initiating.")
        if self.columns is None:
            raise GiraffeError("Columns must be set prior to initiating.")
        if self.initiated:
            raise GiraffeError("Already initiated connection.")
        title, version = get_version_info()
        query_band = "UTILITYNAME={};VERSION={};".format(title, version)
        self.load.add_attribute(TD_QUERY_BAND_SESS_INFO, query_band)
        dml = "insert into {} ({}) values ({});".format(self.table,
            ",".join(['"{}"'.format(f) for f in self.columns.names]),
            ",".join(":{}".format(f) for f in self.columns.safe_names))
        log.verbose("MLoad", "DML: {}".format(dml))
        def _initiate():
            log.info("MLoad", "Initiating Teradata PT request (awaiting server)  ...")
            self.load.initiate(self.columns.tuples(), dml, MARK_DUPLICATE_ROWS)
            self._handle_error()
            log.info("MLoad", "Teradata PT request accepted.")
        try:
            _initiate()
        except (MultiLoadLocked, TransactionAborted) as error:
            log.info("MLoad", "Teradata PT request failed with code '{}'.".format(self.status))
            self.release()
            _initiate()
        if self.processor is None:
            self.processor = pipeline([
                python_to_teradata(self.columns)
            ])
        self.initiated = True

    def checkpoint(self):
        """
        Execute a checkpoint while loading rows. Called automatically
        when loading from a file. Updates the exit code of the driver to
        reflect errors.
        """
        checkpoint_status = self.load.checkpoint()
        if checkpoint_status >= TD_ERROR:
            status_message = MESSAGES.get(int(checkpoint_status), None)
            error_table = [("Error Code", "Error Description")]
            error_table.append((checkpoint_status, status_message))
            log.info("\r{}".format(format_table(error_table)))
        return checkpoint_status

    def cleanup(self):
        """
        Drops any existing work tables, as returned by 
        :meth:`~giraffez.load.TeradataMLoad.tables`.

        :raises `giraffez.errors.TeradataError`: if a Teradata error ocurred
        """
        for table in filter(self.cmd.exists, self.tables):
            log.info("MLoad", "Dropping table '{}'...".format(table))
            self.cmd.drop_table(table)

    def close(self):
        log.info("MLoad", "Closing Teradata PT connection ...")
        if not self.end_acquisition and self.initiated:
            log.info("MLoad", "Acquisition phase was not called before closing.")
            self._end_acquisition()
        self.load.close()
        self.cmd.close()
        log.info("MLoad", "Teradata PT request complete.")

    @property
    def columns(self):
        """
        The list of columns in use.

        :getter: Return the list of columns in use.
            :rtype: `~giraffez.cmd.Columns`
        :setter: Set the columns to be loaded into, as well as their order. If
            loading from a file, these will be determined from the file header.
            Not necessary if you are loading into all columns, in the original
            order.

            :param list field_names: A list of names of columns to be loaded into,
                in the order that the fields of data will be presented in each row
            :raises `giraffez.errors.GiraffeError`: if `field_names` is not a `list`
            :raises `giraffez.errors.GiraffeError`: if the target table has not been set.
        """
        return self._columns

    @columns.setter
    def columns(self, field_names):
        if not isinstance(field_names, list):
            raise GiraffeError("Must set .columns property as type <List>")
        if not self.table:
            raise GiraffeError("Table name not set")
        self._columns.set_filter(field_names)

    @property
    def encoding(self):
        """
        The encoding of the file being loaded.

        :getter: Returns the name of the encoding being used.
        :setter: Set the encoding of the input file if not specified to the 
            constructor of the instance.

            :param str value: Either 'text' or 'archive'
            :raises `giraffez.errors.GiraffeError`: if the `enc` is not one
                of the above
        """
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        if value not in self._valid_encodings:
            raise GiraffeError("{} is not a valid encoding type".format(value))
        self._encoding = value

    @property
    def exit_code(self):
        data = self.load.get_event(TD_Evt_ExitCode)
        if data is None:
            log.info("MLoad", "Update exit code failed.")
            return
        return struct.unpack("h", data)[0]

    def finish(self):
        """
        Finishes the load job. **Called automatically when loading from a
        file, but must be called at the end of the job after loading rows
        individually.**

        :return: The exit code returned when applying rows to the table
        """
        checkpoint_status = self.checkpoint()
        self._end_acquisition()
        self._apply_rows()
        exit_code = self.exit_code
        if exit_code != 0:
            self._handle_error()
        return exit_code

    def from_file(self, filename, table=None, null=DEFAULT_NULL, delimiter=None, panic=True):
        """
        Load from a file into the target table, handling each step of the
        load process.

        Can load from text files, and properly formatted giraffez archive
        files. In both cases, if Gzip compression is detected the file will be
        decompressed while reading and handled appropriately. The encoding is
        determined automatically by the contents of the file.

        It is not necessary to set the columns in use prior to loading from a file.
        In the case of a text file, the header is used to determine column names
        and their order. Valid delimiters include '|', ',', and '\\t' (tab). When 
        loading an archive file, the column information is decoded alongside the data.

        :param str filename: The location of the file to be loaded
        :param str table: The name of the target table, if it was not specified
            to the constructor for the isntance
        :param str null: The string that indicates a null value in the rows being
            inserted from a file. Defaults to 'NULL'
        :param str delimiter: When loading a file, indicates that fields are
            separated by this delimiter. Defaults to `None`, which causes the
            delimiter to be determined from the header of the file. In most
            cases, this behavior is sufficient
        :param bool panic: If `True`, when an error is encountered it will be
            raised. Otherwise, the error will be logged and `self.error_count`
            is incremented.
        :return: The output of the call to
            :meth:`~giraffez.load.TeradataMLoad.finish`
        :raises `giraffez.errors.GiraffeError`: if table was not set and `table`
            is `None`, or if a Teradata error ocurred while retrieving table info.
        :raises `giraffez.errors.GiraffeEncodeError`: if `panic` is `True` and there
            are format errors in the row values.
        """
        if not self.table:
            if not table:
                raise GiraffeError("Table must be set or specified to load a file.")
            self.table = table

        with Reader(filename, delimiter=delimiter) as f:
            self.columns = f.field_names()

            if f.file_type == DELIMITED_TEXT_FILE:
                self.processor = pipeline([
                    text_to_strings(f.delimiter),
                    null_handler(null),
                    python_to_teradata(self.columns, self.allow_precision_loss)
                ])
            else:
                self.processor = lambda s: s

            self._initiate()
            i = 0
            for i, line in enumerate(f, 1):
                self.load_row(line, panic=panic)
                if i % self.checkpoint_interval == 1:
                    log.info("\rMLoad", "Processed {} rows".format(i), console=True)
                    checkpoint_status = self.checkpoint()
                    exit_code = self.exit_code
                    if exit_code != 0:
                        return exit_code
            log.info("\rMLoad", "Processed {} rows".format(i))
            return self.finish()

    def load_row(self, items, panic=True):
        """
        Load a single row into the target table.

        :param list items: A list of values in the row corresponding to the
            fields specified by `~giraffez.load.TeradataMLoad.columns`
        :param bool panic: If `True`, when an error is encountered it will be
            raised. Otherwise, the error will be logged and `self.error_count`
            is incremented.
        :raises `giraffez.errors.GiraffeEncodeError`: if `panic` is `True` and there
            are format errors in the row values.
        :raises `giraffez.errors.GiraffeError`: if table name is not set.
        :raises `giraffez.errors.TeradataError`: if there is a problem
            connecting to Teradata.
        """
        if not self.initiated:
            self._initiate()
        try:
            data = self.processor(items)
            row_status = self.load.put_row(data)
            if not self._handle_error(row_status):
                self.applied_count += 1
        except GiraffeEncodeError as error:
            self.error_count += 1
            if panic:
                raise error
            log.info("MLoad", error)

    def release(self):
        """
        Attempt release of target mload table.

        :raises `giraffez.errors.GiraffeError`: if table was not set by
            the constructor, the :code:`TeradataMLoad.table`, or
            :meth:`~giraffez.load.TeradataMLoad.from_file`.
        """
        if self.table is None:
            raise GiraffeError("Cannot release. Target table has not been set.")
        log.info("MLoad", "Attempting release for table {}".format(self.table))
        self.cmd.release_mload(self.table)

    @property
    def status(self):
        return self.load.status

    @property
    def tables(self):
        """
        The names of the work tables used for loading.

        :return: A list of four tables, each the name of the target table
            with the added suffixes, "_wt", "_log", "_e1", and "_e2"
        :raises `giraffez.errors.GiraffeError`: if table was not set by
            the constructor, the :code:`TeradataMLoad.table`, or
            :meth:`~giraffez.load.TeradataMLoad.from_file`.
        """
        if self.table is None:
            raise GiraffeError("Target table has not been set.")
        return [
            "{}_wt".format(self.table),
            "{}_log".format(self.table),
            "{}_e1".format(self.table),
            "{}_e2".format(self.table),
        ]

    @property
    def table(self):
        """
        The name of the target table.

        :getter: Returns the name of the target table, or :code:`None` if it
            has not been set.
        :setter: Set the name of the target table, if the table name
            was not given to the constructor of the
            :class:`~giraffez.load.TeradataMLoad` instance or
            :meth:`~giraffez.load.TeradataMLoad.from_file`.

            :param str table_name: Name of the table to load into, must include
                all qualifiers such as database name
            :raises `giraffez.errors.GiraffeError`: if the MLoad connection has
                already been initiated, or the :class:`giraffez.Cmd` connection cannot
                be established
            :raises `giraffez.errors.TeradataError`: if the column data could not be
                retrieved from Teradata
        :type: str
        """
        return self._table_name

    @table.setter
    def table(self, table_name):
        if self.initiated:
            raise GiraffeError("Cannot reuse MLoad context for more than one table")
        self._table_name = table_name
        self.load.set_table(table_name)
        try:
            log.info("MLoad", "Requesting column info for table '{}' ...".format(self.table))
            self._columns = self.cmd.get_columns(self.table)
        except MultiLoadTableExists as error:
            log.info("MLoad", "Teradata PT request failed with code '{}'.".format(error.code))
            self.release()
            self._columns = self.cmd.get_columns(self.table)

    @property
    def total_count(self):
        """
        The number of rows applied, plus the number of rows in error.
        """
        return self.applied_count + self.error_count
