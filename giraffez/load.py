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
import struct
import threading

from ._teradatapt import EncoderError, InvalidCredentialsError, MLoad, TeradataError as TeradataPTError

from .constants import *
from .errors import *

from .cmd import TeradataCmd
from .connection import Connection, Context
from .encoders import DateHandler, null_handler
from .fmt import format_table
from .io import ArchiveFileReader, CSVReader, FileReader, JSONReader, Reader
from .logging import log
from .utils import get_version_info, pipeline, suppress_context

from ._compat import *


__all__ = ['BulkLoad']


error_table_count = """
select a.errorcode as code, a.errorfield as field, b.errortext as text, count(*) over (partition by a.errorcode, a.errorfield) as errcount
from {0}_e1 a
join dbc.errormsgs b
on a.errorcode = b.errorcode
group by 1,2,3;
"""

error_table_sample = """
select
a.errorcode as code,
a.errorfield as field,
min(substr(a.hostdata, 0, 30000)) as hostdata
from {0}_e1 a
qualify row_number() over (partition by a.errorcode, a.errorfield order by a.errorcode asc)=1
group by 1,2;
"""


class TeradataBulkLoad(Connection):
    """
    The class for using the TPT API's UPDATE (MLoad) driver to insert a
    large (> ~100k rows) amount of data into an existing Teradata table.

    Exposed under the alias :class:`giraffez.BulkLoad`.

    :param str table: The name of the target table for loading.
    :param str host: Omit to read from :code:`~/.girafferc` configuration file.
    :param str username: Omit to read from :code:`~/.girafferc` configuration file.
    :param str password: Omit to read from :code:`~/.girafferc` configuration file.
    :param int log_level: Specify the desired level of output from the job.
        Possible values are :code:`giraffez.SILENCE`, :code:`giraffez.INFO` (default),
        :code:`giraffez.VERBOSE`, and :code:`giraffez.DEBUG`
    :param str config: Specify an alternate configuration file to be read from,
        when previous paramaters are omitted.
    :param str key_file: Specify an alternate key file to use for configuration decryption
    :param string dsn: Specify a connection name from the configuration file to be
        used, in place of the default.
    :param bool protect: If authentication with Teradata fails and :code:`protect` is :code:`True`,
        locks the connection used in the configuration file. This can be unlocked using the
        command :code:`giraffez config --unlock <connection>`, changing the connection password,
        or via the :meth:`~giraffez.config.Config.unlock_connection` method.
    :param bool coerce_floats: Coerce Teradata decimal types into Python floats
    :param bool cleanup: Attempt to cleanup all work tables
        when context exits.
    :param bool print_error_table: Prints a user-friendly version of the mload
        error table to stderr.
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.TeradataPTError`: if the connection cannot be established

    If the target table is currently under an MLoad lock (such as if the
    previous operation failed), a :code:`release mload` statement will be
    executed on the table, and the load job will be re-attempted.

    Meant to be used, where possible, with python's :code:`with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete.
    """
    checkpoint_interval = 50000

    def __init__(self, table=None, host=None, username=None, password=None,
            log_level=INFO, config=None, key_file=None, dsn=None, protect=False,
            coerce_floats=False, cleanup=False, print_error_table=False):
        super(TeradataBulkLoad, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect)
        # Attributes used with property getter/setters
        self._columns = None
        self._table_name = None
        self.initiated = False
        self.finished = False
        self.coerce_floats = coerce_floats
        self.perform_cleanup = cleanup
        self.exit_code = None
        self.applied_count = 0
        self.error_count = 0
        #: The amount of time spent in idle (waiting for server)
        self.idle_time = 0
        #: Prints the error table when there is an issue, good for troubleshooting jobs
        self.print_error_table = print_error_table
        self.preprocessor = lambda s: s
        if table is not None:
            self.table = table

    def checkpoint(self):
        """
        Execute a checkpoint while loading rows. Called automatically
        when loading from a file. Updates the exit code of the driver to
        reflect errors.
        """
        return self.mload.checkpoint()

    def cleanup(self):
        """
        Drops any existing work tables, as returned by
        :meth:`~giraffez.load.TeradataBulkLoad.tables`.

        :raises `giraffez.TeradataPTError`: if a Teradata error ocurred
        """
        threads = []
        for i, table in enumerate(filter(lambda x: self.mload.exists(x), self.tables)):
            log.info("BulkLoad", "Dropping table '{}'...".format(table))
            t = threading.Thread(target=self.mload.drop_table, args=(table,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @property
    def columns(self):
        """
        The list of columns in use.

        :getter: Return the list of columns in use.
        :setter: Set the columns to be loaded into, as well as their order. If
            loading from a file, these will be determined from the file header.
            Not necessary if you are loading into all columns, in the original
            order. The value must be a :code:`list` of names in the order that
            the fields of data will be presented in each row.

            Raises :class:`~giraffez.errors.GiraffeError` if :code:`field_names`
            is not a :code:`list`.

            Raises :class:`~giraffez.errors.GiraffeError` if the target table
            has not been set.
        :type: :class:`~giraffez.types.Columns`
        """
        return self._columns

    @columns.setter
    def columns(self, field_names):
        if not isinstance(field_names, list):
            raise GiraffeError("Must set .columns property as type <List>")
        fields = []
        for field in field_names:
            field = field.lower()
            if field in fields:
                raise GiraffeError("Cannot set duplicate column: '{}'".format(field))
            fields.append(field)
        self._columns = fields

    def finish(self):
        """
        Finishes the load job. Called automatically when the connection closes.

        :return: The exit code returned when applying rows to the table
        """
        if self.finished:
            return self.exit_code
        checkpoint_status = self.checkpoint()
        self.exit_code = self._exit_code()
        if self.exit_code != 0:
            raise TeradataPTError("BulkLoad job finished with return code '{}'".format(self.exit_code))
        # TODO(chris): should this happen every time?
        if self.applied_count > 0:
            self._end_acquisition()
            self._apply_rows()
        self.exit_code = self._exit_code()
        if self.exit_code != 0:
            raise TeradataPTError("BulkLoad job finished with return code '{}'".format(self.exit_code))
        self.finished = True
        return self.exit_code

    def from_file(self, filename, table=None, delimiter='|', null='NULL',
            panic=True, quotechar='"', parse_dates=False):
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
            separated by this delimiter. Defaults to :code:`None`, which causes the
            delimiter to be determined from the header of the file. In most
            cases, this behavior is sufficient
        :param str quotechar: The character used to quote fields containing special characters,
            like the delimiter.
        :param bool panic: If :code:`True`, when an error is encountered it will be
            raised. Otherwise, the error will be logged and :code:`self.error_count`
            is incremented.
        :return: The output of the call to
            :meth:`~giraffez.load.TeradataBulkLoad.finish`
        :raises `giraffez.errors.GiraffeError`: if table was not set and :code:`table`
            is :code:`None`, or if a Teradata error ocurred while retrieving table info.
        :raises `giraffez.errors.GiraffeEncodeError`: if :code:`panic` is :code:`True` and there
            are format errors in the row values.
        """
        if not self.table:
            if not table:
                raise GiraffeError("Table must be set or specified to load a file.")
            self.table = table
        if not isinstance(null, basestring):
            raise GiraffeError("Expected 'null' to be str, received {}".format(type(null)))
        with Reader(filename, delimiter=delimiter, quotechar=quotechar) as f:
            if not isinstance(f.delimiter, basestring):
                raise GiraffeError("Expected 'delimiter' to be str, received {}".format(type(delimiter)))
            self.columns = f.header
            if isinstance(f, ArchiveFileReader):
                self.mload.set_encoding(ROW_ENCODING_RAW)
                self.preprocessor = lambda s: s
            if parse_dates:
                self.preprocessor = DateHandler(self.columns)
            self._initiate()
            self.mload.set_null(null)
            self.mload.set_delimiter(delimiter)
            i = 0
            for i, line in enumerate(f, 1):
                self.put(line, panic=panic)
                if i % self.checkpoint_interval == 1:
                    log.info("\rBulkLoad", "Processed {} rows".format(i), console=True)
                    checkpoint_status = self.checkpoint()
                    self.exit_code = self._exit_code()
                    if self.exit_code != 0:
                        return self.exit_code
            log.info("\rBulkLoad", "Processed {} rows".format(i))
            return self.finish()

    def put(self, items, panic=True):
        """
        Load a single row into the target table.

        :param list items: A list of values in the row corresponding to the
            fields specified by :code:`self.columns`
        :param bool panic: If :code:`True`, when an error is encountered it will be
            raised. Otherwise, the error will be logged and :code:`self.error_count`
            is incremented.
        :raises `giraffez.errors.GiraffeEncodeError`: if :code:`panic` is :code:`True` and there
            are format errors in the row values.
        :raises `giraffez.errors.GiraffeError`: if table name is not set.
        :raises `giraffez.TeradataPTError`: if there is a problem
            connecting to Teradata.
        """
        if not self.initiated:
            self._initiate()
        try:
            row_status = self.mload.put_row(self.preprocessor(items))
            self.applied_count += 1
        except (TeradataPTError, EncoderError) as error:
            self.error_count += 1
            if panic:
                raise error
            log.info("BulkLoad", error)

    def read_error_table(self):
        with TeradataCmd(log_level=log.level, config=self.config, key_file=self.key_file,
                dsn=self.dsn, silent=True) as cmd:
            result = list(cmd.execute((error_table_count + error_table_sample).format(self.table)))
            if len(result) == 0:
                log.info("No error information available.")
                return
            return result

    def release(self):
        """
        Attempt release of target mload table.

        :raises `giraffez.errors.GiraffeError`: if table was not set by
            the constructor, the :code:`TeradataBulkLoad.table`, or
            :meth:`~giraffez.load.TeradataBulkLoad.from_file`.
        """
        if self.table is None:
            raise GiraffeError("Cannot release. Target table has not been set.")
        log.info("BulkLoad", "Attempting release for table {}".format(self.table))
        self.mload.release(self.table)

    @property
    def status(self):
        return self.mload.status()

    @property
    def tables(self):
        """
        The names of the work tables used for loading.

        :return: A list of four tables, each the name of the target table
            with the added suffixes, "_wt", "_log", "_e1", and "_e2"
        :raises `giraffez.errors.GiraffeError`: if table was not set by
            the constructor, the :code:`TeradataBulkLoad.table`, or
            :meth:`~giraffez.load.TeradataBulkLoad.from_file`.
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
            :class:`~giraffez.load.TeradataBulkLoad` instance or
            :meth:`~giraffez.load.TeradataBulkLoad.from_file`. The value given
            must include all qualifiers such as database name.

            Raises :class:`~giraffez.errors.GiraffeError` if the MLoad connection has
            already been initiated, or the :class:`~giraffez.cmd.TeradataCmd` connection cannot
            be established.

            Raises :class:`~giraffez.TeradataPTError` if the column data could not be
            retrieved from Teradata
        :type: str
        """
        return self._table_name

    @table.setter
    def table(self, table_name):
        if self.initiated:
            raise GiraffeError("Cannot reuse BulkLoad context for more than one table")
        self._table_name = table_name

    @property
    def total_count(self):
        """
        The number of rows applied, plus the number of rows in error.
        """
        return self.applied_count + self.error_count

    def _apply_rows(self):
        log.info("BulkLoad", "Beginning apply phase ...")
        self.mload.apply_rows()
        self._update_apply_count()
        self._update_error_count()
        log.info("BulkLoad", "Apply phase ended.")

    def _close(self, exc=None):
        if not self.initiated:
            return
        if not exc:
            try:
                self.finish()
            except TeradataPTError as error:
                self._close(error)
                if self.print_error_table:
                    for row in self.read_error_table():
                        print(row)
                raise error
        log.info("BulkLoad", "Closing Teradata PT connection ...")
        self.mload.close()
        log.info("BulkLoad", "Teradata PT request complete.")

    def _connect(self, host, username, password):
        self.mload = MLoad(host, username, password)
        title, version = get_version_info()
        query_band = "UTILITYNAME={};VERSION={};".format(title, version)
        self.mload.add_attribute(TD_QUERY_BAND_SESS_INFO, query_band)

    def _end_acquisition(self):
        log.info("BulkLoad", "Ending acquisition phase ...")
        self.mload.end_acquisition()
        log.info("BulkLoad", "Acquisition phase ended.")

    def _exit_code(self):
        data = self.mload.get_event(TD_Evt_ExitCode)
        if data is None:
            log.info("BulkLoad", "Update exit code failed.")
            return
        return struct.unpack("h", data)[0]

    def _initiate(self):
        if not self.table:
            raise GiraffeError("Table must be set prior to initiating.")
        if self.initiated:
            raise GiraffeError("Already initiated connection.")
        try:
            if self.perform_cleanup:
                self.cleanup()
            elif any(filter(lambda x: self.mload.exists(x), self.tables)):
                raise GiraffeError("Cannot continue without dropping previous job tables. Exiting ...")
            log.info("BulkLoad", "Initiating Teradata PT request (awaiting server)  ...")
            start_time = time.time()
            self.mload.initiate(self.table, self.columns)
            self.idle_time = time.time() - start_time
        except InvalidCredentialsError as error:
            if args.protect:
                Config.lock_connection(args.conf, args.dsn, args.key)
            raise error
        self.mload.set_null(None)
        log.info("BulkLoad", "Teradata PT request accepted.")
        self._columns = self.mload.columns()
        self.initiated = True

    def _update_apply_count(self):
        data = self.mload.get_event(TD_Evt_RowCounts64)
        if data is None:
            log.info("BulkLoad", "Update apply row count failed.")
            return
        recv, sent, applied = struct.unpack("QQQ", data)
        log.debug("Debug[2]", "Event[RowCounts64]: r:{}, s:{}, a:{}".format(recv, sent, applied))
        self.applied_count = applied

    def _update_error_count(self):
        data = self.mload.get_event(TD_Evt_ErrorTable2, 1)
        if data is None:
            log.info("BulkLoad", "Update error row count failed.")
            return
        count = struct.unpack("I", data)[0]
        log.debug("Debug[2]", "Event[ErrorTable2]: c:{}".format(count))
        self.error_count = count


class BulkLoad(Context):
    __instance__ = TeradataBulkLoad
