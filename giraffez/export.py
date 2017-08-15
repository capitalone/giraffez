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

from ._teradatapt import Export

from .constants import *
from .errors import *

from ._teradatapt import InvalidCredentialsError
from .config import Config
from .connection import Connection
from .encoders import dict_to_json, TeradataEncoder
from .fmt import truncate
from .logging import log
from .sql import parse_statement, remove_curly_quotes
from .utils import get_version_info, show_warning, suppress_context

from ._compat import *


__all__ = ['TeradataBulkExport']


class TeradataBulkExport(Connection):
    """
    The class for using the Teradata Parallel Transport API to quickly export
    large amounts of data.

    Exposed under the alias :class:`giraffez.BulkExport`.

    :param str query: Either SQL query to execute and return results of, **or** the
        name of a table to export in its entirety
    :param str host: Omit to read from :code:`~/.girafferc` configuration file.
    :param str username: Omit to read from :code:`~/.girafferc` configuration file.
    :param str password: Omit to read from :code:`~/.girafferc` configuration file.
    :param int log_level: Specify the desired level of output from the job.
        Possible values are :code:`giraffez.SILENCE` :code:`giraffez.INFO` (default),
        :code:`giraffez.VERBOSE` and :code:`giraffez.DEBUG`
    :param str config: Specify an alternate configuration file to be read from, when previous paramters are omitted.
    :param str key_file: Specify an alternate key file to use for configuration decryption
    :param string dsn: Specify a connection name from the configuration file to be
        used, in place of the default.
    :param bool protect: If authentication with Teradata fails and :code:`protect` is :code:`True` 
        locks the connection used in the configuration file. This can be unlocked using the
        command :code:`giraffez config --unlock <connection>` changing the connection password,
        or via the :meth:`~giraffez.config.Config.unlock_connection` method.
    :param bool coerce_floats: Coerce Teradata decimal types into Python floats
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.TeradataError`: if the connection cannot be established

    Meant to be used, where possible, with Python's :code:`with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete:

    .. code-block:: python

       with giraffez.BulkExport('dbc.dbcinfo') as export:
           print(export.header)
           for row in export.to_list():
               print(row)
    """

    def __init__(self, query=None, host=None, username=None, password=None,
            log_level=INFO, config=None, key_file=None, dsn=None, protect=False,
            coerce_floats=True):
        super(TeradataBulkExport, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect)
        # Attributes used with property getter/setters
        self._query = None
        self._encoding = None
        self._delimiter = None
        self._null = None
        self.coerce_floats = coerce_floats
        self.initiated = False
        #: The amount of time spent in idle (waiting for server)
        self.idle_time = 0
        self.query = query

    @property
    def columns(self):
        """
        Retrieve columns if necessary, and return them.

        :rtype: :class:`~giraffez.types.Columns`
        """
        return self.export.columns()

    @property
    def delimiter(self):
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        self._delimiter = value
        self.export.set_delimiter(self._delimiter)

    @property
    def null(self):
        return self._null

    @null.setter
    def null(self, value):
        self._null = value
        self.export.set_null(self._null)
    
    @property
    def header(self):
        """
        Return the header for the resulting data.

        :return: String containing the formatted header, either column names
            separated by the current :code:`delimiter` value if :code:`encoding` is 'text',
            or the serialized :class:`~giraffez.types.Columns` object for
            'archive' encoding
        :rtype: str
        :raises `giraffez.errors.GiraffeError`: if the query or table
            has not been specified
        """
        if self.query is None:
            raise GiraffeError("Must set target table or query.")
        if not self.delimiter:
            self.delimiter = DEFAULT_DELIMITER
        return self.delimiter.join(self.columns.names)

    @property
    def query(self):
        """
        :return: The current query if it has been set, otherwise :code:`None`
        :rtype: str
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Set the query to be run and initiate the connection with Teradata.
        Only necessary if the query/table name was not specified as an argument
        to the constructor of the instance.

        :param str query: Valid SQL query to be executed
        """
        if query is None:
            return
        if log.level >= VERBOSE:
            self.options("query", query, 5)
        else:
            self.options("query", truncate(query), 5)
        statements = parse_statement(remove_curly_quotes(query))
        if not statements:
            raise GiraffeError("Unable to parse SQL statement")
        if len(statements) > 1:
            show_warning(("MORE THAN ONE STATEMENT RECEIVED, EXPORT OPERATIONS ALLOW ONE "
                "STATEMENT - ONLY THE FIRST STATEMENT WILL BE USED."), RuntimeWarning)
        statement = statements[0]
        log.debug("Debug[2]", "Statement (sanitized): {!r}".format(statement))
        if not (statement.startswith("select ") or statement.startswith("sel ")):
            statement = "select * from {}".format(statement)
        if statement == self.query:
            return
        else:
            self._query = statement
        self.initiated = False
        # Since CLIv2 is used in set_query (instead of relying on the
        # colums from the TPT Export driver) and set_query will always
        # happen before calls to initiate, set_query will always fail
        # with InvalidCredentialsError before initiate despite initiate
        # presumably failing after this point as well.
        try:
            self.export.set_query(statement)
        except InvalidCredentialsError as error:
            if args.protect:
                Config.lock_connection(args.conf, args.dsn, args.key)
            raise error

    def to_archive(self, writer):
        """
        Writes export archive files in the Giraffez archive format.
        This takes a `giraffez.io.Writer` and writes archive chunks to
        file until all rows for a given statement have been exhausted.

        .. code-block:: python

            with giraffez.BulkExport("database.table_name") as export:
                with giraffez.Writer("database.table_name.tar.gz", 'wb', use_gzip=True) as out:
                    for n in export.to_archive(out):
                        print("Rows: {}".format(n))

        :param `giraffez.io.Writer` writer: A writer handling the archive output

        :rtype: iterator (yields ``int``)
        """
        if 'b' not in writer.mode:
            raise GiraffeError("Archive writer must be in binary mode")
        writer.write(GIRAFFE_MAGIC)
        writer.write(self.columns.serialize())
        i = 0
        for n, chunk in enumerate(self._fetchall(ROW_ENCODING_RAW), 1):
            writer.write(chunk)
            yield TeradataEncoder.count(chunk)

    def to_dict(self):
        """
        Sets the current encoder output to Python `dict` and returns
        a row iterator.

        :rtype: iterator (yields ``dict``)
        """
        return self._fetchall(ROW_ENCODING_DICT)

    def to_json(self):
        """
        Sets the current encoder output to json encoded strings and
        returns a row iterator.

        :rtype: iterator (yields ``str``)
        """
        return self._fetchall(ROW_ENCODING_DICT, processor=dict_to_json)

    def to_list(self):
        """
        Sets the current encoder output to Python `list` and returns
        a row iterator.

        :rtype: iterator (yields ``list``)
        """
        return self._fetchall(ROW_ENCODING_LIST)

    def to_str(self):
        """
        Sets the current encoder output to Python `str` and returns
        a row iterator.

        :rtype: iterator (yields ``str``)
        """
        return self._fetchall(ENCODER_SETTINGS_STRING, coerce_floats=False)

    def _close(self, exc=None):
        log.info("Export", "Closing Teradata PT connection ...")
        self.export.close()
        log.info("Export", "Teradata PT request complete.")

    def _connect(self, host, username, password):
        self.export = Export(host, username, password)
        self.export.add_attribute(TD_QUERY_BAND_SESS_INFO, "UTILITYNAME={};VERSION={};".format(
            *get_version_info()))
        self.delimiter = DEFAULT_DELIMITER
        self.null = DEFAULT_NULL

    def _initiate(self):
        log.info("Export", "Initiating Teradata PT request (awaiting server)  ...")
        start_time = time.time()
        self.export.initiate()
        self.idle_time = time.time() - start_time
        self.initiated = True
        log.info("Export", "Teradata PT request accepted.")
        self.options("delimiter", escape_string(self.delimiter or DEFAULT_DELIMITER), 2)
        self.options("null", self.null or DEFAULT_NULL, 3)
        log.info("Export", "Executing ...")
        log.info(self.options)

    def _fetchall(self, encoding, coerce_floats=None, processor=None):
        if self.query is None:
            raise GiraffeError("Must set target table or query.")
        if not self.initiated:
            self._initiate()
        self.export.set_encoding(encoding)
        if processor is None:
            processor =  identity
        if coerce_floats is None:
            coerce_floats = self.coerce_floats
        if coerce_floats:
            self.export.set_encoding(DECIMAL_AS_FLOAT)
        while True:
            data = self.export.get_buffer()
            if not data:
                break
            for row in data:
                yield processor(row)
