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
    #from . import _tpt
    from giraffez import _tpt
except ImportError:
    GIRAFFE_NOT_FOUND = True

from collections import defaultdict

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

from ._compat import *


__all__ = ['TeradataExport']


class TeradataExport(Connection):
    """
    The class for using the Teradata Parallel Transport API to quickly eport
    large amounts of data.

    Exposed under the alias :class:`giraffez.Export`.

    :param str query: Either SQL query to execute and return results of, **or** the
        name of a table to export in its entirety
    :param str host: Omit to read from :code:`~/.girafferc` configuration file.
    :param str username: Omit to read from :code:`~/.girafferc` configuration file.
    :param str password: Omit to read from :code:`~/.girafferc` configuration file.
    :param str delimiter: The delimiter to use when exporting with the 'text'
        encoding. Defaults to '|'
    :param str null: The string to use to represent a null value with the
        'text' encoding. Defaults to 'NULL'
    :param str encoding: The encoding format in which to export the data.
        Defaults to 'text'.  Possible values are 'text' - delimited text 
        output, 'dict' - to output each row as a :code:`dict` object mapping column names
        to values, 'json' - to output each row as a JSON encoded string, and 
        'archive' to output in giraffez archive format
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
    :raises `giraffez.errors.InvalidCredentialsError`: if the supplied credentials are incorrect
    :raises `giraffez.errors.TeradataError`: if the connection cannot be established

    Meant to be used, where possible, with Python's :code:`with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete:

    .. code-block:: python

       with giraffez.Export('dbc.dbcinfo') as export:
           print(export.header)
           for row in export.results():
               print(row)
    """

    def __init__(self, query=None, host=None, username=None, password=None,
            delimiter=DEFAULT_DELIMITER, null=DEFAULT_NULL, encoding=DEFAULT_ENCODING,
            log_level=INFO, config=None, key_file=None, dsn=None, protect=False):
        if GIRAFFE_NOT_FOUND:
            raise GiraffeNotFound("Export module was not compiled with package (missing TPT API)")
        super(TeradataExport, self).__init__(host, username, password, log_level, config, key_file,
            dsn, protect)

        #: Attributes used with property getter/setters
        self._columns = None
        self._query = None
        self._encoding = None

        self.delimiter = delimiter
        self.null = null
        self.encoding = encoding
        self.initiated = False

        #: Attributes set via property getter/setters
        self.query = query

    def _connect(self, host, username, password):
        self.export = _tpt.Export(host, username, password)
        self._handle_error()

    def _get_columns(self):
        if not self.initiated:
            self._initiate()
        columns = Columns(self.export.columns())
        _columns = defaultdict(int)
        for column in columns:
            if column.original_name in _columns:
                column.title = "{}_{}".format(column.original_name, _columns[column.original_name])
            _columns[column.original_name] += 1
        for column in columns:
            log.verbose("Debug[1]", repr(column))
        self.export.set_columns(columns)
        return columns

    def _handle_error(self):
        if self.export.status >= TD_ERROR:
            if self.export.status == CLI_ERR_INVALID_USER:
                if self.protect:
                    Config.lock_connection(self.config, self.dsn)
                raise InvalidCredentialsError(self.export.error_message())
            raise TeradataError(self.export.error_message())

    def _initiate(self):
        title, version = get_version_info()
        query_band = "UTILITYNAME={};VERSION={};".format(title, version)
        self.export.add_attribute(TD_QUERY_BAND_SESS_INFO, query_band)
        log.info("Export", "Initiating Teradata PT request (awaiting server)  ...")
        self.export.initiate()
        self._handle_error()
        log.info("Export", "Teradata PT request accepted.")
        self.export.get_schema()
        self._handle_error()
        self.initiated = True
        if self.encoding == "text":
            self.processor = identity
        elif self.encoding == "json":
            self.processor = dict_to_json
        elif self.encoding == "dict":
            self.processor = identity
        elif self.encoding == "archive":
            self.processor = identity

        self.options("encoding", self.encoding, 1)
        if self.encoding == "text":
            self.options("delimiter", self.delimiter, 2)
            self.options("null", self.null, 3)
        log.info("Export", "Executing ...")
        log.info(self.options)

    def close(self):
        log.info("Export", "Closing Teradata PT connection ...")
        self.export.close()
        log.info("Export", "Teradata PT request complete.")

    @property
    def columns(self):
        """
        Retrieve columns if necessary, and return them.

        :rtype: :class:`~giraffez.types.Columns`
        """
        if self._columns is None:
            self._columns = self._get_columns()
        return self._columns

    @property
    def encoding(self):
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        if value == self._encoding:
            return
        else:
            self._encoding = value
        self.initiated = False
    
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
        if self.encoding == "archive":
            return self.columns.serialize()
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
        self.export.add_attribute(TD_SELECT_STMT, statement)
        # When new query is set refresh columns
        self._columns = None

    def results(self):
        """
        Generates and yields row results as they are returned from
        Teradata and processed. 
        
        :return: A generator that can be iterated over or coerced into a list.
        :rtype: iterator (yields ``string`` or ``dict``)

        .. code-block:: python

           # iterate over results
           with giraffez.Export('dbc.dbcinfo') as export:
               for row in export.results():
                   print(row)

           # retrieve results as a list
           with giraffez.Export('db.table2') as export:
               results = list(export.results())
        """
        if self.query is None:
            raise GiraffeError("Must set target table or query.")
        if not self.initiated:
            self._initiate()
            self._columns = self._get_columns()
        if self.encoding == "archive":
            def export_get_buffer():
                return self.export.get_buffer_raw()
        elif self.encoding in {"dict", "json"}:
            def export_get_buffer():
                return self.export.get_buffer_dict()
        else:
            def export_get_buffer():
                return self.export.get_buffer_str(self.null, self.delimiter)

        while True:
            data = export_get_buffer()
            if not data:
                break
            for row in data:
                yield self.processor(row)
