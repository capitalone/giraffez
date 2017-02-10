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

import datetime
import threading

from collections import defaultdict

from .constants import *
from .errors import *

from .cmd import TeradataCmd
from .encoders import check_input, null_handler, python_to_sql
from .io import CSVReader, JSONReader, Reader
from .logging import log
from .utils import pipeline


__all__ = ['TeradataLoad']

_columns_cache = {}
_columns_cache_lock = threading.Lock()


class TeradataLoad(TeradataCmd):
    """
    The class for inserting into Teradata tables using CLIv2.

    Exposed under the alias :class:`giraffez.Load`.

    This class should be used for inserting into Teradata tables in all but
    very large (> ~100k rows) cases.

    See :class:`~giraffez.cmd.TeradataCmd` for constructor arguments.

    Meant to be used, where possible, with python's :code:`with` context handler
    to guarantee that connections will be closed gracefully when operation
    is complete:

    .. code-block:: python

       with giraffez.Load() as load:
           load.from_file('database.my_table', 'myfile.txt')
           # continue executing statements and processing results

    Use in this manner guarantees proper exit-handling and disconnection
    when operation is completed (or interrupted).
    """

    def from_file(self, table_name, input_file_name, delimiter=None, null=DEFAULT_NULL,
            date_conversion=False, quotechar='"'):
        """
        Load a text file into the specified :code:`table_name`

        For most insertions, this will be faster and produce less strain on
        Teradata than using :class:`~giraffez.load.TeradataMLoad` (:class:`giraffez.MLoad`).

        Requires that the input file be a properly delimited text file, with a
        header that corresponds to the target fields for insertion. Valid delimiters
        include '|', ',', and '\\t' (tab).

        :param str table_name: The name of the destination table
        :param str input_file_name: The name of the file to read rows from
        :param str delimiter: The delimiter used by the input file (or :code:`None`
            to infer it from the header).
        :param str quotechar: The character used to quote fields containing special characters,
            like the delimiter."
        :param str null: The string used to indicated nulled values in the
            file (defaults to :code:`'NULL'`).
        :param bool date_conversion: If :code:`True`, attempts to coerce date fields
            into a standard format (defaults to :code:`False`).
        :return: A dictionary containing counts of applied rows and errors
        :rtype: dict
        """
        with Reader(input_file_name, delimiter=delimiter, quotechar=quotechar) as f:
            preprocessor = null_handler(null)
            rows = (preprocessor(l) for l in f)
            if isinstance(f, CSVReader):
                self.options("delimiter", f.reader.dialect.delimiter, 1)
                self.options("quote char", f.reader.dialect.quotechar, 2)
            elif isinstance(f, JSONReader):
                self.options("encoding", "json", 1)
            return self.insert(table_name, rows, fields=f.header, date_conversion=date_conversion)

    def insert(self, table_name, rows, fields=None, date_conversion=True):
        """
        Insert Python :code:`list` rows into the specified :code:`table_name`

        :param str table_name: The name of the destination table
        :param list rows: A list of rows. Each row must be a :code:`list` of
            field values.
        :param list fields: The names of the target fields, in the order that
            the data will be presented (defaults to :code:`None` for all columns in the table).
        :param bool date_conversion: If :code:`True`, attempts to coerce date fields
            into a standard format (defaults to :code:`True`).
        :return: A dictionary containing counts of applied rows and errors
        :rtype: dict
        :raises `giraffez.errors.GiraffeEncodeError`: if the number of values in a row does not match
            the length of :code:`fields`
        :raises `giraffez.errors.GiraffeError`: if :code:`panic` is set and the insert statement
            caused an error.
        """
        global _columns_cache
        with _columns_cache_lock:
            if table_name in _columns_cache:
                columns = _columns_cache[table_name]
            else:
                columns = self.get_columns(table_name, silent=False)
                _columns_cache[table_name] = columns
        if fields is None:
            fields = columns.safe_names
        columns.set_filter(fields)
        check_input(columns, fields)
        stats = defaultdict(int)
        processor = pipeline([
            python_to_sql(table_name, columns, date_conversion)
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
                    log.info("Load", error)
                    stats['errors'] += 1
                    continue
                if len(current_block + stmt) > CLI_BLOCK_SIZE:
                    yield current_block
                    current_block = ""
                current_block += stmt
                stats['count'] += 1
            if current_block:
                yield current_block
        log.info("Load", "Executing ...")
        for block in _fetch():
            self.execute(block, sanitize=True, multi_statement=True, silent=True)
            # TODO: should maybe only print once?
            log.info(self.options)
        return stats
