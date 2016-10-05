Advanced Usage
==============

Setting up logging
------------------

The console logging messages emitted to stderr when using giraffez from the command-line can also be enabled in the Python API (although disabled by default). Enabling it in a project is trivial:

.. code-block:: python

    import giraffez

    giraffez.setup_logging()


The console logging messages can also be used with the Python `logging <https://docs.python.org/3/library/logging.html>`_ module: 

.. code-block:: python

    import logging
    import giraffez

    log = logging.getLogger("giraffez.console_logger")
    log.addHandler(...)
    log.setLevel(logging.DEBUG)

Using the graceful shutdown
---------------------------

The signal handler used by the giraffez command-line tool is available to the API as well.  This is what allows the giraffez command to attempt closing Teradata connections down safely before exiting the process (and shutting down cold on the next Ctrl+C).  To use this add this towards the beginning of the project:

.. code-block:: python

    import giraffez

    giraffez.register_graceful_shutdown_signal()

Working with other packages
---------------------------

Results from :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>` and :class:`giraffez.Export <giraffez.export.TeradataExport>` can be
used with other packages that accept common Python data structures.

Using :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>` with pandas:

.. code-block:: python

    import pandas as pd
    with giraffez.Cmd() as cmd:
        result = cmd.execute_one("select infokey, character_length(infokey) as nchars from dbc.dbcinfo")
        df = pd.DataFrame(result.items())

.. code-block:: python

    >>> print(df)
                     infokey  nchars
    0                VERSION       7
    1                RELEASE       7
    2  LANGUAGE SUPPORT MODE      21

Using :class:`giraffez.Export <giraffez.export.TeradataExport>` with pandas:

.. code-block:: python

    with giraffez.Export(encoding='dict') as export:
        export.query = "select infokey, character_length(infokey) as nchars from dbc.dbcinfo"
        df = pd.DataFrame(export.results())

.. code-block:: python

    >>> print(df)
                     infokey  infokey_1
    0                VERSION          7
    1                RELEASE          7
    2  LANGUAGE SUPPORT MODE         21
    >>> print(df.mean())
    nchars    11.666667
    dtype: float64

.. _export-aliasing:

**Note:** because of the way the TPT Export driver functions, the provided alias "nchars" for the computed column in the query is not available. In this case, the source column "infokey" is used. Rather than allow duplicate column names, :class:`giraffez.Export <giraffez.export.TeradataExport>` detects this and enumerates the offending columns. If this behavior is unacceptable or the alias is needed, you may need to use :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>` or modify your query.
 

Exporting to different formats
------------------------------


Using the Python standard library `csv <https://docs.python.org/3/library/csv.html>`_:

.. code-block:: python

    import csv

    with giraffez.Export('dbc.dbcinfo', encoding='dict') as export:
        with open("output.csv", "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=export.columns.names)
            writer.writeheader()
            writer.writerows(export.results())

Or very similar with :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>`:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        result = cmd.execute_one("select * from dbc.dbcinfo")
        with open("output.csv", "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=result.columns.names)
            writer.writerows(result.items())

Here is a way to output a stream of JSON, where each row is a JSON encoded string separated by newline characters:

.. code-block:: python

    with giraffez.Export('database.table_name', encoding='json') as export:
        with open("output.json", "w") as f:
            for row in export.results():
                f.write(row)
                f.write("\n")


Session context
---------------

All statements executed within a ``with`` context block happen within the same Teradata session.  This is particularly useful when dealing with database objects like volatile tables where the table itself is scoped to the session and will disappear once disconnected.  Here is an example of using this session context to create a volatile table and then load it with :class:`giraffez.Load <giraffez.load.TeradataLoad>`:

.. _session-context-load-insert:

.. code-block:: python
   
    with giraffez.Load() as load:
        load.execute("""create multiset volatile table temp_table (
                first_name varchar(50),
                last_name varchar(50),
                email varchar(100)
            ) primary index (first_name, last_name) on commit preserve rows""")

        load.insert('temp_table', [('bruce', 'wayne', 'batman@wayne.co')])
        load.insert('temp_table', [
                ('peter', 'parker'),
                ('clark', 'kent')
            ], fields=['first_name', 'last_name'])
        result = load.execute("select * from temp_table")
   
.. code-block:: python

    >>> print(result)
    {'first_name': 'clark', 'last_name': 'kent', 'email': None}
    {'first_name': 'peter', 'last_name': 'parker', 'email': None}
    {'first_name': 'bruce', 'last_name': 'wayne', 'email': 'batman@wayne.co'}

There are some exceptions to this when dealing with :class:`giraffez.Export <giraffez.export.TeradataExport>` or :class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` since they both use the :ref:`Teradata Parallel Transporter API <teradata-libraries>`, which creates multiple sessions to parallelize the bulk operations.


Exception handling
------------------

In cases where :class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` raises a :class:`GiraffeEncodeError <giraffez.errors.GiraffeEncodeError>` and panics, the release of the table lock can be ensured by wrapping the :class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` code with a try-except that calls :class:`release <giraffez.mload.TeradataMLoad.release>`:

.. code-block:: python

   with giraffez.MLoad("database.table_name") as mload:
       try:
           mload.columns = ["last_name", "first_name"]
           rows = [
               ("Hemingway", "Ernest"),
               ("Chekhov", "Anton"),
               ("Kafka", "Franz")
           ]
           for row in rows:
               mload.load_row(row)
           exit_code = mload.finish()
       except giraffez.GiraffeEncodeError as error:
           mload.release()
           # re-raise the error so that it produces the full traceback
           raise error

.. _archiving:

Archiving tables
----------------







giraffez can be used to store tables outside of Teradata, for archival purposes or simply in order to save space. In order to optimize the speed of these operations, the size of the archived files, and the retention of original data without alteration, the original Teradata binary encoding of rows is used alongside special binary headers and row indicators which preserve the format and schema of the data.

To export a table using the archive output format::

    giraffez export database.table_name database.table_name.gd --archive

This is equivalent to specifying the encoding as 'archive' (``-e archive``) when exporting. Note that because no decoding of the row data is necessary this process is very fast.

To further reduce the size of the resulting archive file, the ``-z`` (``--gzip``) option can be used::

    giraffez export database.table_name database.table_name.gd.gz -az

In order to read an archive file as a delimited text file, use giraffez :ref:`fmt-command`::

    giraffez fmt database.table_name.gd > database.table_name.txt

Loading a file stored in the giraffez archive format is just as easy::

    giraffez mload database.table_name.gd database.another_table_name

Note that because the giraffez archive format is recognized by the ``mload`` module automatically, it is not necessary to specify the encoding of the input file.

Generating config/key files
---------------------------

Special circumstances may require generating config and/or key files via the API.  Generating a new default configuration file can be done by:


.. code-block:: python

    from giraffez import Config

    Config.write_default(".girafferc")

and creating a new encryption key can be achieved with:

.. code-block:: python

    from giraffez.encrypt import create_key_file
    create_key_file(".giraffepg")

While giraffez does look by default for these files in `$HOME/{.girafferc,.giraffepg}` there is no particular naming scheme for these files being enforced and the path specified when creating these files can be someting else completely.
