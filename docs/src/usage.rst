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
        result = cmd.execute("select infokey, character_length(infokey) as nchars from dbc.dbcinfo")
        df = pd.DataFrame(list(result.items()))

.. code-block:: python

    >>> print(df)
                     infokey  nchars
    0                VERSION       7
    1                RELEASE       7
    2  LANGUAGE SUPPORT MODE      21

Using :class:`giraffez.Export <giraffez.export.TeradataExport>` with pandas:

.. code-block:: python

    with giraffez.Export() as export:
        export.query = "select infokey, character_length(infokey) as nchars from dbc.dbcinfo"
        df = pd.DataFrame(list(export.items()))

.. code-block:: python

    >>> print(df)
                     infokey  infokey_1
    0                VERSION          7
    1                RELEASE          7
    2  LANGUAGE SUPPORT MODE         21
    >>> print(df.mean())
    nchars    11.666667
    dtype: float64

Using :class:`giraffez.Load <giraffez.load.TeradataLoad>` with pandas:

.. code-block:: python

    with giraffez.Load("database.table_name") as load:
        load.insert("database.table_name", df.values.tolist(), fields=df.columns.tolist())

Using :class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` with pandas:

.. code-block:: python

    with giraffez.MLoad("database.table_name") as mload:
        # TODO: dont specify all columns if not necessary
        mload.columns = df.columns.tolist()
        for row in df.values.tolist():
            mload.load_row(row)

**Note:** it is extremely important when loading data to make sure that the data types you are using in Python are compatible with the Teradata data type it is being loaded into.  If the type is incorrect it can cause the mload driver to fail with error messages that are sometimes very difficult to diagnose.  It is recommended to work with a small test dataset and a copy of the table when initially creating a `giraffez.MLoad <giraffez.mload.TeradataMLoad>` based script, which will be much easier to troubleshoot.
 

Exporting to different formats
------------------------------


Using the Python standard library `csv <https://docs.python.org/3/library/csv.html>`_:

.. code-block:: python

    import csv

    with giraffez.Export('dbc.dbcinfo') as export:
        with open("output.csv", "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=export.columns.names)
            writer.writeheader()
            for row in export.values():
                writer.writerow(row)
            writer.writerows(list(export.values()))

Or very similar with :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>`:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        result = cmd.execute("select * from dbc.dbcinfo")
        with open("output.csv", "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=result.columns.names)
            for row in result.items():
                writer.writerow(row)

Here is a way to output a stream of JSON, where each row is a JSON encoded string separated by newline characters:

.. code-block:: python

    with giraffez.Export('database.table_name') as export:
        with open("output.json", "w") as f:
            for row in export.json():
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

    >>> print(list(result))
    {'first_name': 'clark', 'last_name': 'kent', 'email': None}
    {'first_name': 'peter', 'last_name': 'parker', 'email': None}
    {'first_name': 'bruce', 'last_name': 'wayne', 'email': 'batman@wayne.co'}

There are some exceptions to this when dealing with :class:`giraffez.Export <giraffez.export.TeradataExport>` or :class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` since they both use the :ref:`Teradata Parallel Transporter API <teradata-libraries>`, which creates multiple sessions to parallelize the bulk operations.


Exception handling
------------------

In cases where :class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` raises an :class:`EncodeError <giraffez.EncodeError>`, the exception be intercepted to modify the control flow.  For example, this can be particularly useful when troubleshooting an unsuccessful mload:

.. code-block:: python

    with giraffez.MLoad("database.table_name") as mload:
        mload.columns = ["last_name", "first_name"]
        rows = [
            ("Hemingway", "Ernest"),
            ("Chekhov", "Anton"),
            ("Kafka", "Franz")
        ]
        for row in rows:
            try:
                mload.load_row(row)
            except giraffez.EncoderError as error:
                # Intercept the exception to print out the row causing
                # the error
                print("Input row: ", repr(row))
                # re-raise the error so that it produces the full
                # traceback or simply continue to the next row
                raise error

This ends up giving a lot of flexibility to make helpful modifications to the behavior of the mload task that isn't available in the official mload utility.

.. _archiving:

Archiving tables
----------------

Teradata storage space can be expensive and often times data will be stored in Teradata because it needs a permanent home.  However, for infrequently used data giraffez gives the ability to safely archive very large tables to less expensive secondary storage as a giraffez archive file.  Before exporting the table you should save the table schema to flat file (so the table can be recreated later)::

    giraffez cmd "show table database.large_table" > database_large_table.sql

Now you can use :ref:`export <export-command>` to save the data to a compressed archive::

    giraffez export database.large_table database_large_table.gd.gz -az

Always try to verify the contents of the file using :ref:`fmt <fmt-command>` before removing any tables.  You can use ``--count`` to get a full count of how many rows are in the archive file and ``--head <n>`` will print the first ``n`` rows to the console.

If everything looks ok then you can `drop` your original table. All that is necessary to recreate this table later is to use :ref:`cmd <cmd-command>` to recreate the table::

    giraffez cmd database_large_table.sql

and then load the file back with :ref:`mload <mload-command>`::

    giraffez mload database.table_name.gd database.another_table_name

The :ref:`mload <mload-command>` command automatically detects the files type as a giraffez archive file so it doesn't need any additional options.

You can also archive only a portion of a large table and use `delete` to remove only the parts of the table that have been archived::

    giraffez export "select * from database.large_table where report_dt >= '2015-01-01' and report_dt < '2016-01-01'" 20150101_20160101_database_large_table.gd.gz -az
    giraffez cmd "delete * from database.large_table where report_dt >= '2015-01-01' and report_dt < '2016-01-01'"


**Note:** As always be **very** careful with any type of actions that remove valuable data.

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

While giraffez does look by default for these files in :code:`$HOME/{.girafferc,.giraffepg}` there is no particular naming scheme for these files being enforced and the path specified when creating these files can be someting else completely.

**Note:** the giraffez configuration file must have permissions 0600 set, and encryption key files must have the permissions 0400. These are applied automatically when the methods above are used.
