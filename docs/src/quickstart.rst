Quickstart
==========

Getting started with giraffez begins with ensuring that the giraffez package is imported:

.. code-block:: python

    >>> import giraffez


Executing statements
--------------------

Executing statements is a fundamental part of interacting with Teradata and giraffez makes use of the :ref:`Teradata CLIv2 <teradata-libraries>` to provide this critical functionality.  This includes any tasks that are normally executed through official Teradata tools like SQL Assistant or BTEQ.

The first step to executing a statement is setting up a :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>` object using the ``with`` context.  Using ``with`` is important because it automatically closes the connection when the context ends, and ensures that open connections aren't littered all over Teradata server.  All examples going forward are going to use and expect this context so let's set it up:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        # Connection is established

        # Any code using the connection will go here at this indent level
        # since the 'with' context maintains the connection for us

    # Connection is closed when leaving context

Next, let's perform a common function of working with any database by querying a table/view in the database:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        results = cmd.execute("select * from dbc.dbcinfo")

.. code-block:: python

    >>> print(results)
    Cursor(statements=1, multi_statement=False, prepare=False, coerce_floats=True, parse_dates=False)

This returns a :class:`Cursor <giraffez.cmd.Cursor>` for the provided query and printing it shows the various options that are set for :meth:`execute <giraffez.cmd.TeradataCmd.execute>`.  Here we can see that it contains only one query statement and multiple parallel statement mode is disabled.  Reading from the cursor is fairly straight forward:

.. code-block:: python

    >>> for row in results: # the same as doing `for row in result.values()`
    ...     print(row)
    ...
    {'infodata': '15.00.05.01c', 'infokey': 'VERSION'}
    {'infodata': '15.00.05.01', 'infokey': 'RELEASE'}
    {'infodata': 'Standard', 'infokey': 'LANGUAGE SUPPORT MODE'}

It is just as easy to execute multiple synchronous statements:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        results = cmd.execute("""
            select * from dbc.dbcinfo;
            select count(*) as total from dbc.dbcinfo;
        """)

Multiple parallel statement mode is helpful when you need to run insert statements in parallel or need to execute multiple queries at the same (similar to a union but they are returned as different logical result sets).  Taking advantage of this only requires giving :meth:`execute <giraffez.cmd.TeradataCmd.execute>` multiple statements separated by a semi-colon and setting the ``multi_statement`` keyword argument:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        results = cmd.execute("""
            select * from dbc.dbcinfo;
            select count(*) as total from dbc.dbcinfo;
        """, multi_statement=True)

The two statements will execute as part of the same request, and yield from the same results iterator:

.. code-block:: python

    >>> for row in results:
    ...     print(row)
    ...
    {'infodata': '15.00.05.01c', 'infokey': 'VERSION'}
    {'infodata': '15.00.05.01', 'infokey': 'RELEASE'}
    {'infodata': 'Standard', 'infokey': 'LANGUAGE SUPPORT MODE'}
    {'total': 3}

Working with cursors
--------------------

Executing statements returns a `cursor-like object <giraffez.cmd.Cursor>` that enables traversal over the executing statements.  This works in a similar manner to other database client software that also uses a cursor, and just as with libraries like `pyodbc` one must read completely from the cursor to execute the statements and exhaust the records returned by Teradata:


.. code-block:: python

    >>> for row in results:
    ...     pass
    ...

In cases where one does not need the data but needs to make sure all statements execute, the :meth:`readall <giraffez.cmd.Cursor.readall>` method can be used:

.. code-block:: python

    >>> results.readall()
    3

The only value returned is the number of rows that were read.


Accessing statement metadata
----------------------------

Consider the following set of statements:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        results = cmd.execute("""
            select * dbc.dbcinfo;
            select * from dbc.dbcinfo;
            select count(*) as total from dbc.dbcinfo;
        """, panic=False)

The first statement will fail due to a syntax error and with panic turned off :meth:`execute <giraffez.cmd.TeradataCmd.execute>` will move onto the next statement without raising an exception.  The results end up looking like this:

.. code-block:: python

    >>> for row in results:
    ...     print(row)
    ...
    {'infodata': '15.00.05.01c', 'infokey': 'VERSION'}
    {'infodata': '15.00.05.01', 'infokey': 'RELEASE'}
    {'infodata': 'Standard', 'infokey': 'LANGUAGE SUPPORT MODE'}
    {'total': 3}


The column information can be accessed easily via the :class:`Columns <giraffez.types.Columns>` object:

.. code-block:: python

    >>> results.columns
    Column(total integer(4) N -(10)9)

However, in the case where there are more than one statement passed to :meth:`execute <giraffez.cmd.TeradataCmd.execute>` these will be the columns for the last statement to be executed successfully.  Luckily, getting the columns and other useful metadata (like if an error occured) is pretty easy:

.. code-block:: python

    >>> for i, statement in enumerate(results.statements, 1):
    ...     print("Statment[{}]:".format(i))
    ...     print(repr(statement.columns))
    ...
    Statement[1]:
    None
    Statement[2]:
    Column(infokey varchar(60) N X(30))
    Column(infodata varchar(32768) Y X(16384))
    Statement[3]:
    Column(total integer(4) N -(10)9)


Other useful metadata is available as well, such as the statement errors:

.. code-block:: python

    >>> for i, statement in enumerate(results.statements, 1):
    ...     print("Statement[{}]: {!r}".format(i, statement.error))
    ...
    Statement[1]: TeradataError('3706: Syntax error: SELECT * must have a FROM clause.')
    Statement[2]: None
    Statement[3]: None

Additionally, the error is the actual error instance so can be raised:

.. code-block:: python

    >>> for i, statement in enumerate(results.statements, 1):
    ...     if statement.error:
    ...         raise error
    Traceback (most recent call last):
      File "./example.py", line 20, in <module>
        raise statement.error
      File "../giraffez/cmd.py", line 91, in _execute
        self.conn.execute(str(statement), prepare_only=self.prepare_only)
    giraffez.TeradataError: 3706: Syntax error: SELECT * must have a FROM clause.


Loading data into a table
-------------------------

:class:`giraffez.Load <giraffez.load.TeradataLoad>` is a subclass of :class:`giraffez.Cmd <giraffez.cmd.TeradataCmd>` and it aims to provide an easier to use abstraction for loading small amounts of data.  The most straight-forward way to use it is reading delimited data from a file:

.. code-block:: python

    with giraffez.Load() as load:
        stats = load.from_file('database.table_name', 'my_data.txt')

This requires a delimited header to be provided as the first line of the file and it returns a :class:`dict` with the number of rows inserted and the number of errors encountered:

.. code-block:: python

    >>> print(stats)
    {'count': 3, 'errors': 0}

A more involved example can be found in :ref:`Advanced Usage <session-context-load-insert>` that loads the rows individually using :meth:`insert <giraffez.Load.insert>`.


Exporting large amounts of data
-------------------------------

When exporting a large amount of data from Teradata (many millions of rows), :class:`giraffez.Export <giraffez.export.TeradataExport>` should be used.  It makes use of the bulk driver provided by the :ref:`Teradata Parallel Transporter API <teradata-libraries>` to retrieve data from Teradata effificently.  This powerful export can be setup without needing to create complex fastexport scripts:

.. code-block:: python

    with giraffez.Export('dbc.dbcinfo') as export:
        for row in export.values():
            print(row)

More options are detailed over in the API referrence for :class:`giraffez.Export <giraffez.export.TeradataExport>`.


Loading large amounts of data
-----------------------------

:class:`giraffez.MLoad <giraffez.mload.TeradataMLoad>` is best utilized for loading large (> ~100k rows) datasets. It makes use of the :ref:`Teradata Parallel Transporter API <teradata-libraries>` bulk update driver which is generally faster than :class:`giraffez.Load <giraffez.load.TeradataLoad>` and has less of a performance impact on Teradata server when dealing with larger datasets.

Just like with :class:`giraffez.Load <giraffez.load.TeradataLoad>`, fields and delimiter for the input data are inferred from the header:

.. code-block:: python
   
    with giraffez.MLoad("database.table_name") as mload:
        exit_code = mload.from_file("input.txt")

And an exit code is returned, indicating the result:

.. code-block:: python

    >>> print(exit_code)
    12

The exit code is returned from :ref:`Teradata Parallel Transporter API <teradata-libraries>` and can be ``0``, ``2``, ``4``, ``8``, or ``12``.  This is intrinsic to how the driver works and any exit code other than 0 indicates some kind of failure to complete and is the same code returned when running a MultiLoad job using Teradata's ``mload`` command-line tool.

Another important feature is loading rows individually when dealing with information that isn't being read from a file:

.. code-block:: python
   
    with giraffez.MLoad("database.table_name") as mload:
        mload.columns = ["last_name", "first_name"]
        rows = [
            ("Hemingway", "Ernest"),
            ("Chekhov", "Anton"),
            ("Kafka", "Franz")
        ]
        for row in rows:
            mload.load_row(row)
        exit_code = mload.finish()
        print("finished load with code {0}".format(exit_code))
        if exit_code == 0:
            mload.cleanup()


Configuring giraffez
--------------------

The :class:`giraffez.Config <giraffez.config.Config>` class is a convenient wrapper for accessing and modifying settings and credentials in your :code:`.girafferc` file.  While most of the time these settings will be set and updated on the command-line, there may be a need to access these settings via Python:

.. code-block:: python

    with giraffez.Config() as config:
        default_connection = config.get_value("connections.default")
        print(default_connection)


However, a more likely scenario is that giraffez will also be used to store other sensitive information in the :code:`.girafferc` file.  :class:`giraffez.Secret <giraffez.secret.Secret>` can be used to handle the storage and retrieval of this information, allowing for this sensitive information to be encrypted at rest and still be easily accessible:

.. code-block:: python

    with giraffez.Secret(mode='w') as secret:
        secret.set("sso.username", "username")
        secret.set("sso.password", "password")

And using them in a script looks something like:

.. code-block:: python
   
    import requests

    with giraffez.Secret() as secret:
        sso_username, sso_password = secret("sso.username, sso.password")

    proxy_url = "https://{0}:{1}@proxy.example.com".format(sso_username, sso_password)
    r = requests.get("https://www.google.com", proxies={"http": proxy_url, "https": proxy_url})
    print(r.text)
