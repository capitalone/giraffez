Quickstart
==========

Getting started with giraffez begins with ensuring that the giraffez package is imported:

.. code-block:: python

    >>> import giraffez


Executing statements
--------------------

Executing statements is a fundamental part of interacting with Teradata and giraffez makes use of the :ref:`Teradata CLIv2 <teradata-libraries>` to provide this critical functionality.  This includes any tasks that are normally executed through oficial Teradata tools like SQL Assistant or BTEQ.

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
    {'infodata': '15.00.05.01c', 'infokey': 'VERSION'}
    {'infodata': '15.00.05.01', 'infokey': 'RELEASE'}
    {'infodata': 'Standard', 'infokey': 'LANGUAGE SUPPORT MODE'}


It is important to note that :meth:`execute <giraffez.cmd.TeradataCmd.execute>` executes multiple statements in parallel by default.  This is helpful when needing to accomplish actions like running multiple insert statements at the same time, or returning results from two or more select statements.  Taking advantage of this only requires giving :meth:`execute <giraffez.cmd.TeradataCmd.execute>` multiple statements separated by a semi-colon:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        results = cmd.execute("""
            select * from dbc.dbcinfo;
            select count(*) as total from dbc.dbcinfo;
        """)

.. code-block:: python

    >>> for result in results:
    ...     for row in result:
    ...         print(row)
    ...
    {'infodata': '15.00.05.01c', 'infokey': 'VERSION'}
    {'infodata': '15.00.05.01', 'infokey': 'RELEASE'}
    {'infodata': 'Standard', 'infokey': 'LANGUAGE SUPPORT MODE'}
    {'total': 3}

This behavior of Teradata is the reason why :meth:`execute <giraffez.cmd.TeradataCmd.execute>` returns a :class:`Results <giraffez.types.Results>` object instead of a single :class:`Result <giraffez.types.Result>`.  The :class:`Results <giraffez.types.Results>` provies a convenient method for retreiving only the first result (:meth:`one <giraffez.types.Results.one>`) or :meth:`execute_one <giraffez.cmd.TeradataCmd.execute_one>` can be used instead:

.. code-block:: python

    with giraffez.Cmd() as cmd:
        result = cmd.execute_one("select * from dbc.dbcinfo")
    
.. code-block:: python

    >>> for row in result:
    ...     print(row)
    ...
    {'infodata': '15.00.05.01c', 'infokey': 'VERSION'}
    {'infodata': '15.00.05.01', 'infokey': 'RELEASE'}
    {'infodata': 'Standard', 'infokey': 'LANGUAGE SUPPORT MODE'}

The column information can be accessed easily via the :class:`Columns <giraffez.types.Columns>` object:

.. code-block:: python

    >>> result.columns
    Column(infokey varchar(60) N X(30))
    Column(infodata varchar(32768) Y X(16384))


Sometimes you will want to run multiple queries synchronously and that can be performed easily by using :meth:`execute_many <giraffez.cmd.TeradataCmd.execute_many>` instead of :meth:`execute <giraffez.cmd.TeradataCmd.execute>` or :meth:`execute_one <giraffez.cmd.TeradataCmd.execute_one>`.


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
       for row in export.results():
           print(row)


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
