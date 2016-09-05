Python API
==========

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

giraffez modules
----------------

giraffez.cmd module
###################

.. automodule:: giraffez.cmd
    :members:

giraffez.export module
######################

.. automodule:: giraffez.export
    :members:

giraffez.config module
######################

.. automodule:: giraffez.config
    :members:

giraffez.load module
####################

.. automodule:: giraffez.load
    :members:

giraffez.mload module
#####################

.. automodule:: giraffez.mload
    :members:

giraffez.types module
#####################

.. automodule:: giraffez.types
    :members:

giraffez.encoders module
########################

.. automodule:: giraffez.encoders
    :members:

giraffez.errors module
######################

.. automodule:: giraffez.errors
    :members:
    :undoc-members:


API Usage Examples
------------------

In all of the following examples, assume :code:`import giraffez` is present in the
script before the snippet being run.

Export
######

Export a table/view:

.. code-block:: python

   with giraffez.Export('dbc.dbcinfo') as export:
       for row in export.results():
           print(row)

Export a table and silence log output:

.. code-block:: python

   with giraffez.Export('dbc.dbcinfo', log_level=giraffez.SILENCE) as export:
       for row in export.results():
           print(row)

Export a query, print header, and change delimiter:

.. code-block:: python

   with giraffez.Export('select * from dbc.dbcinfo', delimiter=',') as export:
       print(export.header)
       for row in export.results():
           print(row)

Write results as a CSV (quoting fields):

.. code-block:: python

   with giraffez.Export('database.table_name') as export:
       with open("output.csv", "wb") as f:
           header = ",".join(export.columns.names)
           f.write("{0}\n".format(header))
           for row in export.results():
               fields = ['"{0}"'.format(x) for x in row.split("|")]
               f.write("{0}\n".format(",".join(fields)))

Write results as JSON:

.. code-block:: python

   with giraffez.Export('database.table_name', encoding='json') as export:
       with open("output.json", "wb") as f:
           # Each row is a JSON encoded string
           for row in export.results():
               f.write("{0}\n".format(row))

Export data directly into pandas:

.. code-block:: python

   with giraffez.Export() as export:
       export.encoding = "dict"
       export.query = "select infokey, character_length(infodata) from dbc.dbcinfo"
       df = pd.DataFrame(export.results())
       print(df)
       print(df.mean())

Cmd
###

Simple select statement:

.. code-block:: python
   
   with giraffez.Cmd() as cmd:
       cmd.execute("select * from dbc.dbcinfo")

Execute multiple SQL statements at a time:

.. code-block:: python

   with giraffez.Cmd() as cmd:
       # Or cmd.execute_many(...)
       results = cmd.execute("""
           select * from dbc.dbcinfo;
           select * from dbc.tables;
       """)

       # Loop through results

       for result in results:
           print("Query Results:")
           for row in result:
               print(row)

       # First result of first query

       print(results.one().first())
       
.. code-block:: python

   with giraffez.Cmd() as cmd:
       result = cmd.execute_one("select * from dbc.tables")

       # Result stores the columns as a ColumnList object

       print(result.columns)

       # Iterate and inspect column/column types

       for column in result.columns:
           print(column, column.type)


Results of queries are Python objects (i.e. :class:`datetime`, :class:`int`, :class:`str`):

.. code-block:: python

   import datetime

   with giraffez.Cmd() as cmd:
       result = cmd.execute_one("select max(load_dt) from dbc.dbcinfo") # or execute(...).one()

       # Get the first row from the Result object

       row = result.first() 
       time_since_latest = datetime.datetime.now() - row[0] 
       print(time_since_latest)
       # 7 days, 15:05:26.236456

       result = cmd.execute("select field, count(*) from database.table_name group by field")

       # Further aggregation

       total = 0
       for row in result:
           print(row[0], row[1])
           total += row[1]
       print("Total: {0}".format(total))

.. code-block:: python

   with giraffez.Cmd() as cmd:
       result = cmd.execute_one("select top 50 * from database.table_name")
       with open("output.csv", "wb") as f:
           header = ",".join(result.columns.names)
           f.write("{0}\n".format(header))
           for row in result:
               fields = ['"{0}"'.format(x) for x in row]
               f.write("{0}\n".format(",".join(fields)))

In the same ``with`` context block, the same connection to Teradata is used.
That means that if you are operating on volatile tables, you must perform all
queries within the same "session":

.. code-block:: python

   with giraffez.Cmd() as cmd:
       cmd.execute("create volatile table table_name (id INTEGER, name VARCHAR(30)) with data on commit preserve rows")
       # ...

       # Same session as "create volatile table" statement
       cmd.execute("select * from table_name")


   # In a separate session, volatile table will not exist
   with giraffez.Cmd() as cmd:
       cmd.execute("select * from table_name") # raises ObjectDoesNotExist

Some statements in Teradata SQL (e.g. create table, drop table, collect stats) are not allowed to follow others in the same
command. When a query should be split by statement and each executed independently within the same session, use the
:meth:`~giraffez.Cmd.execute_many` method:

.. code-block:: python
   
   with giraffez.Cmd() as cmd:
       results = cmd.execute_many("""create volatile table table_name (
           id           INTEGER,
           first_name   VARCHAR(30),
           last_name    VARCHAR(30)
       ) on commit preserve rows;
       -- Comments are stripped
       insert into table_name (first_name, last_name) values ('peter', 'parker');
       insert into table_name (first_name, last_name) values ('bruce', 'wayne');

       select id, first_name from table_name;""")

       # All results are returned. The query was 4 statements, 
       #  so 4 giraffez.Result objects are returned.
       for result in results:
           print result

This is equivalent to manually separating the statements in your query and
running them using :meth:`~giraffez.Cmd.execute` within the same session (``with``
context). **Note that** :meth:`~giraffez.Cmd.execute` **sends the query to be executed
to Teradata in "blocks", rather than statements, which is why certain statements
are not allowed among others when using this method.**


Load
####

:class:`giraffez.Load` is a subclass of :class:`giraffez.Cmd`.

Load into a table (quickly!) from a file:

.. code-block:: python

   with giraffez.Load() as load:
       stats = load.from_file('database.table_name', 'my_data.txt')
       print(stats)

Create a temp table, insert from Python:

.. code-block:: python
   
   with giraffez.Load() as load:
       load.execute("""create multiset volatile table temp_table (
           first_name varchar(50),
           last_name varchar(50),
           email varchar(100)
       ) primary index (first_name, last_name) on commit preserve rows""")
   
       # Insert into all columns

       stats = load.insert('temp_table', [('bruce', 'wayne', 'batman@wayne.co')])
       print(stats)

       # Insert into a subset of columns

       stats = load.insert('temp_table', [
               ('peter', 'parker'),
               ('clark', 'kent')
           ], fields=['first_name', 'last_name'])
       print(stats)
   
       print(load.execute("select * from temp_table").first())


Config
######

The :class:`giraffez.Config` class is a convenient wrapper for accessing and modifying settings and credentials in your :code:`.girafferc` file.

.. code-block:: python

   with giraffez.Config() as config:
       default_connection = config.get_value("connections.default")
       print(default_connection)


Secret
######

:class:`giraffez.Secret` handles encryption and decryption of sensitive information in your :code:`.girafferc`, making it easy to set and retrieve these values programmatically -- without risking exposure of data.

Writing with :class:`giraffez.Secret` is automatically encrypted:

.. code-block:: python

   with giraffez.Secret(mode='w') as secret:
       secret.set("sso.username", "username")
       secret.set("sso.password", "password")

Call :code:`secret` with the paths to values you want to retrieve. This is an effective way to manage frequently used information that should be kept private:

.. code-block:: python

   import os
   import sys
   
   import requests

   with giraffez.Secret() as secret:
       sso_username, sso_password = secret("sso.username, sso.password")

   proxy_url = "https://{0}:{1}@proxy.example.com".format(sso_username, sso_password)
   r = requests.get("https://www.google.com", proxies={"http": proxy_url, "https": proxy_url})
   print(r)
   print(r.text)


MLoad
#####

:class:`giraffez.MLoad` is best utilized for loading large (> ~100k rows) datasets. It makes use of the TPTAPI Update driver which is generally slower than :class:`giraffez.Load` (which uses CLIv2) for small data sets, and has a larger performance impact on Teradata.

When loading a file, fields and delimiter for the input data are inferred from the header:

.. code-block:: python
   
   with giraffez.MLoad("database.table_name") as mload:
       exit_code = mload.from_file("input.txt")
       print("finished load with code {0}".format(exit_code))
   
Loading Individual rows:

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

In cases where `giraffez.MLoad` raises a `GiraffeEncodeError` and panics, the release of the table lock can be ensured by wrapping the `giraffez.MLoad` code with a try-except that calls `giraffez.MLoad.release()`:

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
