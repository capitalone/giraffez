Commands
========

giraffez has several modules that can be accessed in Python or via the command-line script ``giraffez``. These modules can be easily accessed by running the ``giraffez`` command and following the usage help. Each module will print help relevant to the module being used.

There are certain shared options that any of the following commands will receive:

- ``-v`` or ``--verbose`` (``-vv`` for DEBUG level output) 
   sets the log output level to "verbose", for more informative feedback while executing commands
- ``-D <connection name>`` or ``--dsn <connection name>`` 
   specifies a connection from the ``.girafferc`` file to be used in place of the one designated ``default``
- ``-c <config file>`` or ``--conf <config file>`` 
   specifies the location of the configuration file to read connection information from (uses ``~/.girafferc`` if this option is not specified)
- ``-k <key file>`` or ``--key <key file>`` 
   specifies the location of the encryption key file to decrypt configuration information from (uses ``~/.giraffepg`` if this option is not specified)

**To see a list of available commands, run** ``giraffez --help``. **To view options not mentioned below for a specific command, run** ``giraffez <command> --help``.

.. _config-command:

config
------

The config module is used to view and edit the YAML configuration file that giraffez uses (aka ``.girafferc``). If this is the first time using giraffez the best way to create a new configuration file is::

    giraffez config --init

By default, this creates a new configuration file in ``$HOME/.girafferc`` with default values. Since the ``.girafferc`` file will contain sensitive information like username/password, the file permissions are set to 0600 to keep others from having access to read this file. To now view your ``.girafferc``::

    giraffez config --list

This should look something like this:

.. code-block:: yaml

   connections:
     default: db1
     db1:
       host: null
       password: null
       username: null
   verbosity: 0

giraffez uses this configuration file much like how unixodbc uses ``odbc.ini`` to define connections. To use the default db1 connection you must now set your username/password::

    giraffez config --set connections.db1.host <hostname>
    giraffez config --set connections.db1.username <username>
    giraffez config --set connections.db1.password <password>

The YAML configuration file is lazily initiated so adding new connections is as simple as just setting the values::

    giraffez config --set connections.mydb1.host db1.local
    giraffez config --set connections.mydb1.username abc123
    giraffez config --set connections.mydb1.password password123

The name of the connection is what is used when using the cmd, export, or load modules. The default is used if no DSN is passed with the ``giraffez <subcommand> -D <DSN>`` command. If you want to change the default connection::

    giraffez config --set connections.default mydb1

.. _cmd-command:

cmd
---

The ``cmd`` module is used to issue Teradata commands via the CLIv2. Originally it was needed to facilitate the load feature, but there was no reason not to make it available as a module. Using the ``cmd`` module is simple::

    giraffez cmd "select * from dbc.dbcinfo"

The option ``-t`` (or ``--table-output``) may be specified to format the result of the executed statement(s) into a table (for readability).

The ``cmd`` module is not meant to be used for high-output queries -- in those cases, the ``export`` module should be used.

.. _export-command:

export
------

The ``export`` module provides easy access to the Teradata PT Export driver. Teradata's export utility, fastexp, makes use of this but has many issues making it unsuitable for use. In addition to being unintuitive and difficult to setup, debug, and log, it has no capacity or function to actually decode the Teradata binary format it generates (leading to the use of "hack" solutions, such as casting all columns in a query to VARCHAR and concatenating them). 

giraffez utilizes the speed of fastexport (fastexp) with two additional goals: providing an easy to use interface, and decoding the packed binary row data, taking great care to preserve all data type information. 

A simple query that writes the contents of an entire table to STDOUT looks like this::

    giraffez export database.table_name

This is equivalent to writing (the argument to the ``export`` command can be either a table name, or a SQL query)::

    giraffez export "select * from database.table_name"

Further options can be specified to format the output of the command or modify its execution.

For example, if you want to write the contents of ``database.table_name`` to a tab-delimited file, this could be accomplished with::

    giraffez export database.table_name output_file.txt -d '\t'

Where ``output_file.txt`` specifies the output location (a filename), and ``-d`` specifies the delimiter. Note that because tab is a special character it has been written *escaped* as \t and quoted so that it is passed literally. If name of the output file is omitted, the result of the command will be written to standard out (from which it may be redirected, if desired.)

.. _fmt-command:

fmt
---

The ``fmt`` module provides utilities for reformatting and interacting with data, stored in flatfiles or the Giraffe Archive format (see :ref:`archiving`).

Currently, ``fmt`` can be used to read giraffez archive files by decoding their data::

    giraffez fmt table_archive.gd

This would write the contents of the archive to STDOUT. Using the ``-d`` option you can change the default delimiter '|' to output with a tab delimiter::

    giraffez fmt -d '| to \t' table_archive.gd > output.txt

Or, to change a tab-delimited file to use pipes::

    giraffez fmt -d '\t to |' data.txt > output.txt

Similarly the ``-n`` (``--null``) option can be used to transform null text in a data file. To change from 'None' to 'NULL'::

    giraffez fmt -n 'None to NULL' data.text > output.txt

.. _load-command:

load
----

The ``load`` module provides an interface to insert data into an existing Teradata table using the Teradata CLIv2 driver.

Initiating a load into an existing table can be done like so::

    giraffez load source_file.txt database.table_name

The ``load`` command necessitates that a header is present in the source file. The delimiter for the rows of the file is inferred from the content of the header, as well as the names and order of the table's target columns.  By default, delimiters within "double quotes" are ignored; use ``--quote-char`` to change the default quote character.

.. _mload-command:

mload
-----

The ``mload`` module provides an interface to insert data into an existing Teradata table using the Teradata PT API's Update driver (aka mload). It is designed to be as user-friendly as the export module by handling many of the "nuances" of mload. Much like the export module, you are not required to make a script file. 

Another important difference between giraffez and mload is that you do not need to define the input or destination schema for the target table. giraffez infers the target columns from the header of the source file you give it.

Initiating a load into an existing table can be done like so::

    giraffez mload source_file.txt database.table_name

As with ``load``, the ``mload`` command uses the data file's header to determine the columns used in the destination table.  By default, delimiters within "double quotes" are ignored; use ``--quote-char`` to change the default quote character.

During the process of a load, you may be prompted to drop existing work tables for the target table. giraffez will handle the management of these auxiliary tables for you, but by default you will be prompted for a decision before they are dropped. To automatically answer "yes" for these questions (and drop the auxilliary tables should they already exist), specify the ``-y`` (or ``--drop-all``) flag with the command::

    giraffez load source_file.txt database.table_name -y

The ``mload`` command is also used to :ref:`reload archived data <archiving>`.

.. _run-command:

run
---

The run module allows for scripted jobs via YAML formatted files. If you needed to collect statistics after a giraffez load then you could use this script:

``example_job.yml``:

.. code-block:: yaml

   - type: load 
     settings:
       table: database.table_name
       input_file: source_file.txt
   - type: cmd
     settings:
       query: "collect statistics on database.table_name column(col1, col2)"
   - type: export 
     settings:
       query: database.table_name
       encoding: text
       output_file: output_file.txt

Then:::

    giraffez run example_job.yml

For good measure we also exported the table again (because, reasons). The job files makes it easy to add giraffez commands to batch job systems (like pcron).

.. _shell-command:

shell
-----

The ``shell`` module provides an interactive shell environment similar to BTEQ, using the Teradata CLIv2 library. It is most useful for testing SQL queries and investigating or interacting with tables -- the ``shell`` module is not meant for large export or load jobs.

One use of the ``shell`` module is to write and execute several queries, and to interact with the results, all within the same session (retaining volatile tables), much like using the BTEQ prompt, or Teradata SQL Assistant.

Example usage (including the original shell prompt to distinguish input lines)::

    $ giraffez shell
    giraffez> help
    giraffez> table on
    giraffez> select * from dbc.dbcinfo
    ...

Here we are entering the giraffez shell, invoking the builtin "help" command, toggling table output "on" (can also be done by specifying ``-t`` or ``--table`` with ``giraffez shell``), and executing a SQL query.

.. _secret-command:

secret
------

The ``secret`` module is a convenient way to access sensitive data from your ``.girafferc``, such as passwords or other credentials::

    PROXY_USER="$(giraffez secret sso.username)"
    PROXY_PASS="$(giraffez secret sso.password)"
    export http_proxy='http://$PROXY_USER:$PROXY_PASS@proxy.example.com/'

When accessing settings from ``giraffez secret``, keys will first be checked relative to the root (for example, ``connections.db1.username``) and then attempted relative to the ``secure`` key -- above, the full path of the returned value is ``secure.sso.username``.

Any value set under the ``secure`` root key will be encrypted so that it can be used to protect any kind of sensitive information. While reading from ``secure`` can be accomplished from ``giraffez secret``, the values must be set by ``giraffez config`` like so::

    giraffez config --set secure.sso.username abc123
