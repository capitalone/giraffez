.. image:: https://img.shields.io/pypi/v/giraffez.svg
     :target: https://pypi.python.org/pypi/giraffez
     :alt: PyPi
.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
     :target: https://www.apache.org/licenses/LICENSE-2.0
     :alt: License

|

.. image:: https://raw.githubusercontent.com/capitalone/giraffez/master/docs/logo.png

Giraffez is a client library for `Teradata <http://www.teradata.com/>`_, made to be fast and user-friendly. It shares many features with the official Teradata tools, but makes them available in an easy-to-use command-line interface and Python API.


Features
########

- `Export <http://capitalone.io/giraffez/command-line.html#export>`_ from Teradata to a flatfile or archival format, retaining data type information
- `Load <http://capitalone.io/giraffez/command-line.html#load>`_ into existing Teradata tables using the CLIv2 drivers
- `MultiLoad <http://capitalone.io/giraffez/command-line.html#mload>`_ into existing Teradata tables using the Teradata PT Update (MLOAD)
- `Execute <http://capitalone.io/giraffez/command-line.html#cmd>`_ SQL statements using the Teradata CLIv2 library -- performing ad hoc queries and commands
- `Interactive shell <http://capitalone.io/giraffez/command-line.html#shell>`_ for executing Teradata queries -- much like BTEQ, but with a friendly interface
- `Store <http://capitalone.io/giraffez/command-line.html#config>`_ your connection information and sets of credentials securely and use them automatically
- `Run <http://capitalone.io/giraffez/command-line.html#run>`_ job files in YAML format
- `Python API <http://capitalone.io/giraffez/api.html#giraffez-modules>`_ provides a convenient and efficient way to interface with Teradata
- `Archive <http://capitalone.io/giraffez/command-line.html#archiving>`_ Teradata tables to save space. These can easily be `transformed to flatfiles <http://capitalone.io/giraffez/command-line.html#fmt>`_ or `loaded <http://capitalone.io/giraffez/command-line.html#mload>`_ back into Teradata
- `Secret <http://capitalone.io/giraffez/command-line.html#secret>`_ Manage encrypted credentials securely using the ``giraffez secret`` and ``giraffez config`` commands

Links
#####

- `Documentation <http://capitalone.io/giraffez>`_

Quick Install
#############

::

  $ pip install giraffez
  
See the `setup instructions <http://capitalone.io/giraffez/intro.html#giraffez-setup>`_ for requirements and details on how to configure your environment.

Contributors
############

We welcome Your interest in Capital One’s Open Source Projects (the
“Project”). Any Contributor to the Project must accept and sign an
Agreement indicating agreement to the license terms below. Except for
the license granted in this Agreement to Capital One and to recipients
of software distributed by Capital One, You reserve all right, title,
and interest in and to Your Contributions; this Agreement does not
impact Your rights to use Your own Contributions for any other purpose

`Download the Agreement <https://docs.google.com/forms/d/e/1FAIpQLSfwtl1s6KmpLhCY6CjiY8nFZshDwf_wrmNYx1ahpsNFXXmHKw/viewform>`_

Code of Conduct
###############

This project adheres to the `Open Code of Conduct <http://www.capitalone.io/codeofconduct/>`_ By participating, you are
expected to honor this code.
