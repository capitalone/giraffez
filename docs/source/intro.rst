############
Introduction
############

Requirements
------------

Python packages

- `PyYAML <http://pyyaml.org/>`_
- `PyCrypto <https://www.dlitz.net/software/pycrypto/>`_
- *(optional)* `ujson <https://github.com/esnme/ultrajson>`_ (See the :ref:`FAQ <ujson>` for more information)

Third-party libraries

- `Teradata Call-Level Interface Version 2 <http://downloads.teradata.com/download/connectivity/teradata-cliv2-for-linux>`_ (CLIv2)
- `Teradata Parallel Transporter API <https://developer.teradata.com/tools/articles/teradata-parallel-transporter/teradata-parallel-transporter-1-basics>`_ (TPT API)

Features of giraffez like ``giraffez config`` and ``giraffez secret`` do not rely on either of the Third-party libraries, however, to reduce complexity of the package installation, all of the requirements must be installed and configured for any giraffez features.  This was mostly due to how pip works when installing a package with optional logical components.  Pip silences the installation output and only shows the output should an exception be encountered during the installation process.  This was creating a lot of confusion for users so the decision was made that all dependencies should be met when installing giraffez.

More information on these libraries can be found :ref:`here <teradata-libraries>`.

If installing giraffez from a `Python Wheel <http://pythonwheels.com/>`_ there are some additional requirements:

- `pip <https://pip.pypa.io/en/stable/>`_ >= 8.1.2
- `wheel <http://pythonwheels.com/>`_ >= 0.29.0

Some other requirements may be necessary if building from source:

- Python development headers or static or lib
- C/C++ compiler (tested with GCC and MSVC)
- Teradata CLIv2 and TPT API headers/lib files


Environment
-----------

giraffez attempts to find the required Teradata libraries automatically during install but can be manually specified by setting the ``TERADATA_HOME`` environment variable to the desired path.

The ``TERADATA_HOME`` variable is only necessary during installation and only if the required Teradata libraries are not present in the default install directories.  Some users experience problems after a successful installation due to missing environment variables.  This is due to the fact that giraffez can install using the default locations to find and compile the giraffez C extensions, however, the Python runtime relies on environmental settings to load dynamically loaded shared libraries and this takes place before the defaults can be added by a Python package (i.e. giraffez can't add the default paths/settings to Python at runtime to find these libraries).  This is generally addressed by ensuring that the correct environment variables are set for the respective platforms.

For Windows, the locations for `TELAPI.lib` and `opermsgs.dll` must be in the Windows PATH variable.

For Unix-like platforms, the path that contains `libtelapi.so` must be added to the ``LD_LIBRARY_PATH`` environment variable, and the path containing `opermsgs.cat` must be added to the ``NLS_PATH`` environment variable with ``%N`` placed at the end.

For all other issues installing and configuring Teradata libraries, use the documents provided by `Teradata Information <http://www.info.teradata.com/>` or by engaging with the `Teradata Community <https://community.teradata.com>`.


Install / Update
----------------

To install or update via pip::

   pip install --upgrade giraffez

To install from source::

   git clone https://github.com/capitalone/giraffez
   cd giraffez
   python setup.py install

Bash auto-completion
--------------------

A bash auto-complete script is provided that can be installed fairly easily into the system by copying it into the bash-completion folder::

   cp giraffez-completion.bash /etc/bash_completion.d/


It will be installed the next time you open a new terminal or if you want to activate in the current session::

   source ~/.bashrc


Note that this requires the use of bash as your shell. It will most likely work with other shells with little modification but has not been tested. Additionally, this puts the completion script in the system's completion folder, but can be used locally by simply sourcing the file (i.e. ``source <path>/giraffez-completion.bash``) in your shell's rc file.


Compatibility and Supported Platforms
-------------------------------------

While we do not expressly support any particular platform (Windows, Linux, Mac, etc), the focus on compatibility means that giraffez will work on every platform that also runs Python.

giraffez consists of two components: a Python package and a Python C/C++ extension. The Python package is single-source Python2/3 compatible (i.e. the same Python code works across all major Python versions). The C/C++ extension is C90/C++98 ISO compliant and therefore should be compatible with a wide array of system platforms and architectures. Compatibility is obviously an important consideration for giraffez, however, it does rely heavily on proprietary pre-compiled libraries provided by Teradata. This may complicate getting started with giraffez due to the availability of these libraries and relative difficulty for installing/configuring correctly on different platforms. Read more about these libraries :ref:`here <teradata-libraries>`.
