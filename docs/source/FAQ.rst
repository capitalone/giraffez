.. _faq:

Frequently Asked Questions
==========================

Why the name giraffez?
----------------------

The original intention was to create a C++ command-line tool that quickly *dumped* a Teradata table to a file. The first name was "Teradumps" (which is an awful name), for Teradata and "dump-s" (aka "dump to string"). When trying to think up a better name, we found ourselves thinking, "Hadoop has an elephant, what's another cool animal? How about a giraffe?". And, thus, GiraffeDumps was the name at the time. Later as more functionality was added to the package it was decided to come up with a final name that better represents the project (it moved beyond just dumping from Teradata), and the name giraffez was where we landed.

.. _teradata-libraries:

What are the Teradata Call-Level Interface Version 2 and Teradata Parallel Transporter API and how do I get them?
-----------------------------------------------------------------------------------------------------------------

.. glossary::

    *Teradata Call-Level Interface Version 2 (CLIv2)*
        The official Teradata C API, which provides an interface for communication with a Teradata server. This library is the same used by Teradata to create all their client-side tools. It is supported on most major platforms of Windows, Linux, Unix, and Unix-like operation systems (Mac OS) and supports processor architectures for x86 (32-bit and 64-bit), Power PC, Itanium 2, PA-RISC, and Opteron.

    *Teradata Parallel Transporter API (TPT API)*
        Official Teradata library that exposes many common and simplified interfaces to Teradata via C++. This library is built upon the CLIv2 and gives support for bulk operations such as Export and Load.



While these libraries have wide support, the platform-specific installation of these can vary wildly and be problematic. Since these are dependencies for almost all giraffez features, it means that these will need to be installed and configured correctly for giraffez to be able to work properly. If having trouble with these contact your system administrator and/or checkout the Teradata support forums. Teradata is a commercial product only, and these drivers are both under proprietary license, so getting these drivers may not be available outside of a relationship with Teradata.

The CLIv2 is offered for free for Windows, Linux, HP-UX, AIX, and Solaris via http://downloads.teradata.com (login required) and is bundled in the Teradata Tools and Utilities for Windows download. The TPT API is only available with the Teradata Tools and Utilities for Windows, and is distributed for other platforms via their `paid support portal <https://tays.teradata.com>`_ and via the media provided with the purchased server software. The TTU packages are not publicly offered for Linux or MacOS, however, both libraries *should* be available in this package (unsure if TPT was included in packages prior to 15.00).

.. _ujson:

Why is ujson an optional requirement?
-------------------------------------

When performing an Export to json, the ujson package is much faster than Python's standard library json. Python's json package is much slower because it provides a much more complete json marshaling package, whereas ujson is sufficiently complete for the purpose of creating well-formed json from this type of data.

.. _export-header:

Why are the column aliases incorrect when using Export?
-------------------------------------------------------

A regression was introduced after version 13.10 of the Teradata Parallel Transporter API that causes inconsistent behavior with column aliases in Export operations.  When dealing with columns that are aliased the Teradata library does not return the name specified in the alias.  It seemed that when dealing with columns that are aliased that it would instead return the underlying column name or when dealing with computed fields returning a blank string.

This affects any part of giraffez that makes use of the :class:`giraffez.Export <giraffez.export.TeradataExport>` class (it does not affect `giraffez.Cmd <giraffez.cmd.TeradataCmd>`).  Unfortunately, this is rooted in the proprietary Teradata libraries and cannot be fixed within giraffez, so the recommendation is to simply hardcode the column names with the query in those cases.
