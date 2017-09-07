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

import sys
import json
import time
import argparse
import itertools
import subprocess
import shlex
import re
import io

import yaml

try:
    from . import _teradata
except ImportError:
    pass

from .constants import *
from .errors import *

from .config import Config, message_write_default
from .cmd import TeradataCmd
from .encoders import null_handler, TeradataEncoder
from .encrypt import create_key_file
from .export import TeradataBulkExport
from .fmt import format_table, replace_cr
from .io import ArchiveFileReader, FileReader, Reader, Writer, \
    file_delimiter, file_exists, home_file
from .logging import colors, log, setup_logging
from .load import TeradataBulkLoad
from .parser import Argument, Command
from .shell import GiraffeShell
from .sql import parse_statement
from .utils import pipeline, prompt_bool, readable_time, \
    register_shutdown_signal, register_graceful_shutdown_signal, \
    show_warning, timer

from ._compat import *


setup_logging()

# Registers the standard shutdown handler using C signals. Since parts
# of the giraffez code will execute without acquiring the GIL, Python's
# normal Ctrl+C behavior will not work. This signal handler attempts
# to remedy this by setting a signal handler in C that imitates the
# desired behavior. The goal is to avoid having to acquire the GIL but
# this could be changed in the future if the cost of acquiring the GIL
# in the C extension is shown to be minimal.
register_shutdown_signal()


class CmdCommand(Command):
    name = "cmd"
    description = "Execute Teradata commands"
    usage = "giraffez cmd <query> [options]"
    help = "Execute commands in Teradata (using TDCLIV2)"

    arguments = [
        Argument("query", help="Query command (string or file)"),
        Argument("-t", "--table-output", default=False, help="Print in table format"),
        Argument("-j", "--json", default=False, help="Export in json format"),
        Argument("-d", "--delimiter", default='\t', help="Text delimiter"),
        Argument("-n", "--null", default='NULL', help="Set null character"),
        Argument("--header", default=False, help="Do prepend header"),
    ]

    def run(self, args):
        if args.json:
            args.header = False
        elif args.table_output:
            args.header = True
        args.delimiter = unescape_string(args.delimiter)
        start_time = time.time()
        register_graceful_shutdown_signal()
        with TeradataCmd(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as cmd:
            cmd.options("table_output", args.table_output)
            with Writer() as out:
                result = cmd.execute(args.query, header=args.header)
                if out.is_stdout:
                    log.info(colors.green(colors.bold("-"*32)))
                if args.json:
                    for row in result.to_dict():
                        out.writen(json.dumps(row))
                elif args.table_output and not (args.query.lower().startswith("show") or \
                        args.query.lower().startswith("help")):
                    rows = [replace_cr(row) for row in result]
                    out.writen("\r{}".format(format_table(rows)))
                else:
                    for row in result:
                        out.writen(args.delimiter.join([replace_cr(str(x)) for x in row]))
                if out.is_stdout:
                    log.info(colors.green(colors.bold("-"*32)))
                count = sum([n.count for n in result.statements])
                log.info("Results", "{} rows in {}".format(count, readable_time(time.time() - start_time)))


class ConfigCommand(Command):
    name = "config"
    description = "giraffez configuration"
    usage = "giraffez config <options>"
    help = "View/Edit giraffez configuration"

    arguments = [
        Argument("-n", "--no-newline", action="store_true",
            help="Do not output a newline with value from --get"),
        Argument("-d", "--decrypt", action="store_true",
            help="Decrypt the contents of the settings when using --list"),

        Argument("--init", action="store_true",
            help="Initialize a new configuration file at [--config]", group="mutual"),
        Argument("--get", metavar="key",
            help="Retrieve the key at the given path, separating nested keys with '.'",
            group="mutual"),
        Argument("-l", "--list", action="store_true",
            help="Show the contents of the configuration file", group="mutual"),
        Argument("--set", nargs=2, metavar=("key", "value"),
            help="Set the key to the given value", group="mutual"),
        Argument("--unset", metavar="key", help="Unset the given key", group="mutual"),
        Argument("--unlock", metavar="dsn",
            help="Unlock the connection with the given DSN", group="mutual"),
    ]

    def run(self, args):
        if args.conf is None:
            args.conf = home_file(".girafferc")
        if args.key is None:
            args.key = home_file(".giraffepg")
        if args.init:
            if file_exists(args.conf):
                if not prompt_bool(("Configuration file '{}' found, would you like to overwrite "
                    "it with defaults?").format(args.conf), default=False):
                    return
            result = Config.write_default(args.conf)
            if result:
                log.write(colors.green(result))
                log.write(message_write_default.format(args.conf))
            else:
                log.write(colors.fail("Was not successful"))
            create_key_file(args.key)
            log.write("Key file '{}' created successfully.".format(args.key))
        elif args.get is not None:
            key = args.get
            with Config(conf=args.conf, key_file=args.key) as c:
                value = c.get_value(key)
            if value == -1:
                log.write("{}: not set".format(key))
            else:
                if not isinstance(value, str):
                    value = json.dumps(value, indent=4, sort_keys=True)
                if not args.no_newline:
                    value = "{}:\n{}\n".format(key, value)
                log.write(value)
        elif args.list:
            with Config(conf=args.conf, key_file=args.key) as c:
                log.write(c.list_value(args.decrypt))
        elif args.set is not None:
            key, value = args.set
            with Config(conf=args.conf, mode="w", key_file=args.key) as c:
                c.set_value(key, value)
                c.write()
        elif args.unset is not None:
            with Config(conf=args.conf, mode="w", key_file=args.key) as c:
                c.unset_value(args.unset)
                c.write()
        elif args.unlock is not None:
            Config.unlock_connection(args.conf, args.unlock, args.key)


class ExportCommand(Command):
    name = "export"
    description = "Export data from Teradata"
    usage = "giraffez export <query> [output_file] [options]"
    help = "Export data from Teradata"

    arguments = [
        Argument("query", help="Table or query to export"),
        Argument("output_file", nargs="?", help="Output file or pipe"),
        Argument("-a", "--archive", default=False, help="Export in archive format (packed binary)"),
        Argument("-z", "--gzip", default=False, help="Use gzip compression (only for archive)"),
        Argument("-j", "--json", default=False, help="Export in json format"),
        Argument("-d", "--delimiter", default="|", help="Text delimiter"),
        Argument("-n", "--null", default="NULL", help="Set null character"),
        Argument("--no-header", default=False, help="Do not prepend header"),
    ]

    def run(self, args):
        args.delimiter = unescape_string(args.delimiter)
        if args.archive:
            if not args.output_file:
                log.write("An output file must be specified when using archive encoding")
                return
            args.no_header = False
        elif args.json:
            args.no_header = True

        register_graceful_shutdown_signal()
        with TeradataBulkExport(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as export:
            export.query = args.query
            start_time = time.time()
            if args.archive:
                with Writer(args.output_file, 'wb', use_gzip=args.gzip) as out:
                    i = 0
                    for n in export.to_archive(out):
                        i += n
                        log.info("\rExport", "Processed {} rows".format(int(round(i, -5))), console=True)
                    log.info("\rExport", "Processed {} rows".format(i))
            else:
                with Writer(args.output_file, use_gzip=args.gzip) as out:
                    export.options("output", out.name, 4)
                    export.options("delimiter", escape_string(args.delimiter), 2)
                    export.options("null", args.null, 3)
                    if args.json:
                        exportfn = export.to_json
                        export.options("encoding", "json", 5)
                    else:
                        exportfn = lambda: export.to_str(args.delimiter, args.null)
                        export.options("encoding", "str", 5)
                    export._initiate()
                    if out.is_stdout:
                        log.info(colors.green(colors.bold("-"*32)))
                    if not args.no_header:
                        out.writen(args.delimiter.join(export.columns.names))
                    i = 0
                    for i, row in enumerate(exportfn(), 1):
                        if i % 100000 == 0 and args.output_file:
                            log.info("\rExport", "Processed {} rows".format(i), console=True)
                        out.writen(row)
                    if args.output_file:
                        log.info("\rExport", "Processed {} rows".format(i))
                    if out.is_stdout:
                        log.info(colors.green(colors.bold("-"*32)))
            total_time = time.time() - start_time
            log.info("Results", "{} rows in {}".format(i, readable_time(total_time - export.idle_time)))
            log.info("Results", "Time spent idle: {}".format(readable_time(export.idle_time)))
            log.info("Results", "Total time spent: {}".format(readable_time(total_time)))


class ExternalCommand(Command):
    name = "external"
    description = "Run external commands"
    usage = "giraffez external <command> [options]"
    help = "Run external shell commands from a YAML job file."

    arguments = [
        Argument("command", help="External shell command to run"),
        Argument("--silent", default=False),
        Argument("--shell", default=False),
        Argument("--panic", default=False),
    ]

    def run(self, args):
        stdout, stderr = None, None
        if args.silent:
            log.level = SILENCE
            stdout, stderr = subprocess.PIPE, subprocess.PIPE
        parsed_command = args.command if args.shell else shlex.split(args.command)
        try:
            return_code = subprocess.call(parsed_command, stdout=stdout, stderr=stderr, shell=args.shell)
        except OSError:
            return_code = 2
        log.info("Finished with return code: {}".format(return_code))
        if args.panic and return_code != 0:
            error = GiraffeError("Command '{}' finished with exit code {} and panic is set, aborting...".format(args.command, return_code))
            error.code = return_code
            raise error
        return return_code


class FmtCommand(Command):
    name = "fmt"
    description = "Formatting tools"
    usage = "giraffez fmt <input_file> [options]"
    help = "Teradata format helper"

    arguments = [
        Argument("input_file", help="Input to be transformed"),
        Argument("-r", "--regex", help="search and replace (regex)"),
        Argument("--head", const=9, nargs="?", type=int, help="Only output N rows"),
        Argument("--header", default=False, help="Output table header"),
        Argument("--count", default=False, help="Count the table")
    ]

    @timer
    def run(self, args):
        with Reader(args.input_file) as f:
            if args.count:
                log.verbose("Debug[1]", "File type: ", f)
                if isinstance(f, ArchiveFileReader):
                    columns = f.columns
                else:
                    columns = f.header
                for column in columns:
                    log.verbose("Debug[1]", repr(column))
                i = 0
                for i, line in enumerate(f, 1):
                    pass
                log.info("Lines: ", i)
                return
            elif args.head:
                for i, line in enumerate(f, 1):
                    if args.head < i:
                        break
                    sys.stdout.write(line)
                return
        with io.open(args.input_file) as f:
            if args.header:
                print(f.header)
            if args.regex:
                parts = args.regex.split('/')
                if len(parts) < 3 or parts[0] != "s":
                    raise GiraffeError("Regex pattern '{}' not recoginzed.".format(args.regex))
                pattern = re.compile(parts[1])
                processor = lambda line: pattern.sub(parts[2], line)
            i = 0
            for i, line in enumerate(f, 1):
                if args.head and args.head < i:
                    break
                sys.stdout.write(processor(line))


class InsertCommand(Command):
    name = "insert"
    description = "Insert data into Teradata (CLIv2)"
    usage = "giraffez insert <input_file> <table> [options]"
    help = "Insert data to Teradata table (CLIv2)"

    arguments = [
        Argument("input_file", help="Input file"),
        Argument("table", help="Name of table"),
        Argument("-d", "--delimiter", default=None, help="Text delimiter"),
        Argument("-n", "--null", default='NULL', help="Set null character"),
        Argument("--quote-char", default='"', help=("One-character string used to quote fields "
            "containing special characters")),
        Argument("--parse-dates", default=False, help=("Enable automatic conversion of "
            "dates/timestamps")),
        Argument("-p", "--no-panic", default=False, help="Prevents normal panic behavior on error")
    ]

    def run(self, args):
        if not file_exists(args.input_file):
            raise FileNotFound("File '{}' does not exist.".format(args.input_file))

        if FileReader.check_length(args.input_file, MLOAD_THRESHOLD):
            show_warning(("USING LOAD TO INSERT MORE THAN {} ROWS IS NOT RECOMMENDED - USE MLOAD "
                "INSTEAD!").format(MLOAD_THRESHOLD), UserWarning)

        if args.delimiter is None:
            args.delimiter = file_delimiter(args.input_file)

        register_graceful_shutdown_signal()
        with TeradataCmd(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn, panic=args.no_panic) as load:
            load.options("source", args.input_file, 0)
            load.options("output", args.table, 1)
            load.options("null", args.null, 5)
            start_time = time.time()
            result = load.insert(args.table, args.input_file, null=args.null, delimiter=args.delimiter,
                parse_dates=args.parse_dates, quotechar=args.quote_char)
            log.info("Results", "{} errors; {} rows in {}".format(result['errors'], result['count'], readable_time(time.time() - start_time)))


class LoadCommand(Command):
    name = "load"
    description = "Load data into Teradata (MultiLoad)"
    usage = "giraffez load <input_file> <table> [options]"
    help = "Load data to existing Teradata table (MultiLoad)"

    arguments = [
        Argument("input_file", help="Input file"),
        Argument("table", help="Name of table"),
        Argument("-d", "--delimiter", default=None, help="Text delimiter [default: (inferred from header)]"),
        Argument("-n", "--null", default='NULL', help="Set null character"),
        Argument("-y", "--drop-all", default=False, help="Drop tables"),
        Argument("--quote-char", default='"', help="One-character string used to quote fields containing special characters"),
        Argument("--coerce-floats", default=False, help="Allow floating-point numbers to be converted to fixed-precision numbers."),
        Argument("--parse-dates", default=False, help=("Enable automatic conversion of "
            "dates/timestamps")),
    ]

    def run(self, args):
        if args.delimiter is not None:
            args.delimiter = unescape_string(args.delimiter)
        if not file_exists(args.input_file):
            raise GiraffeError("File '{}' does not exist.".format(args.input_file))

        if not FileReader.check_length(args.input_file, MLOAD_THRESHOLD):
            show_warning(("USING MLOAD TO INSERT LESS THAN {} ROWS IS NOT RECOMMENDED - USE LOAD "
                "INSTEAD!").format(MLOAD_THRESHOLD), UserWarning)

        register_graceful_shutdown_signal()
        with TeradataBulkLoad(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as load:
            load.coerce_floats = args.coerce_floats
            load.table = args.table
            if args.drop_all is True:
                load.cleanup()
            else:
                existing_tables = list(filter(lambda x: load.mload.exists(x), load.tables))
                if len(existing_tables) > 0:
                    log.info("BulkLoad", "Previous work tables found:")
                    for i, table in enumerate(existing_tables, 1):
                        log.info('  {}: "{}"'.format(i, table))
                    if prompt_bool("Do you want to drop these tables?", default=True):
                        load.cleanup()
                    else:
                        log.fatal("Cannot continue without dropping previous job tables. Exiting ...")
            log.info("BulkLoad", "Executing ...")
            log.info('  source      => "{}"'.format(args.input_file))
            log.info('  output      => "{}"'.format(args.table))
            start_time = time.time()
            exit_code = load.from_file(args.input_file, delimiter=args.delimiter, null=args.null, quotechar=args.quote_char, parse_dates=args.parse_dates)
            total_time = time.time() - start_time
            log.info("BulkLoad", "Teradata PT request finished with exit code '{}'".format(exit_code))
            log.info("Results", "...")
            log.info('  Successful   => "{}"'.format(load.applied_count))
            log.info('  Unsuccessful => "{}"'.format(load.error_count))
            log.info("Results", "{} rows in {}".format(load.total_count, readable_time(total_time - load.idle_time)))
            log.info("Results", "Time spent idle: {}".format(readable_time(load.idle_time)))
            log.info("Results", "Total time spent: {}".format(readable_time(total_time)))
            if exit_code != 0:
                with TeradataCmd(log_level=args.log_level, config=args.conf, key_file=args.key,
                        dsn=args.dsn, silent=True) as cmd:
                    result = list(cmd.execute("select a.errorcode, a.errorfield, count(*) over (partition by a.errorcode, a.errorfield) as errorcount, b.errortext, min(substr(a.hostdata, 0, 30000)) as hostdata from {}_e1 a join dbc.errormsgs b on a.errorcode = b.errorcode qualify row_number() over (partition by a.errorcode, a.errorfield order by a.errorcode asc)=1 group by 1,2,4".format(args.table)))
                    if len(result) == 0:
                        log.info("No error information available.")
                        return
                    error_table = [("Error Code", "Field", "Error Count", "Error Description")]
                    samples = []
                    for row in result:
                        error_table.append((row[0], row[1], row[2], row[3]))
                        samples.append(repr(row[4]))
                    #log.level = args.log_level
                    log.info("\n{}\n".format(format_table(error_table)))
                    log.info("Sample Row Error Data:")
                    log.info("\t{}".format("\n".join(samples)))
                    #log.level = SILENCE


class RunCommand(Command):
    name = "run"
    description = "Run YAML job scripts"
    usage = "giraffez run <job_file> [options]"
    help = "Run giraffez job file (YAML)"

    arguments = [
        Argument("job_file", help="YAML script file"),
        Argument("definitions", default=[], nargs="*", help="Define variables used in the target job file. Arguments should be of the format <name>=<value>, and used in job files as {<name>}.')"),
    ]

    def run(self, args):
        params = {}
        for d in args.definitions:
            d = d.strip()
            if "=" in d:
                l, r = d.split("=")
                params[l] = r
            else:
                log.fatal("Invalid definition '{}': arguments must be of the format 'name=value'.".format(d))
        try:
            with open(args.job_file, "rb") as f:
                data = f.read()
        except IOError:
            log.fatal("Job file '{}' could not be read.".format(args.job_file))
        try:
            data = data.format(**params)
        except KeyError as k:
            log.fatal("Job file '{}' contained unknown format variable {}".format(args.job_file, k))
        try:
            job_config = yaml.load(data)
        except ValueError:
            job_config = False
        commands = {
            "cmd": CmdCommand,
            "config": ConfigCommand,
            "export": ExportCommand,
            "external": ExternalCommand,
            "fmt": FmtCommand,
            "load": LoadCommand,
            "run": RunCommand,
            "shell": ShellCommand,
            "secret": SecretCommand,
        }
        register_graceful_shutdown_signal()
        for job in job_config:
            job_type = job.get("type")
            if not job_type:
                log.fatal("No job")
            command = commands.get(job_type)
            if not command:
                log.fatal("No comm")
            c = command()
            default_args = c.parse_known_args()[0]
            all_args = argparse.Namespace()
            for (k, v) in default_args._get_kwargs():
                setattr(all_args, k, v)
            for k, v in job.get("settings", {}).items():
                setattr(all_args, k, v)
            c.run(all_args)


class SecretCommand(Command):
    name = "secret"
    description = "giraffez encrypted storage"
    usage = "giraffez secret <key>"
    help = "Retrieve encrypted settings"


    arguments = [
        Argument("key_value"),
        Argument("-n", default=False),
    ]

    def run(self, args):
        with Config(conf=args.conf, key_file=args.key) as c:
            if not args.key_value.startswith("secure.") and \
                    not args.key_value.startswith("connections."):
                key = "secure.{}".format(args.key_value)
            else:
                key = args.key_value
            value = c.get_value(key)
            if isinstance(value, basestring):
                sys.stdout.write(value)
                if args.n:
                    sys.stdout.write("\n")
            else:
                sys.stderr.write("Key '{}' not found\n".format(key))
                sys.exit(1)


class ShellCommand(Command):
    name = "shell"
    description = "Interactive Teradata shell"
    usage = "giraffez shell [options]"
    help = "Interactive Teradata shell"

    arguments = [
        Argument("-t", "--table-output", default=False, help="Print in table format"),
    ]

    def run(self, args):
        with TeradataCmd(log_level=SILENCE, config=args.conf, key_file=args.key, dsn=args.dsn) as cmd:
            shell = GiraffeShell(cmd)
            shell.table_output = args.table_output
            try:
                shell.cmdloop()
            except KeyboardInterrupt:
                log.fatal()
