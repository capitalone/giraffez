# -*- coding: utf-8 -*-

import sys
import json
import time
import argparse
import itertools
import subprocess
import shlex

import yaml

try:
    from . import _encoder
except ImportError:
    pass

from .config import *
from .constants import *
from .cmd import *
from .encoders import *
from .encrypt import *
from .errors import *
from .export import *
from .fmt import *
from .io import *
from .logging import *
from .load import *
from .mload import *
from .parser import *
from .shell import *
from .sql import *
from .types import *
from .utils import *

from ._compat import *


setup_logging()

class CmdCommand(Command):
    name = "cmd"
    description = "Execute Teradata commands"
    usage = "giraffez cmd <query> [options]"
    help = "Execute commands in Teradata (using TDCLIV2)"

    arguments = [
        Argument("query", help="Query command (string or file)"),
        Argument("-t", "--table-output", default=False, help="Print in table format"),
    ]

    def run(self, args):
        file_name = None
        if " " not in args.query:
            if file_exists(args.query):
                file_name = args.query
                args.query = FileReader.read_all(file_name)
        start_time = time.time()
        with TeradataCmd(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as cmd:
            with Writer() as out:
                statements = parse_statement(args.query)
                if file_name:
                    cmd.options("source", file_name)
                cmd.options("table_output", args.table_output)
                for s in statements:
                    start_time = time.time()
                    result = cmd.execute_one(s)
                    if out.is_stdout:
                        log.info(colors.green(colors.bold("-"*32)))
                    if not result:
                        log.info("Results", "{} rows in {}".format(0, readable_time(time.time() - start_time)))
                        continue
                    if args.table_output and not (args.query.lower().startswith("show") or \
                            args.query.lower().startswith("help")):
                        result.rows.insert(0, result.columns.names)
                        out.writen("\r{}".format(format_table(result.rows)))
                    else:
                        for row in result:
                            out.writen("\t".join([str(x) for x in row]))
                    if out.is_stdout:
                        log.info(colors.green(colors.bold("-"*32)))
                    log.info("Results", "{} rows in {}".format(len(result), readable_time(time.time() - start_time)))


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
                log.write(MESSAGE_WRITE_DEFAULT.format(args.conf))
            else:
                log.write(colors.fail("Was not successful"))
            create_key_file(args.key)
            log.write("Key file '{}' created successfully.".format(args.key))
        elif args.get is not None:
            key = args.get
            with Config(args.conf) as c:
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
            with Config(args.conf) as c:
                log.write(c.list_value(args.decrypt))
        elif args.set is not None:
            key, value = args.set
            with Config(args.conf, "w") as c:
                c.set_value(key, value)
                c.write()
        elif args.unset is not None:
            with Config(args.conf, "w") as c:
                c.unset_value(args.unset)
                c.write()
        elif args.unlock is not None:
            Config.unlock_connection(args.conf, args.unlock)


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
        Argument("-e", "--encoding", default=DEFAULT_ENCODING, help="Export encoding"),
        Argument("-d", "--delimiter", default=DEFAULT_DELIMITER, help="Text delimiter"),
        Argument("-n", "--null", default=DEFAULT_NULL, help="Set null character"),
        Argument("--no-header", default=False, help="Do not prepend header"),
    ]

    def run(self, args):
        args.delimiter = unescape_string(args.delimiter)
        if args.encoding == "archive" or args.archive:
            if not args.output_file:
                log.write("An output file must be specified when using archive encoding")
                return
            args.encoding = "archive"
            args.archive = True
            args.no_header = False 
        elif args.encoding == "json":
            args.no_header = True

        with TeradataExport(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as export:
            if args.delimiter is not None:
                export.delimiter = args.delimiter
            if args.null is not None:
                export.null = args.null
            export.encoding = args.encoding
            export.query = args.query

            with Writer(args.output_file, archive=args.archive, use_gzip=args.gzip) as out:
                # Call this so the output is in the correct order rather than allowing
                # results lazily call _initiate
                export._initiate()
                export._columns = export._get_columns()
                export.options("output", out.name, 4)
                if out.is_stdout:
                    log.info(colors.green(colors.bold("-"*32)))

                if not args.no_header:
                    out.writen(export.header)

                start_time = time.time()
                i = 0
                if args.archive:
                    for n, chunk in enumerate(export.results(), 1):
                        i += _encoder.Encoder.count_rows(chunk)
                        if n % 50 == 0 and args.output_file:
                            log.info("\rExport", "Processed {} rows".format(int(round(i, -5))), console=False)
                        out.writen(chunk)
                    log.info("\rExport", "Processed {} rows".format(i))
                else:
                    for i, row in enumerate(export.results(), 1):
                        if i % 100000 == 0 and args.output_file:
                            log.info("\rExport", "Processed {} rows".format(i), console=True)
                        out.writen(row)
                    if args.output_file:
                        log.info("\rExport", "Processed {} rows".format(i))
                    if out.is_stdout:
                        log.info(colors.green(colors.bold("-"*32)))
            log.info("Results", "{} rows in {}".format(i, readable_time(time.time() - start_time)))


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
        Argument("-d", "--delimiter", help="Transform delimiter"),
        Argument("-n", "--null", help="Transform null ex. 'None to NULL'"),
        Argument("--head", const=9, nargs="?", type=int, help="Only output N rows"),
        Argument("--count", default=False, help="Count the table")
    ]

    def run(self, args):
        with Reader(args.input_file) as f:
            log.verbose("Debug[1]", "File type: ", FILE_TYPE_MAP.get(f.file_type))
            if f.file_type == DELIMITED_TEXT_FILE:
                columns = f.field_names()
            else:
                columns = f.columns()
            for column in columns:
                log.verbose("Debug[1]", repr(column))
            if args.count:
                i = 0
                for i, line in enumerate(f, 1):
                    pass
                log.info("Lines: ", i)
            else:
                processors = []
                if args.delimiter:
                    src_delimiter, dst_delimiter = args.delimiter.split(" to ", 1)
                    dst_delimiter = unescape_string(dst_delimiter)
                    if src_delimiter != f.delimiter:
                        src_delimiter = f.delimiter
                else:
                    src_delimiter = f.delimiter
                    dst_delimiter = f.delimiter
                if dst_delimiter is None:
                    dst_delimiter = DEFAULT_DELIMITER
                if f.file_type == GIRAFFE_ARCHIVE_FILE:
                    encoder = _encoder.Encoder(columns)
                    processors.append(encoder.unpack_row)
                else:
                    processors.append(text_to_strings(src_delimiter))
                if args.null:
                    src_null, dst_null = args.null.split(" to ", 1)
                    processors.append(null_handler(src_null))
                else:
                    dst_null = DEFAULT_NULL
                processors.append(python_to_strings(dst_null))
                processors.append(strings_to_text(dst_delimiter))
                processor = pipeline(processors)
                i = 0
                for i, line in enumerate(f, 1):
                    if args.head and args.head < i:
                        break
                    new_line = processor(line)
                    print(new_line)


class LoadCommand(Command):
    name = "load"
    description = "Load data into Teradata (CLIv2)"
    usage = "giraffez load <input_file> <table> [options]"
    help = "Load data to Teradata table (CLIv2)"

    arguments = [
        Argument("input_file", help="Input file"),
        Argument("table", help="Name of table"),
        Argument("-d", "--delimiter", default=None, help="Text delimiter"),
        Argument("-n", "--null", default=DEFAULT_NULL, help="Set null character"),
        Argument("--date-conversion", default=False),
        Argument("--disable-date-conversion", default=False, help=("Disable automatic conversion of "
            "dates/timestamps")),
    ]

    def run(self, args):
        if args.date_conversion:
            show_warning("The '--date-conversion' option is enabled by default", DeprecationWarning)

        if not file_exists(args.input_file):
            raise FileNotFound("File '{}' does not exist.".format(args.input_file))

        if FileReader.check_length(args.input_file, MLOAD_THRESHOLD):
            show_warning(("USING LOAD TO INSERT MORE THAN {} ROWS IS NOT RECOMMENDED - USE MLOAD "
                "INSTEAD!").format(MLOAD_THRESHOLD), UserWarning)

        date_conversion = not args.disable_date_conversion

        if args.delimiter is None:
            args.delimiter = FileReader.check_delimiter(args.input_file)

        with TeradataLoad(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as load:
            log.info("Load", "Executing ...")
            log.info('  source      => "{}"'.format(args.input_file))
            log.info('  output      => "{}"'.format(args.table))
            log.info('  delimiter   => "{}"'.format(args.delimiter))
            log.info('  null        => "{}"'.format(args.null))
            start_time = time.time()
            result = load.from_file(args.table, args.input_file, null=args.null, delimiter=args.delimiter,
                date_conversion=date_conversion)
            log.info("Results", "{} errors; {} rows in {}".format(result['errors'], result['count'], readable_time(time.time() - start_time)))


class MLoadCommand(Command):
    name = "mload"
    description = "Load data into Teradata (MultiLoad)"
    usage = "giraffez mload <input_file> <table> [options]"
    help = "Load data to existing Teradata table (MultiLoad)"

    arguments = [
        Argument("input_file", help="Input file"),
        Argument("table", help="Name of table"),
        Argument("-e", "--encoding", default=DEFAULT_ENCODING, help="Input file encoding"),
        Argument("-d", "--delimiter", default=None, help="Text delimiter [default: (inferred from header)]"),
        Argument("-n", "--null", default=DEFAULT_NULL, help="Set null character"),
        Argument("-y", "--drop-all", default=False, help="Drop tables"),
        Argument("--allow-precision-loss", default=False, help="Allow floating-point numbers to be converted to fixed-precision numbers.")
    ]

    def run(self, args):
        if args.encoding not in ["archive", "text"]:
            raise GiraffeError("'{}' is not an encoder.".format(args.encoding))
        if not file_exists(args.input_file):
            raise GiraffeError("File '{}' does not exist.".format(args.input_file))

        header = FileReader.read_header(args.input_file)
        if header == GIRAFFE_MAGIC:
            args.encoding = "archive"

        if not FileReader.check_length(args.input_file, MLOAD_THRESHOLD):
            show_warning(("USING MLOAD TO INSERT LESS THAN {} ROWS IS NOT RECOMMENDED - USE LOAD "
                "INSTEAD!").format(MLOAD_THRESHOLD), UserWarning)

        with TeradataMLoad(log_level=args.log_level, config=args.conf, key_file=args.key,
                dsn=args.dsn) as mload:
            mload.encoding = args.encoding
            mload.allow_precision_loss = args.allow_precision_loss
            mload.table = args.table
            if args.drop_all is True:
                mload.cleanup()
            else:
                existing_tables = filter_list(mload.cmd.exists, mload.tables)
                if len(existing_tables) > 0:
                    log.info("MLoad", "Previous work tables found:")
                    for i, table in enumerate(existing_tables, 1):
                        log.info('  {}: "{}"'.format(i, table))
                    if prompt_bool("Do you want to drop these tables?", default=True):
                        for table in existing_tables:
                            log.info("MLoad", "Dropping table '{}'...".format(table))
                            mload.cmd.drop_table(table)
                    else:
                        log.fatal("Cannot continue without dropping previous job tables. Exiting ...")
            log.info("MLoad", "Executing ...")
            log.info('  source      => "{}"'.format(args.input_file))
            log.info('  output      => "{}"'.format(args.table))
            start_time = time.time()
            exit_code = mload.from_file(args.input_file, delimiter=args.delimiter, null=args.null)
            log.info("MLoad", "Teradata PT request finished with exit code '{}'".format(exit_code))
            log.info("Results", "...")
            log.info('  Successful   => "{}"'.format(mload.applied_count))
            log.info('  Unsuccessful => "{}"'.format(mload.error_count))
            log.info("Results", "{} rows in {}".format(mload.total_count, readable_time(time.time() - start_time)))
            if exit_code != 0:
                # TODO: such todo
                if prompt_bool(("Executed with return code {}, would you like to query the "
                        "error table?").format(exit_code)):
                    result = mload.cmd.execute_one("select errorcode, errorfield, min(hostdata) as hostdata, count(*) over (partition by errorcode, errorfield) as errorcount from {}_e1 qualify row_number() over (partition by errorcode, errorfield order by errorcode asc)=1 group by 1,2".format(args.table))
                    if len(result) == 0:
                        log.info("No error information available.")
                        return
                    error_table = [("Error Code", "Field", "Error Count", "Error Description")]
                    samples = []
                    for row in result:
                        msg = MESSAGES.get(int(row[0]), None)
                        error_table.append((row[0], row[1], row[3], msg))
                        samples.append(repr(row[2]))
                    log.info("\n{}\n".format(format_table(error_table)))
                    log.info("Sample Row Error Data:")
                    log.info("\t{}".format("\n".join(samples)))


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
        with Config(args.conf, args.key) as c:
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
