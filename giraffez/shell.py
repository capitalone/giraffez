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

from __future__ import absolute_import

import cmd as _cmd
import os
import sys
import time
import atexit
import itertools
import functools
import threading

from .constants import *
from .errors import *

from ._teradata import TeradataError
from .fmt import format_table, replace_cr
from .logging import log, setup_logging
from .utils import get_version_info, timer


setup_logging()


class Spinner(threading.Thread):
    def __init__(self):
        super(Spinner, self).__init__()
        self._stop_event = threading.Event()
        self._ended = threading.Event()
        self._lock = threading.Lock()

    def run(self):
        spinner = itertools.cycle(["-", "/", "|", "\\"])
        while True:
            sys.stderr.write(next(spinner))
            time.sleep(0.1)
            if self.stopped:
                sys.stderr.flush()
                sys.stderr.write("\b")
                break
            sys.stderr.flush()
            sys.stderr.write("\b")
        self._ended.set()

    @property
    def running(self):
        return self._ended.isSet()

    def stop(self):
        self._stop_event.set()

    @property
    def stopped(self):
        return self._stop_event.isSet()


def spinner(f):
    @functools.wraps(f)
    def wrapper(*args, **kwds):
        spinner = Spinner()
        spinner.start()
        try:
            result = f(*args, **kwds)
            spinner.stop()
            spinner.join()
        except Exception as error:
            spinner.stop()
            spinner.join()
            raise error
        return result
    return wrapper

def save_history():
    import os
    # TODO(chris): doesn't work on windows?
    import readline
    path = os.path.expanduser("~/.giraffehistory")
    readline.write_history_file(path)


class GiraffeShell(_cmd.Cmd):
    """Interactive shell for giraffez"""

    prompt = "giraffez> "
    intro = "giraffez-shell version: {}".format(get_version_info()[1])
    table_output = True

    start_chars = ("(", "{")
    end_chars = (")", "}")

    help_commands = ("help session", "help table", "help column", "help view")

    def __init__(self, cmd):
        import readline
        _cmd.Cmd.__init__(self)
        path = os.path.expanduser("~/.giraffehistory")
        if os.path.exists(path):
            readline.read_history_file(path)
        atexit.register(save_history)
        self.cmd = cmd

    @timer
    @spinner
    def command(self, query):
        return self.cmd.execute(query)

    def complete_table(self, text, line, begidx, endidx):
        table_values = ["on", "off"]
        completions = [f for f in table_values if f.startswith(text)]
        return completions

    def default(self, line):
        if line.startswith(".help"):
            line = line[1:]
        try:
            sc = sum([line.count(c) for c in self.start_chars])
            ec = sum([line.count(c) for c in self.end_chars])
            if sc != ec:
                while True:
                    q = input("       > ")
                    if not q:
                        break
                    line += q
                    sc = sum([line.count(c) for c in self.start_chars])
                    ec = sum([line.count(c) for c in self.end_chars])
                    if sc == ec:
                        break
        except KeyboardInterrupt:
            sys.stderr.write("\n")
            return
        try:
            result = self.command(line)
        except TeradataError as error:
            log.write("{0!r}".format(error))
            return
        if not result:
            log.write("No results")
            return
        if self.table_output and not (line.lower().startswith("show") or line.lower().startswith("help")):
            log.write()
            rows = [replace_cr(row) for row in result]
            rows.insert(0, result.columns.names)
            sys.stdout.write(format_table(rows))
            sys.stdout.write("\n")
        else:
            for row in result:
                log.write("\t".join([str(replace_cr(x)) for x in row]))

    def do_EOF(self, line):
        return True

    def do_exit(self, line):
        sys.exit()

    def do_horse(self, line):
        """It totally prints a horse."""
        log.write(ascii_giraffe)

    def do_table(self, line):
        """Display results in table format"""
        if len(line) > 0:
            if line.strip().lower() == "on":
                log.write("Table ON")
                self.table_output = True
                return
            elif line.strip().lower() == "off":
                log.write("Table OFF")
                self.table_output = False
                return
        log.write("Table output: {}".format("ON" if self.table_output else "OFF"))

    def precmd(self, line):
        if line.strip().lower().startswith(self.help_commands):
            line = ".{}".format(line)
        return _cmd.Cmd.precmd(self, line)
