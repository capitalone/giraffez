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

import os
import sys
import logging
import multiprocessing

from .constants import *
from .errors import *

from ._compat import *


__all__ = ['colors', 'log', 'setup_logging']


logging_level_map = {
    SILENCE: logging.NOTSET,
    INFO: logging.INFO,
    VERBOSE: logging.DEBUG,
    DEBUG: logging.DEBUG
}

def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper

@run_once
def setup_logging():
    l = logging.getLogger("giraffez.console_logger")
    l.addHandler(logging.StreamHandler(sys.stderr))
    l.setLevel(logging.DEBUG)

logging_logger = logging.getLogger("giraffez.console_logger")

CONSOLE_HEADER = "\033[95m"
CONSOLE_BLUE = "\033[94m"
CONSOLE_GREEN = "\033[92m"
CONSOLE_GRAY = "\033[37m"
CONSOLE_WHITE = "\033[97m"
CONSOLE_WARNING = "\033[93m"
CONSOLE_ENDC = "\033[0m"
CONSOLE_FAIL = "\033[91m"
CONSOLE_BOLD = "\033[1m"
CONSOLE_UNDERLINE = "\033[4m" 
CONSOLE_DISABLED = "\033[38;5;240m" 


class colors(object):
    is_colorful = True

    @classmethod
    def colorize(cls, color, s):
        if colors.is_colorful:
            return "{}{}\033[0m".format(color, s)
        return s

    @classmethod
    def blue(cls, s):
        return cls.colorize(CONSOLE_BLUE, s)

    @classmethod
    def green(cls, s):
        return cls.colorize(CONSOLE_GREEN, s)

    @classmethod
    def gray(cls, s):
        return cls.colorize(CONSOLE_GRAY, s)

    @classmethod
    def white(cls, s):
        return cls.colorize(CONSOLE_WHITE, s)

    @classmethod
    def disabled(cls, s):
        return cls.colorize(CONSOLE_DISABLED, s)

    @classmethod
    def bold(cls, s):
        return cls.colorize(CONSOLE_BOLD, s)

    @classmethod
    def fail(cls, s):
        return cls.colorize(CONSOLE_FAIL, s)


def highlight(s):
    return colors.white(colors.bold("{}: ".format(s)))


class Logger(object):
    def __init__(self, fd=None, level=INFO):
        self.fd = fd
        self.level = level

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, v):
        if v not in [SILENCE, INFO, VERBOSE, DEBUG]:
            raise GiraffeError("Invalid log level set")
        self._level = v

    def _write(self, args, level=None, console=False):
        if level is not None:
            if self.level < level:
                return
        args = list(args)
        if len(args) == 0:
            return
        elif len(args) == 1:
            category = None
        else:
            category = args.pop(0)
        if category is not None:
            category = highlight(category)
        if isinstance(args[0], Exception):
            args = [repr(arg) for arg in args]
        else:
            args = [arg if isinstance(arg, basestring) else str(arg) for arg in args]
        logging_level = logging_level_map.get(level, logging.INFO)
        msg = "".join([str(m) for m in args])
        if category:
            msg = "{}{}".format(category, msg)
        if console:
            sys.stderr.write(msg)
        else:
            logging_logger.log(logging_level, str(msg))
        return self

    def write(self, *args, **kwargs):
        return self._write(args, level=None, console=kwargs.get("console", False))

    def info(self, *args, **kwargs):
        return self._write(args, level=INFO, console=kwargs.get("console", False))

    def verbose(self, *args, **kwargs):
        return self._write(args, level=VERBOSE, console=kwargs.get("console", False))

    def debug(self, *args, **kwargs):
        return self._write(args, level=DEBUG, console=kwargs.get("console", False))

    def fatal(self, msg=""):
        self._write(["System", msg])
        sys.exit(1)


class LockingLogger(Logger):
    def __init__(self, fd=None, level=INFO):
        super(LockingLogger, self).__init__(fd, level)
        self.lock = multiprocessing.Lock()

    def _write(self, args, level=None, console=True):
        with self.lock:
            super(LockingLogger, self)._write(args, level=level, console=console)


log = LockingLogger(sys.stderr, level=INFO)
